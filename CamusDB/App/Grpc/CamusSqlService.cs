
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Transactions;
using CamusDB.Core.SQLParser;
using CamusDB.App.Services;
using CamusDB.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using ProtoColType = CamusDB.Grpc.ColumnType;

namespace CamusDB.App.Grpc;

/// <summary>
/// gRPC service that exposes the SQL-centric surface (ExecuteQuery, ExecuteNonQuery, ExecuteDdl,
/// and the transaction lifecycle) as a thin adapter over <see cref="CommandExecutor"/> and
/// <see cref="HttpTransactionCoordinator"/>, exactly mirroring the REST controllers.
///
/// <para>The per-statement execution is deliberately decoupled from the concrete wire-message
/// shape: query rows flow through an <see cref="IQueryRowSink"/> and the domain error mapping lives
/// in <see cref="GrpcErrorMapper"/> applied at the RPC boundary (<see cref="InvokeAsync{T}"/>), not
/// inside the execution core. This lets the upcoming duplex <c>BatchExecute</c> reuse the same core
/// with a different sink (multiplexed by request id) and its own in-band error reporting, rather than
/// duplicating the txn/commit/retry logic.</para>
///
/// <para>Streaming invariant for <see cref="ExecuteQuery"/>: the output-column schema is written
/// first (one <c>ResultSchema</c> message) even when the result set is empty, then rows are
/// streamed positionally one at a time — never buffered. The autocommit transaction commits after
/// the last row; a mid-stream client cancel rolls it back so no partial read locks leak. A retryable
/// serialization conflict is replayed only while nothing has been written yet — once a message is on
/// the wire it is reported as the terminal error instead, since a replay would re-emit the stream.</para>
/// </summary>
public sealed class CamusSqlService : CamusSql.CamusSqlBase
{
    private readonly CommandExecutor executor;
    private readonly HttpTransactionCoordinator transactions;
    private readonly ILogger<ICamusDB> logger;

    public CamusSqlService(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        ILogger<ICamusDB> logger)
    {
        this.executor     = executor;
        this.transactions = transactions;
        this.logger       = logger;
    }

    // ─── ExecuteQuery (server-streaming) ─────────────────────────────────────

    /// <summary>
    /// Executes a SQL SELECT or SHOW statement and streams the results schema-first.
    ///
    /// Ordering guarantee: exactly one <c>ResultSchema</c> message is written before any rows, even
    /// for an empty result set. The autocommit transaction is committed after the last row (its
    /// causal token is appended to the response trailers); a client cancel triggers rollback so no
    /// partial read locks are leaked.
    /// </summary>
    public override Task ExecuteQuery(
        SqlRequest request,
        IServerStreamWriter<QueryStreamMessage> responseStream,
        ServerCallContext context)
        => InvokeStreamingAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            string sql = request.Sql ?? "";
            NodeAst ast = SQLParserProcessor.Parse(sql);
            QueryStreamSink sink = new(responseStream);

            // SHOW DATABASES / BRANCHES / ANCESTORS need no db context or transaction.
            if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors)
            {
                ExecuteSQLTicket ticket = new(
                    txnState: null!,
                    database: request.Database,
                    sql: sql,
                    parameters: ToColumnValueMap(request.Parameters)
                );
                await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
                return;
            }

            // Explicit (caller-supplied) transaction — client owns the lifecycle.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                await RunExplicitTxnQuery(request, sql, handle, sink, ct).ConfigureAwait(false);
                return;
            }

            // Autocommit: retry transparently on transient serialization failures, but only while
            // nothing has been streamed yet (see RunAutocommitQuery).
            (CamusIsolationLevel? reqLevel, _, _) = ParseLevelMode(request);
            bool retry = (reqLevel ?? CamusDBConfig.DefaultIsolationLevel) == CamusIsolationLevel.Serializable;

            HLCTimestamp commitToken = await RunAutocommitQuery(request, sql, sink, retry, ct).ConfigureAwait(false);

            if (!commitToken.IsNull())
            {
                context.ResponseTrailers.Add("camus-causal-token-n", commitToken.N.ToString());
                context.ResponseTrailers.Add("camus-causal-token-l", commitToken.L.ToString());
                context.ResponseTrailers.Add("camus-causal-token-c", ((uint)commitToken.C).ToString());
            }
        });

    /// <summary>
    /// Core query execution, agnostic to the output transport: runs the ticket, writes the schema
    /// through <paramref name="sink"/> (always, even for an empty result), then streams each row.
    /// Returns the resolved <see cref="DatabaseDescriptor"/> so an autocommit caller can commit.
    /// Throws domain exceptions unchanged — the RPC boundary maps them.
    /// </summary>
    private async Task<DatabaseDescriptor> StreamQueryAsync(
        ExecuteSQLTicket ticket,
        IQueryRowSink sink,
        CancellationToken ct)
    {
        QuerySchemaHolder schemaHolder = new();
        (DatabaseDescriptor db, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(ticket, schemaOut: schemaHolder).ConfigureAwait(false);

        await sink.WriteSchemaAsync(schemaHolder.Schema, ct).ConfigureAwait(false);
        await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
            await sink.WriteRowAsync(row.Row, schemaHolder.Schema, ct).ConfigureAwait(false);

        return db;
    }

    private async Task RunExplicitTxnQuery(
        SqlRequest request,
        string sql,
        TxnHandle handle,
        IQueryRowSink sink,
        CancellationToken ct)
    {
        KvTransaction? txnState = null;
        try
        {
            txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            ExecuteSQLTicket ticket = new(
                txnState: txnState,
                database: request.Database,
                sql: sql,
                parameters: ToColumnValueMap(request.Parameters)
            );
            await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            if (txnState is not null)
                await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs an autocommit query: begin a promoted read-only transaction, stream schema + rows, and
    /// commit after the last row. When <paramref name="retry"/> is set, a transient serialization
    /// failure replays from a fresh BEGIN — but <b>only while nothing has been streamed yet</b>
    /// (<c>!sink.HasWritten</c>); once output is on the wire the conflict is reported as the terminal
    /// error, because replaying would re-emit the schema/rows and corrupt the stream.
    /// </summary>
    private async Task<HLCTimestamp> RunAutocommitQuery(
        SqlRequest request,
        string sql,
        IQueryRowSink sink,
        bool retry,
        CancellationToken ct)
    {
        HLCTimestamp? causalToken = ToCausalToken(request.CausalTokenN, request.CausalTokenL, request.CausalTokenC);
        HLCTimestamp commitToken = default;

        async Task<HLCTimestamp> Attempt(CancellationToken innerCt)
        {
            KvTransaction tx = await transactions.BeginReadOnlyAsync(
                request.Database, promote: true, causalToken, innerCt).ConfigureAwait(false);
            try
            {
                ExecuteSQLTicket ticket = new(
                    txnState: tx,
                    database: request.Database,
                    sql: sql,
                    parameters: ToColumnValueMap(request.Parameters)
                );
                DatabaseDescriptor db = await StreamQueryAsync(ticket, sink, innerCt).ConfigureAwait(false);
                commitToken = await transactions.CommitAsync(db, tx, innerCt).ConfigureAwait(false);
                return commitToken;
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                throw;
            }
        }

        if (retry)
            await SerializableRetryHelper.ExecuteAutocommitAsync(
                Attempt, canRetry: () => !sink.HasWritten, cancellationToken: ct).ConfigureAwait(false);
        else
            await Attempt(ct).ConfigureAwait(false);

        return commitToken;
    }

    // ─── ExecuteNonQuery ──────────────────────────────────────────────────────

    /// <summary>
    /// Executes a DML statement (INSERT / UPDATE / DELETE) and returns the affected-row count.
    /// Mirrors the REST <c>/execute-sql-non-query</c> path exactly. Non-streaming, so the standard
    /// replay-from-BEGIN retry applies unchanged.
    /// </summary>
    public override Task<NonQueryReply> ExecuteNonQuery(SqlRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            (CamusIsolationLevel? reqLevel, CamusTransactionMode? reqMode, KeyValueTransactionLocking? reqLocking) =
                ParseLevelMode(request);

            // Explicit transaction — client handles retry and lifecycle.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    ExecuteSQLTicket ticket = new(
                        txnState: txnState,
                        database: request.Database,
                        sql: request.Sql ?? "",
                        parameters: ToColumnValueMap(request.Parameters)
                    );
                    ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                    return new NonQueryReply { AffectedRows = result.ModifiedRows };
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit.
            int modifiedRows = 0;
            HLCTimestamp causalToken = default;

            async Task AutocommitDml(CancellationToken innerCt)
            {
                KvTransaction tx = await transactions.StartAsync(
                    request.Database, reqLevel, reqMode, reqLocking, cancellationToken: innerCt).ConfigureAwait(false);
                try
                {
                    ExecuteSQLTicket ticket = new(
                        txnState: tx,
                        database: request.Database,
                        sql: request.Sql ?? "",
                        parameters: ToColumnValueMap(request.Parameters)
                    );
                    ExecuteNonSQLResult r = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, innerCt).ConfigureAwait(false);
                    modifiedRows = r.ModifiedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                    throw;
                }
            }

            CamusIsolationLevel resolvedLevel = reqLevel ?? CamusDBConfig.DefaultIsolationLevel;
            if (resolvedLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitDml).ConfigureAwait(false);
            else
                await AutocommitDml(ct).ConfigureAwait(false);

            NonQueryReply reply = new() { AffectedRows = modifiedRows };
            if (!causalToken.IsNull())
                ApplyCausalToken(reply, causalToken);
            return reply;
        });

    // ─── ExecuteDdl ───────────────────────────────────────────────────────────

    /// <summary>
    /// Executes a DDL statement (CREATE / ALTER / DROP / RENAME).
    /// Mirrors the REST <c>/execute-sql-ddl</c> path exactly.
    /// </summary>
    public override Task<DdlReply> ExecuteDdl(SqlRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            string sql = request.Sql ?? "";
            NodeAst ast = SQLParserProcessor.Parse(sql);

            bool isDbManagement = ast.nodeType is
                NodeType.CreateDatabase or NodeType.CreateDatabaseIfNotExists or
                NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists or
                NodeType.DropDatabase or NodeType.DropDatabaseIfExists or
                NodeType.RenameDatabase;

            KvTransaction? txnState = null;
            bool newTransaction = false;

            try
            {
                if (!isDbManagement)
                {
                    (CamusIsolationLevel? reqLevel, CamusTransactionMode? reqMode, KeyValueTransactionLocking? reqLocking) =
                        ParseLevelMode(request);

                    if (request.TxnHandle is { TxnIdPt: > 0 } handle)
                    {
                        txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    }
                    else
                    {
                        txnState = await transactions.StartAsync(
                            request.Database, reqLevel, reqMode, reqLocking, cancellationToken: ct).ConfigureAwait(false);
                        newTransaction = true;
                    }
                }

                ExecuteSQLTicket ticket = new(
                    txnState: txnState!,
                    database: request.Database,
                    sql: sql,
                    parameters: ToColumnValueMap(request.Parameters)
                );

                ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ticket).ConfigureAwait(false);

                HLCTimestamp commitToken = default;
                if (newTransaction)
                    commitToken = await transactions.CommitAsync(result.Database, txnState!).ConfigureAwait(false);

                DdlReply reply = new();
                if (!commitToken.IsNull())
                    ApplyCausalToken(reply, commitToken);
                return reply;
            }
            catch (Exception)
            {
                if (txnState is not null)
                    await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                throw;
            }
        });

    // ─── BatchExecute (duplex) ────────────────────────────────────────────────

    /// <summary>
    /// Pipelines many <c>ExecuteQuery</c> / <c>ExecuteNonQuery</c> statements over one bidirectional
    /// stream so a client avoids a unary round-trip per statement. Each inbound request carries a
    /// client-monotonic <c>request_id</c>; every response echoes it, and responses for different ids
    /// interleave on the shared stream (the client demultiplexes by id).
    ///
    /// <para>Ops that share a <see cref="TxnHandle"/> execute in <b>arrival order</b> (a per-handle
    /// serial chain preserves the <c>KvTransaction</c> ordering and read-your-writes the coordinator
    /// session assumes); autocommit ops (no handle) run concurrently up to
    /// <see cref="CamusDBConfig.GrpcBatchMaxInFlight"/>, which also backpressures the read loop.</para>
    ///
    /// <para>Per-op outcome is reported <b>in-band</b> — a <c>QueryComplete</c> / <c>NonQueryReply</c>
    /// on success (carrying the causal token), a <c>BatchError</c> on failure (carrying the
    /// <c>CADBxxxx</c> code so the client can apply the retry taxonomy) — because gRPC trailers are
    /// per-call and this call carries many ops. A failed op never tears down the others. Batched ops
    /// are not server-retried: a retryable conflict surfaces as its code and the client replays that
    /// op. A dropped stream rolls back any in-flight autocommit work via each op's own handler.</para>
    /// </summary>
    public override async Task BatchExecute(
        IAsyncStreamReader<BatchExecuteRequest> requestStream,
        IServerStreamWriter<BatchExecuteResponse> responseStream,
        ServerCallContext context)
    {
        CancellationToken ct = context.CancellationToken;
        SemaphoreSlim writeLock = new(1, 1);
        int maxInFlight = Math.Max(1, CamusDBConfig.GrpcBatchMaxInFlight);
        SemaphoreSlim inFlight = new(maxInFlight, maxInFlight);

        // Per-txn-handle serial chains so same-handle ops run strictly in arrival order.
        Dictionary<string, Task> chains = new();
        List<Task> ops = new();

        try
        {
            await foreach (BatchExecuteRequest req in requestStream.ReadAllAsync(ct).ConfigureAwait(false))
            {
                await inFlight.WaitAsync(ct).ConfigureAwait(false);   // backpressure

                string? handleKey = HandleKey(req.Request?.TxnHandle);
                Task op;
                if (handleKey is not null)
                {
                    Task prev = chains.TryGetValue(handleKey, out Task? p) ? p : Task.CompletedTask;
                    op = RunBatchOpAfterAsync(prev, req, responseStream, writeLock, ct);
                    chains[handleKey] = op;
                }
                else
                {
                    op = RunBatchOpAsync(req, responseStream, writeLock, ct);
                }

                _ = op.ContinueWith(_ => inFlight.Release(), TaskScheduler.Default);
                ops.Add(op);
            }

            await Task.WhenAll(ops).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Client dropped the stream. In-flight ops observe ct and roll back their own autocommit
            // work; drain best-effort so nothing is left running.
            try { await Task.WhenAll(ops).ConfigureAwait(false); } catch { /* handled per-op */ }
        }
    }

    private static string? HandleKey(TxnHandle? handle)
        => handle is { TxnIdPt: > 0 } h ? $"{h.TxnIdPt}:{h.TxnIdCounter}" : null;

    /// <summary>Chains a batch op after the previous op for the same handle so they run in order.</summary>
    private async Task RunBatchOpAfterAsync(
        Task prev,
        BatchExecuteRequest req,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        // RunBatchOpAsync never throws, so the previous op cannot fault the chain — but await it
        // defensively so a same-handle op only starts once its predecessor has fully completed.
        try { await prev.ConfigureAwait(false); } catch { /* predecessor reported its own outcome */ }
        await RunBatchOpAsync(req, stream, writeLock, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes one batched op and reports its outcome in-band. Never throws — a domain error becomes
    /// a terminal <c>BatchError</c> for this op's <c>request_id</c>, leaving other ops untouched.
    /// </summary>
    private async Task RunBatchOpAsync(
        BatchExecuteRequest req,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        try
        {
            SqlRequest request = req.Request
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Batch request is missing 'request'");

            if (req.Kind == BatchStatementKind.Query)
                await RunBatchQueryAsync(req.RequestId, request, stream, writeLock, ct).ConfigureAwait(false);
            else
                await RunBatchNonQueryAsync(req.RequestId, request, stream, writeLock, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stream cancelled — the op's own handler already rolled back; don't write onto a dead stream.
        }
        catch (CamusDBException ex)
        {
            logger.LogError("{Name}: {Message}", ex.GetType().Name, ex.Message);
            await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
            {
                RequestId = req.RequestId,
                Error = new BatchError { Code = ex.Code, Message = ex.Message },
            }, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError("{Name}: {Message}", ex.GetType().Name, ex.Message);
            await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
            {
                RequestId = req.RequestId,
                Error = new BatchError { Code = "CADB0000", Message = "Internal server error" },
            }, ct).ConfigureAwait(false);
        }
    }

    private async Task RunBatchQueryAsync(
        int requestId,
        SqlRequest request,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        string sql = request.Sql ?? "";
        BatchQuerySink sink = new(stream, writeLock, requestId);
        NodeAst ast = SQLParserProcessor.Parse(sql);
        HLCTimestamp commitToken = default;

        if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors)
        {
            ExecuteSQLTicket ticket = new(
                txnState: null!, database: request.Database, sql: sql,
                parameters: ToColumnValueMap(request.Parameters));
            await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
        }
        else if (request.TxnHandle is { TxnIdPt: > 0 } handle)
        {
            // Explicit transaction — the client owns the lifecycle, so no commit and no auto-rollback.
            KvTransaction txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            ExecuteSQLTicket ticket = new(
                txnState: txnState, database: request.Database, sql: sql,
                parameters: ToColumnValueMap(request.Parameters));
            await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
        }
        else
        {
            // Autocommit: begin a promoted read-only txn, stream, commit. Not server-retried — a
            // retryable conflict propagates and is reported as BatchError for the client to replay.
            HLCTimestamp? causalToken = ToCausalToken(request.CausalTokenN, request.CausalTokenL, request.CausalTokenC);
            KvTransaction tx = await transactions.BeginReadOnlyAsync(
                request.Database, promote: true, causalToken, ct).ConfigureAwait(false);
            try
            {
                ExecuteSQLTicket ticket = new(
                    txnState: tx, database: request.Database, sql: sql,
                    parameters: ToColumnValueMap(request.Parameters));
                DatabaseDescriptor db = await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
                commitToken = await transactions.CommitAsync(db, tx, ct).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                throw;
            }
        }

        // Success terminator (schema was already written first by the sink). Best-effort: a failed
        // terminal write must NOT become a BatchError — the read completed successfully.
        QueryComplete complete = new();
        if (!commitToken.IsNull())
        {
            complete.CausalTokenN = commitToken.N;
            complete.CausalTokenL = commitToken.L;
            complete.CausalTokenC = (long)(uint)commitToken.C;
        }
        await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
        {
            RequestId = requestId,
            QueryComplete = complete,
        }, ct).ConfigureAwait(false);
    }

    private async Task RunBatchNonQueryAsync(
        int requestId,
        SqlRequest request,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        (CamusIsolationLevel? reqLevel, CamusTransactionMode? reqMode, KeyValueTransactionLocking? reqLocking) =
            ParseLevelMode(request);
        NonQueryReply reply;

        if (request.TxnHandle is { TxnIdPt: > 0 } handle)
        {
            // Explicit transaction — client owns commit/rollback.
            KvTransaction txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            ExecuteSQLTicket ticket = new(
                txnState: txnState, database: request.Database, sql: request.Sql ?? "",
                parameters: ToColumnValueMap(request.Parameters));
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
            reply = new NonQueryReply { AffectedRows = result.ModifiedRows };
        }
        else
        {
            KvTransaction tx = await transactions.StartAsync(
                request.Database, reqLevel, reqMode, reqLocking, cancellationToken: ct).ConfigureAwait(false);
            int rows;
            HLCTimestamp token;
            try
            {
                ExecuteSQLTicket ticket = new(
                    txnState: tx, database: request.Database, sql: request.Sql ?? "",
                    parameters: ToColumnValueMap(request.Parameters));
                ExecuteNonSQLResult r = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                token = await transactions.CommitAsync(r.Database, tx, ct).ConfigureAwait(false);
                rows = r.ModifiedRows;
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                throw;
            }

            reply = new NonQueryReply { AffectedRows = rows };
            if (!token.IsNull())
                ApplyCausalToken(reply, token);
        }

        // Best-effort terminal: the mutation already committed, so a failed write must not be
        // reported as a BatchError (that would invite a double-applying retry).
        await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
        {
            RequestId = requestId,
            NonQuery = reply,
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes one batch response under <paramref name="writeLock"/> (gRPC forbids concurrent writes),
    /// swallowing failures. Used for terminal messages (QueryComplete / NonQueryReply / BatchError):
    /// a delivery failure must not cascade — the op's real outcome already happened.
    /// </summary>
    private static async Task TryWriteBatchAsync(
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        BatchExecuteResponse msg,
        CancellationToken ct)
    {
        try
        {
            await writeLock.WaitAsync(ct).ConfigureAwait(false);
            try { await stream.WriteAsync(msg, ct).ConfigureAwait(false); }
            finally { writeLock.Release(); }
        }
        catch
        {
            // Stream gone or cancelled — best effort.
        }
    }

    // ─── Transaction lifecycle ────────────────────────────────────────────────

    /// <summary>
    /// Begins an explicit transaction. The Kahuna session start is deferred so a following
    /// <c>SET TRANSACTION LOCKING</c> can still choose the locking mode.
    /// </summary>
    public override Task<TxnHandle> StartTransaction(StartTxnRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CamusIsolationLevel? level   = ToIsolationLevel(request.IsolationLevel);
            CamusTransactionMode? mode   = ToTransactionMode(request.TransactionMode);
            KeyValueTransactionLocking? locking = ToLocking(request.Locking);

            KvTransaction tx = await transactions.StartAsync(
                request.Database, level, mode, locking, deferStart: true).ConfigureAwait(false);

            return new TxnHandle
            {
                TxnIdPt      = tx.ClientId.L,
                TxnIdCounter = (uint)tx.ClientId.C,
            };
        });

    /// <summary>
    /// Commits an explicit transaction identified by its <see cref="TxnHandle"/>.
    /// A duplicate commit on the same handle is rejected by the finalize gate in
    /// <see cref="HttpTransactionCoordinator"/> rather than being double-driven.
    /// </summary>
    public override Task<CommitReply> CommitTransaction(TxnHandle request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            KvTransaction txnState = transactions.GetState(request.TxnIdPt, (uint)request.TxnIdCounter);
            HLCTimestamp token = await transactions.CommitTrackedAsync(txnState).ConfigureAwait(false);
            CommitReply reply = new();
            if (!token.IsNull())
                ApplyCausalToken(reply, token);
            return reply;
        });

    /// <summary>Rolls back an explicit transaction identified by its <see cref="TxnHandle"/>.</summary>
    public override Task<RollbackReply> RollbackTransaction(TxnHandle request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            KvTransaction txnState = transactions.GetState(request.TxnIdPt, (uint)request.TxnIdCounter);
            await transactions.RollbackAsync(txnState).ConfigureAwait(false);
            return new RollbackReply();
        });

    /// <summary>Health-check ping.</summary>
    public override Task<PingReply> Ping(PingRequest request, ServerCallContext context)
        => Task.FromResult(new PingReply { Message = "pong" });

    // ─── RPC boundary: domain-error → RpcException mapping ─────────────────────

    /// <summary>
    /// Runs <paramref name="body"/> and translates a thrown <see cref="CamusDBException"/> (or any
    /// unexpected exception) into an <see cref="RpcException"/> via the domain-code → gRPC
    /// <c>StatusCode</c> + error-trailer mapping in <see cref="GrpcErrorMapper"/>, logging first.
    /// Cancellation and already-mapped <see cref="RpcException"/>s propagate unchanged.
    /// The execution core throws domain exceptions; mapping happens only here so the batch handler can
    /// map the same exceptions to its in-band error message instead.
    /// </summary>
    private async Task<T> InvokeAsync<T>(Func<Task<T>> body)
    {
        try
        {
            return await body().ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (CamusDBException ex)
        {
            logger.LogError("{Name}: {Message}", ex.GetType().Name, ex.Message);
            throw GrpcErrorMapper.ToRpcException(ex);
        }
        catch (Exception ex)
        {
            logger.LogError("{Name}: {Message}", ex.GetType().Name, ex.Message);
            throw GrpcErrorMapper.ToRpcException(ex);
        }
    }

    /// <summary>Void-returning (server-streaming) counterpart of <see cref="InvokeAsync{T}"/>.</summary>
    private Task InvokeStreamingAsync(Func<Task> body)
        => InvokeAsync(async () => { await body().ConfigureAwait(false); return true; });

    // ─── Message builders ────────────────────────────────────────────────────

    /// <summary>
    /// Builds the ordered output-column <c>ResultSchema</c>. Shared by the unary query stream and the
    /// batch stream so both describe columns identically.
    /// </summary>
    internal static ResultSchema BuildResultSchema(IReadOnlyList<DerivedColumnSchema> schema)
    {
        ResultSchema rs = new();
        foreach (DerivedColumnSchema col in schema)
            rs.Columns.Add(new ColumnSchema
            {
                Name = col.Name,
                Type = (ProtoColType)(int)col.Type,
            });
        return rs;
    }

    /// <summary>
    /// Builds a positional <c>ResultRow</c> whose values align to <paramref name="schema"/>.
    /// Missing dictionary keys (null cells) map to a typed NULL value via <see cref="GrpcValueCodec"/>.
    /// </summary>
    internal static ResultRow BuildResultRow(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<DerivedColumnSchema> schema)
    {
        ResultRow rr = new();
        foreach (DerivedColumnSchema col in schema)
        {
            ColumnValue? cv = row.TryGetValue(col.Name, out ColumnValue? v) ? v : null;
            rr.Values.Add(GrpcValueCodec.ToProto(cv));
        }
        return rr;
    }

    /// <summary>
    /// Builds the <c>QueryStreamMessage</c> that wraps a <c>ResultSchema</c>.
    /// Written once as the first message in every unary query stream, even for an empty result.
    /// </summary>
    internal static QueryStreamMessage BuildSchema(IReadOnlyList<DerivedColumnSchema> schema)
        => new() { Schema = BuildResultSchema(schema) };

    /// <summary>Wraps a positional <c>ResultRow</c> in a unary <c>QueryStreamMessage</c>.</summary>
    internal static QueryStreamMessage BuildRow(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<DerivedColumnSchema> schema)
        => new() { Row = BuildResultRow(row, schema) };

    // ─── Causal token helpers ────────────────────────────────────────────────

    // transactions.CommitAsync returns the Kommander HLCTimestamp (N, L, C); all three components are
    // carried so the token is a faithful, non-lossy copy (N is part of HLC equality / ordering).

    private static void ApplyCausalToken(NonQueryReply reply, HLCTimestamp token)
    {
        reply.CausalTokenN = token.N;
        reply.CausalTokenL = token.L;
        reply.CausalTokenC = (long)(uint)token.C;
    }

    private static void ApplyCausalToken(DdlReply reply, HLCTimestamp token)
    {
        reply.CausalTokenN = token.N;
        reply.CausalTokenL = token.L;
        reply.CausalTokenC = (long)(uint)token.C;
    }

    private static void ApplyCausalToken(CommitReply reply, HLCTimestamp token)
    {
        reply.CausalTokenN = token.N;
        reply.CausalTokenL = token.L;
        reply.CausalTokenC = (long)(uint)token.C;
    }

    // ─── Request helpers ─────────────────────────────────────────────────────

    private static (CamusIsolationLevel?, CamusTransactionMode?, KeyValueTransactionLocking?) ParseLevelMode(
        SqlRequest request)
    {
        CamusIsolationLevel? level  = ToIsolationLevel(request.IsolationLevel);
        CamusTransactionMode? mode  = ToTransactionMode(request.TransactionMode);
        KeyValueTransactionLocking? locking = ToLocking(request.Locking);
        return (level, mode, locking);
    }

    private static CamusIsolationLevel? ToIsolationLevel(IsolationLevel level) => level switch
    {
        IsolationLevel.ReadCommitted  => CamusIsolationLevel.ReadCommitted,
        IsolationLevel.Serializable   => CamusIsolationLevel.Serializable,
        _                             => null,
    };

    private static CamusTransactionMode? ToTransactionMode(TransactionMode mode) => mode switch
    {
        TransactionMode.ReadWrite => CamusTransactionMode.ReadWrite,
        TransactionMode.ReadOnly  => CamusTransactionMode.ReadOnly,
        _                         => null,
    };

    private static KeyValueTransactionLocking? ToLocking(LockingMode locking) => locking switch
    {
        LockingMode.Pessimistic => KeyValueTransactionLocking.Pessimistic,
        LockingMode.Optimistic  => KeyValueTransactionLocking.Optimistic,
        _                       => null,
    };

    private static HLCTimestamp? ToCausalToken(int n, long l, long c)
    {
        if (n == 0 && l == 0 && c == 0)
            return null;
        return new HLCTimestamp(n, l, (uint)c);
    }

    private static Dictionary<string, ColumnValue>? ToColumnValueMap(
        Google.Protobuf.Collections.MapField<string, Value> protoMap)
    {
        if (protoMap.Count == 0)
            return null;

        Dictionary<string, ColumnValue> result = new(protoMap.Count);
        foreach (KeyValuePair<string, Value> kv in protoMap)
            result[kv.Key] = GrpcValueCodec.FromProto(kv.Value);
        return result;
    }
}

/// <summary>
/// Sink for schema-first positional query output, decoupled from the concrete gRPC message shape so
/// one execution core (<see cref="CamusSqlService"/>) drives both the unary <c>ExecuteQuery</c>
/// stream and the upcoming multiplexed batch stream. <see cref="HasWritten"/> reports whether any
/// message has gone on the wire, so an autocommit caller can decide whether a retryable conflict may
/// still be replayed (only before the first write).
/// </summary>
internal interface IQueryRowSink
{
    /// <summary>True once any schema or row message has been handed to the transport.</summary>
    bool HasWritten { get; }

    ValueTask WriteSchemaAsync(IReadOnlyList<DerivedColumnSchema> schema, CancellationToken ct);

    ValueTask WriteRowAsync(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<DerivedColumnSchema> schema,
        CancellationToken ct);
}

/// <summary>
/// <see cref="IQueryRowSink"/> that writes <c>QueryStreamMessage</c>s onto a unary server stream.
/// Marks <see cref="HasWritten"/> before each write: once a write has been attempted the stream may
/// already carry bytes, so a replay must not re-emit.
/// </summary>
internal sealed class QueryStreamSink : IQueryRowSink
{
    private readonly IServerStreamWriter<QueryStreamMessage> stream;

    public bool HasWritten { get; private set; }

    public QueryStreamSink(IServerStreamWriter<QueryStreamMessage> stream) => this.stream = stream;

    public async ValueTask WriteSchemaAsync(IReadOnlyList<DerivedColumnSchema> schema, CancellationToken ct)
    {
        HasWritten = true;
        await stream.WriteAsync(CamusSqlService.BuildSchema(schema), ct).ConfigureAwait(false);
    }

    public async ValueTask WriteRowAsync(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<DerivedColumnSchema> schema,
        CancellationToken ct)
    {
        HasWritten = true;
        await stream.WriteAsync(CamusSqlService.BuildRow(row, schema), ct).ConfigureAwait(false);
    }
}

/// <summary>
/// <see cref="IQueryRowSink"/> that writes a query's schema/rows as <c>BatchExecuteResponse</c>
/// messages tagged with the op's <c>request_id</c>, serialized against a shared write lock (many
/// ops share one duplex stream and gRPC forbids concurrent writes). Schema/row writes propagate
/// failures so a broken stream aborts the query and rolls back; the terminal QueryComplete is
/// written best-effort by the caller.
/// </summary>
internal sealed class BatchQuerySink : IQueryRowSink
{
    private readonly IServerStreamWriter<BatchExecuteResponse> stream;
    private readonly SemaphoreSlim writeLock;
    private readonly int requestId;

    public bool HasWritten { get; private set; }

    public BatchQuerySink(IServerStreamWriter<BatchExecuteResponse> stream, SemaphoreSlim writeLock, int requestId)
    {
        this.stream    = stream;
        this.writeLock = writeLock;
        this.requestId = requestId;
    }

    public async ValueTask WriteSchemaAsync(IReadOnlyList<DerivedColumnSchema> schema, CancellationToken ct)
    {
        HasWritten = true;
        await WriteLockedAsync(new BatchExecuteResponse
        {
            RequestId = requestId,
            Schema    = CamusSqlService.BuildResultSchema(schema),
        }, ct).ConfigureAwait(false);
    }

    public async ValueTask WriteRowAsync(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<DerivedColumnSchema> schema,
        CancellationToken ct)
    {
        HasWritten = true;
        await WriteLockedAsync(new BatchExecuteResponse
        {
            RequestId = requestId,
            Row       = CamusSqlService.BuildResultRow(row, schema),
        }, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteLockedAsync(BatchExecuteResponse msg, CancellationToken ct)
    {
        await writeLock.WaitAsync(ct).ConfigureAwait(false);
        try { await stream.WriteAsync(msg, ct).ConfigureAwait(false); }
        finally { writeLock.Release(); }
    }
}
