
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using CamusDB.Core;
using CamusDB.Core.Diagnostics;
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
    private readonly IHostApplicationLifetime appLifetime;
    private readonly ForegroundRequestGauge loadGauge;

    public CamusSqlService(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        ILogger<ICamusDB> logger,
        IHostApplicationLifetime appLifetime,
        ForegroundRequestGauge loadGauge)
    {
        this.executor     = executor;
        this.transactions = transactions;
        this.logger       = logger;
        this.appLifetime  = appLifetime;
        this.loadGauge    = loadGauge;
    }

    /// <summary>
    /// Resolves the authenticated principal from the request's <c>authorization</c> metadata (a
    /// <c>Bearer</c> token), mirroring the REST bearer resolution. Returns <c>null</c> when
    /// <see cref="CamusDBConfig.AuthenticationEnabled"/> is off. When on, a missing/invalid/expired
    /// token throws <see cref="CamusDBErrorCodes.AuthenticationFailed"/> (mapped to the gRPC error
    /// status at the RPC boundary), so the engine gate never sees a null principal while auth is on.
    /// </summary>
    private async Task<Principal?> ResolvePrincipalAsync(ServerCallContext context)
    {
        if (!CamusDBConfig.AuthenticationEnabled)
            return null;

        EnsureSecureTransport(context);

        string? authorization = context.RequestHeaders.GetValue("authorization");
        string? bearer = authorization is not null
                         && authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : null;

        return await executor.ResolvePrincipalAsync(bearer).ConfigureAwait(false);
    }

    /// <summary>
    /// Refuses a plaintext gRPC call when authentication is enabled and TLS is required, mirroring the
    /// REST <c>EnsureSecureTransport</c>. A loopback peer is exempt for single-host development. Skipped
    /// when the call has no ASP.NET <see cref="HttpContext"/> (e.g. in-process unit tests) since
    /// transport security cannot be assessed there.
    /// </summary>
    private static void EnsureSecureTransport(ServerCallContext context)
    {
        if (!CamusDBConfig.AuthenticationEnabled || !CamusDBConfig.RequireTlsWhenAuthEnabled)
            return;

        HttpContext? http;
        try
        {
            http = context.GetHttpContext();
        }
        catch
        {
            return; // no ASP.NET HttpContext (e.g. an in-process unit-test call) — cannot assess transport
        }

        if (http is null || http.Request.IsHttps)
            return;

        System.Net.IPAddress? remote = http.Connection.RemoteIpAddress;
        if (remote is not null && System.Net.IPAddress.IsLoopback(remote))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InsecureTransport,
            "Authentication is enabled and requires a TLS connection");
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
            Principal? principal = await ResolvePrincipalAsync(context).ConfigureAwait(false);
            string sql = request.Sql ?? "";
            NodeAst ast = SQLParserProcessor.Parse(sql);
            QueryStreamSink sink = new(responseStream);

            // SHOW DATABASES / BRANCHES / ANCESTORS need no db context or transaction.
            if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors or NodeType.ShowOrphanDatabases)
            {
                ExecuteSQLTicket ticket = new(
                    txnState: null!,
                    database: request.Database,
                    sql: sql,
                    parameters: ToColumnValueMap(request.Parameters),
                    principal: principal
                );
                await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
                return;
            }

            // Explicit (caller-supplied) transaction — client owns the lifecycle.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                await RunExplicitTxnQuery(request, sql, handle, sink, principal, ct).ConfigureAwait(false);
                return;
            }

            // Autocommit: retry transparently on transient serialization failures, but only while
            // nothing has been streamed yet (see RunAutocommitQuery).
            (CamusIsolationLevel? reqLevel, _, _) = ParseLevelMode(request);
            bool retry = (reqLevel ?? CamusDBConfig.DefaultIsolationLevel) == CamusIsolationLevel.Serializable;

            HLCTimestamp commitToken = await RunAutocommitQuery(request, sql, sink, retry, principal, ct).ConfigureAwait(false);

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
        Principal? principal,
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
                parameters: ToColumnValueMap(request.Parameters),
                principal: principal
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
        Principal? principal,
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
                    parameters: ToColumnValueMap(request.Parameters),
                    principal: principal
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
            Principal? principal = await ResolvePrincipalAsync(context).ConfigureAwait(false);
            (CamusIsolationLevel? reqLevel, CamusTransactionMode? reqMode, KeyValueTransactionLocking? reqLocking) =
                ParseLevelMode(request);

            // Mirrors the REST non-query path: a database-scoped statement returns no descriptor, so
            // it must bypass both the transaction and the commit rather than be handed a null.
            if (StatementScope.IsDatabaseScopedMutation(SQLParserProcessor.Parse(request.Sql ?? "").nodeType))
            {
                ExecuteSQLTicket dbScopedTicket = new(
                    txnState: null!,
                    database: request.Database,
                    sql: request.Sql ?? "",
                    parameters: ToColumnValueMap(request.Parameters),
                    principal: principal
                );

                await executor.ExecuteNonSQLQuery(dbScopedTicket).ConfigureAwait(false);
                return new NonQueryReply { AffectedRows = 0 };
            }

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
                        parameters: ToColumnValueMap(request.Parameters),
                        principal: principal
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
                        parameters: ToColumnValueMap(request.Parameters),
                        principal: principal
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
            Principal? principal = await ResolvePrincipalAsync(context).ConfigureAwait(false);
            string sql = request.Sql ?? "";
            NodeAst ast = SQLParserProcessor.Parse(sql);

            bool isDbManagement = StatementScope.IsDatabaseScopedMutation(ast.nodeType);

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
                    parameters: ToColumnValueMap(request.Parameters),
                    principal: principal
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
    /// Pipelines many statements over one bidirectional stream so a client avoids a unary round-trip
    /// per statement. Besides <c>QUERY</c> / <c>NON_QUERY</c>, the transaction lifecycle rides the same
    /// stream — <c>START</c> begins a transaction and returns its handle in-band, <c>COMMIT</c> /
    /// <c>ROLLBACK</c> finalize it — so a whole unit of work (begin, statements, commit) is one duplex
    /// call and many concurrent transactions keep the stream busy. Each inbound request carries a
    /// client-monotonic <c>request_id</c>; every response echoes it, and responses for different ids
    /// interleave on the shared stream (the client demultiplexes by id).
    ///
    /// <para>Ops that share a <see cref="TxnHandle"/> execute in <b>arrival order</b> (a per-handle
    /// serial chain preserves the <c>KvTransaction</c> ordering and read-your-writes the coordinator
    /// session assumes); a <c>START</c> carries no handle yet and runs concurrently, and autocommit ops
    /// (no handle) run concurrently up to <see cref="CamusDBConfig.GrpcBatchMaxInFlight"/>, which also
    /// backpressures the read loop. The chain is per stream, so a client must pin all of one
    /// transaction's ops to a single stream for the ordering to hold.</para>
    ///
    /// <para>Per-op outcome is reported <b>in-band</b> — <c>QueryComplete</c> / <c>NonQueryReply</c> /
    /// <c>start_reply</c> / <c>commit_reply</c> / <c>rollback_reply</c> on success (carrying the causal
    /// token where one exists), a <c>BatchError</c> on failure (carrying the <c>CADBxxxx</c> code so the
    /// client can apply the retry taxonomy) — because gRPC trailers are per-call and this call carries
    /// many ops. A failed op never tears down the others. Batched ops are not server-retried: a
    /// retryable conflict surfaces as its code and the client replays that op. A transaction that a
    /// <c>START</c> opened on this stream and never finalized is rolled back on teardown so a dropped or
    /// half-closed stream cannot orphan it; in-flight autocommit work rolls back via each op's own
    /// handler.</para>
    /// </summary>
    public override async Task BatchExecute(
        IAsyncStreamReader<BatchExecuteRequest> requestStream,
        IServerStreamWriter<BatchExecuteResponse> responseStream,
        ServerCallContext context)
    {
        // BatchExecute is a long-lived duplex stream that a multiplexing client keeps open across many
        // operations, blocking below in ReadAllAsync between requests. context.CancellationToken only
        // fires on CLIENT disconnect, so on a graceful SERVER shutdown Kestrel would otherwise wait for
        // this "active" streaming call the full host ShutdownTimeout (~30s). Link the host's
        // ApplicationStopping token so shutdown ends the read loop promptly; the finally block still rolls
        // back any transaction left open on the stream.
        using CancellationTokenSource shutdownLinked =
            CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken, appLifetime.ApplicationStopping);
        CancellationToken ct = shutdownLinked.Token;

        // Resolve the principal once for the whole stream. When auth is enabled a missing/invalid token
        // fails the entire batch call here (before any op runs), which is the desired fail-closed shape.
        Principal? principal = await ResolvePrincipalAsync(context).ConfigureAwait(false);

        SemaphoreSlim writeLock = new(1, 1);
        int maxInFlight = Math.Max(1, CamusDBConfig.GrpcBatchMaxInFlight);
        SemaphoreSlim inFlight = new(maxInFlight, maxInFlight);

        // Per-txn-handle serial chains so same-handle ops run strictly in arrival order. The chain is
        // per stream (this call): it orders only same-handle ops that arrive here, which is why the
        // client must pin all of a transaction's ops to one stream — see the client routing contract.
        Dictionary<string, Task> chains = new();
        List<Task> ops = new();

        // Transactions begun by a batched START on this stream but not yet finalized. A stream that
        // ends (normally or by cancellation) without a matching COMMIT/ROLLBACK must not leak an open
        // transaction, so any survivor here is rolled back on teardown. Written from concurrent op
        // handlers, hence concurrent.
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles = new();

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
                    op = RunBatchOpAfterAsync(prev, req, responseStream, writeLock, startedHandles, principal, ct);
                    chains[handleKey] = op;
                }
                else
                {
                    op = RunBatchOpAsync(req, responseStream, writeLock, startedHandles, principal, ct);
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
        finally
        {
            await RollbackStartedSurvivorsAsync(startedHandles).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Rolls back any transaction that a batched START opened on a stream that is now tearing down
    /// without a matching COMMIT/ROLLBACK, so a client crash or half-close cannot orphan an open
    /// transaction. Best-effort and idempotent: a handle already finalized elsewhere throws from
    /// <see cref="HttpTransactionCoordinator.GetState"/> and is treated as nothing-to-do. Uses no
    /// cancellation token because it must still run when the stream's token is already cancelled.
    /// </summary>
    private async Task RollbackStartedSurvivorsAsync(
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles)
    {
        foreach (KeyValuePair<string, (long pt, uint counter)> entry in startedHandles.ToArray())
        {
            if (!startedHandles.TryRemove(entry.Key, out _))
                continue;
            try
            {
                KvTransaction tx = transactions.GetState(entry.Value.pt, entry.Value.counter);
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                // Already finalized/unknown — nothing to roll back.
            }
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
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles,
        Principal? principal,
        CancellationToken ct)
    {
        // RunBatchOpAsync never throws, so the previous op cannot fault the chain — but await it
        // defensively so a same-handle op only starts once its predecessor has fully completed.
        try { await prev.ConfigureAwait(false); } catch { /* predecessor reported its own outcome */ }
        await RunBatchOpAsync(req, stream, writeLock, startedHandles, principal, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes one batched op and reports its outcome in-band. Never throws — a domain error becomes
    /// a terminal <c>BatchError</c> for this op's <c>request_id</c>, leaving other ops untouched.
    /// </summary>
    private async Task RunBatchOpAsync(
        BatchExecuteRequest req,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles,
        Principal? principal,
        CancellationToken ct)
    {
        // Count each batched op individually (not the long-lived duplex stream) toward the foreground
        // load signal, so a node saturated purely over BatchExecute still backs off auto-analyze.
        loadGauge.Increment();

        // Per-op request instrumentation over the multiplexed batch transport: this is the real
        // one-logical-operation boundary, not the duplex stream's lifetime.
        string operation = MapBatchOperation(req.Kind);
        string outcome = ServerDiagnostics.Tags.Outcome.Ok;
        long requestStart = Stopwatch.GetTimestamp();
        ServerDiagnostics.AddRequestInFlight(ServerDiagnostics.Tags.Transport.GrpcBatch, 1);

        // Root span for one logical operation carried over the multiplexed stream (not the duplex
        // connection lifetime). Child spans (parse/execute/commit) parent to it via Activity.Current.
        using System.Diagnostics.Activity? requestSpan = ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Request);
        requestSpan?.SetTag("operation", operation);
        requestSpan?.SetTag("transport", ServerDiagnostics.Tags.Transport.GrpcBatch);
        try
        {
            SqlRequest request = req.Request
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Batch request is missing 'request'");

            switch (req.Kind)
            {
                case BatchStatementKind.Query:
                    await RunBatchQueryAsync(req.RequestId, request, stream, writeLock, principal, ct).ConfigureAwait(false);
                    break;
                case BatchStatementKind.Start:
                    await RunBatchStartAsync(req.RequestId, request, stream, writeLock, startedHandles, ct).ConfigureAwait(false);
                    break;
                case BatchStatementKind.Commit:
                    await RunBatchCommitAsync(req.RequestId, request, stream, writeLock, startedHandles, ct).ConfigureAwait(false);
                    break;
                case BatchStatementKind.Rollback:
                    await RunBatchRollbackAsync(req.RequestId, request, stream, writeLock, startedHandles, ct).ConfigureAwait(false);
                    break;
                case BatchStatementKind.NonQuery:
                case BatchStatementKind.Unspecified:
                default:
                    await RunBatchNonQueryAsync(req.RequestId, request, stream, writeLock, principal, ct).ConfigureAwait(false);
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            // Stream cancelled — the op's own handler already rolled back; don't write onto a dead stream.
            outcome = ServerDiagnostics.Tags.Outcome.Canceled;
        }
        catch (CamusDBException ex)
        {
            outcome = ClassifyOutcome(ex.Code);
            logger.LogError("{Name}: {Message}", ex.GetType().Name, ex.Message);
            await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
            {
                RequestId = req.RequestId,
                Error = new BatchError { Code = ex.Code, Message = ex.Message },
            }, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            outcome = ServerDiagnostics.Tags.Outcome.InternalError;
            logger.LogError("{Name}: {Message}", ex.GetType().Name, ex.Message);
            await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
            {
                RequestId = req.RequestId,
                Error = new BatchError { Code = "CADB0000", Message = "Internal server error" },
            }, ct).ConfigureAwait(false);
        }
        finally
        {
            loadGauge.Decrement();
            ServerDiagnostics.AddRequestInFlight(ServerDiagnostics.Tags.Transport.GrpcBatch, -1);
            ServerDiagnostics.RecordRequest(
                operation, ServerDiagnostics.Tags.Transport.GrpcBatch, outcome,
                Stopwatch.GetElapsedTime(requestStart).TotalMilliseconds);
            requestSpan?.SetTag("outcome", outcome);
            if (outcome != ServerDiagnostics.Tags.Outcome.Ok)
                requestSpan?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
        }
    }

    /// <summary>Maps a batch statement kind to the bounded <c>operation</c> metric tag.</summary>
    private static string MapBatchOperation(BatchStatementKind kind) => kind switch
    {
        BatchStatementKind.Query => ServerDiagnostics.Tags.Operation.Query,
        BatchStatementKind.Start => ServerDiagnostics.Tags.Operation.Begin,
        BatchStatementKind.Commit => ServerDiagnostics.Tags.Operation.Commit,
        BatchStatementKind.Rollback => ServerDiagnostics.Tags.Operation.Rollback,
        _ => ServerDiagnostics.Tags.Operation.NonQuery,
    };

    /// <summary>Maps a domain error code to a bounded <c>outcome</c> tag (conflict classes vs other domain errors).</summary>
    private static string ClassifyOutcome(string code) => code switch
    {
        "CADB0502" or "CADB0504" or "CADB0505" => ServerDiagnostics.Tags.Outcome.Conflict,
        _ => ServerDiagnostics.Tags.Outcome.DomainError,
    };

    private async Task RunBatchQueryAsync(
        int requestId,
        SqlRequest request,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        Principal? principal,
        CancellationToken ct)
    {
        string sql = request.Sql ?? "";
        BatchQuerySink sink = new(stream, writeLock, requestId);
        NodeAst ast = SQLParserProcessor.Parse(sql);
        HLCTimestamp commitToken = default;

        if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors or NodeType.ShowOrphanDatabases)
        {
            ExecuteSQLTicket ticket = new(
                txnState: null!, database: request.Database, sql: sql,
                parameters: ToColumnValueMap(request.Parameters), principal: principal);
            await StreamQueryAsync(ticket, sink, ct).ConfigureAwait(false);
        }
        else if (request.TxnHandle is { TxnIdPt: > 0 } handle)
        {
            // Explicit transaction — the client owns the lifecycle, so no commit and no auto-rollback.
            KvTransaction txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            ExecuteSQLTicket ticket = new(
                txnState: txnState, database: request.Database, sql: sql,
                parameters: ToColumnValueMap(request.Parameters), principal: principal);
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
                    parameters: ToColumnValueMap(request.Parameters), principal: principal);
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
        Principal? principal,
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
                parameters: ToColumnValueMap(request.Parameters), principal: principal);
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
                    parameters: ToColumnValueMap(request.Parameters), principal: principal);
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
    /// Begins a transaction from a batched START op and returns its server-minted handle in-band. The
    /// handle is recorded in <paramref name="startedHandles"/> so a stream that ends without a matching
    /// COMMIT/ROLLBACK rolls it back on teardown. START carries no handle, so it is never chained — the
    /// client awaits this reply before sending the transaction's statements, so it never races them.
    /// </summary>
    private async Task RunBatchStartAsync(
        int requestId,
        SqlRequest request,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles,
        CancellationToken ct)
    {
        (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking) =
            ParseLevelMode(request);

        KvTransaction tx = await transactions.StartAsync(
            request.Database, level, mode, locking, deferStart: true, cancellationToken: ct).ConfigureAwait(false);

        TxnHandle handle = new()
        {
            TxnIdPt      = tx.ClientId.L,
            TxnIdCounter = (uint)tx.ClientId.C,
        };
        startedHandles[$"{handle.TxnIdPt}:{handle.TxnIdCounter}"] = (handle.TxnIdPt, handle.TxnIdCounter);

        await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
        {
            RequestId  = requestId,
            StartReply = handle,
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Commits the transaction named by a batched COMMIT op's <c>txn_handle</c> and returns its causal
    /// token in-band. The handle is removed from <paramref name="startedHandles"/> only on success, so a
    /// failed commit (e.g. a conflict, or the finalize gate rejecting a duplicate) still leaves teardown
    /// free to roll back if the transaction is somehow still open. Chained after the handle's statements.
    /// </summary>
    private async Task RunBatchCommitAsync(
        int requestId,
        SqlRequest request,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles,
        CancellationToken ct)
    {
        if (request.TxnHandle is not { TxnIdPt: > 0 } handle)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "COMMIT batch op requires a txn_handle");

        KvTransaction txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
        HLCTimestamp token = await transactions.CommitTrackedAsync(txnState, ct).ConfigureAwait(false);
        startedHandles.TryRemove($"{handle.TxnIdPt}:{handle.TxnIdCounter}", out _);

        CommitReply reply = new();
        if (!token.IsNull())
            ApplyCausalToken(reply, token);

        await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
        {
            RequestId   = requestId,
            CommitReply = reply,
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Rolls back the transaction named by a batched ROLLBACK op's <c>txn_handle</c>. Removes the handle
    /// from <paramref name="startedHandles"/> on success so teardown does not roll it back a second time.
    /// Chained after the handle's statements.
    /// </summary>
    private async Task RunBatchRollbackAsync(
        int requestId,
        SqlRequest request,
        IServerStreamWriter<BatchExecuteResponse> stream,
        SemaphoreSlim writeLock,
        ConcurrentDictionary<string, (long pt, uint counter)> startedHandles,
        CancellationToken ct)
    {
        if (request.TxnHandle is not { TxnIdPt: > 0 } handle)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ROLLBACK batch op requires a txn_handle");

        // Idempotent: a statement earlier in this chain may have failed and already rolled the
        // transaction back, which is precisely when a client sends ROLLBACK. The handle is dropped
        // either way so teardown does not try again.
        await transactions.RollbackByIdAsync(handle.TxnIdPt, (uint)handle.TxnIdCounter, ct).ConfigureAwait(false);
        startedHandles.TryRemove($"{handle.TxnIdPt}:{handle.TxnIdCounter}", out _);

        await TryWriteBatchAsync(stream, writeLock, new BatchExecuteResponse
        {
            RequestId     = requestId,
            RollbackReply = new RollbackReply(),
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

    /// <summary>
    /// Rolls back an explicit transaction identified by its <see cref="TxnHandle"/>. Idempotent: a
    /// handle that is no longer tracked — because a failed statement or the idle reaper already
    /// rolled it back — succeeds as a no-op, since the requested end state has been reached.
    /// </summary>
    public override Task<RollbackReply> RollbackTransaction(TxnHandle request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            await transactions.RollbackByIdAsync(request.TxnIdPt, (uint)request.TxnIdCounter).ConfigureAwait(false);
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
