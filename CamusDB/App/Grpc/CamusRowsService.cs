
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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Transactions;
using CamusDB.App.Services;
using CamusDB.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using EngineQueryFilter  = CamusDB.Core.CommandsExecutor.Models.QueryFilter;
using ProtoQueryFilter   = CamusDB.Grpc.QueryFilter;
using ProtoOrderBy       = CamusDB.Grpc.OrderBy;
using ProtoOrderDir      = CamusDB.Grpc.OrderDirection;
using CoreColumnType     = CamusDB.Core.Catalogs.Models.ColumnType;

namespace CamusDB.App.Grpc;

/// <summary>
/// gRPC service that exposes typed row-level CRUD (Insert, Query, QueryById, Update, Delete)
/// without requiring callers to compose SQL. Mirrors <c>InsertController</c>, <c>QueryController</c>,
/// <c>UpdateController</c>, and <c>DeleteController</c> in the REST layer, using the same ticket
/// types and executor patterns.
///
/// <para>Streaming invariant for <see cref="Query"/> and <see cref="QueryById"/>: exactly one
/// <c>ResultSchema</c> message is written first even for empty result sets, followed by zero or
/// more positional <c>ResultRow</c> messages. The output-column schema is derived from the
/// <b>table's column definitions</b> (opened once up front), not from row values — so the reported
/// column types are correct even when the first row holds a NULL, and an empty result still carries
/// the full column schema (matching the SQL path's empty-result guarantee). Rows stream one at a
/// time; nothing is buffered.</para>
///
/// <para>Error mapping and autocommit/explicit-transaction split follow the same patterns as
/// <see cref="CamusSqlService"/>: domain exceptions are translated at the RPC boundary via
/// <see cref="GrpcErrorMapper"/> and the <see cref="InvokeAsync{T}"/> / <see cref="InvokeStreamingAsync"/>
/// wrappers.</para>
/// </summary>
public sealed class CamusRowsService : CamusRows.CamusRowsBase
{
    private readonly CommandExecutor executor;
    private readonly HttpTransactionCoordinator transactions;
    private readonly ILogger<ICamusDB> logger;

    public CamusRowsService(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        ILogger<ICamusDB> logger)
    {
        this.executor     = executor;
        this.transactions = transactions;
        this.logger       = logger;
    }

    // ─── InsertRow ────────────────────────────────────────────────────────────

    /// <summary>
    /// Inserts one row into the target table. Mirrors <c>InsertController.Insert</c> exactly.
    /// Autocommit with Serializable retry when the resolved isolation level demands it.
    /// </summary>
    public override Task<NonQueryReply> InsertRow(InsertRowRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            KeyValueTransactionLocking? reqLocking = ToLocking(request.Locking);
            Dictionary<string, ColumnValue> values = ToColumnValueMap(request.Values);

            // Explicit transaction — client owns the lifecycle.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    InsertTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.Database,
                        tableName: request.Table,
                        values: [values]
                    );
                    InsertResult result = await executor.Insert(ticket).ConfigureAwait(false);
                    return new NonQueryReply { AffectedRows = result.InsertedRows };
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit.
            int insertedRows = 0;
            HLCTimestamp causalToken = default;

            async Task AutocommitInsert(CancellationToken innerCt)
            {
                KvTransaction tx = await transactions.StartAsync(
                    request.Database, null, null, reqLocking, cancellationToken: innerCt).ConfigureAwait(false);
                try
                {
                    InsertTicket ticket = new(
                        txnState: tx,
                        databaseName: request.Database,
                        tableName: request.Table,
                        values: [values]
                    );
                    InsertResult r = await executor.Insert(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, innerCt).ConfigureAwait(false);
                    insertedRows = r.InsertedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                    throw;
                }
            }

            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitInsert).ConfigureAwait(false);
            else
                await AutocommitInsert(ct).ConfigureAwait(false);

            NonQueryReply reply = new() { AffectedRows = insertedRows };
            if (!causalToken.IsNull())
                ApplyCausalToken(reply, causalToken);
            return reply;
        });

    // ─── Query (server-streaming, schema-first) ───────────────────────────────

    /// <summary>
    /// Queries a table using filter and ordering parameters, streaming results schema-first.
    /// Mirrors <c>QueryController.Query</c> but emits <c>QueryStreamMessage</c> rather than a JSON
    /// array, using a read-only autocommit transaction promoted from the global snapshot.
    ///
    /// The output-column schema is derived from the first row in the result: column names and types
    /// are read from the <c>ColumnValue</c> entries. When the result is empty, an empty schema is
    /// emitted and the stream ends.
    /// </summary>
    public override Task Query(
        RowQueryRequest request,
        IServerStreamWriter<QueryStreamMessage> responseStream,
        ServerCallContext context)
        => InvokeStreamingAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            List<EngineQueryFilter>? filters = ToFilters(request.Filters);
            List<QueryOrderBy>? orderBy = ToOrderBy(request.OrderBy);

            // Open the table once to derive the output schema from its column definitions (correct
            // types, and a full schema even for an empty result — unlike deriving from row values).
            (DatabaseDescriptor db, TableDescriptor table) = await OpenAsync(request.Database, request.Table).ConfigureAwait(false);
            IReadOnlyList<DerivedColumnSchema> schema = SchemaFromTable(table);

            // Explicit transaction.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    QueryTicket ticket = BuildQueryTicket(txnState, request, filters, orderBy);
                    await StreamQueryRowsAsync(ticket, schema, responseStream, ct).ConfigureAwait(false);
                    return;
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit: promoted read-only transaction.
            HLCTimestamp? causalToken = ToCausalToken(request.CausalTokenN, request.CausalTokenL, request.CausalTokenC);
            KvTransaction tx = await transactions.BeginReadOnlyAsync(
                request.Database, promote: true, causalToken, ct).ConfigureAwait(false);
            try
            {
                QueryTicket ticket = BuildQueryTicket(tx, request, filters, orderBy);
                await StreamQueryRowsAsync(ticket, schema, responseStream, ct).ConfigureAwait(false);
                HLCTimestamp commitToken = await transactions.CommitAsync(db, tx, ct).ConfigureAwait(false);
                if (!commitToken.IsNull())
                {
                    context.ResponseTrailers.Add("camus-causal-token-n", commitToken.N.ToString());
                    context.ResponseTrailers.Add("camus-causal-token-l", commitToken.L.ToString());
                    context.ResponseTrailers.Add("camus-causal-token-c", ((uint)commitToken.C).ToString());
                }
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                throw;
            }
        });

    // ─── QueryById (server-streaming, schema-first) ───────────────────────────

    /// <summary>
    /// Looks up a single row by its primary key id and streams it schema-first.
    /// The output schema is inferred from the returned row's column types; if no row matches
    /// the id, an empty schema is emitted and the stream ends.
    /// </summary>
    public override Task QueryById(
        RowByIdRequest request,
        IServerStreamWriter<QueryStreamMessage> responseStream,
        ServerCallContext context)
        => InvokeStreamingAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;

            // Open the table once for the output schema (from column definitions, not row values).
            (DatabaseDescriptor db, TableDescriptor table) = await OpenAsync(request.Database, request.Table).ConfigureAwait(false);
            IReadOnlyList<DerivedColumnSchema> schema = SchemaFromTable(table);

            async Task RunQueryById(KvTransaction txnState)
            {
                QueryByIdTicket ticket = new(
                    txnState: txnState,
                    databaseName: request.Database,
                    tableName: request.Table,
                    id: request.Id
                );

                await StreamQueryByIdAsync(ticket, schema, responseStream, ct).ConfigureAwait(false);
            }

            // Explicit transaction.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    await RunQueryById(txnState).ConfigureAwait(false);
                    return;
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit: promoted read-only transaction. The database descriptor from OpenAsync lets
            // CommitAsync handle both zero-snapshot and promoted txns, avoiding the tracked-only
            // restriction of CommitTrackedAsync.
            HLCTimestamp? causalToken = ToCausalToken(request.CausalTokenN, request.CausalTokenL, request.CausalTokenC);
            KvTransaction tx = await transactions.BeginReadOnlyAsync(
                request.Database, promote: true, causalToken, ct).ConfigureAwait(false);
            try
            {
                await RunQueryById(tx).ConfigureAwait(false);
                HLCTimestamp commitToken = await transactions.CommitAsync(db, tx, ct).ConfigureAwait(false);
                if (!commitToken.IsNull())
                {
                    context.ResponseTrailers.Add("camus-causal-token-n", commitToken.N.ToString());
                    context.ResponseTrailers.Add("camus-causal-token-l", commitToken.L.ToString());
                    context.ResponseTrailers.Add("camus-causal-token-c", ((uint)commitToken.C).ToString());
                }
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                throw;
            }
        });

    // ─── UpdateRows ───────────────────────────────────────────────────────────

    /// <summary>
    /// Updates rows matching the supplied filters. Mirrors <c>UpdateController.Update</c> exactly,
    /// using plain (non-expression) values and filter-based row selection.
    /// </summary>
    public override Task<NonQueryReply> UpdateRows(UpdateRowsRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            KeyValueTransactionLocking? reqLocking = ToLocking(request.Locking);
            Dictionary<string, ColumnValue> plainValues = ToColumnValueMap(request.Values);
            List<EngineQueryFilter>? filters = ToFilters(request.Filters);

            // Explicit transaction.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    UpdateTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.Database,
                        tableName: request.Table,
                        plainValues: plainValues,
                        exprValues: null,
                        where: null,
                        filters: filters,
                        parameters: null
                    );
                    UpdateResult result = await executor.Update(ticket).ConfigureAwait(false);
                    return new NonQueryReply { AffectedRows = result.UpdatedRows };
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit.
            int updatedRows = 0;
            HLCTimestamp causalToken = default;

            async Task AutocommitUpdate(CancellationToken innerCt)
            {
                KvTransaction tx = await transactions.StartAsync(
                    request.Database, null, null, reqLocking, cancellationToken: innerCt).ConfigureAwait(false);
                try
                {
                    UpdateTicket ticket = new(
                        txnState: tx,
                        databaseName: request.Database,
                        tableName: request.Table,
                        plainValues: plainValues,
                        exprValues: null,
                        where: null,
                        filters: filters,
                        parameters: null
                    );
                    UpdateResult r = await executor.Update(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, innerCt).ConfigureAwait(false);
                    updatedRows = r.UpdatedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                    throw;
                }
            }

            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitUpdate).ConfigureAwait(false);
            else
                await AutocommitUpdate(ct).ConfigureAwait(false);

            NonQueryReply reply = new() { AffectedRows = updatedRows };
            if (!causalToken.IsNull())
                ApplyCausalToken(reply, causalToken);
            return reply;
        });

    // ─── UpdateById ───────────────────────────────────────────────────────────

    /// <summary>
    /// Updates the single row identified by <paramref name="request"/> id, using a primary-key
    /// equality filter on the <c>id</c> column. Equivalent to <see cref="UpdateRows"/> with a
    /// single filter targeting the row's id.
    /// </summary>
    public override Task<NonQueryReply> UpdateById(UpdateByIdRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            KeyValueTransactionLocking? reqLocking = ToLocking(request.Locking);
            Dictionary<string, ColumnValue> plainValues = ToColumnValueMap(request.Values);
            (_, TableDescriptor table) = await OpenAsync(request.Database, request.Table).ConfigureAwait(false);
            List<EngineQueryFilter> filters = [PrimaryKeyFilter(table, request.Id)];

            // Explicit transaction.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    UpdateTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.Database,
                        tableName: request.Table,
                        plainValues: plainValues,
                        exprValues: null,
                        where: null,
                        filters: filters,
                        parameters: null
                    );
                    UpdateResult result = await executor.Update(ticket).ConfigureAwait(false);
                    return new NonQueryReply { AffectedRows = result.UpdatedRows };
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit.
            int updatedRows = 0;
            HLCTimestamp causalToken = default;

            async Task AutocommitUpdateById(CancellationToken innerCt)
            {
                KvTransaction tx = await transactions.StartAsync(
                    request.Database, null, null, reqLocking, cancellationToken: innerCt).ConfigureAwait(false);
                try
                {
                    UpdateTicket ticket = new(
                        txnState: tx,
                        databaseName: request.Database,
                        tableName: request.Table,
                        plainValues: plainValues,
                        exprValues: null,
                        where: null,
                        filters: filters,
                        parameters: null
                    );
                    UpdateResult r = await executor.Update(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, innerCt).ConfigureAwait(false);
                    updatedRows = r.UpdatedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                    throw;
                }
            }

            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitUpdateById).ConfigureAwait(false);
            else
                await AutocommitUpdateById(ct).ConfigureAwait(false);

            NonQueryReply reply = new() { AffectedRows = updatedRows };
            if (!causalToken.IsNull())
                ApplyCausalToken(reply, causalToken);
            return reply;
        });

    // ─── DeleteRows ───────────────────────────────────────────────────────────

    /// <summary>
    /// Deletes rows matching the supplied filters. Mirrors <c>DeleteController.Delete</c> exactly.
    /// </summary>
    public override Task<NonQueryReply> DeleteRows(DeleteRowsRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            KeyValueTransactionLocking? reqLocking = ToLocking(request.Locking);
            List<EngineQueryFilter>? filters = ToFilters(request.Filters);

            // Explicit transaction.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    DeleteTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.Database,
                        tableName: request.Table,
                        where: null,
                        filters: filters
                    );
                    DeleteResult result = await executor.Delete(ticket).ConfigureAwait(false);
                    return new NonQueryReply { AffectedRows = result.DeletedRows };
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit.
            int deletedRows = 0;
            HLCTimestamp causalToken = default;

            async Task AutocommitDelete(CancellationToken innerCt)
            {
                KvTransaction tx = await transactions.StartAsync(
                    request.Database, null, null, reqLocking, cancellationToken: innerCt).ConfigureAwait(false);
                try
                {
                    DeleteTicket ticket = new(
                        txnState: tx,
                        databaseName: request.Database,
                        tableName: request.Table,
                        where: null,
                        filters: filters
                    );
                    DeleteResult r = await executor.Delete(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, innerCt).ConfigureAwait(false);
                    deletedRows = r.DeletedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                    throw;
                }
            }

            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitDelete).ConfigureAwait(false);
            else
                await AutocommitDelete(ct).ConfigureAwait(false);

            NonQueryReply reply = new() { AffectedRows = deletedRows };
            if (!causalToken.IsNull())
                ApplyCausalToken(reply, causalToken);
            return reply;
        });

    // ─── DeleteById ───────────────────────────────────────────────────────────

    /// <summary>
    /// Deletes the single row identified by the supplied id, using a primary-key equality filter
    /// on the <c>id</c> column. Equivalent to <see cref="DeleteRows"/> with a single filter.
    /// </summary>
    public override Task<NonQueryReply> DeleteById(RowByIdRequest request, ServerCallContext context)
        => InvokeAsync(async () =>
        {
            CancellationToken ct = context.CancellationToken;
            KeyValueTransactionLocking? reqLocking = ToLocking(request.Locking);
            (_, TableDescriptor table) = await OpenAsync(request.Database, request.Table).ConfigureAwait(false);
            List<EngineQueryFilter> filters = [PrimaryKeyFilter(table, request.Id)];

            // Explicit transaction.
            if (request.TxnHandle is { TxnIdPt: > 0 } handle)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
                    DeleteTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.Database,
                        tableName: request.Table,
                        where: null,
                        filters: filters
                    );
                    DeleteResult result = await executor.Delete(ticket).ConfigureAwait(false);
                    return new NonQueryReply { AffectedRows = result.DeletedRows };
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit.
            int deletedRows = 0;
            HLCTimestamp causalToken = default;

            async Task AutocommitDeleteById(CancellationToken innerCt)
            {
                KvTransaction tx = await transactions.StartAsync(
                    request.Database, null, null, reqLocking, cancellationToken: innerCt).ConfigureAwait(false);
                try
                {
                    DeleteTicket ticket = new(
                        txnState: tx,
                        databaseName: request.Database,
                        tableName: request.Table,
                        where: null,
                        filters: filters
                    );
                    DeleteResult r = await executor.Delete(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, innerCt).ConfigureAwait(false);
                    deletedRows = r.DeletedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, innerCt).ConfigureAwait(false);
                    throw;
                }
            }

            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitDeleteById).ConfigureAwait(false);
            else
                await AutocommitDeleteById(ct).ConfigureAwait(false);

            NonQueryReply reply = new() { AffectedRows = deletedRows };
            if (!causalToken.IsNull())
                ApplyCausalToken(reply, causalToken);
            return reply;
        });

    // ─── Core streaming helpers ───────────────────────────────────────────────

    /// <summary>
    /// Executes a <see cref="QueryTicket"/> and streams results schema-first into
    /// <paramref name="responseStream"/> against the pre-derived <paramref name="schema"/> (from the
    /// table's column definitions). The schema is written first even when no rows follow.
    /// </summary>
    private async Task StreamQueryRowsAsync(
        QueryTicket ticket,
        IReadOnlyList<DerivedColumnSchema> schema,
        IServerStreamWriter<QueryStreamMessage> responseStream,
        CancellationToken ct)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket).ConfigureAwait(false);

        await responseStream.WriteAsync(CamusSqlService.BuildSchema(schema), ct).ConfigureAwait(false);
        await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
            await responseStream.WriteAsync(CamusSqlService.BuildRow(row.Row, schema), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a <see cref="QueryByIdTicket"/> and streams the result (at most one row) schema-first
    /// into <paramref name="responseStream"/> against the pre-derived <paramref name="schema"/>.
    /// The schema is written first even when no row matches the id.
    /// </summary>
    private async Task StreamQueryByIdAsync(
        QueryByIdTicket ticket,
        IReadOnlyList<DerivedColumnSchema> schema,
        IServerStreamWriter<QueryStreamMessage> responseStream,
        CancellationToken ct)
    {
        IAsyncEnumerable<Dictionary<string, ColumnValue>> cursor =
            await executor.QueryById(ticket).ConfigureAwait(false);

        await responseStream.WriteAsync(CamusSqlService.BuildSchema(schema), ct).ConfigureAwait(false);
        await foreach (Dictionary<string, ColumnValue> row in cursor.WithCancellation(ct).ConfigureAwait(false))
            await responseStream.WriteAsync(CamusSqlService.BuildRow(row, schema), ct).ConfigureAwait(false);
    }

    // ─── RPC boundary ─────────────────────────────────────────────────────────

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

    private Task InvokeStreamingAsync(Func<Task> body)
        => InvokeAsync(async () => { await body().ConfigureAwait(false); return true; });

    // ─── Schema / primary-key helpers ─────────────────────────────────────────

    /// <summary>Opens the database and table descriptors for metadata (schema, primary-key index).</summary>
    private async Task<(DatabaseDescriptor db, TableDescriptor table)> OpenAsync(string database, string table)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(database).ConfigureAwait(false);
        TableDescriptor td = await executor.OpenTableWithDescriptor(
            db, new OpenTableTicket(database, table)).ConfigureAwait(false);
        return (db, td);
    }

    /// <summary>
    /// Builds the ordered output-column schema from a table's readable column definitions, so column
    /// types are authoritative (not inferred from a possibly-NULL first row) and an empty result still
    /// carries the full schema. Non-readable columns (e.g. a dropped/absent online element) are omitted.
    /// </summary>
    private static IReadOnlyList<DerivedColumnSchema> SchemaFromTable(TableDescriptor table)
    {
        List<DerivedColumnSchema> cols = new();
        foreach (TableColumnSchema col in table.Schema.Columns ?? [])
            if (SchemaElementStateRules.IsReadable(col))
                cols.Add(new DerivedColumnSchema(col.Name, col.Type));
        return cols;
    }

    /// <summary>
    /// Builds an equality filter on the table's actual primary-key column (resolved from the
    /// <c>~pk</c> index), used by <see cref="UpdateById"/> / <see cref="DeleteById"/>. Resolving the
    /// real column name — rather than hardcoding <c>"id"</c> — keeps these consistent with
    /// <see cref="QueryById"/> for tables whose primary key is named or typed differently. The id
    /// value is typed as <see cref="CoreColumnType.Id"/> for an Id key (matching the engine's by-id
    /// convention) and otherwise as a string, letting filter coercion match Uuid/String keys.
    /// </summary>
    private static EngineQueryFilter PrimaryKeyFilter(TableDescriptor table, string id)
    {
        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? pk) ||
            pk.Columns.Length == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation, "Table doesn't have a primary key index");

        string pkColumn = pk.Columns[0];
        CoreColumnType pkType = CoreColumnType.Id;
        foreach (TableColumnSchema col in table.Schema.Columns ?? [])
            if (col.Name == pkColumn) { pkType = col.Type; break; }

        ColumnValue value = pkType == CoreColumnType.Id
            ? new ColumnValue(CoreColumnType.Id, id)
            : new ColumnValue(CoreColumnType.String, id);

        return new EngineQueryFilter(pkColumn, "=", value);
    }

    // ─── Request helpers ──────────────────────────────────────────────────────

    private static QueryTicket BuildQueryTicket(
        KvTransaction txnState,
        RowQueryRequest request,
        List<EngineQueryFilter>? filters,
        List<QueryOrderBy>? orderBy)
        => new(
            txnState: txnState,
            databaseName: request.Database,
            tableName: request.Table,
            index: string.IsNullOrEmpty(request.IndexName) ? null : request.IndexName,
            projection: null,
            filters: filters,
            where: null,
            orderBy: orderBy,
            limit: null,
            offset: null,
            parameters: null
        );

    private static void ApplyCausalToken(NonQueryReply reply, HLCTimestamp token)
    {
        reply.CausalTokenN = token.N;
        reply.CausalTokenL = token.L;
        reply.CausalTokenC = (long)(uint)token.C;
    }

    private static Dictionary<string, ColumnValue> ToColumnValueMap(
        Google.Protobuf.Collections.MapField<string, Value> protoMap)
    {
        Dictionary<string, ColumnValue> result = new(protoMap.Count);
        foreach (KeyValuePair<string, Value> kv in protoMap)
            result[kv.Key] = GrpcValueCodec.FromProto(kv.Value);
        return result;
    }

    private static List<EngineQueryFilter>? ToFilters(
        Google.Protobuf.Collections.RepeatedField<ProtoQueryFilter> protoFilters)
    {
        if (protoFilters.Count == 0)
            return null;

        List<EngineQueryFilter> result = new(protoFilters.Count);
        foreach (ProtoQueryFilter f in protoFilters)
            result.Add(new EngineQueryFilter(f.ColumnName, f.Op, GrpcValueCodec.FromProto(f.Value)));
        return result;
    }

    private static List<QueryOrderBy>? ToOrderBy(
        Google.Protobuf.Collections.RepeatedField<ProtoOrderBy> protoOrderBy)
    {
        if (protoOrderBy.Count == 0)
            return null;

        List<QueryOrderBy> result = new(protoOrderBy.Count);
        foreach (ProtoOrderBy o in protoOrderBy)
            result.Add(new QueryOrderBy(
                o.ColumnName,
                o.Direction == ProtoOrderDir.OrderDescending
                    ? OrderType.Descending
                    : OrderType.Ascending
            ));
        return result;
    }

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
}
