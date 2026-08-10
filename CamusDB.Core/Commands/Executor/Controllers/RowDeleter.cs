
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.Diagnostics;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Controllers.Ttl;
using CamusDB.Core.Statistics;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowDeleter
{
    private readonly ILogger<ICamusDB> logger;
    private readonly StatisticsManager? _stats;

    public RowDeleter(ILogger<ICamusDB> logger, StatisticsManager? stats = null)
    {
        this.logger = logger;
        _stats = stats;
    }

    /// <param name="allowMaterializedView">
    /// True only for the engine's own row removal when a relation is being dropped. A materialized
    /// view refuses user DML, but dropping one still has to clear its rows, and that removal is the
    /// consequence of a statement that was already authorized against the materialized view itself.
    /// </param>
    public async Task<int> Delete(
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        DeleteTicket ticket,
        bool allowMaterializedView = false)
    {
        if (!allowMaterializedView)
            MaterializedViewAccessGuard.RequireWritable(table);

        DeleteFluxState state = new(
            queryExecutor: queryExecutor,
            database: database,
            table: table,
            ticket: ticket
        );

        FluxMachine<DeleteFluxSteps, DeleteFluxState> machine = new(state);

        return await DeleteInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes an explicit set of rows that a row-level TTL sweep found expired, re-asserting the expiry
    /// predicate on each one under the mutation lock. Returns how many rows were deleted and how many
    /// were spared by the re-check.
    ///
    /// <para><b>Why this exists instead of a <c>DELETE … WHERE</c>.</b> A TTL span is a range of
    /// <em>row id</em>, and row id is not addressable from a WHERE clause — there is no SQL predicate
    /// that means "this span". The sweep therefore arrives with row ids in hand.</para>
    ///
    /// <para><b>Why it re-checks rather than trusting the scan.</b> Between the scan that found a row
    /// expired and this delete, another transaction may have extended its expiry — and on a session or
    /// heartbeat table, which is exactly what TTL is for, that is the single most common write there is.
    /// Deleting by primary key alone would silently destroy live data. The re-check costs nothing extra
    /// because <see cref="KvTableStore.GetRowsBatchLockedForMutation"/> has already re-read and decoded
    /// the row under the lock that makes the answer trustworthy.</para>
    ///
    /// <para>A row that fails the re-check is dropped from the batch and counted, never retried in a
    /// tight loop: the sweep will see it again next run, by which time it may legitimately have
    /// expired.</para>
    /// </summary>
    public async Task<(int deleted, int skipped)> DeleteExpiredRowsAsync(
        TableDescriptor table,
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        string expirationColumn,
        long cutoffEpochMs,
        CancellationToken cancellationToken = default)
    {
        if (rowIds.Count == 0)
            return (0, 0);

        ReadOnlyMemory<byte>?[] rawRows = await table.Store.GetRowsBatchLockedForMutation(tx, rowIds, cancellationToken).ConfigureAwait(false);

        // Index writability is fixed for the statement, so filter once here instead of per row; the
        // decode below is narrowed to the columns this method actually consumes — the index key
        // columns (for CollectIndexDeletes) plus the expiry column (for the re-check). Values for
        // those columns are identical to a full decode; other columns are simply never materialized.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);
        HashSet<string> requiredColumns = CollectIndexKeyColumns(writableIndexes);
        requiredColumns.Add(expirationColumn);
        RowEncoder.DictionaryDecodeState decodeState = new();

        List<KvTableStore.RowDelete> batch = new(rowIds.Count);
        int skipped = 0;

        for (int i = 0; i < rowIds.Count; i++)
        {
            ObjectIdValue rowId = rowIds[i];
            ReadOnlyMemory<byte>? data = rawRows[i];

            // Unlike the user-facing delete path, a row that vanished is not an error here: a
            // concurrent user DELETE removing the same expired row is a perfectly ordinary race, and
            // the sweep's goal (the row is gone) is already satisfied.
            if (data is null || data.Value.Length == 0)
                continue;

            Dictionary<string, ColumnValue> writableRow = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, data.Value,
                requiredColumns: requiredColumns,
                visibilitySchemaVersion: table.Schema.Version,
                decodeState: decodeState).ConfigureAwait(false);

            if (!TtlExpiryPredicate.IsExpired(writableRow, expirationColumn, cutoffEpochMs))
            {
                skipped++;
                continue;
            }

            batch.Add(new KvTableStore.RowDelete
            {
                RowId = rowId,
                IndexEntries = CollectIndexDeletes(writableIndexes, rowId, writableRow),
            });
        }

        if (batch.Count == 0)
            return (0, skipped);

        // The same batched primitive the user-facing delete uses, so the row and every one of its index
        // entries go in one transaction — an expired row that lost its row but kept an index entry would
        // make index-only scans return rows that no longer exist.
        await table.Store.DeleteRowsBatch(tx, batch).ConfigureAwait(false);

        foreach (KvTableStore.RowDelete row in batch)
            Log.LogRowDeleted(logger, row.RowId);

        return (batch.Count, skipped);
    }

    private static CompositeColumnValue GetColumnValue(Dictionary<string, ColumnValue> rowValues, string[] columnNames, ColumnValue? extraUniqueValue = null)
    {
        ColumnValue[] columnValues = new ColumnValue[extraUniqueValue is null ? columnNames.Length : columnNames.Length + 1];

        for (int i = 0; i < columnNames.Length; i++)
        {
            string name = columnNames[i];

            if (!rowValues.TryGetValue(name, out ColumnValue? columnValue))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field '" + name + "'"
                );

            columnValues[i] = columnValue;
        }

        if (extraUniqueValue is not null)
            columnValues[^1] = extraUniqueValue;

        return new CompositeColumnValue(columnValues);
    }

    /// <summary>
    /// Union of the key columns of every writable index, as a case-insensitive set matching the
    /// decode dictionary's comparer. Used to narrow <see cref="RowEncoder.DecodeWritableAsync"/>
    /// to the columns the delete path actually consumes — safe only because deleted rows are never
    /// re-encoded from the decoded dictionary.
    /// </summary>
    private static HashSet<string> CollectIndexKeyColumns(IReadOnlyList<TableIndexSchema> writableIndexes)
    {
        HashSet<string> columns = new(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < writableIndexes.Count; i++)
        {
            foreach (string columnName in writableIndexes[i].Columns)
                columns.Add(columnName);
        }

        return columns;
    }

    /// <summary>
    /// Returns true when any of the index's columns is absent from the row or holds a NULL value.
    /// Such a row is exempt from a unique index (NULLs are distinct) and carries no index entry.
    /// </summary>
    private static bool HasNullKeyColumn(Dictionary<string, ColumnValue> rowValues, string[] columnNames)
    {
        foreach (string name in columnNames)
        {
            if (!rowValues.TryGetValue(name, out ColumnValue? value) || value.Type == ColumnType.Null)
                return true;
        }

        return false;
    }

    private async Task<FluxAction> LocateTupleToDelete(DeleteFluxState state)
    {
        DeleteTicket ticket = state.Ticket;

        // Decode only the columns the WHERE/filter needs during the locate scan.
        // The write phase calls LoadWritableRow which does a full decode per matched row.
        // Returns null when the WHERE contains subquery nodes — fall back to full decode.
        IReadOnlySet<string>? locateColumns = RequiredColumnAnalyzer.ComputeForLocate(
            ticket.Where, ticket.Filters, exprValues: null);

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null,
            limit: ticket.Limit,
            offset: null,
            parameters: ticket.Parameters,
            locateColumns: locateColumns,
            exclusivePredicateLocks: true
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        SpillableRowList rowList = new(QueryExecutionContext.For(state.Database));
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            await rowList.AddAsync(row).ConfigureAwait(false);
        await rowList.SealAsync().ConfigureAwait(false);
        state.RowsToDelete = rowList;

        return FluxAction.Continue;
    }

    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.RowsToDelete.Count == 0)
        {
            logger.LogError("Invalid rows to delete");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        KvTransaction tx = state.Ticket.TxnState;

        // Drain RowsToDelete in bounded chunks so a DELETE over a huge matched set does not
        // hold an O(matched) list on the heap between scan and the Kahuna round-trip. The chunk
        // size is tied to SpillEffectiveThreshold so force-spill tests drive both the row-buffer
        // spill and the mutation batch with one knob. RowsToDelete is already sealed (the full
        // match set is materialized before any delete), so chunked draining preserves the
        // Halloween barrier.
        int chunkSize = state.Database.Options.SpillEffectiveThreshold;
        List<ObjectIdValue> chunk = new(Math.Min(chunkSize, 64));

        await foreach (QueryResultRow row in state.RowsToDelete.EnumerateAsync().ConfigureAwait(false))
        {
            chunk.Add(row.RowId);

            if (chunk.Count >= chunkSize)
            {
                await FlushDeleteChunk(table, tx, chunk, state).ConfigureAwait(false);
                chunk.Clear();
            }
        }

        if (chunk.Count > 0)
            await FlushDeleteChunk(table, tx, chunk, state).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Reads the raw bytes for one chunk of matched row ids in a single batch round-trip, decodes each
    /// to determine its index entries, and deletes the rows and their index entries in one batch.
    /// The read uses <see cref="KvTableStore.GetRowsBatchLockedForMutation"/> so a Serializable+RW
    /// delete holds the same shared point locks a per-row <c>GetRow</c> would — without them an
    /// index-scan-located delete could read a row lock-free and miss a concurrent modify-commit,
    /// deleting a stale index entry and orphaning the concurrently-written one.
    /// </summary>
    private async Task FlushDeleteChunk(
        TableDescriptor table,
        KvTransaction tx,
        List<ObjectIdValue> chunk,
        DeleteFluxState state)
    {
        ReadOnlyMemory<byte>?[] rawRows = await table.Store.GetRowsBatchLockedForMutation(tx, chunk).ConfigureAwait(false);

        // Index writability is fixed for the statement, so filter once per chunk instead of per row;
        // the decode below is narrowed to the index key columns — the only values this path consumes
        // (the row bytes are deleted wholesale, never re-encoded). Values for those columns are
        // identical to a full decode; other columns are simply never materialized.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);
        HashSet<string> requiredColumns = CollectIndexKeyColumns(writableIndexes);
        RowEncoder.DictionaryDecodeState decodeState = new();

        List<KvTableStore.RowDelete> batch = new(chunk.Count);
        for (int i = 0; i < chunk.Count; i++)
        {
            ObjectIdValue rowId = chunk[i];
            ReadOnlyMemory<byte>? data = rawRows[i];
            if (data is null || data.Value.Length == 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Row '{rowId}' disappeared before delete");

            Dictionary<string, ColumnValue> writableRow = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, data.Value,
                requiredColumns: requiredColumns,
                visibilitySchemaVersion: table.Schema.Version,
                decodeState: decodeState).ConfigureAwait(false);

            batch.Add(new KvTableStore.RowDelete
            {
                RowId = rowId,
                IndexEntries = CollectIndexDeletes(writableIndexes, rowId, writableRow),
            });
        }

        if (_stats is not null && batch.Count > _stats.DeleteBatchMaxChunkSeen)
            _stats.DeleteBatchMaxChunkSeen = batch.Count;

        await table.Store.DeleteRowsBatch(tx, batch).ConfigureAwait(false);
        state.DeletedRows += batch.Count;

        foreach (KvTableStore.RowDelete row in batch)
            Log.LogRowDeleted(logger, row.RowId);
    }

    /// <summary>
    /// Collects the secondary-index entries to delete for a row, or <see langword="null"/> when the
    /// table has no writable index entry applicable to it. The list is built only once the first entry
    /// exists so a no-index (or all-NULL-key) delete allocates no per-row collection.
    /// </summary>
    private static IReadOnlyList<KvTableStore.IndexDelete>? CollectIndexDeletes(
        IReadOnlyList<TableIndexSchema> writableIndexes,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> row
    )
    {
        List<KvTableStore.IndexDelete>? entries = null;

        for (int idx = 0; idx < writableIndexes.Count; idx++)
        {
            TableIndexSchema index = writableIndexes[idx];

            if (index.Type == IndexType.Unique)
            {
                // NULLs are distinct: a row with a NULL (or absent) value in any indexed column never
                // had a unique index entry, so there is nothing to delete.
                if (HasNullKeyColumn(row, index.Columns))
                    continue;

                CompositeColumnValue key = GetColumnValue(row, index.Columns);
                (entries ??= new()).Add(new KvTableStore.IndexDelete(index.KvId, key, rowId, Unique: true));
            }
            else if (index.Type == IndexType.Multi)
            {
                CompositeColumnValue key = GetColumnValue(row, index.Columns, new ColumnValue(ColumnType.Id, rowId.ToString()));
                (entries ??= new()).Add(new KvTableStore.IndexDelete(index.KvId, key, rowId, Unique: false));
            }
        }

        return entries;
    }

    private async Task<int> DeleteInternal(FluxMachine<DeleteFluxSteps, DeleteFluxState> machine, DeleteFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(DeleteFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteFluxSteps.DeleteRowsAndIndexesFromDisk, DeleteRowsAndIndexesFromDisk);

        try
        {
            while (!machine.IsAborted)
                await machine.RunStep(machine.NextStep()).ConfigureAwait(false);
        }
        finally
        {
            await state.RowsToDelete.DisposeAsync().ConfigureAwait(false);
        }

        TimeSpan timeTaken = timer.GetElapsedTime();

        Log.LogRowsDeleted(logger, state.DeletedRows, timeTaken);

        return state.DeletedRows;
    }
}
