
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
    /// because <see cref="KvTableStore.LockAndReadRowsForMutationAsync"/> locks the row before it reads
    /// it, and that lock is what makes the answer trustworthy: no transaction can extend the expiry
    /// between the re-check and the delete.</para>
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

        // Index writability is fixed for the statement, so filter once here instead of per row; the
        // decode below is narrowed to the columns this method actually consumes — the index key
        // columns (for CollectIndexDeletes) plus the expiry column (for the re-check). Values for
        // those columns are identical to a full decode; other columns are simply never materialized.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);
        HashSet<string> requiredColumns = CollectIndexKeyColumns(writableIndexes);
        requiredColumns.Add(expirationColumn);
        RowEncoder.DictionaryDecodeState decodeState = new();

        (ReadOnlyMemory<byte>?[] rawRows, List<int>?[] outOfLine) = await ReadRowsForDeleteAsync(table, tx, rowIds, requiredColumns, cancellationToken).ConfigureAwait(false);

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
                decodeState: decodeState
            ).ConfigureAwait(false);

            if (!TtlExpiryPredicate.IsExpired(writableRow, expirationColumn, cutoffEpochMs))
            {
                skipped++;
                continue;
            }

            batch.Add(new()
            {
                RowId = rowId,
                IndexEntries = CollectIndexDeletes(writableIndexes, rowId, writableRow),
                LargeValueOrdinals = outOfLine[i],
            });
        }

        if (batch.Count == 0)
            return (0, skipped);

        // The same batched primitive the user-facing delete uses, so the row and every one of its index
        // entries go in one transaction — an expired row that lost its row but kept an index entry would
        // make index-only scans return rows that no longer exist.
        await table.Store.DeleteRowsBatch(tx, batch, cancellationToken).ConfigureAwait(false);

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
            ticket.Where, 
            ticket.Filters, 
            exprValues: null
        );

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
            exclusivePredicateLocks: true,
            probe: ticket.Probe
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        // Buffer row-id-only records: the mutation phase reads RowId alone and re-reads every row
        // under its lock (FlushDeleteChunk), so the scanned values are dead weight after the
        // predicate has accepted the row. Retaining the scanned row would pin, per match, the
        // decoded values — and for a borrowed-backed row its full KV bytes — for the life of the
        // buffer, and would spill those values byte-for-byte when the buffer overflows to disk.
        // The scan still runs to completion and the list still seals before the first mutation,
        // so the full match set is fixed up front (Halloween barrier) exactly as before.
        SpillableRowList rowList = new(QueryExecutionContext.For(state.Database, queryTicket));
        
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            await rowList.AddAsync(new(row.RowId, QueryResultRow.EmptyRow)).ConfigureAwait(false);
        
        await rowList.SealAsync().ConfigureAwait(false);
        
        state.RowsToDelete = rowList;
        state.LocateTicket = queryTicket;

        return FluxAction.Continue;
    }

    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.RowsToDelete.Count == 0 || state.LocateTicket is null)
        {
            logger.LogError("Invalid rows to delete");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        KvTransaction tx = state.Ticket.TxnState;

        // Re-evaluated on each row read under the write lock; see FlushDeleteChunk.
        MutationRowRecheck recheck = MutationRowRecheck.Build(table.Schema, state.Ticket.Where, state.Ticket.Filters);

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
            // Drain-time retention check: the buffer holds row-id-only records, so any drained
            // record with columns means the locate phase retained scanned values it must not.
            // Reading Count on a lazy row is a layout lookup — it materializes nothing.
            if (_stats is not null && row.Row.Count > _stats.DmlLocateBufferMaxColumnsSeen)
                _stats.DmlLocateBufferMaxColumnsSeen = row.Row.Count;

            chunk.Add(row.RowId);

            if (chunk.Count >= chunkSize)
            {
                await FlushDeleteChunk(table, tx, chunk, state, recheck).ConfigureAwait(false);
                chunk.Clear();
            }
        }

        if (chunk.Count > 0)
            await FlushDeleteChunk(table, tx, chunk, state, recheck).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Test-only interleaving hook, awaited after the locate scan has chosen a chunk's rows and before the write phase
    /// locks and reads them again. Lets a test commit a competing change inside that window, which no external caller
    /// can time deterministically. Null (zero-cost) in production.
    /// </summary>
    internal Func<Task>? TestBeforeWriteHook;

    /// <summary>
    /// Deletes one chunk of located rows: <b>lock, then read, then re-check, then delete</b>.
    ///
    /// <para><see cref="KvTableStore.LockAndReadRowsForMutationAsync"/> takes the exclusive row locks
    /// before it reads the rows, so the read returns the latest committed row and nothing can change it
    /// before this transaction ends. The index entries to delete come from that read: a row read without
    /// its lock could miss a concurrent commit that changed an indexed column, and the delete would then
    /// remove the stale entry and orphan the new one. The predicate is re-evaluated on the same read
    /// (<see cref="MutationRowRecheck"/>), so a row that a concurrent commit moved out of the WHERE is
    /// kept, and a row that a concurrent commit already deleted is skipped. Neither is counted.</para>
    /// </summary>
    private async Task FlushDeleteChunk(
        TableDescriptor table,
        KvTransaction tx,
        List<ObjectIdValue> chunk,
        DeleteFluxState state,
        MutationRowRecheck recheck)
    {
        // Index writability is fixed for the statement, so filter once per chunk instead of per row;
        // the decode below is narrowed to the index key columns and the re-checked predicate's
        // columns — the only values this path consumes (the row bytes are deleted wholesale, never
        // re-encoded). Values for those columns are identical to a full decode; other columns are
        // simply never materialized. A predicate column that cannot be named exactly decodes all.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);
        HashSet<string>? requiredColumns = null;
        if (recheck.Columns is not null)
        {
            requiredColumns = CollectIndexKeyColumns(writableIndexes);
            requiredColumns.UnionWith(recheck.Columns);
        }
        RowEncoder.DictionaryDecodeState decodeState = new();

        // Test-only interleaving point; see TestBeforeWriteHook. Read once so a concurrent clear cannot fault it.
        if (TestBeforeWriteHook is { } beforeWriteHook)
            await beforeWriteHook().ConfigureAwait(false);

        (ReadOnlyMemory<byte>?[] rawRows, List<int>?[] outOfLine) = await ReadRowsForDeleteAsync(table, tx, chunk, requiredColumns, default).ConfigureAwait(false);

        List<KvTableStore.RowDelete> batch = new(chunk.Count);
        
        for (int i = 0; i < chunk.Count; i++)
        {
            ObjectIdValue rowId = chunk[i];
            ReadOnlyMemory<byte>? data = rawRows[i];

            // A concurrent transaction deleted the row and committed after the locate scan chose it.
            if (data is null || data.Value.Length == 0)
                continue;

            Dictionary<string, ColumnValue> writableRow = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, data.Value,
                requiredColumns: requiredColumns,
                visibilitySchemaVersion: table.Schema.Version,
                decodeState: decodeState
           ).ConfigureAwait(false);

            // A concurrent commit changed the row after the locate scan chose it, and it no longer
            // matches the statement's predicate.
            if (!recheck.IsEmpty && !await recheck.MatchesAsync(state.QueryExecutor, state.Database, state.LocateTicket!, writableRow).ConfigureAwait(false))
                continue;

            batch.Add(new()
            {
                RowId = rowId,
                IndexEntries = CollectIndexDeletes(writableIndexes, rowId, writableRow),
                LargeValueOrdinals = outOfLine[i],
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
    /// Locks rows for deletion and then reads them (<see cref="KvTableStore.LockAndReadRowsForMutationAsync"/>),
    /// and returns, per row, the variable ordinals of its out-of-line values beside bytes in which only
    /// <paramref name="requiredColumns"/> are resolved (every column when it is null).
    ///
    /// <para>The ordinals come from the stored pointers, read before any value is resolved, so the
    /// delete names every key the row points at without fetching a single large value. Only a large
    /// value that is also an index key column — the one thing the delete must compare — is fetched.</para>
    /// </summary>
    private static async Task<(ReadOnlyMemory<byte>?[] rows, List<int>?[] outOfLine)> ReadRowsForDeleteAsync(
        TableDescriptor table,
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        IReadOnlySet<string>? requiredColumns,
        CancellationToken cancellationToken)
    {
        ReadOnlyMemory<byte>?[] rows = await table.Store.LockAndReadRowsForMutationAsync(tx, rowIds, cancellationToken, LargeValueFetch.Raw).ConfigureAwait(false);

        List<int>?[] outOfLine = new List<int>?[rows.Length];
        
        bool anyMarked = false;
        
        for (int i = 0; i < rows.Length; i++)
        {
            if (rows[i] is { } row && RowStorageForms.HasTrailer(row.Span))
            {
                outOfLine[i] = RowStorageForms.OutOfLineOrdinals(row.Span);
                anyMarked = true;
            }
        }

        if (anyMarked)
            await table.Store.ResolveLargeValuesAsync(tx, rowIds, rows, LargeValueFetch.Columns(table.Schema, requiredColumns), cancellationToken).ConfigureAwait(false);

        return (rows, outOfLine);
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
                (entries ??= []).Add(new(index.KvId, key, rowId, Unique: true));
            }
            else if (index.Type == IndexType.Multi)
            {
                CompositeColumnValue key = GetColumnValue(row, index.Columns, new(ColumnType.Id, rowId.ToString()));
                (entries ??= []).Add(new(index.KvId, key, rowId, Unique: false));
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
