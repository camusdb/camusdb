
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Linq;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.Diagnostics;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Updates multiple rows by the specified filters
/// </summary>
public sealed class RowUpdater
{
    private readonly ILogger<ICamusDB> logger;

    public RowUpdater(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void ValidateIfColumnExists(List<TableColumnSchema> columns, Dictionary<string, TableIndexSchema> indexes, string columnName)
    {
        bool hasColumn = false;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (string.IsNullOrEmpty(columnName))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid or empty column name in values list");

            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsWritable(column))
            {
                hasColumn = true;
                break;
            }
        }

        if (!hasColumn)
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Unknown column '{columnName}' in column list"
            );

        if (indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? indexSchema))
        {
            if (indexSchema.Columns.Contains(columnName, StringComparer.OrdinalIgnoreCase))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Cannot update primary key field");
        }
    }

    private static void ValidatePlainValues(List<TableColumnSchema> columns, Dictionary<string, TableIndexSchema> indexes, Dictionary<string, ColumnValue> plainValues)
    {
        if (plainValues.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        foreach (KeyValuePair<string, ColumnValue> columnValue in plainValues)
            ValidateIfColumnExists(columns, indexes, columnValue.Key);

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!SchemaElementStateRules.IsWritable(columnSchema))
                continue;

            if (!columnSchema.NotNull)
                continue;

            if (!plainValues.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                continue;

            if (columnValue.Type == ColumnType.Null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.NotNullViolation,
                    $"Column '{columnSchema.Name}' cannot be null"
                );
            }
        }
    }

    private static void ValidateExprValues(List<TableColumnSchema> columns, Dictionary<string, TableIndexSchema> indexes, Dictionary<string, NodeAst> exprValues)
    {
        if (exprValues.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        foreach (KeyValuePair<string, NodeAst> columnValue in exprValues)
            ValidateIfColumnExists(columns, indexes, columnValue.Key);

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!SchemaElementStateRules.IsWritable(columnSchema))
                continue;

            if (!columnSchema.NotNull)
                continue;

            if (!exprValues.TryGetValue(columnSchema.Name, out NodeAst? columnValue))
                continue;

            if (columnValue.nodeType == NodeType.Null)
                throw new CamusDBException(CamusDBErrorCodes.NotNullViolation, $"Column '{columnSchema.Name}' cannot be null");
        }
    }

    private static void Validate(TableDescriptor table, UpdateTicket ticket)
    {
        if (ticket.PlainValues is not null && ticket.ExprValues is not null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Cannot specify both plan and sql expr values at the same time");

        List<TableColumnSchema> columns = table.Schema.Columns!;
        Dictionary<string, TableIndexSchema> indexes = table.Indexes;

        if (ticket.PlainValues is not null)
            ValidatePlainValues(columns, indexes, ticket.PlainValues);

        if (ticket.ExprValues is not null)
            ValidateExprValues(columns, indexes, ticket.ExprValues);
    }

    internal async Task<int> Update(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, UpdateTicket ticket)
    {
        MaterializedViewAccessGuard.RequireWritable(table);
        Validate(table, ticket);

        UpdateFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor
        );

        FluxMachine<UpdateFluxSteps, UpdateFluxState> machine = new(state);

        return await UpdateInternal(machine, state).ConfigureAwait(false);
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
    /// Walks a unique index's key columns once and reports, for the old and the new image of a row,
    /// whether the row qualifies for an entry (no absent and no NULL indexed column — NULLs are
    /// distinct, so such a row carries no unique entry) and whether the key values are identical.
    /// <para>
    /// This reads the indexed cells directly, so an update that leaves the index untouched — the
    /// common case — never builds a <see cref="CompositeColumnValue"/> for either image.
    /// </para>
    /// <para>
    /// The qualification pass completes before any value is compared, and the comparison runs only
    /// when both images qualify — the same order the two composite keys imposed when they were built
    /// first and compared afterwards. It walks the columns in order and stops at the first difference,
    /// which is what <see cref="CompositeColumnValue.CompareTo"/> does over the same values, so a
    /// later pair that would fail to compare is reached in exactly the same cases as before.
    /// <paramref name="keyUnchanged"/> is only meaningful when both images qualify.
    /// </para>
    /// </summary>
    private static void CompareUniqueKeyColumns(
        Dictionary<string, ColumnValue> oldRow,
        Dictionary<string, ColumnValue> newRow,
        string[] columnNames,
        out bool oldHasKey,
        out bool newHasKey,
        out bool keyUnchanged)
    {
        bool oldQualifies = true;
        bool newQualifies = true;

        for (int i = 0; i < columnNames.Length; i++)
        {
            string name = columnNames[i];

            if (!oldRow.TryGetValue(name, out ColumnValue? oldValue) || oldValue.Type == ColumnType.Null)
                oldQualifies = false;

            if (!newRow.TryGetValue(name, out ColumnValue? newValue) || newValue.Type == ColumnType.Null)
                newQualifies = false;
        }

        oldHasKey = oldQualifies;
        newHasKey = newQualifies;
        keyUnchanged = true;

        if (!oldQualifies || !newQualifies)
            return;

        // Both images qualify, so every indexed column is present and non-NULL in both.
        for (int i = 0; i < columnNames.Length; i++)
        {
            if (oldRow[columnNames[i]].CompareTo(newRow[columnNames[i]]) != 0)
            {
                keyUnchanged = false;
                return;
            }
        }
    }

    /// <summary>
    /// Walks a non-unique index's key columns once and reports whether the old and the new image of a
    /// row hold identical key values. The row id tie-breaker appended to a non-unique key is the same
    /// value on both sides, so it can never change the outcome and is not compared here.
    /// <para>
    /// A key column absent from either row is an internal error, as it is when the composite key is
    /// built. Both images are checked in full — the old one first, then the new one — before any value
    /// is compared, so the failure names the same column, in the same order, that building the two
    /// keys up front reported.
    /// </para>
    /// </summary>
    private static bool MultiKeyColumnsUnchanged(
        Dictionary<string, ColumnValue> oldRow,
        Dictionary<string, ColumnValue> newRow,
        string[] columnNames)
    {
        RequireKeyColumns(oldRow, columnNames);
        RequireKeyColumns(newRow, columnNames);

        for (int i = 0; i < columnNames.Length; i++)
        {
            if (oldRow[columnNames[i]].CompareTo(newRow[columnNames[i]]) != 0)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Throws when any of the index's key columns is absent from the row, with the same error and the
    /// same message the composite-key builder raises for the first missing column.
    /// </summary>
    private static void RequireKeyColumns(Dictionary<string, ColumnValue> rowValues, string[] columnNames)
    {
        for (int i = 0; i < columnNames.Length; i++)
        {
            if (!rowValues.ContainsKey(columnNames[i]))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field '" + columnNames[i] + "'"
                );
        }
    }

    /// <summary>
    /// Materializes the stored/payload (INCLUDE) tuple for a covering index, or <see langword="null"/>
    /// for a plain index. Called lazily, only from a branch that emits an index entry, so an update
    /// whose entry is skipped (key and included values unchanged) never serializes or byte-checks a
    /// payload it would discard.
    /// </summary>
    private static byte[]? BuildIncludeTuple(TableIndexSchema index, Dictionary<string, ColumnValue> newRow, CamusDBOptions options)
        => index.HasIncludeColumns
            ? IndexIncludeValueCodec.EncodeTupleChecked(index.IncludeColumns, newRow, index.Name, options)
            : null;

    private async Task<FluxAction> LocateTuplesToUpdate(UpdateFluxState state)
    {
        UpdateTicket ticket = state.Ticket;

        // Restrict scan-time decode to columns needed by the WHERE filter and any
        // expression-based SET values (e.g. SET col = old_col + 1). Rejected candidates
        // are only partially decoded. The write phase batch-loads full rows from raw bytes
        // and decodes them. Returns null when the WHERE contains subquery nodes — fall back
        // to full decode.
        IReadOnlySet<string>? locateColumns = RequiredColumnAnalyzer.ComputeForLocate(
            ticket.Where, ticket.Filters, ticket.ExprValues);

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

        SpillableRowList rowList = new(QueryExecutionContext.For(state.Database, queryTicket));
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            await rowList.AddAsync(row).ConfigureAwait(false);
        await rowList.SealAsync().ConfigureAwait(false);
        state.RowsToUpdate = rowList;

        return FluxAction.Continue;
    }

    private static void CheckForNotNulls(TableDescriptor table, Dictionary<string, ColumnValue> rowValues)
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!SchemaElementStateRules.IsWritable(columnSchema))
                continue;

            if (!columnSchema.NotNull)
                continue;

            if (!rowValues.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                continue;

            if (columnValue.Type == ColumnType.Null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.NotNullViolation,
                    $"Column '{columnSchema.Name}' cannot be null"
                );
            }
        }

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!SchemaElementStateRules.IsWritable(columnSchema))
                continue;

            if (columnSchema.Type != ColumnType.String && columnSchema.Type != ColumnType.Bytes)
                continue;

            if (!rowValues.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                continue;

            if (columnValue.Type == ColumnType.Null)
                continue;

            EnforceLengthBound(columnSchema, columnValue);
        }
    }

    private static void CoerceRowValues(TableDescriptor table, Dictionary<string, ColumnValue> rowValues)
    {
        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (!rowValues.TryGetValue(column.Name, out ColumnValue? val))
                continue;

            ColumnValue coerced = CastScalarFunctions.CoerceToColumnType(val, column);
            if (!ReferenceEquals(coerced, val))
                rowValues[column.Name] = coerced;
        }
    }

    private static void EnforceLengthBound(TableColumnSchema column, ColumnValue value)
    {
        if (column.Type == ColumnType.String)
        {
            string s = value.StrValue ?? "";
            int max = column.MaxLength ?? CamusDBConstants.DefaultStringMaxLength;
            if (s.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {s.Length})");
        }
        else if (column.Type == ColumnType.Bytes)
        {
            byte[] b = value.BytesValue ?? [];
            int max = column.MaxLength ?? CamusDBConstants.DefaultBytesMaxLength;
            if (b.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {b.Length})");
        }
    }

    private static Dictionary<string, ColumnValue> GetNewUpdatedRow(
        Dictionary<string, ColumnValue> currentRow,
        QueryResultRow queryRow,
        UpdateTicket ticket
    )
    {
        // Keyed case-insensitively so a SET clause written in a different case than the schema
        // (SET UserName = ... for a column stored as "username") overwrites the existing slot
        // rather than adding a second, case-variant key that the re-encode would ignore.
        // The copy constructor takes the BCL's bulk-copy path when the source dictionary uses the
        // same comparer (which the decode paths guarantee), skipping a per-column rehash per row.
        Dictionary<string, ColumnValue> rowValues = new(currentRow, StringComparer.OrdinalIgnoreCase);

        if (ticket.PlainValues is not null)
        {
            foreach (KeyValuePair<string, ColumnValue> keyValuePair in ticket.PlainValues)
                rowValues[keyValuePair.Key] = keyValuePair.Value;

            return rowValues;
        }

        if (ticket.ExprValues is not null)
        {
            foreach (KeyValuePair<string, NodeAst> keyValuePair in ticket.ExprValues)
                rowValues[keyValuePair.Key] = SqlExecutor.EvalExpr(keyValuePair.Value, queryRow.Row, ticket.Parameters);

            return rowValues;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid values in update ticket");
    }

    private async Task<FluxAction> UpdateRowsAndIndexes(UpdateFluxState state)
    {
        if (state.RowsToUpdate is null)
        {
            logger.LogWarning("Invalid rows to update");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;
        KvTransaction tx = ticket.TxnState;

        // Drain the matched-row buffer in bounded chunks so the heap retains at most chunkSize
        // writable rows and their index mutation sets simultaneously. Mirrors the delete path.
        int chunkSize = state.Database.Options.SpillEffectiveThreshold;
        List<QueryResultRow> chunkRows = new(Math.Min(chunkSize, 64));

        await foreach (QueryResultRow queryRow in state.RowsToUpdate.EnumerateAsync().ConfigureAwait(false))
        {
            chunkRows.Add(queryRow);

            if (chunkRows.Count >= chunkSize)
            {
                await FlushUpdateChunk(table, tx, ticket, chunkRows, state).ConfigureAwait(false);
                chunkRows.Clear();
            }
        }

        if (chunkRows.Count > 0)
            await FlushUpdateChunk(table, tx, ticket, chunkRows, state).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    private async Task FlushUpdateChunk(
        TableDescriptor table,
        KvTransaction tx,
        UpdateTicket ticket,
        List<QueryResultRow> chunkRows,
        UpdateFluxState state)
    {
        // Batch-load raw row bytes for the whole chunk in one Kahuna round-trip. Uses the
        // lock-acquiring batch read so a Serializable+RW update holds the same shared point locks on the
        // read rows that a per-row GetRow would — without them, an index-scan-located update could read a
        // row lock-free and miss a concurrent commit, deleting a stale index entry.
        List<ObjectIdValue> rowIds = new(chunkRows.Count);
        for (int i = 0; i < chunkRows.Count; i++)
            rowIds.Add(chunkRows[i].RowId);
        ReadOnlyMemory<byte>?[] rawRows = await table.Store.GetRowsBatchLockedForMutation(tx, rowIds).ConfigureAwait(false);

        // The rewritten row is stored under the current schema version, so its positional layout is
        // fixed for the whole chunk. Compiled once here rather than per row.
        CompiledRowCodec codec = await table.GetRowCodecAsync(tx.TransactionId, table.Schema.Version).ConfigureAwait(false);
        List<TableColumnSchema> schemaColumns = table.Schema.Columns!;

        // Index writability is fixed for the statement (the transaction pins the schema version),
        // so filter once per chunk instead of re-evaluating per index per row.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);

        List<KvTableStore.RowUpdate> batch = new(chunkRows.Count);

        // Per-chunk decode-plan cache: every row at the same stored schema version shares one resolved
        // plan instead of re-running schema-history lookups and visibility resolution per row.
        RowEncoder.DictionaryDecodeState decodeState = new();

        for (int i = 0; i < chunkRows.Count; i++)
        {
            QueryResultRow queryRow = chunkRows[i];
            ObjectIdValue rowId = queryRow.RowId;
            ReadOnlyMemory<byte>? rawData = rawRows[i];

            if (rawData is null || rawData.Value.Length == 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Row '{rowId}' disappeared before update");

            Dictionary<string, ColumnValue> oldRow = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, rawData.Value,
                visibilitySchemaVersion: table.Schema.Version,
                decodeState: decodeState).ConfigureAwait(false);

            Dictionary<string, ColumnValue> newRow = GetNewUpdatedRow(oldRow, queryRow, ticket);

            CoerceRowValues(table, newRow);
            CheckForNotNulls(table, newRow);
            CheckEnforcer.EnforceOnRow(table, newRow);

            byte[] newData = codec.EncodeStorageValue(RowSlotAdapter.FromRow(schemaColumns, newRow));

            (IReadOnlyList<KvTableStore.IndexDelete>? oldIndexEntries,
             IReadOnlyList<KvTableStore.IndexWrite>? newIndexEntries) =
                CollectIndexUpdates(writableIndexes, rowId, oldRow, newRow, state.Database.Options);

            batch.Add(new KvTableStore.RowUpdate
            {
                RowId = rowId,
                NewRowData = newData,
                OldIndexEntries = oldIndexEntries,
                NewIndexEntries = newIndexEntries,
            });
        }

        await table.Store.UpdateRowsBatch(tx, batch).ConfigureAwait(false);

        state.ModifiedRows += batch.Count;

        foreach (KvTableStore.RowUpdate row in batch)
            Log.LogRowUpdated(logger, row.RowId);
    }

    /// <summary>
    /// Collects the old (to remove) and new (to put) secondary-index entries for a row, including only
    /// entries whose indexed columns actually changed. Either list is <see langword="null"/> when it has
    /// no entries, and each is built only once its first entry exists — so an unchanged-index update (the
    /// common case) allocates no per-row index lists.
    /// </summary>
    private static (IReadOnlyList<KvTableStore.IndexDelete>? Old, IReadOnlyList<KvTableStore.IndexWrite>? New)
        CollectIndexUpdates(
            IReadOnlyList<TableIndexSchema> writableIndexes,
            ObjectIdValue rowId,
            Dictionary<string, ColumnValue> oldRow,
            Dictionary<string, ColumnValue> newRow,
            CamusDBOptions options)
    {
        List<KvTableStore.IndexDelete>? oldEntries = null;
        List<KvTableStore.IndexWrite>? newEntries = null;

        // The row id tie-breaker appended to every non-unique key is the same value for every index of
        // this row, and ColumnValue is immutable, so one instance is built on first use and shared.
        ColumnValue? rowIdValue = null;

        for (int idx = 0; idx < writableIndexes.Count; idx++)
        {
            TableIndexSchema index = writableIndexes[idx];

            // Stored/payload (INCLUDE) columns live in the entry value, not the key. So an update that
            // leaves the key identical but changes an included value must still rewrite the entry value
            // (in place, on the same key) or a covering read would return stale payload. Computing
            // `includeChanged` only compares values (no serialization); the tuple is serialized lazily,
            // in the branches that actually emit an entry, so an update of an unrelated column pays
            // nothing for a covering index whose entry is skipped.
            bool includeChanged = index.HasIncludeColumns && IncludeColumnsChanged(index, oldRow, newRow);

            if (index.Type == IndexType.Unique)
            {
                // NULLs are distinct: a row with a NULL (or absent) value in any indexed column has no
                // unique index entry. So only delete the old entry if the old row had one, and only put
                // the new entry if the new row qualifies (value->NULL removes it, NULL->value adds it).
                // NULL qualification and key equality come from one pass over the indexed columns, so an
                // update that leaves this index alone builds no composite key at all.
                CompareUniqueKeyColumns(oldRow, newRow, index.Columns, out bool oldHasKey, out bool newHasKey, out bool keyUnchanged);

                // When the indexed columns are unchanged, skip the delete+insert cycle. On a root this
                // avoids a needless round-trip; on a branch it is required for correctness: the delete
                // writes a tombstone and a subsequent SetIfNotExists on the same key would hit that
                // tombstone and incorrectly throw DuplicateUniqueKeyValue (tombstone-replace for
                // in-place unique updates is not yet supported). The inherited index entry already
                // points to the correct rowId, and the row fetch returns the branch-local value via
                // the branch-aware read path.
                if (oldHasKey && newHasKey && keyUnchanged)
                {
                    // Key identical. Only work to do is refreshing the covered payload: overwrite the
                    // existing entry value in place (same key, this row's rowId) — no delete, and Set
                    // rather than SetIfNotExists (which would no-op on the already-present key).
                    if (includeChanged)
                        (newEntries ??= new()).Add(new KvTableStore.IndexWrite(index.KvId, GetColumnValue(newRow, index.Columns), Unique: true, IncludeTuple: BuildIncludeTuple(index, newRow, options), Overwrite: true));
                    continue;
                }

                if (oldHasKey)
                    (oldEntries ??= new()).Add(new KvTableStore.IndexDelete(index.KvId, GetColumnValue(oldRow, index.Columns), rowId, Unique: true));

                if (newHasKey)
                    (newEntries ??= new()).Add(new KvTableStore.IndexWrite(index.KvId, GetColumnValue(newRow, index.Columns), Unique: true, IncludeTuple: BuildIncludeTuple(index, newRow, options)));
            }
            else if (index.Type == IndexType.Multi)
            {
                // Skip unchanged entries — same correctness requirement as the unique index case above.
                // The comparison reads the indexed cells directly, so an untouched index builds neither
                // composite key and does not even need the row id tie-breaker value.
                if (MultiKeyColumnsUnchanged(oldRow, newRow, index.Columns))
                {
                    // Key (incl. rowId tie-breaker) identical: overwrite the entry value in place to
                    // refresh the covered payload; no delete needed.
                    if (includeChanged)
                    {
                        rowIdValue ??= new(ColumnType.Id, rowId.ToString());

                        (newEntries ??= new()).Add(new KvTableStore.IndexWrite(index.KvId, GetColumnValue(newRow, index.Columns, rowIdValue), Unique: false, IncludeTuple: BuildIncludeTuple(index, newRow, options), Overwrite: true));
                    }

                    continue;
                }

                rowIdValue ??= new(ColumnType.Id, rowId.ToString());

                (oldEntries ??= new()).Add(new KvTableStore.IndexDelete(index.KvId, GetColumnValue(oldRow, index.Columns, rowIdValue), rowId, Unique: false));
                (newEntries ??= new()).Add(new KvTableStore.IndexWrite(index.KvId, GetColumnValue(newRow, index.Columns, rowIdValue), Unique: false, IncludeTuple: BuildIncludeTuple(index, newRow, options)));
            }
        }

        return (oldEntries, newEntries);
    }

    /// <summary>
    /// True when any of the index's stored/payload (INCLUDE) column values differs between the old and
    /// new row (a missing/absent value compares as NULL). Drives the in-place value rewrite for an
    /// update that leaves the key unchanged but changes covered payload.
    /// </summary>
    private static bool IncludeColumnsChanged(TableIndexSchema index, Dictionary<string, ColumnValue> oldRow, Dictionary<string, ColumnValue> newRow)
    {
        foreach (string includeColumn in index.IncludeColumns)
        {
            ColumnValue oldValue = oldRow.TryGetValue(includeColumn, out ColumnValue? o) ? o : ColumnValue.Null;
            ColumnValue newValue = newRow.TryGetValue(includeColumn, out ColumnValue? n) ? n : ColumnValue.Null;

            if (oldValue.CompareTo(newValue) != 0)
                return true;
        }

        return false;
    }

    private async Task<int> UpdateInternal(FluxMachine<UpdateFluxSteps, UpdateFluxState> machine, UpdateFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(UpdateFluxSteps.LocateTupleToUpdate, LocateTuplesToUpdate);
        machine.When(UpdateFluxSteps.UpdateRowsAndIndexes, UpdateRowsAndIndexes);

        try
        {
            while (!machine.IsAborted)
                await machine.RunStep(machine.NextStep()).ConfigureAwait(false);
        }
        finally
        {
            if (state.RowsToUpdate is not null)
                await state.RowsToUpdate.DisposeAsync().ConfigureAwait(false);
        }

        TimeSpan timeTaken = timer.GetElapsedTime();

        Log.LogRowsUpdated(logger, state.ModifiedRows, timeTaken);

        return state.ModifiedRows;
    }
}
