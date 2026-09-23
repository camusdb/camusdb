
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
using CamusDB.Core.Statistics;
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

    private readonly StatisticsManager? _stats;

    public RowUpdater(ILogger<ICamusDB> logger, StatisticsManager? stats = null)
    {
        this.logger = logger;
        _stats = stats;
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

        return new(columnValues);
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

        // Restrict scan-time decode to the columns the WHERE filter needs. The SET expressions are
        // evaluated in the write phase, on the row it reads after it locks the row, so their columns
        // are not needed here. Returns null when the WHERE contains subquery nodes — fall back to
        // full decode.
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
            exclusivePredicateLocks: true,
            probe: ticket.Probe
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        // The buffer keeps row-id-only records. The write phase never reads the located row's values:
        // it locks each row, reads it again, re-checks the predicate and evaluates every SET expression
        // against that locked read (see FlushUpdateChunk). Retaining the scanned row would pin, per
        // match, the decoded values (and for a borrowed-backed row its full KV bytes) for the life of
        // the buffer, and would spill those values when the buffer overflows to disk. The scan still
        // runs to completion and the list still seals before the first mutation, so the full match set
        // is fixed up front (Halloween barrier).
        SpillableRowList rowList = new(QueryExecutionContext.For(state.Database, queryTicket));

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            await rowList.AddAsync(new(row.RowId, QueryResultRow.EmptyRow)).ConfigureAwait(false);

        await rowList.SealAsync().ConfigureAwait(false);

        state.RowsToUpdate = rowList;
        state.LocateTicket = queryTicket;

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

    /// <summary>
    /// Builds the new image of a row. Every SET expression is evaluated against
    /// <paramref name="currentRow"/>, the row the write phase read after it locked the row, and never
    /// against the row the locate scan returned. The locate read holds no lock at Read Committed, so a
    /// transaction that committed between the two reads would otherwise be overwritten by a value
    /// computed from the older row: <c>SET v = concat(v, 'x')</c> would drop the other append.
    /// Expressions read the old image only, so <c>SET a = b, b = a</c> swaps the two values.
    /// </summary>
    private static Dictionary<string, ColumnValue> GetNewUpdatedRow(
        Dictionary<string, ColumnValue> currentRow,
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
                rowValues[keyValuePair.Key] = SqlExecutor.EvalExpr(keyValuePair.Value, currentRow, ticket.Parameters);

            return rowValues;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid values in update ticket");
    }

    /// <summary>
    /// Test-only interleaving hook, awaited after the locate scan has chosen a chunk's rows and before the write phase
    /// locks and reads them again. Lets a test commit a competing update inside that window, which no external caller can
    /// time deterministically. Null (zero-cost) in production.
    /// </summary>
    internal Func<Task>? TestBeforeWriteHook;

    private async Task<FluxAction> UpdateRowsAndIndexes(UpdateFluxState state)
    {
        if (state.RowsToUpdate is null || state.LocateTicket is null)
        {
            logger.LogWarning("Invalid rows to update");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;
        KvTransaction tx = ticket.TxnState;

        // The predicate and every SET expression are evaluated against the locked read, so the columns
        // they reference must be decoded even on a row that carries its untouched large values.
        MutationRowRecheck recheck = MutationRowRecheck.Build(table.Schema, ticket.Where, ticket.Filters);
        IReadOnlySet<string>? evaluatedColumns = CollectEvaluatedColumns(table.Schema, ticket, recheck);

        // Drain the matched-row buffer in bounded chunks so the heap retains at most chunkSize
        // writable rows and their index mutation sets simultaneously. Mirrors the delete path.
        int chunkSize = state.Database.Options.SpillEffectiveThreshold;
        List<QueryResultRow> chunkRows = new(Math.Min(chunkSize, 64));

        await foreach (QueryResultRow queryRow in state.RowsToUpdate.EnumerateAsync().ConfigureAwait(false))
        {
            // Drain-time retention check: the buffer holds row-id-only records, so a drained record
            // with columns means the locate phase retained scanned values it must not. Reading Count
            // on a lazy row is a layout lookup — it materializes nothing.
            if (_stats is not null && queryRow.Row.Count > _stats.DmlLocateBufferMaxColumnsSeen)
                _stats.DmlLocateBufferMaxColumnsSeen = queryRow.Row.Count;

            chunkRows.Add(queryRow);

            if (chunkRows.Count >= chunkSize)
            {
                await FlushUpdateChunk(table, tx, ticket, chunkRows, state, recheck, evaluatedColumns).ConfigureAwait(false);
                chunkRows.Clear();
            }
        }

        if (chunkRows.Count > 0)
            await FlushUpdateChunk(table, tx, ticket, chunkRows, state, recheck, evaluatedColumns).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// The schema-cased columns that the write phase evaluates on the locked read: the re-checked
    /// predicate's columns and the columns every SET expression reads. Null means every column, which
    /// is the answer whenever one of them cannot be named exactly (a subquery left in a SET expression,
    /// a name that is not a column of the current schema).
    /// </summary>
    private static IReadOnlySet<string>? CollectEvaluatedColumns(TableSchema schema, UpdateTicket ticket, MutationRowRecheck recheck)
    {
        if (recheck.Columns is null)
            return null;

        HashSet<string> referenced = new(recheck.Columns, StringComparer.OrdinalIgnoreCase);

        if (ticket.ExprValues is not null)
        {
            foreach (KeyValuePair<string, NodeAst> entry in ticket.ExprValues)
            {
                if (RequiredColumnAnalyzer.ContainsSubqueryNode(entry.Value))
                    return null;

                QueryExpressionWalker.CollectColumnReferences(entry.Value, referenced);
            }
        }

        return MutationRowRecheck.ToSchemaColumns(schema, referenced);
    }

    /// <summary>
    /// Updates one chunk of located rows. The order is the fix for a lost update at Read Committed, and
    /// it must be kept: <b>lock, then read, then re-check, then compute, then write</b>.
    ///
    /// <para>The locate scan reads without a lock. If the write phase computed <c>SET v = f(v)</c> from
    /// a row it read before it held the row's exclusive lock, a transaction that committed in between
    /// would be overwritten by a value computed from the older row, and both transactions would
    /// commit. <see cref="KvTableStore.LockAndReadRowsForMutationAsync"/> takes the exclusive row locks
    /// before the read, so the read returns the latest committed row and no other writer can change it
    /// until this transaction ends. The predicate is then evaluated again on that row
    /// (<see cref="MutationRowRecheck"/>): a row that a concurrent commit moved out of the WHERE, or
    /// deleted, is skipped and not counted. The old index entries also come from that read, so a
    /// concurrent change of an indexed column cannot leave a stale entry behind.</para>
    /// </summary>
    private async Task FlushUpdateChunk(
        TableDescriptor table,
        KvTransaction tx,
        UpdateTicket ticket,
        List<QueryResultRow> chunkRows,
        UpdateFluxState state,
        MutationRowRecheck recheck,
        IReadOnlySet<string>? evaluatedColumns)
    {
        List<ObjectIdValue> rowIds = new(chunkRows.Count);

        for (int i = 0; i < chunkRows.Count; i++)
            rowIds.Add(chunkRows[i].RowId);

        // Test-only interleaving point; see TestBeforeWriteHook. Read once so a concurrent clear cannot fault it.
        if (TestBeforeWriteHook is { } beforeWriteHook)
            await beforeWriteHook().ConfigureAwait(false);

        // Lock the chunk's row keys, then batch-load their bytes in one Kahuna round trip. The bytes are
        // read raw: which large values to fetch is decided per row below.
        ReadOnlyMemory<byte>?[] rawRows = await table.Store.LockAndReadRowsForMutationAsync(tx, rowIds, default, LargeValueFetch.Raw).ConfigureAwait(false);

        // The rewritten row is stored under the current schema version, so its positional layout is
        // fixed for the whole chunk. Compiled once here rather than per row.
        CompiledRowCodec codec = await table.GetRowCodecAsync(tx.TransactionId, table.Schema.Version).ConfigureAwait(false);
        List<TableColumnSchema> schemaColumns = table.Schema.Columns!;
        LargeValuePolicy largeValuePolicy = LargeValuePolicy.For(schemaColumns, state.Database.Options);

        // Index writability is fixed for the statement (the transaction pins the schema version),
        // so filter once per chunk instead of re-evaluating per index per row.
        List<TableIndexSchema> writableIndexes = SchemaElementStateRules.CollectWritableIndexes(table.Schema, table.Indexes);

        UpdateCarryPlan carry = UpdateCarryPlan.Build(table, ticket, writableIndexes, evaluatedColumns);
        
        (ReadOnlyMemory<byte>?[] oldRows, ReadOnlyMemory<byte>?[] storedRows, List<int>?[] oldOutOfLine, bool[] carried) =
            await ResolveRowsForUpdateAsync(table, tx, rowIds, rawRows, carry).ConfigureAwait(false);

        List<KvTableStore.RowUpdate> batch = new(chunkRows.Count);

        // Per-chunk decode-plan caches: every row at the same stored schema version shares one resolved
        // plan instead of re-running schema-history lookups and visibility resolution per row. A row that
        // carries its untouched large values decodes a narrower column set, so it has its own cache.
        RowEncoder.DictionaryDecodeState decodeState = new();
        RowEncoder.DictionaryDecodeState carryDecodeState = new();

        for (int i = 0; i < chunkRows.Count; i++)
        {
            ObjectIdValue rowId = chunkRows[i].RowId;
            ReadOnlyMemory<byte>? rawData = oldRows[i];

            // A concurrent transaction deleted the row and committed after the locate scan chose it.
            // There is nothing left to update, which is what the statement would have seen had it
            // started later.
            if (rawData is null || rawData.Value.Length == 0)
                continue;

            Dictionary<string, ColumnValue> oldRow = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, rawData.Value,
                requiredColumns: carried[i] ? carry.DecodedColumns : null,
                visibilitySchemaVersion: table.Schema.Version,
                decodeState: carried[i] ? carryDecodeState : decodeState
            ).ConfigureAwait(false);

            // A concurrent commit changed the row after the locate scan chose it, and it no longer
            // matches the statement's predicate.
            if (!recheck.IsEmpty && !await recheck.MatchesAsync(state.QueryExecutor, state.Database, state.LocateTicket!, oldRow).ConfigureAwait(false))
                continue;

            Dictionary<string, ColumnValue> newRow = GetNewUpdatedRow(oldRow, ticket);

            CoerceRowValues(table, newRow);
            CheckForNotNulls(table, newRow);
            CheckEnforcer.EnforceOnRow(table, newRow);

            // A carried cell is copied from the row as stored, never from the resolved row: resolution
            // clears the marks of a cell the update decodes, and copying that plain cell would write the
            // large value inline again instead of keeping its pointer.
            EncodedRow encoded = carried[i]
                ? codec.EncodeStorageValue(RowSlotAdapter.FromRow(schemaColumns, newRow), largeValuePolicy, storedRows[i]!.Value.Span, carry.CarryMask)
                : codec.EncodeStorageValue(RowSlotAdapter.FromRow(schemaColumns, newRow), largeValuePolicy);

            (IReadOnlyList<KvTableStore.IndexDelete>? oldIndexEntries,
             IReadOnlyList<KvTableStore.IndexWrite>? newIndexEntries) =
                CollectIndexUpdates(writableIndexes, rowId, oldRow, newRow, state.Database.Options);

            batch.Add(new()
            {
                RowId = rowId,
                NewRowData = encoded.StorageValue,
                OldIndexEntries = oldIndexEntries,
                NewIndexEntries = newIndexEntries,
                LargeValues = encoded.OutOfLine,
                LargeValueDeletes = UnreferencedLargeValues(oldOutOfLine[i], encoded, carried[i] ? carry.CarriedVariableOrdinals : null),
            });
        }

        await table.Store.UpdateRowsBatch(tx, batch).ConfigureAwait(false);

        state.ModifiedRows += batch.Count;

        foreach (KvTableStore.RowUpdate row in batch)
            Log.LogRowUpdated(logger, row.RowId);
    }

    /// <summary>
    /// Which columns of the current schema an update carries from the old row, and which columns it
    /// decodes. Two separate questions:
    /// <list type="bullet">
    ///   <item><b>Carry</b> (copy the stored cell, marks included, and write no large value): every
    ///   <c>string</c>, <c>bytes</c> or array column the statement does not assign. Its value cannot
    ///   change, so its stored form stays valid.</item>
    ///   <item><b>Decode</b>: every column that is not carried, plus a carried column that a writable
    ///   index uses as a key or INCLUDE column, that a CHECK constraint reads, or that the re-checked
    ///   predicate or a SET expression reads. The update compares, re-validates or evaluates those
    ///   values, so they are resolved, but the stored cell is still carried.</item>
    /// </list>
    /// A column that must be decoded is not a reason to write its large value again: a CHECK on a large
    /// column would otherwise make every small update rewrite the large value.
    /// </summary>
    private sealed class UpdateCarryPlan
    {
        /// <summary>Per stored ordinal of the current schema version: true when the column is carried.</summary>
        public required bool[] CarryMask { get; init; }

        /// <summary>Variable ordinals of the carried columns, in the current layout.</summary>
        public required HashSet<int> CarriedVariableOrdinals { get; init; }

        /// <summary>
        /// The names decoded from a row that carries: every column that is not carried, and each carried
        /// column that an index or a CHECK constraint reads.
        /// </summary>
        public required IReadOnlySet<string> DecodedColumns { get; init; }

        public bool IsEmpty => CarriedVariableOrdinals.Count == 0;

        /// <param name="evaluatedColumns">
        /// The columns the write phase evaluates on the locked read (predicate re-check and SET
        /// expressions), in schema case; null means every column.
        /// </param>
        public static UpdateCarryPlan Build(TableDescriptor table, UpdateTicket ticket, List<TableIndexSchema> writableIndexes, IReadOnlySet<string>? evaluatedColumns)
        {
            HashSet<string> assigned = new(StringComparer.OrdinalIgnoreCase);

            if (ticket.PlainValues is not null)
                assigned.UnionWith(ticket.PlainValues.Keys);
            if (ticket.ExprValues is not null)
                assigned.UnionWith(ticket.ExprValues.Keys);

            HashSet<string> validated = new(StringComparer.OrdinalIgnoreCase);

            foreach (TableIndexSchema index in writableIndexes)
            {
                validated.UnionWith(index.Columns);
                validated.UnionWith(index.IncludeColumns);
            }

            if (table.Schema.CheckConstraints is { } checks)
            {
                foreach (CheckConstraintSchema check in checks)
                    validated.UnionWith(check.ReferencedColumns);
            }

            List<TableColumnSchema> columns = table.Schema.Columns!;

            if (evaluatedColumns is null)
            {
                foreach (TableColumnSchema column in columns)
                    validated.Add(column.Name);
            }
            else
            {
                validated.UnionWith(evaluatedColumns);
            }

            bool[] mask = new bool[columns.Count];
            HashSet<int> carriedOrdinals = [];
            HashSet<string> decoded = new(StringComparer.Ordinal);
            int variableOrdinal = 0;

            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];
                bool variable = TableColumnSchema.SupportsStorageStrategy(column.Type);

                if (variable && SchemaElementStateRules.IsWritable(column) && !assigned.Contains(column.Name))
                {
                    mask[i] = true;
                    carriedOrdinals.Add(variableOrdinal);

                    if (validated.Contains(column.Name))
                        decoded.Add(column.Name);
                }
                else
                {
                    decoded.Add(column.Name);
                }

                if (variable)
                    variableOrdinal++;
            }

            return new UpdateCarryPlan { CarryMask = mask, CarriedVariableOrdinals = carriedOrdinals, DecodedColumns = decoded };
        }
    }

    /// <summary>
    /// Decides, per row, whether the update carries untouched large values, and resolves exactly the
    /// cells the update then reads.
    ///
    /// <para>A row carries only when it has marked cells and was written under the current schema
    /// version: a carried cell is copied verbatim into the new row, and its out-of-line key is derived
    /// from the cell's variable ordinal, which is stable only within one layout. Such a row resolves only
    /// the columns <see cref="UpdateCarryPlan.DecodedColumns"/> names, so an update of a small column
    /// never rewrites a large value, and fetches one only when an index or a CHECK constraint reads it.
    /// A marked row of an older layout resolves everything and is rewritten in the new form. An unmarked
    /// row needs no resolution.</para>
    ///
    /// <para>Returns the resolved rows for decoding and, aligned with them, the rows as stored. A carried
    /// cell must be copied from the stored row, because resolution replaces a decoded cell with its plain
    /// value. The out-of-line ordinals of each old row are read from its pointers before resolution, so
    /// the caller can name the old keys the new row no longer points at.</para>
    /// </summary>
    private static async Task<(ReadOnlyMemory<byte>?[] rows, ReadOnlyMemory<byte>?[] storedRows, List<int>?[] outOfLine, bool[] carried)> ResolveRowsForUpdateAsync(
        TableDescriptor table,
        KvTransaction tx,
        List<ObjectIdValue> rowIds,
        ReadOnlyMemory<byte>?[] rawRows,
        UpdateCarryPlan carry)
    {
        List<int>?[] outOfLine = new List<int>?[rawRows.Length];
        bool[] carried = new bool[rawRows.Length];
        ReadOnlyMemory<byte>?[] storedRows = (ReadOnlyMemory<byte>?[])rawRows.Clone();

        List<ObjectIdValue>? carryIds = null, fullIds = null;
        List<int>? carryPositions = null, fullPositions = null;

        for (int i = 0; i < rawRows.Length; i++)
        {
            if (rawRows[i] is not { } row || !RowStorageForms.HasTrailer(row.Span))
                continue;

            outOfLine[i] = RowStorageForms.OutOfLineOrdinals(row.Span);

            int storedVersion = RowStorageForms.StoredVersion(System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(row.Span));
            if (!carry.IsEmpty && storedVersion == table.Schema.Version)
            {
                carried[i] = true;
                (carryIds ??= []).Add(rowIds[i]);
                (carryPositions ??= []).Add(i);
            }
            else
            {
                (fullIds ??= []).Add(rowIds[i]);
                (fullPositions ??= []).Add(i);
            }
        }

        if (carryIds is not null)
            await ResolveSubsetAsync(table, tx, rawRows, carryIds, carryPositions!, LargeValueFetch.Columns(table.Schema, carry.DecodedColumns)).ConfigureAwait(false);

        if (fullIds is not null)
            await ResolveSubsetAsync(table, tx, rawRows, fullIds, fullPositions!, null).ConfigureAwait(false);

        return (rawRows, storedRows, outOfLine, carried);
    }

    private static async Task ResolveSubsetAsync(
        TableDescriptor table,
        KvTransaction tx,
        ReadOnlyMemory<byte>?[] rows,
        List<ObjectIdValue> ids,
        List<int> positions,
        LargeValueFetch? fetch)
    {
        ReadOnlyMemory<byte>?[] subset = new ReadOnlyMemory<byte>?[positions.Count];
        for (int i = 0; i < positions.Count; i++)
            subset[i] = rows[positions[i]];

        await table.Store.ResolveLargeValuesAsync(tx, ids, subset, fetch).ConfigureAwait(false);

        for (int i = 0; i < positions.Count; i++)
            rows[positions[i]] = subset[i];
    }

    /// <summary>
    /// The old out-of-line ordinals the new row no longer points at: every old pointer ordinal, minus the
    /// ordinals the new row carries (their key is left untouched) and minus the ordinals the new row
    /// writes again (their key is overwritten in place). Null when nothing is left to delete.
    /// </summary>
    private static List<int>? UnreferencedLargeValues(List<int>? oldOrdinals, EncodedRow encoded, HashSet<int>? carriedOrdinals)
    {
        if (oldOrdinals is null)
            return null;

        List<int>? deletes = null;
        foreach (int ordinal in oldOrdinals)
        {
            if (carriedOrdinals is not null && carriedOrdinals.Contains(ordinal))
                continue;

            bool rewritten = false;
            if (encoded.OutOfLine is { } writes)
            {
                for (int w = 0; w < writes.Count; w++)
                {
                    if (writes[w].VariableOrdinal == ordinal)
                    {
                        rewritten = true;
                        break;
                    }
                }
            }

            if (!rewritten)
                (deletes ??= []).Add(ordinal);
        }

        return deletes;
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
