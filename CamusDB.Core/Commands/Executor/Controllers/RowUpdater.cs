
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

            if (string.Equals(column.Name, columnName) && SchemaElementStateRules.IsWritable(column))
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

        if (indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? indexSchema))
        {
            if (indexSchema.Columns.Contains(columnName))
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
            exclusivePredicateLocks: true
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        SpillableRowList rowList = new();
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
            int max = column.MaxLength ?? CamusDBConfig.DefaultStringMaxLength;
            if (s.Length > max)
                throw new CamusDBException(
                    CamusDBErrorCodes.ValueTooLong,
                    $"value too long for column '{column.Name}' (max {max}, got {s.Length})");
        }
        else if (column.Type == ColumnType.Bytes)
        {
            byte[] b = value.BytesValue ?? [];
            int max = column.MaxLength ?? CamusDBConfig.DefaultBytesMaxLength;
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
        Dictionary<string, ColumnValue> rowValues = new(currentRow.Count);

        foreach (KeyValuePair<string, ColumnValue> keyValue in currentRow)
            rowValues[keyValue.Key] = keyValue.Value;

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
        int chunkSize = CamusDBConfig.SpillEffectiveThreshold;
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
        // Batch-load raw row bytes for the whole chunk in one Kahuna round-trip.
        List<ObjectIdValue> rowIds = chunkRows.Select(r => r.RowId).ToList();
        byte[]?[] rawRows = await table.Store.GetRowsBatch(tx, rowIds).ConfigureAwait(false);

        List<KvTableStore.RowUpdate> batch = new(chunkRows.Count);

        for (int i = 0; i < chunkRows.Count; i++)
        {
            QueryResultRow queryRow = chunkRows[i];
            ObjectIdValue rowId = queryRow.RowId;
            byte[]? rawData = rawRows[i];

            if (rawData is null || rawData.Length == 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Row '{rowId}' disappeared before update");

            Dictionary<string, ColumnValue> oldRow = await RowEncoder.DecodeWritableAsync(
                table.Schema, tx.TransactionId, rowId, rawData,
                visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);

            Dictionary<string, ColumnValue> newRow = GetNewUpdatedRow(oldRow, queryRow, ticket);

            CoerceRowValues(table, newRow);
            CheckForNotNulls(table, newRow);

            byte[] newData = RowEncoder.Encode(table.Schema, newRow, rowId);

            KvTableStore.RowUpdate rowUpdate = new() { RowId = rowId, NewRowData = newData };
            CollectIndexUpdates(table, rowId, oldRow, newRow, rowUpdate);
            batch.Add(rowUpdate);
        }

        await table.Store.UpdateRowsBatch(tx, batch).ConfigureAwait(false);

        state.ModifiedRows += batch.Count;

        foreach (KvTableStore.RowUpdate row in batch)
            Log.LogRowUpdated(logger, row.RowId);
    }

    private static void CollectIndexUpdates(
        TableDescriptor table,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> oldRow,
        Dictionary<string, ColumnValue> newRow,
        KvTableStore.RowUpdate rowUpdate)
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (!SchemaElementStateRules.IsWritableIndex(table.Schema, index))
                continue;

            if (index.Type == IndexType.Unique)
            {
                // NULLs are distinct: a row with a NULL (or absent) value in any indexed column has no
                // unique index entry. So only delete the old entry if the old row had one, and only put
                // the new entry if the new row qualifies (value->NULL removes it, NULL->value adds it).
                bool oldHasKey = !HasNullKeyColumn(oldRow, index.Columns);
                bool newHasKey = !HasNullKeyColumn(newRow, index.Columns);

                CompositeColumnValue? oldKey = oldHasKey ? GetColumnValue(oldRow, index.Columns) : null;
                CompositeColumnValue? newKey = newHasKey ? GetColumnValue(newRow, index.Columns) : null;

                // When the indexed columns are unchanged, skip the delete+insert cycle. On a root this
                // avoids a needless round-trip; on a branch it is required for correctness: the delete
                // writes a tombstone and a subsequent SetIfNotExists on the same key would hit that
                // tombstone and incorrectly throw DuplicateUniqueKeyValue (tombstone-replace for
                // in-place unique updates is not yet supported). The inherited index entry already
                // points to the correct rowId, and the row fetch returns the branch-local value via
                // the branch-aware read path.
                if (oldHasKey && newHasKey && oldKey!.CompareTo(newKey!) == 0)
                    continue;

                if (oldHasKey)
                    rowUpdate.OldIndexEntries.Add(new KvTableStore.IndexDelete(index.KvId, oldKey!, rowId, Unique: true));

                if (newHasKey)
                    rowUpdate.NewIndexEntries.Add(new KvTableStore.IndexWrite(index.KvId, newKey!, Unique: true));
            }
            else if (index.Type == IndexType.Multi)
            {
                ColumnValue rowIdValue = new(ColumnType.Id, rowId.ToString());

                CompositeColumnValue oldKey = GetColumnValue(oldRow, index.Columns, rowIdValue);
                CompositeColumnValue newKey = GetColumnValue(newRow, index.Columns, rowIdValue);

                // Skip unchanged entries — same correctness requirement as the unique index case above.
                if (oldKey.CompareTo(newKey) == 0)
                    continue;

                rowUpdate.OldIndexEntries.Add(new KvTableStore.IndexDelete(index.KvId, oldKey, rowId, Unique: false));
                rowUpdate.NewIndexEntries.Add(new KvTableStore.IndexWrite(index.KvId, newKey, Unique: false));
            }
        }
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
