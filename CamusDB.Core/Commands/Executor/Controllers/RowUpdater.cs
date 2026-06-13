
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

    private async Task<FluxAction> LocateTuplesToUpdate(UpdateFluxState state)
    {
        UpdateTicket ticket = state.Ticket;

        // R16: restrict scan-time decode to columns needed by the WHERE filter and any
        // expression-based SET values (e.g. SET col = old_col + 1). Rejected candidates
        // are only partially decoded. The write phase re-reads and fully decodes each
        // matched row via LoadWritableRow before applying the change.
        // Returns null when the WHERE contains subquery nodes — fall back to full decode.
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
            locateColumns: locateColumns
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        state.RowsToUpdate = await cursor.ToListAsync();

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

        foreach (QueryResultRow queryRow in state.RowsToUpdate)
        {
            ObjectIdValue rowId = queryRow.RowId;
            Dictionary<string, ColumnValue> oldRow = await LoadWritableRow(state.Database, table, tx, rowId).ConfigureAwait(false);
            Dictionary<string, ColumnValue> rowValues = GetNewUpdatedRow(oldRow, queryRow, ticket);

            CheckForNotNulls(table, rowValues);

            byte[] buffer = RowEncoder.Encode(table.Schema, rowValues, rowId);

            await table.Store.UpdateRow(tx, rowId, buffer).ConfigureAwait(false);

            await UpdateUniqueIndexes(table, tx, rowId, oldRow, rowValues).ConfigureAwait(false);

            await UpdateMultiIndexes(table, tx, rowId, oldRow, rowValues).ConfigureAwait(false);

            logger.LogInformation("Row with rowid {RowId} updated", rowId);

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    private static async Task<Dictionary<string, ColumnValue>> LoadWritableRow(
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId
    )
    {
        byte[]? data = await table.Store.GetRow(tx, rowId).ConfigureAwait(false);
        if (data is null || data.Length == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Row '{rowId}' disappeared before update");

        return await RowEncoder.DecodeWritableAsync(
            table.Schema,
            tx.TransactionId,
            rowId,
            data,
            visibilitySchemaVersion: table.Schema.Version).ConfigureAwait(false);
    }

    private static async Task UpdateUniqueIndexes(
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> oldRow,
        Dictionary<string, ColumnValue> newRow
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Unique || !SchemaElementStateRules.IsWritableIndex(table.Schema, index))
                continue;

            CompositeColumnValue oldKey = GetColumnValue(oldRow, index.Columns);
            CompositeColumnValue newKey = GetColumnValue(newRow, index.Columns);

            await table.Store.DeleteIndexEntry(tx, index.Name, oldKey, rowId, unique: true).ConfigureAwait(false);
            await table.Store.PutIndexEntry(tx, index.Name, newKey, rowId, unique: true).ConfigureAwait(false);
        }
    }

    private static async Task UpdateMultiIndexes(
        TableDescriptor table,
        KvTransaction tx,
        ObjectIdValue rowId,
        Dictionary<string, ColumnValue> oldRow,
        Dictionary<string, ColumnValue> newRow
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Multi || !SchemaElementStateRules.IsWritableIndex(table.Schema, index))
                continue;

            ColumnValue rowIdValue = new(ColumnType.Id, rowId.ToString());

            CompositeColumnValue oldKey = GetColumnValue(oldRow, index.Columns, rowIdValue);
            CompositeColumnValue newKey = GetColumnValue(newRow, index.Columns, rowIdValue);

            await table.Store.DeleteIndexEntry(tx, index.Name, oldKey, rowId, unique: false).ConfigureAwait(false);
            await table.Store.PutIndexEntry(tx, index.Name, newKey, rowId, unique: false).ConfigureAwait(false);
        }
    }

    private async Task<int> UpdateInternal(FluxMachine<UpdateFluxSteps, UpdateFluxState> machine, UpdateFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(UpdateFluxSteps.LocateTupleToUpdate, LocateTuplesToUpdate);
        machine.When(UpdateFluxSteps.UpdateRowsAndIndexes, UpdateRowsAndIndexes);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogInformation(
            "Updated {Rows} rows, Time taken: {Time}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
