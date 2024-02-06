
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Updates multiple rows by the specified filters
/// </summary>
public sealed class RowUpdater
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private readonly ILogger<ICamusDB> logger;

    public RowUpdater(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Validates that the column name exists in the current table schema
    /// </summary>
    /// <param name="columns"></param>
    /// <param name="columnName"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void ValidateIfColumnExists(List<TableColumnSchema> columns, Dictionary<string, TableIndexSchema> indexes, string columnName)
    {
        bool hasColumn = false;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (string.IsNullOrEmpty(columnName))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid or empty column name in values list");

            if (string.Equals(column.Name, columnName))
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

    /// <summary>
    /// Validates that all columns and values in the update statement are valid
    /// This validation is performed when the values are simple columnvalues (not expressions)
    /// </summary>
    /// <param name="columns"></param>
    /// <param name="indexes"></param>
    /// <param name="plainValues"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void ValidatePlainValues(List<TableColumnSchema> columns, Dictionary<string, TableIndexSchema> indexes, Dictionary<string, ColumnValue> plainValues)
    {
        if (plainValues.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        foreach (KeyValuePair<string, ColumnValue> columnValue in plainValues)
            ValidateIfColumnExists(columns, indexes, columnValue.Key);

        foreach (TableColumnSchema columnSchema in columns)
        {
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

    /// <summary>
    /// Validates that all columns and values in the update statement are valid
    /// This validation is performed when the values are SQL expressions.
    /// </summary>
    /// <param name="columns"></param>
    /// <param name="indexes"></param>
    /// <param name="exprValues"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void ValidateExprValues(List<TableColumnSchema> columns, Dictionary<string, TableIndexSchema> indexes, Dictionary<string, NodeAst> exprValues)
    {
        if (exprValues.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        foreach (KeyValuePair<string, NodeAst> columnValue in exprValues)
            ValidateIfColumnExists(columns, indexes, columnValue.Key);

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!columnSchema.NotNull)
                continue;

            if (!exprValues.TryGetValue(columnSchema.Name, out NodeAst? columnValue))
                continue;

            // This is a superficial validation of the data. It only works if the value to be updated is exactly NULL.
            // For example: UPDATE robots SET name = NULL.
            if (columnValue.nodeType == NodeType.Null)
                throw new CamusDBException(CamusDBErrorCodes.NotNullViolation, $"Column '{columnSchema.Name}' cannot be null");
        }
    }

    /// <summary>
    /// Validates that all columns and values in the update statement are valid
    /// </summary>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void Validate(TableDescriptor table, UpdateTicket ticket) // @todo optimize this
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

    /// <summary>
    /// Schedules a new Update operation by the specified filters
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> Update(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, UpdateTicket ticket)
    {
        // Performs a superficial validation of the ticket to ensure that everything is correct.
        // However, the values must be validated again later against the internal state of the transaction.
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

    /// <summary>
    /// Returns the specified column values as a composite value.
    /// </summary>
    /// <param name="rowValues"></param>
    /// <param name="columnNames"></param>
    /// <param name="extraUniqueValue"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
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
    /// We need to locate the row tuples to Update
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTuplesToUpdate(UpdateFluxState state)
    {
        UpdateTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
            txnType: TransactionType.Write,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: ticket.Parameters
        );

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        // @todo we need to take a snapshot of the data to prevent deadlocks
        // but probably need to optimize this for larger datasets
        state.RowsToUpdate = await cursor.ToListAsync();

        //Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);*/

        return FluxAction.Continue;
    }

    /// <summary>
    /// Checks if a row with the same primary key is already added to table
    /// </summary>
    /// <param name="table"></param>
    /// <param name="keyName"></param>
    /// <param name="uniqueIndex"></param>
    /// <param name="txnId"></param>
    /// <param name="values"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private static async Task CheckUniqueKeyViolations(
        TableDescriptor table,
        string keyName,
        BTree<CompositeColumnValue, BTreeTuple> uniqueIndex,
        HLCTimestamp txnId,
        Dictionary<string, ColumnValue> values,
        string[] columnNames
    )
    {
        CompositeColumnValue uniqueValue = GetColumnValue(values, columnNames);

        BTreeTuple? rowTuple = await uniqueIndex.Get(TransactionType.Write, txnId, uniqueValue).ConfigureAwait(false);

        if (rowTuple is not null && !rowTuple.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                "Duplicate entry for key \"" + table.Name + "." + keyName + "\" " + uniqueValue
            );
    }

    /// <summary>
    /// Check for unique key violations on every unique index the table has
    /// </summary>
    /// <param name="table"></param>
    /// <param name="txnId"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private static async Task CheckUniqueKeys(TableDescriptor table, HLCTimestamp txnId, Dictionary<string, ColumnValue> values)
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (index.Key == "~pk")
                continue;

            BTree<CompositeColumnValue, BTreeTuple> uniqueIndex = index.Value.BTree;

            await CheckUniqueKeyViolations(table, index.Key, uniqueIndex, txnId, values, index.Value.Columns).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Acquire write locks on the indices to ensure consistency in writing.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> TryAdquireLocks(UpdateFluxState state)
    {
        await state.Ticket.TxnState.TryAdquireWriteLocks(state.Table).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Updates the row on the disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateRowsAndIndexes(UpdateFluxState state)
    {
        if (state.RowsToUpdate is null)
        {
            Console.WriteLine("Invalid rows to update");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;
        BufferPoolManager tablespace = state.Database.BufferPool;

        foreach (QueryResultRow queryRow in state.RowsToUpdate)
        {
            Dictionary<string, ColumnValue> rowValues = GetNewUpdatedRow(queryRow, ticket);

            CheckForNotNulls(table, rowValues);

            await CheckUniqueKeys(table, ticket.TxnState.TxnId, rowValues).ConfigureAwait(false);

            BTreeTuple tuple = UpdateNewRowVersionDisk(tablespace, table, state, queryRow, rowValues);

            await UpdateTableIndex(state, tuple).ConfigureAwait(false);

            await UpdateUniqueIndexes(state, table, ticket, tuple, queryRow).ConfigureAwait(false);

            await UpdateMultiIndexes(state, table, ticket, tuple, queryRow).ConfigureAwait(false);

            logger.LogInformation(
                "Row with rowid {SlotOne} updated to page {SlotTwo}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// Validates if any NOT NULL constraint is being violated with respect to the new proposed values to be updated.
    /// </summary>
    /// <param name="table"></param>
    /// <param name="rowValues"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void CheckForNotNulls(TableDescriptor table, Dictionary<string, ColumnValue> rowValues)
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        foreach (TableColumnSchema columnSchema in columns)
        {
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

    /// <summary>
    /// Assigns the plain column values to a fresh copy of the original row returned in the query, 
    /// or evaluates the SQL expressions with reference to the values in the original row and placeholders.
    /// </summary>
    /// <param name="row"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private static Dictionary<string, ColumnValue> GetNewUpdatedRow(QueryResultRow row, UpdateTicket ticket)
    {
        Dictionary<string, ColumnValue> rowValues = new(row.Row.Count); // create a fresh copy

        foreach (KeyValuePair<string, ColumnValue> keyValue in row.Row)
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
                rowValues[keyValuePair.Key] = SqlExecutor.EvalExpr(keyValuePair.Value, row.Row, ticket.Parameters);

            return rowValues;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid values in update ticket");
    }

    /// <summary>
    /// Serializes the new values of the row with their respective new page addresses and also 
    /// assigns a new row offset where the new version of the row will be stored.
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="table"></param>
    /// <param name="state"></param>
    /// <param name="row"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    private BTreeTuple UpdateNewRowVersionDisk(
        BufferPoolManager tablespace,
        TableDescriptor table,
        UpdateFluxState state,
        QueryResultRow row,
        Dictionary<string, ColumnValue> rowValues
    )
    {
        //Console.WriteLine("Original tuple: {0}", row.Tuple);        

        byte[] buffer = rowSerializer.Serialize(table, rowValues, row.Tuple.SlotOne);

        // Allocate a new page for the row
        BTreeTuple tuple = new(
            slotOne: row.Tuple.SlotOne,
            slotTwo: tablespace.GetNextFreeOffset()
        );

        //Console.WriteLine("New tuple: {0}", tuple);

        tablespace.WriteDataToPageBatch(state.Ticket.TxnState.ModifiedPages, tuple.SlotTwo, 0, buffer);

        return tuple;
    }

    private async Task UpdateTableIndex(UpdateFluxState state, BTreeTuple tuple)
    {
        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: state.Database.BufferPool,
            index: state.Table.Rows,
            txnId: state.Ticket.TxnState.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: tuple.SlotOne,
            value: tuple.SlotTwo,
            modifiedPages: state.Ticket.TxnState.ModifiedPages
        );

        // Main table index stores rowid pointing to page offset
        await indexSaver.Save(saveUniqueOffsetIndex);

        state.Ticket.TxnState.MainTableDeltas.Add((state.Table.Rows, tuple));
    }

    private async Task UpdateUniqueIndexes(        
        UpdateFluxState state,
        TableDescriptor table,
        UpdateTicket ticket,
        BTreeTuple tuple,
        QueryResultRow row
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Unique)
                continue;

            BTree<CompositeColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(row.Row, index.Columns);

            SaveIndexTicket saveIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: uniqueIndex,
                txnId: ticket.TxnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: tuple,
                modifiedPages: state.Ticket.TxnState.ModifiedPages
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            await indexSaver.Save(saveIndexTicket).ConfigureAwait(false);

            state.Ticket.TxnState.UniqueIndexDeltas.Add((uniqueIndex, uniqueKeyValue, tuple));
        }
    }

    private async Task UpdateMultiIndexes(
        UpdateFluxState state,
        TableDescriptor table,
        UpdateTicket ticket,
        BTreeTuple tuple,
        QueryResultRow row
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Multi)
                continue;

            BTree<CompositeColumnValue, BTreeTuple> multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(row.Row, index.Columns, new ColumnValue(ColumnType.Id, tuple.SlotOne.ToString()));

            SaveIndexTicket saveIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: multiIndex,
                txnId: ticket.TxnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: tuple,
                modifiedPages: state.Ticket.TxnState.ModifiedPages
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            await indexSaver.Save(saveIndexTicket);

            state.Ticket.TxnState.MultiIndexDeltas.Add((multiIndex, multiKeyValue, tuple));
        }
    }           

    /// <summary>
    /// Executes the flux state machine to update records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    internal async Task<int> UpdateInternal(FluxMachine<UpdateFluxSteps, UpdateFluxState> machine, UpdateFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(UpdateFluxSteps.TryAdquireLocks, TryAdquireLocks);
        machine.When(UpdateFluxSteps.LocateTupleToUpdate, LocateTuplesToUpdate);        
        machine.When(UpdateFluxSteps.UpdateRowsAndIndexes, UpdateRowsAndIndexes);        

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
            "Updated {Rows} rows, Time taken: {Time}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
