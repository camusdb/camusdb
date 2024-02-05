
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.Time;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions.Models;

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Inserts a single row into a table
/// </summary>
internal sealed class RowInserter
{
    private readonly ILogger<ICamusDB> logger;

    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    /// <summary>
    /// 
    /// </summary>
    /// <param name="logger"></param>
    public RowInserter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Validates that all columns and values in the insert statement are valid
    /// </summary>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void Validate(TableDescriptor table, InsertTicket ticket) // @todo optimize this
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            // Step #1. Check for unknown columns
            foreach (KeyValuePair<string, ColumnValue> columnValue in values)
            {
                bool hasColumn = false;

                for (int i = 0; i < columns.Count; i++)
                {
                    TableColumnSchema column = columns[i];
                    if (column.Name == columnValue.Key)
                    {
                        hasColumn = true;
                        break;
                    }
                }

                if (!hasColumn)
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownColumn,
                        $"Unknown column '{columnValue.Key}' in column list"
                    );
            }

            // Step #2. Check for not null violations
            foreach (TableColumnSchema columnSchema in columns)
            {
                if (!columnSchema.NotNull)
                    continue;

                if (!values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.NotNullViolation,
                        $"Column '{columnSchema.Name}' cannot be null"
                    );
                }

                if (columnValue.Type == ColumnType.Null)
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.NotNullViolation,
                        $"Column '{columnSchema.Name}' cannot be null"
                    );
                }
            }
        }
    }

    /// <summary>
    /// 
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
                    $"A null value was found for unique key field '{name}'"
                );

            columnValues[i] = columnValue;
        }

        if (extraUniqueValue is not null)
            columnValues[^1] = extraUniqueValue;

        return new CompositeColumnValue(columnValues);
    }

    /// <summary>
    /// Schedules a new insert operation
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<int> Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        Validate(table, ticket);

        InsertFluxState state = new(
            database: database,
            table: table,
            ticket: ticket            
        );

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state);

        return await InsertInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// Schedules a new insert operation by passing the flux state directly
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    public async Task<int> InsertWithState(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        return await InsertInternal(machine, state).ConfigureAwait(false);
    }

    private async Task<FluxAction> InsertRowsAndIndexes(InsertFluxState state)
    {
        if (state.Ticket.Values is null)
        {
            Console.WriteLine("Invalid rows to update");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        InsertTicket ticket = state.Ticket;
        BufferPoolManager tablespace = state.Database.BufferPool;
        List<BufferPageOperation> modifiedPages = ticket.TxnState.ModifiedPages;

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            await CheckUniqueKeys(table, ticket.TxnState.TxnId, values).ConfigureAwait(false);

            BTreeTuple tuple = InsertNewRowIntoDisk(tablespace, table, values, modifiedPages);

            await UpdateTableIndex(tablespace, ticket.TxnState, tuple, table.Rows, modifiedPages).ConfigureAwait(false);

            await UpdateUniqueIndexes(tablespace, table, ticket.TxnState, tuple, values, modifiedPages).ConfigureAwait(false);

            await UpdateMultiIndexes(tablespace, table, ticket.TxnState, tuple, values, modifiedPages).ConfigureAwait(false);

            logger.LogInformation(
                "Row with rowid {SlotOne} inserted into page {SlotTwo}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.InsertedRows++;
        }

        return FluxAction.Continue;
    }

    private static async Task CheckUniqueKeys(TableDescriptor table, HLCTimestamp txnId, Dictionary<string, ColumnValue> values)
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            TableIndexSchema indexSchema = index.Value;

            if (indexSchema.Type != IndexType.Unique)
                continue;

            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex = indexSchema.BTree;

            await CheckUniqueKeyViolations(table, txnId, uniqueIndex, values, indexSchema.Columns).ConfigureAwait(false);
        }
    }

    private static async Task CheckUniqueKeyViolations(
        TableDescriptor table,
        HLCTimestamp txnId,
        BTree<CompositeColumnValue, BTreeTuple> uniqueIndex,
        Dictionary<string, ColumnValue> values,
        string[] columnNames
    )
    {
        CompositeColumnValue? uniqueValue = GetColumnValue(values, columnNames);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"The primary key of the table '{table.Name}' is not present in the list of values."
            );

        BTreeTuple? rowTuple = await uniqueIndex.Get(TransactionType.ReadOnly, txnId, uniqueValue).ConfigureAwait(false);

        if (rowTuple is not null && !rowTuple.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                $"Duplicate entry for key '{table.Name}' {uniqueValue}"
            );
    }

    /// <summary>
    /// Acquire write locks on the indices to ensure consistency in writing.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> TryAdquireLocks(InsertFluxState state)
    {
        await state.Ticket.TxnState.TryAdquireLocks(state.Table).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// If there are no unique key violations we can allocate a tuple to insert the row
    /// Allocate a new page in the buffer pool and insert the serialized row into it    
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="table"></param>
    /// <param name="values"></param>
    /// <param name="modifiedPages"></param>
    /// <returns></returns>
    private BTreeTuple InsertNewRowIntoDisk(
        BufferPoolManager tablespace,
        TableDescriptor table,
        Dictionary<string, ColumnValue> values,
        List<BufferPageOperation> modifiedPages
    )
    {
        BTreeTuple tuple = new(
            slotOne: tablespace.GetNextRowId(),
            slotTwo: tablespace.GetNextFreeOffset()
        );

        byte[] rowBuffer = rowSerializer.Serialize(table, values, tuple.SlotOne);

        // Insert data to the page offset
        tablespace.WriteDataToPageBatch(modifiedPages, tuple.SlotTwo, 0, rowBuffer);

        return tuple;
    }

    /// <summary>
    /// Every table has a B+Tree index where the data can be easily located by rowid
    /// We take the page created in the previous step and insert it into the tree
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="txnId"></param>
    /// <param name="tuple"></param>
    /// <param name="rowsIndex"></param>
    /// <returns></returns>
    private async Task UpdateTableIndex(
        BufferPoolManager tablespace,        
        TransactionState txnState,        
        BTreeTuple tuple,
        BTree<ObjectIdValue, ObjectIdValue> rowsIndex,
        List<BufferPageOperation> modifiedPages
    )
    {
        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: tablespace,
            index: rowsIndex,
            txnId: txnState.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: tuple.SlotOne,
            value: tuple.SlotTwo,
            modifiedPages: modifiedPages
        );

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

        txnState.MainTableDeltas.Add((rowsIndex, tuple));
    }

    /// <summary>
    /// Unique keys are updated after inserting the actual row
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="indexes"></param>
    /// <param name="txnId"></param>
    /// <param name="tuple"></param>
    /// <param name="values"></param>
    /// <param name="modifiedPages"></param>
    /// <returns></returns>
    private async Task UpdateUniqueIndexes(
        BufferPoolManager tablespace,
        TableDescriptor table,
        TransactionState txnState,
        BTreeTuple tuple,
        Dictionary<string, ColumnValue> values,
        List<BufferPageOperation> modifiedPages
    )
    {        
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Unique)
                continue;

            BTree<CompositeColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(values, index.Columns);

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: tablespace,
                index: uniqueIndex,
                txnId: txnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: tuple,
                modifiedPages: modifiedPages
            );

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            txnState.UniqueIndexDeltas.Add((uniqueIndex, uniqueKeyValue, tuple));
        }
    }

    /// <summary>
    /// In the last step multi indexes are updated
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="indexes"></param>
    /// <param name="txnId"></param>
    /// <param name="tuple"></param>
    /// <param name="values"></param>
    /// <param name="modifiedPages"></param>
    /// <returns></returns>
    private async Task UpdateMultiIndexes(
        BufferPoolManager tablespace,
        TableDescriptor table,
        TransactionState txnState,
        BTreeTuple tuple,
        Dictionary<string, ColumnValue> values,
        List<BufferPageOperation> modifiedPages
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema index = kv.Value;

            if (index.Type != IndexType.Multi)
                continue;

            BTree<CompositeColumnValue, BTreeTuple> multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(values, index.Columns, new ColumnValue(ColumnType.Id, tuple.SlotOne.ToString()));

            SaveIndexTicket saveIndexTicket = new(
                tablespace: tablespace,
                index: multiIndex,
                txnId: txnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: tuple,
                modifiedPages: modifiedPages
            );

            await indexSaver.Save(saveIndexTicket).ConfigureAwait(false);

            txnState.MultiIndexDeltas.Add((multiIndex, multiKeyValue, tuple));
        }     
    }

    /// <summary>
    /// Creates a new flux machine and runs all steps in order
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<int> InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        Stopwatch timer = Stopwatch.StartNew();

        machine.When(InsertFluxSteps.TryAdquireLocks, TryAdquireLocks);
        machine.When(InsertFluxSteps.InsertRowsAndIndexes, InsertRowsAndIndexes);        

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
             "Inserted {Rows} rows, Time taken: {Time}",
             state.InsertedRows,
             timeTaken.ToString(@"m\:ss\.fff")
         );

        return state.InsertedRows;
    }
}
