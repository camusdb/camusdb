
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
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.Util.Trees;

using System.Diagnostics;
using Microsoft.Extensions.Logging;
using CamusDB.Core.Util.Time;
using QUT.Gppg;
using System.Net.Sockets;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Inserts a single row into a table
/// </summary>
internal sealed class RowInserter
{
    private readonly ILogger<ICamusDB> logger;

    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private readonly DMLUniqueKeySaver insertUniqueKeySaver = new();

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
                    "A null value was found for unique key field '" + name + "'"
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
            ticket: ticket,
            indexes: GetIndexInsertPlan(table)
        );

        FluxMachine<InsertFluxSteps, InsertFluxState> machine = new(state);

        await InsertInternal(machine, state).ConfigureAwait(false);

        return 1;
    }

    /// <summary>
    /// Schedules a new insert operation by passing the flux state directly
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    public async Task InsertWithState(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        await InsertInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// Step #1. Creates a new insert plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static InsertFluxIndexState GetIndexInsertPlan(TableDescriptor table)
    {
        InsertFluxIndexState indexState = new();

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type == IndexType.Unique)
            {
                indexState.UniqueIndexes.Add(index.Value);
                continue;
            }

            if (index.Value.Type == IndexType.Multi)
            {
                indexState.MultiIndexes.Add(index.Value);
                continue;
            }

            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unkown index type: " + index.Value.Type);
        }

        return indexState;
    }

    /// <summary>
    /// Step #2. Check for unique key violations
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> CheckUniqueKeysStep(InsertFluxState state)
    {
        await insertUniqueKeySaver.CheckUniqueKeys(state.Table, state.Ticket).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// If there are no unique key violations we can allocate a tuple to insert the row
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> AllocateInsertTuple(InsertFluxState state)
    {
        BufferPoolManager tablespace = state.Database.BufferPool;

        state.RowTuple.SlotOne = tablespace.GetNextRowId();
        state.RowTuple.SlotTwo = tablespace.GetNextFreeOffset();

        return Task.FromResult(FluxAction.Continue);
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

        foreach (var values in ticket.Values)
        {
            //Dictionary<string, ColumnValue> rowValues = GetNewUpdatedRow(queryRow, ticket);

            //CheckForNotNulls(table, rowValues);

            await CheckUniqueKeys(table, ticket.TxnId, values).ConfigureAwait(false);

            BTreeTuple tuple = InsertNewRowIntoDisk(tablespace, table.Rows, state, values);

            await UpdateTableIndex(tablespace, ticket.TxnId, tuple, values).ConfigureAwait(false);

            await UpdateUniqueIndexes(state, ticket, tuple, queryRow).ConfigureAwait(false);

            await UpdateMultiIndexes(state, ticket, tuple, queryRow).ConfigureAwait(false);

            logger.LogInformation(
                "Row with rowid {SlotOne} updated to page {SlotTwo}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    private static async Task CheckUniqueKeys(TableDescriptor table, HLCTimestamp txnId, Dictionary<string, ColumnValue> values)
    {
        //await insertUniqueKeySaver.CheckUniqueKeys(state.Table, state.Ticket).ConfigureAwait(false);

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex = index.Value.BTree;

            await CheckUniqueKeyViolations(table, txnId, uniqueIndex, values, index.Value.Columns);
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
    private async Task<FluxAction> AdquireLocks(InsertFluxState state)
    {
        state.Locks.Add(await state.Table.Rows.WriterLockAsync());

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
            state.Locks.Add(await index.BTree.WriterLockAsync());

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
            state.Locks.Add(await index.BTree.WriterLockAsync());

        return FluxAction.Continue;
    }

    /// <summary>
    /// Allocate a new page in the buffer pool and insert the serialized row into it    
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="table"></param>
    /// <param name="state"></param>
    /// <param name="values"></param>
    /// <returns></returns>
    private BTreeTuple InsertNewRowIntoDisk(BufferPoolManager tablespace, InsertFluxState state, Dictionary<string, ColumnValue> values)
    {
        BTreeTuple tuple = new(
            slotOne: tablespace.GetNextRowId(),
            slotTwo: tablespace.GetNextFreeOffset()
        );

        byte[] rowBuffer = rowSerializer.Serialize(state.Table, values, tuple.SlotOne);

        // Insert data to the page offset
        tablespace.WriteDataToPageBatch(state.ModifiedPages, tuple.SlotTwo, 0, rowBuffer);

        return tuple;
    }

    /// <summary>
    /// Every table has a B+Tree index where the data can be easily located by rowid
    /// We take the page created in the previous step and insert it into the tree
    /// </summary>
    /// <param name="tablespace"></param>
    /// <param name="txnId"></param>
    /// <param name="tuple"></param>
    /// <param name="rows"></param>
    /// <returns></returns>
    private async Task UpdateTableIndex(
        BufferPoolManager tablespace,
        HLCTimestamp txnId,
        BTreeTuple tuple,
        BTree<ObjectIdValue, ObjectIdValue> rows,
        List<BufferPageOperation> modifiedPages
    )
    {
        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: tablespace,
            index: rows,
            txnId: txnId,
            commitState: BTreeCommitState.Uncommitted,
            key: tuple.SlotOne,
            value: tuple.SlotTwo,
            modifiedPages: modifiedPages
        );

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);
    }

    /// <summary>
    /// Unique keys are updated after inserting the actual row
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateUniqueIndexes(InsertFluxState state)
    {
        if (state.Indexes.UniqueIndexes.Count == 0)
            return FluxAction.Continue;

        InsertTicket insertTicket = state.Ticket;

        List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BTree<CompositeColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(insertTicket.Values, index.Columns);

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: uniqueIndex,
                txnId: insertTicket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: state.RowTuple,
                modifiedPages: state.ModifiedPages
            );

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            deltas.Add((uniqueIndex, uniqueKeyValue));
        }

        state.Indexes.UniqueIndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// In the last step multi indexes are updated
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateMultiIndexes(InsertFluxState state)
    {
        if (state.Indexes.MultiIndexes.Count == 0)
            return FluxAction.Continue;

        InsertTicket insertTicket = state.Ticket;

        List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
        {
            BTree<CompositeColumnValue, BTreeTuple> multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(insertTicket.Values, index.Columns, new ColumnValue(ColumnType.Id, state.RowTuple.SlotOne.ToString()));

            SaveIndexTicket saveIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: multiIndex,
                txnId: insertTicket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: state.RowTuple,
                modifiedPages: state.ModifiedPages
            );

            await indexSaver.Save(saveIndexTicket).ConfigureAwait(false);

            deltas.Add((multiIndex, multiKeyValue));
        }

        state.Indexes.MultiIndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// Commit the changes in the indices after being sure that the insert had no issues.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(InsertFluxState state)
    {
        if (state.RowTuple is null)
            return FluxAction.Abort;

        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: state.Database.BufferPool,
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            commitState: BTreeCommitState.Committed,
            key: state.RowTuple.SlotOne,
            value: state.RowTuple.SlotTwo,
            modifiedPages: state.ModifiedPages
        );

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

        if (state.Indexes.UniqueIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue uniqueKeyValue) uniqueIndex in state.Indexes.UniqueIndexDeltas)
            {
                SaveIndexTicket saveUniqueIndexTicket = new(
                    tablespace: state.Database.BufferPool,
                    index: uniqueIndex.index,
                    txnId: state.Ticket.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: uniqueIndex.uniqueKeyValue,
                    value: state.RowTuple,
                    modifiedPages: state.ModifiedPages
                );

                await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);
            }
        }

        if (state.Indexes.MultiIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue multiKeyValue) multIndex in state.Indexes.MultiIndexDeltas)
            {
                SaveIndexTicket saveMultiIndexTicket = new(
                    tablespace: state.Database.BufferPool,
                    index: multIndex.index,
                    txnId: state.Ticket.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: multIndex.multiKeyValue,
                    value: state.RowTuple,
                    modifiedPages: state.ModifiedPages
                );

                await indexSaver.Save(saveMultiIndexTicket).ConfigureAwait(false);
            }
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// Apply all the changes to the modified pages in an ACID operation
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(InsertFluxState state)
    {
        state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Release all the locks acquired in the previous steps
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ReleaseLocks(InsertFluxState state)
    {
        foreach (IDisposable disposable in state.Locks)
            disposable.Dispose();

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Creates a new flux machine and runs all steps in order
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        Stopwatch timer = Stopwatch.StartNew();

        machine.When(InsertFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(InsertFluxSteps.InsertRowsAndIndexes, InsertRowsAndIndexes);
        machine.When(InsertFluxSteps.UpdateUniqueIndexes, UpdateUniqueIndexes);
        machine.When(InsertFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(InsertFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(InsertFluxSteps.ApplyPageOperations, ApplyPageOperations);
        machine.When(InsertFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
            "Row {SlotOne} inserted at {SlotTwo}, Time taken: {Time}",
            state.RowTuple.SlotOne,
            state.RowTuple.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff")
        );
    }
}
