
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Flux;
using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;
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

        // Step #1. Check for unknown columns
        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
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

            if (!ticket.Values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
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

        await InsertInternal(machine, state);

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
        await InsertInternal(machine, state);
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
        await insertUniqueKeySaver.CheckUniqueKeys(state.Table, state.Ticket);

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

    /// <summary>
    /// Allocate a new page in the buffer pool and insert the serializated row into it
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> InsertToPageStep(InsertFluxState state)
    {
        BufferPoolManager tablespace = state.Database.BufferPool;

        byte[] rowBuffer = rowSerializer.Serialize(state.Table, state.Ticket.Values, state.RowTuple.SlotOne);

        // Insert data to the page offset
        tablespace.WriteDataToPageBatch(state.ModifiedPages, state.RowTuple.SlotTwo, 0, rowBuffer);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Every table has a B+Tree index where the data can be easily located by rowid
    /// We take the page created in the previous step and insert it into the tree
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateTableIndex(InsertFluxState state)
    {
        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            key: state.RowTuple.SlotOne,
            value: state.RowTuple.SlotTwo
        );

        // Main table index stores rowid pointing to page offeset
        state.Indexes.MainIndexDeltas = await indexSaver.Save(saveUniqueOffsetIndex);

        return FluxAction.Continue;
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
    /// Unique keys are updated after inserting the actual row
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateUniqueIndexes(InsertFluxState state)
    {
        if (state.Indexes.UniqueIndexes.Count == 0)
            return FluxAction.Continue;

        InsertTicket insertTicket = state.Ticket;

        List<(BTree<CompositeColumnValue, BTreeTuple>, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BTree<CompositeColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(insertTicket.Values, index.Columns);

            SaveIndexTicket saveUniqueIndexTicket = new(
                index: uniqueIndex,
                txnId: insertTicket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: state.RowTuple
            );

            deltas.Add((uniqueIndex, await indexSaver.Save(saveUniqueIndexTicket)));
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

        List<(BTree<CompositeColumnValue, BTreeTuple>, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
        {
            BTree<CompositeColumnValue, BTreeTuple> multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(insertTicket.Values, index.Columns, new ColumnValue(ColumnType.Id, state.RowTuple.SlotOne.ToString()));

            SaveIndexTicket saveIndexTicket = new(
                index: multiIndex,
                txnId: insertTicket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: state.RowTuple
            );

            deltas.Add((multiIndex, await indexSaver.Save(saveIndexTicket)));
        }

        state.Indexes.MultiIndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(InsertFluxState state)
    {
        if (state.Indexes.MainIndexDeltas is null)
            return FluxAction.Abort;

        foreach (BTreeMvccEntry<ObjectIdValue> btreeEntry in state.Indexes.MainIndexDeltas.MvccEntries)
            btreeEntry.CommitState = BTreeCommitState.Committed;

        await indexSaver.Persist(state.Database.BufferPool, state.Table.Rows, state.ModifiedPages, state.Indexes.MainIndexDeltas);

        if (state.Indexes.UniqueIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) uniqueIndex in state.Indexes.UniqueIndexDeltas)
            {
                foreach (BTreeMvccEntry<BTreeTuple> uniqueIndexEntry in uniqueIndex.deltas.MvccEntries)
                    uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

                await indexSaver.Persist(state.Database.BufferPool, uniqueIndex.index, state.ModifiedPages, uniqueIndex.deltas);
            }
        }

        if (state.Indexes.MultiIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) multiIndex in state.Indexes.MultiIndexDeltas)
            {
                foreach (BTreeMvccEntry<BTreeTuple> multiIndexEntry in multiIndex.deltas.MvccEntries)
                    multiIndexEntry.CommitState = BTreeCommitState.Committed;

                await indexSaver.Persist(state.Database.BufferPool, multiIndex.index, state.ModifiedPages, multiIndex.deltas);
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
    /// Creates a new flux machine and runs all steps in order
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task InsertInternal(FluxMachine<InsertFluxSteps, InsertFluxState> machine, InsertFluxState state)
    {
        Stopwatch timer = Stopwatch.StartNew();

        machine.When(InsertFluxSteps.CheckUniqueKeys, CheckUniqueKeysStep);
        machine.When(InsertFluxSteps.AllocateInsertTuple, AllocateInsertTuple);
        machine.When(InsertFluxSteps.InsertToPage, InsertToPageStep);
        machine.When(InsertFluxSteps.UpdateTableIndex, UpdateTableIndex);
        machine.When(InsertFluxSteps.UpdateUniqueIndexes, UpdateUniqueIndexes);
        machine.When(InsertFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(InsertFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(InsertFluxSteps.ApplyPageOperations, ApplyPageOperations);

        // machine.WhenAbort(ReleaseLocks);?

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
            "Row {0} inserted at {1}, Time taken: {2}",
            state.RowTuple.SlotOne,
            state.RowTuple.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff")
        );
    }
}
