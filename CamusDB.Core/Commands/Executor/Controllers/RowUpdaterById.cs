
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.Trees.Experimental;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Updates a single row specified by the id
/// Should be faster than using the Query Executor
/// </summary>
public sealed class RowUpdaterById
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private readonly RowDeserializer rowDeserializer = new();

    private readonly ILogger<ICamusDB> logger;

    public RowUpdaterById(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Validates that all columns and values in the update statement are valid
    /// </summary>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void Validate(TableDescriptor table, UpdateByIdTicket ticket) // @todo optimize this
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;

        if (ticket.Values is null || ticket.Values.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        // Step #1. Check for unknown columns
        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
        {
            bool hasColumn = false;

            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];

                if (string.IsNullOrEmpty(columnValue.Key))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid or empty column name in values list");

                if (string.Equals(column.Name, columnValue.Key))
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

            // Step #2. Check if primary key is going to be updated
            if (table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? indexSchema))
            {
                if (indexSchema.Columns.Contains(columnValue.Key))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Cannot update primary key field");
            }
        }        

        // Step #2. Check for not null violations
        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!columnSchema.NotNull)
                continue;

            if (!ticket.Values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
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
    /// Step #1. Creates a new update plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static UpdateByIdFluxIndexState GetIndexUpdatePlan(TableDescriptor table)
    {
        UpdateByIdFluxIndexState indexState = new();

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
        }

        return indexState;
    }

    /// <summary>
    /// Schedules a new Update operation by the row id
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> UpdateById(DatabaseDescriptor database, TableDescriptor table, UpdateByIdTicket ticket)
    {
        Validate(table, ticket);

        UpdateByIdFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            indexes: GetIndexUpdatePlan(table)
        );

        FluxMachine<UpdateByIdFluxSteps, UpdateByIdFluxState> machine = new(state);

        return await UpdateByIdInternal(machine, state).ConfigureAwait(false);
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
    /// We need to locate the row tuple to Update
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTupleToUpdate(UpdateByIdFluxState state)
    {
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        UpdateByIdTicket ticket = state.Ticket;

        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        state.RowTuple = await index.BTree.Get(TransactionType.Write, ticket.TxnId, new CompositeColumnValue(columnId)).ConfigureAwait(false);

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            logger.LogWarning("Index Pk={0} does not exist", ticket.Id);

            return FluxAction.Abort;
        }

        byte[] data = await tablespace.GetDataFromPage(state.RowTuple.SlotTwo).ConfigureAwait(false);
        if (data.Length == 0)
        {
            logger.LogWarning("Index RowId={0} has an empty page data", ticket.Id);

            return FluxAction.Abort;
        }

        state.ColumnValues = rowDeserializer.Deserialize(table.Schema, data);

        logger.LogInformation("Data to Update Pk={0} is at page offset {1}/{2}", ticket.Id, state.RowTuple.SlotOne, state.RowTuple.SlotTwo);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Updates unique indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateUniqueIndexes(UpdateByIdFluxState state)
    {
        UpdateByIdTicket ticket = state.Ticket;

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            return FluxAction.Abort;
        }

        List<(BPlusTree<CompositeColumnValue, BTreeTuple>, BPlusTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BPlusTree<CompositeColumnValue, BTreeTuple>? uniqueIndex = index.BTree;

            CompositeColumnValue uniqueKeyValue = GetColumnValue(state.ColumnValues, index.Columns);            

            SaveIndexTicket saveUniqueIndexTicket = new(
                index: uniqueIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: state.RowTuple
            );

            deltas.Add((uniqueIndex, await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false)));
        }

        state.Indexes.UniqueIndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// Updates multi indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateMultiIndexes(UpdateByIdFluxState state)
    {
        UpdateByIdTicket ticket = state.Ticket;

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            logger.LogWarning("Index Pk={0} does not exist", ticket.Id);

            return FluxAction.Abort;
        }

        List<(BPlusTree<CompositeColumnValue, BTreeTuple>, BPlusTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
        {
            BPlusTree<CompositeColumnValue, BTreeTuple>? multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(state.ColumnValues, index.Columns, new ColumnValue(ColumnType.Id, state.RowTuple.SlotOne.ToString()));          

            SaveIndexTicket saveUniqueIndexTicket = new(
                index: multiIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: state.RowTuple
            );

            deltas.Add((multiIndex, await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false)));
        }

        state.Indexes.MultiIndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// Updates the row on the disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> UpdateRowToDisk(UpdateByIdFluxState state)
    {
        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            logger.LogWarning("Invalid row to Update {0}", state.Ticket.Id);

            return Task.FromResult(FluxAction.Abort);
        }

        TableDescriptor table = state.Table;
        UpdateByIdTicket ticket = state.Ticket;
        BufferPoolManager tablespace = state.Database.BufferPool;

        foreach (KeyValuePair<string, ColumnValue> keyValuePair in ticket.Values)
            state.ColumnValues[keyValuePair.Key] = keyValuePair.Value;

        byte[] buffer = rowSerializer.Serialize(table, state.ColumnValues, state.RowTuple.SlotOne);

        // Allocate a new page for the row
        BTreeTuple tuple = new(
            slotOne: state.RowTuple.SlotOne,
            slotTwo: tablespace.GetNextFreeOffset()
        );

        state.RowTuple = tuple;

        tablespace.WriteDataToPageBatch(state.ModifiedPages, state.RowTuple.SlotTwo, 0, buffer);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Every table has a B+Tree index where the data can be easily located by rowid
    /// We update the rowid to point to the new page offset
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateTableIndex(UpdateByIdFluxState state)
    {
        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            logger.LogWarning("Invalid row to Update {0}", state.Ticket.Id);

            return FluxAction.Abort;
        }

        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            key: state.RowTuple.SlotOne,
            value: state.RowTuple.SlotTwo
        );

        // Main table index stores rowid pointing to page offset
        state.Indexes.MainIndexDeltas = await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(UpdateByIdFluxState state)
    {
        if (state.Indexes.MainIndexDeltas is null)
            return FluxAction.Abort;

        foreach (BTreeMvccEntry<ObjectIdValue> btreeEntry in state.Indexes.MainIndexDeltas.MvccEntries)
            btreeEntry.CommitState = BTreeCommitState.Committed;

        await indexSaver.Persist(state.Database.BufferPool, state.Table.Rows, state.ModifiedPages, state.Indexes.MainIndexDeltas).ConfigureAwait(false);

        if (state.Indexes.UniqueIndexDeltas is not null)
        {
            foreach ((BPlusTree<CompositeColumnValue, BTreeTuple> index, BPlusTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) uniqueIndex in state.Indexes.UniqueIndexDeltas)
            {
                foreach (BTreeMvccEntry<BTreeTuple> uniqueIndexEntry in uniqueIndex.deltas.MvccEntries)
                    uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

                await indexSaver.Persist(state.Database.BufferPool, uniqueIndex.index, state.ModifiedPages, uniqueIndex.deltas).ConfigureAwait(false);
            }
        }

        if (state.Indexes.MultiIndexDeltas is not null)
        {
            foreach ((BPlusTree<CompositeColumnValue, BTreeTuple> index, BPlusTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) multIndex in state.Indexes.MultiIndexDeltas)
            {
                foreach (BTreeMvccEntry<BTreeTuple> multiIndexEntry in multIndex.deltas.MvccEntries)
                    multiIndexEntry.CommitState = BTreeCommitState.Committed;

                await indexSaver.Persist(state.Database.BufferPool, multIndex.index, state.ModifiedPages, multIndex.deltas).ConfigureAwait(false);
            }
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(UpdateByIdFluxState state)
    {
        state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to update a record by id
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    internal async Task<int> UpdateByIdInternal(FluxMachine<UpdateByIdFluxSteps, UpdateByIdFluxState> machine, UpdateByIdFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        UpdateByIdTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(UpdateByIdFluxSteps.LocateTupleToUpdate, LocateTupleToUpdate);
        machine.When(UpdateByIdFluxSteps.UpdateRow, UpdateRowToDisk);
        machine.When(UpdateByIdFluxSteps.UpdateTableIndex, UpdateTableIndex);
        machine.When(UpdateByIdFluxSteps.UpdateUniqueIndexes, UpdateUniqueIndexes);
        machine.When(UpdateByIdFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(UpdateByIdFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(UpdateByIdFluxSteps.ApplyPageOperations, ApplyPageOperations);

        //machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        if (state.RowTuple is null)
        {
            logger.LogWarning(
                "Row pk {0} not found, Time taken: {1}",
                ticket.Id,
                timeTaken.ToString(@"m\:ss\.fff")
            );

            return 0;
        }

        logger.LogInformation(
            "Row pk {0} with id {1} updated to page {2}, Time taken: {3}",
            ticket.Id,
            state.RowTuple?.SlotOne,
            state.RowTuple?.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return 1;
    }
}
