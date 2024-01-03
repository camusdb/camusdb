
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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// 
/// </summary>
internal sealed class RowDeleterById
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowDeserializer rowDeserializer = new();

    /// <summary>
    /// Schedules a new delete operation by the row id
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    public async Task<int> DeleteById(DatabaseDescriptor database, TableDescriptor table, DeleteByIdTicket ticket)
    {
        DeleteByIdFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            indexes: GetIndexDeletePlan(table)
        );

        FluxMachine<DeleteByIdFluxSteps, DeleteByIdFluxState> machine = new(state);

        return await DeleteByIdInternal(machine, state);
    }

    /// <summary>
    /// Step #1. Creates a new delete index plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static DeleteByIdFluxIndexState GetIndexDeletePlan(TableDescriptor table)
    {
        DeleteByIdFluxIndexState indexState = new();

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            indexState.UniqueIndexes.Add(index.Value);
        }

        return indexState;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="columnValues"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    private static ColumnValue? GetColumnValue(Dictionary<string, ColumnValue> columnValues, string name)
    {
        if (columnValues.TryGetValue(name, out ColumnValue? columnValue))
            return columnValue;

        return null;
    }

    /// <summary>
    /// We need to locate the row tuple to delete
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTupleToDelete(DeleteByIdFluxState state)
    {
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        DeleteByIdTicket ticket = state.Ticket;

        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        if (index.UniqueRows is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        state.RowTuple = await index.UniqueRows.Get(ticket.TxnId, columnId);

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            return FluxAction.Abort;
        }

        byte[] data = await tablespace.GetDataFromPage(state.RowTuple.SlotTwo);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return FluxAction.Abort;
        }

        state.ColumnValues = rowDeserializer.Deserialize(table.Schema, data);

        Console.WriteLine("Data to Delete Pk={0} is at page offset {1}/{2}", ticket.Id, state.RowTuple.SlotOne, state.RowTuple.SlotTwo);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Deletes references to the row from the unique indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteUniqueIndexes(DeleteByIdFluxState state)
    {
        DeleteByIdTicket ticket = state.Ticket;

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            return FluxAction.Abort;
        }

        List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            ColumnValue? uniqueKeyValue = GetColumnValue(state.ColumnValues, index.Column);

            if (uniqueKeyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field " + index.Column
                );

            SaveUniqueIndexTicket saveUniqueIndexTicket = new(
                index: uniqueIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: new BTreeTuple(new(), new())
            );

            deltas.Add((uniqueIndex, await indexSaver.Save(saveUniqueIndexTicket)));
        }

        state.Indexes.UniqueIndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// Deletes multi indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteMultiIndexes(DeleteByIdFluxState state)
    {
        await DeleteMultiIndexes(state.Database, state.Table, state.ColumnValues);

        return FluxAction.Continue;
    }

    private async Task DeleteMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolManager tablespace = database.BufferPool;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo update in parallel
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.MultiRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            ColumnValue? columnValue = GetColumnValue(columnValues, index.Value.Column);
            if (columnValue is null) // @todo check what to to here
                continue;

            BTreeMulti<ColumnValue> multiIndex = index.Value.MultiRows;

            await indexSaver.Remove(tablespace, multiIndex, columnValue);
        }
    }

    /// <summary>
    /// Every table has a B+Tree index where the data can be easily located by rowid
    /// We update the rowid to point to a null offset
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateTableIndex(DeleteByIdFluxState state)
    {
        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            Console.WriteLine("Invalid row to Update {0}", state.Ticket.Id);
            return FluxAction.Abort;
        }

        SaveUniqueOffsetIndexTicket saveUniqueOffsetIndex = new(
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            key: state.RowTuple.SlotOne,
            value: new ObjectIdValue()
        );

        // Main table index stores rowid pointing to null offset
        state.Indexes.MainIndexDeltas = await indexSaver.Save(saveUniqueOffsetIndex);

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(DeleteByIdFluxState state)
    {
        if (state.Indexes.MainIndexDeltas is null)
            return FluxAction.Abort;

        foreach (BTreeMvccEntry<ObjectIdValue> btreeEntry in state.Indexes.MainIndexDeltas.Entries)
            btreeEntry.CommitState = BTreeCommitState.Committed;

        await indexSaver.Persist(state.Database.BufferPool, state.Table.Rows, state.ModifiedPages, state.Indexes.MainIndexDeltas);

        if (state.Indexes.UniqueIndexDeltas is null)
            return FluxAction.Continue;

        foreach ((BTree<ColumnValue, BTreeTuple?> index, BTreeMutationDeltas<ColumnValue, BTreeTuple?> deltas) uniqueIndex in state.Indexes.UniqueIndexDeltas)
        {
            foreach (BTreeMvccEntry<BTreeTuple?> uniqueIndexEntry in uniqueIndex.deltas.Entries)
                uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

            await indexSaver.Persist(state.Database.BufferPool, uniqueIndex.index, state.ModifiedPages, uniqueIndex.deltas);
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// Write changes to the modified pages to disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(DeleteByIdFluxState state)
    {
        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            Console.WriteLine("Invalid row to delete {0}", state.Ticket.Id);
            return Task.FromResult(FluxAction.Abort);
        }

        state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to delete a record by id
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    public async Task<int> DeleteByIdInternal(FluxMachine<DeleteByIdFluxSteps, DeleteByIdFluxState> machine, DeleteByIdFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        DeleteByIdTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();
        
        machine.When(DeleteByIdFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteByIdFluxSteps.DeleteUniqueIndexes, DeleteUniqueIndexes);
        machine.When(DeleteByIdFluxSteps.DeleteMultiIndexes, DeleteMultiIndexes);
        machine.When(DeleteByIdFluxSteps.UpdateTableIndex, UpdateTableIndex);
        machine.When(DeleteByIdFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(DeleteByIdFluxSteps.ApplyPageOperations, ApplyPageOperations);

        // machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        if (state.RowTuple is null)
        {
            Console.WriteLine(
                "Row pk {0} not found, Time taken: {1}",
                ticket.Id,
                timeTaken.ToString(@"m\:ss\.fff")
            );

            return 0;
        }

        Console.WriteLine(
            "Row pk {0} with id {1} deleted from page offset {2}, Time taken: {3}, Modified pages: {4}",
            ticket.Id,
            state.RowTuple?.SlotOne,
            state.RowTuple?.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff"),
            state.ModifiedPages.Count
        );

        return 1;
    }
}
