
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
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// 
/// </summary>
internal sealed class RowDeleterById
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowDeserializer rowDeserializer = new();

    private readonly ILogger<ICamusDB> logger;

    public RowDeleterById(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

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

        return await DeleteByIdInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// Step #1. Creates a new update plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static DeleteByIdFluxIndexState GetIndexDeletePlan(TableDescriptor table)
    {
        DeleteByIdFluxIndexState indexState = new();

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
    /// 
    /// </summary>
    /// <param name="columnValues"></param>
    /// <param name="columnNames"></param>
    /// <returns></returns>
    private static CompositeColumnValue GetColumnValue(Dictionary<string, ColumnValue> rowValues, string[] columnNames)
    {
        ColumnValue[] columnValues = new ColumnValue[columnNames.Length];

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

        return new CompositeColumnValue(columnValues);
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

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        using IDisposable _ = await index.BTree.ReaderLockAsync().ConfigureAwait(false);

        state.RowTuple = await index.BTree.Get(TransactionType.Write, ticket.TxnState.TxnId, new CompositeColumnValue(columnId)).ConfigureAwait(false);

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            //Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            return FluxAction.Abort;
        }

        byte[] data = await tablespace.GetDataFromPage(state.RowTuple.SlotTwo).ConfigureAwait(false);
        if (data.Length == 0)
        {
            //Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return FluxAction.Abort;
        }

        state.ColumnValues = rowDeserializer.Deserialize(table.Schema, state.RowTuple.SlotOne, data);

        //Console.WriteLine("Data to Delete Pk={0} is at page offset {1}/{2}", ticket.Id, state.RowTuple.SlotOne, state.RowTuple.SlotTwo);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Acquire write locks on the indices to ensure consistency in writing.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    private async Task<FluxAction> AdquireLocks(DeleteByIdFluxState state)
    {
        state.Locks.Add(await state.Table.Rows.WriterLockAsync().ConfigureAwait(false));

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
            state.Locks.Add(await index.BTree.WriterLockAsync().ConfigureAwait(false));

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
            state.Locks.Add(await index.BTree.WriterLockAsync().ConfigureAwait(false));

        return FluxAction.Continue;
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
            logger.LogWarning("Invalid row to Update {Id}", state.Ticket.Id);

            return FluxAction.Abort;
        }

        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: state.Database.BufferPool,
            index: state.Table.Rows,
            txnId: state.Ticket.TxnState.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: state.RowTuple.SlotOne,
            value: new ObjectIdValue(),
            modifiedPages: state.ModifiedPages
        );

        // Main table index stores rowid pointing to null offset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

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
            logger.LogWarning("Index Pk={Id} does not exist", ticket.Id);

            return FluxAction.Abort;
        }

        BTreeTuple nullTuple = new(new(), new());
        List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BTree<CompositeColumnValue, BTreeTuple>? uniqueIndex = index.BTree;            

            CompositeColumnValue uniqueKeyValue = GetColumnValue(state.ColumnValues, index.Columns);            

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: uniqueIndex,
                txnId: ticket.TxnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: nullTuple,
                modifiedPages: state.ModifiedPages
            );

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            deltas.Add((uniqueIndex, uniqueKeyValue, nullTuple));
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
        DeleteByIdTicket ticket = state.Ticket;

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            logger.LogWarning("Index Pk={Id} does not exist", ticket.Id);

            return FluxAction.Abort;
        }

        BTreeTuple nullTuple = new(new(), new());
        List<(BTree<CompositeColumnValue, BTreeTuple>, CompositeColumnValue, BTreeTuple)> deltas = new();

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
        {
            BTree<CompositeColumnValue, BTreeTuple>? multiIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(state.ColumnValues, index.Columns);

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: multiIndex,
                txnId: ticket.TxnState.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: nullTuple,
                modifiedPages: state.ModifiedPages
            );

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            deltas.Add((multiIndex, multiKeyValue, nullTuple));
        }

        state.Indexes.MultiIndexDeltas = deltas;

        return FluxAction.Continue;
    }        

    /// <summary>
    /// Commit the changes in the indices after being sure that the update had no issues.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(DeleteByIdFluxState state)
    {
        DeleteByIdTicket ticket = state.Ticket;

        if (state.RowTuple is null || state.RowTuple.IsNull())
        {
            logger.LogWarning("Index Pk={Id} does not exist", ticket.Id);

            return FluxAction.Abort;
        }

        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: state.Database.BufferPool,
            index: state.Table.Rows,
            txnId: state.Ticket.TxnState.TxnId,
            commitState: BTreeCommitState.Committed,
            key: state.RowTuple.SlotOne,
            value: new ObjectIdValue(),
            modifiedPages: state.ModifiedPages
        );

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

        if (state.Indexes.UniqueIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue uniqueKeyValue, BTreeTuple tuple) uniqueIndex in state.Indexes.UniqueIndexDeltas)
            {
                SaveIndexTicket saveUniqueIndexTicket = new(
                    tablespace: state.Database.BufferPool,
                    index: uniqueIndex.index,
                    txnId: state.Ticket.TxnState.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: uniqueIndex.uniqueKeyValue,
                    value: uniqueIndex.tuple,
                    modifiedPages: state.ModifiedPages
                );

                await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);
            }
        }

        if (state.Indexes.MultiIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue multiKeyValue, BTreeTuple tuple) multiIndex in state.Indexes.MultiIndexDeltas)
            {
                SaveIndexTicket saveMultiIndexTicket = new(
                    tablespace: state.Database.BufferPool,
                    index: multiIndex.index,
                    txnId: state.Ticket.TxnState.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: multiIndex.multiKeyValue,
                    value: multiIndex.tuple,
                    modifiedPages: state.ModifiedPages
                );

                await indexSaver.Save(saveMultiIndexTicket).ConfigureAwait(false);
            }
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
            logger.LogWarning("Invalid row to delete {Id}", state.Ticket.Id);

            return Task.FromResult(FluxAction.Abort);
        }

        state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Release all the locks acquired in the previous steps
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ReleaseLocks(DeleteByIdFluxState state)
    {
        foreach (IDisposable disposable in state.Locks)
            disposable.Dispose();

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
        machine.When(DeleteByIdFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(DeleteByIdFluxSteps.DeleteUniqueIndexes, DeleteUniqueIndexes);
        machine.When(DeleteByIdFluxSteps.DeleteMultiIndexes, DeleteMultiIndexes);
        machine.When(DeleteByIdFluxSteps.UpdateTableIndex, UpdateTableIndex);
        machine.When(DeleteByIdFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(DeleteByIdFluxSteps.ApplyPageOperations, ApplyPageOperations);
        machine.When(DeleteByIdFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        if (state.RowTuple is null)
        {
            logger.LogWarning(
                "Row pk {Id} not found, Time taken: {Time}",
                ticket.Id,
                timeTaken.ToString(@"m\:ss\.fff")
            );

            return 0;
        }

        logger.LogInformation(
            "Row pk {Id} with id {SlowOne} deleted from page offset {SlotTwo}, Time taken: {Time}, Modified pages: {Modified}",
            ticket.Id,
            state.RowTuple?.SlotOne,
            state.RowTuple?.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff"),
            state.ModifiedPages.Count
        );

        return 1;
    }
}
