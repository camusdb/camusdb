
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
internal sealed class RowDeleter
{
    private readonly ILogger<ICamusDB> logger;

    private readonly IndexSaver indexSaver = new();    

    public RowDeleter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Step #1. Creates a new delete plan for the table defining which unique indexes will be updated
    /// </summary>
    /// <param name="table"></param>
    /// <returns></returns>
    private static DeleteFluxIndexState GetIndexDeletePlan(TableDescriptor table, DeleteTicket ticket)
    {
        DeleteFluxIndexState indexState = new();

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            indexState.UniqueIndexes.Add(index.Value);
        }

        return indexState;
    }

    /// <summary>
    /// Schedules a new delete operation by the specified filter criteria
    /// </summary>
    /// <param name="queryExecutor"></param>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns>The number of deleted rows</returns>
    public async Task<int> Delete(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, DeleteTicket ticket)
    {
        DeleteFluxState state = new(
            queryExecutor: queryExecutor,
            database: database,
            table: table,
            ticket: ticket,
            indexes: GetIndexDeletePlan(table, ticket)
        );

        FluxMachine<DeleteFluxSteps, DeleteFluxState> machine = new(state);

        return await DeleteInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="rowValues"></param>
    /// <param name="columnNames"></param>
    /// <returns></returns>
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
    /// We need to locate the row tuples to delete
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTupleToDelete(DeleteFluxState state)
    {
        DeleteTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnId: ticket.TxnId,
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
            parameters: null
        );

        var cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        // @todo we need to take a snapshot of the data to prevent deadlocks
        // but probably need to optimize this for larger datasets
        state.RowsToDelete = await cursor.ToListAsync().ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Acquire write locks on the indices to ensure consistency in writing.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    private async Task<FluxAction> AdquireLocks(DeleteFluxState state)
    {
        state.Locks.Add(await state.Table.Rows.WriterLockAsync().ConfigureAwait(false));

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
            state.Locks.Add(await index.BTree.WriterLockAsync().ConfigureAwait(false));

        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
            state.Locks.Add(await index.BTree.WriterLockAsync().ConfigureAwait(false));

        return FluxAction.Continue;
    }

    /// <summary>
    /// Deletes the row from disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.RowsToDelete is null || state.RowsToDelete.Count == 0)
        {
            logger.LogError("Invalid rows to delete");

            return FluxAction.Abort;
        }

        DeleteTicket ticket = state.Ticket;        

        ObjectIdValue nullPageOffset = new();

        foreach (QueryResultRow row in state.RowsToDelete)
        {
            BTreeTuple tuple = new(row.Tuple.SlotOne, nullPageOffset);

            await DeleteFromTableIndex(state, tuple).ConfigureAwait(false);

            await UpdateUniqueIndexes(state, ticket, tuple, row).ConfigureAwait(false);

            await UpdateMultiIndexes(state, ticket, tuple, row).ConfigureAwait(false);            

            logger.LogInformation(
                "Row with rowid {SlotOne} deleted to tombstone page {SlotTwo}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.DeletedRows++;
        }

        return FluxAction.Continue;
    }

    private async Task DeleteFromTableIndex(DeleteFluxState state, BTreeTuple tuple)
    {
        SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
            tablespace: state.Database.BufferPool,
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: tuple.SlotOne,
            value: tuple.SlotTwo,
            modifiedPages: state.ModifiedPages
        );

        // Main table index stores rowid pointing to page offset
        await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);

        state.Indexes.MainTableDeltas.Add(tuple);
    }

    private async Task UpdateUniqueIndexes(
        DeleteFluxState state,
        DeleteTicket ticket,
        BTreeTuple tuple,
        QueryResultRow row
    )
    {        
        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue? uniqueKeyValue = GetColumnValue(row.Row, index.Columns);            

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: uniqueIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: uniqueKeyValue,
                value: tuple,
                modifiedPages: state.ModifiedPages
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            state.Indexes.UniqueIndexDeltas.Add((uniqueIndex, uniqueKeyValue, tuple));
        }        
    }

    private async Task UpdateMultiIndexes(
        DeleteFluxState state,
        DeleteTicket ticket,
        BTreeTuple tuple,
        QueryResultRow row
    )
    {        
        foreach (TableIndexSchema index in state.Indexes.MultiIndexes)
        {
            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex = index.BTree;

            CompositeColumnValue multiKeyValue = GetColumnValue(row.Row, index.Columns, new ColumnValue(ColumnType.Id, tuple.SlotOne.ToString()));            

            SaveIndexTicket saveUniqueIndexTicket = new(
                tablespace: state.Database.BufferPool,
                index: uniqueIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: multiKeyValue,
                value: tuple,
                modifiedPages: state.ModifiedPages
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

            state.Indexes.MultiIndexDeltas.Add((uniqueIndex, multiKeyValue, tuple));
        }        
    }

    /// <summary>
    /// Commit the changes in the indices after being sure that the update had no issues.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(DeleteFluxState state)
    {
        foreach (BTreeTuple tuple in state.Indexes.MainTableDeltas)
        {
            SaveOffsetIndexTicket saveUniqueOffsetIndex = new(
               tablespace: state.Database.BufferPool,
               index: state.Table.Rows,
               txnId: state.Ticket.TxnId,
               commitState: BTreeCommitState.Committed,
               key: tuple.SlotOne,
               value: tuple.SlotTwo,
               modifiedPages: state.ModifiedPages
           );

            // Main table index stores rowid pointing to page offeset
            await indexSaver.Save(saveUniqueOffsetIndex).ConfigureAwait(false);
        }

        if (state.Indexes.UniqueIndexDeltas is not null)
        {
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue uniqueKeyValue, BTreeTuple tuple) uniqueIndex in state.Indexes.UniqueIndexDeltas)
            {
                SaveIndexTicket saveUniqueIndexTicket = new(
                    tablespace: state.Database.BufferPool,
                    index: uniqueIndex.index,
                    txnId: state.Ticket.TxnId,
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
            foreach ((BTree<CompositeColumnValue, BTreeTuple> index, CompositeColumnValue multiKeyValue, BTreeTuple tuple) multIndex in state.Indexes.MultiIndexDeltas)
            {
                SaveIndexTicket saveMultiIndexTicket = new(
                    tablespace: state.Database.BufferPool,
                    index: multIndex.index,
                    txnId: state.Ticket.TxnId,
                    commitState: BTreeCommitState.Committed,
                    key: multIndex.multiKeyValue,
                    value: multIndex.tuple,
                    modifiedPages: state.ModifiedPages
                );

                await indexSaver.Save(saveMultiIndexTicket).ConfigureAwait(false);
            }
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(DeleteFluxState state)
    {
        if (state.ModifiedPages.Count > 0)
            state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Release all the locks acquired in the previous steps
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ReleaseLocks(DeleteFluxState state)
    {
        foreach (IDisposable disposable in state.Locks)
            disposable.Dispose();

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to delete a set of records that match the specified criteria
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    public async Task<int> DeleteInternal(FluxMachine<DeleteFluxSteps, DeleteFluxState> machine, DeleteFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        DeleteTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(DeleteFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(DeleteFluxSteps.DeleteRowsAndIndexesFromDisk, DeleteRowsAndIndexesFromDisk);
        machine.When(DeleteFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(DeleteFluxSteps.ApplyPageOperations, ApplyPageOperations);        
        machine.When(DeleteFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
            "Deleted {0} rows, Time taken: {1}",
            state.DeletedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.DeletedRows;
    }
}
