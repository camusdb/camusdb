
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
internal sealed class RowDeleter
{
    private readonly IndexSaver indexSaver = new();

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

        return await DeleteInternal(machine, state);
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
    /// We need to locate the row tuples to delete
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> LocateTupleToDelete(DeleteFluxState state)
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
            parameters: null
        );

        state.DataCursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        return Task.FromResult(FluxAction.Continue);
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
    /// Deletes the row from disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteRowsAndIndexesFromDisk(DeleteFluxState state)
    {
        if (state.DataCursor is null)
        {
            Console.WriteLine("Invalid rows to delete");
            return FluxAction.Abort;
        }

        DeleteTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;        
        BufferPoolManager tablespace = state.Database.BufferPool;
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? mainTableDeltas;
        List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)>? uniqueIndexDeltas;

        // @todo we need to take a snapshot of the data to prevent deadlocks
        // but probably need to optimize this for larger datasets
        List<QueryResultRow> rowsToDelete = await state.DataCursor.ToListAsync();

        ObjectIdValue nullPageOffset = new();

        foreach (QueryResultRow row in rowsToDelete)
        {
            BTreeTuple tuple = new(row.Tuple.SlotOne, nullPageOffset);
            
            mainTableDeltas = await DeleteFromTableIndex(state, tuple);

            uniqueIndexDeltas = await UpdateUniqueIndexes(state, ticket, tuple, row);

            await PersistIndexChanges(state, mainTableDeltas, uniqueIndexDeltas);

            Console.WriteLine(
                "Row with rowid {0} deleted to page {1}",
                tuple.SlotOne,
                tuple.SlotTwo
            );

            state.DeletedRows++;
        }
        
        return FluxAction.Continue;
    }

    private async Task<BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>?> DeleteFromTableIndex(DeleteFluxState state, BTreeTuple tuple)
    {
        SaveUniqueOffsetIndexTicket saveUniqueOffsetIndex = new(
            index: state.Table.Rows,
            txnId: state.Ticket.TxnId,
            key: tuple.SlotOne,
            value: tuple.SlotTwo
        );

        // Main table index stores rowid pointing to page offset
        return await indexSaver.Save(saveUniqueOffsetIndex);
    }

    private async Task<List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)>> UpdateUniqueIndexes(DeleteFluxState state, DeleteTicket ticket, BTreeTuple tuple, QueryResultRow row)
    {
        List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)> deltas = new();

        //Console.WriteLine("Updating unique indexes {0}", state.Indexes.UniqueIndexes.Count);

        foreach (TableIndexSchema index in state.Indexes.UniqueIndexes)
        {
            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            ColumnValue? uniqueKeyValue = GetColumnValue(row.Row, index.Column);

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
                value: tuple
            );

            //Console.WriteLine("Saving unique index {0} {1} {2}", uniqueIndex, uniqueKeyValue, tuple);

            deltas.Add((uniqueIndex, await indexSaver.Save(saveUniqueIndexTicket)));
        }

        return deltas;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task PersistIndexChanges(DeleteFluxState state, BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? mainIndexDeltas, List<(BTree<ColumnValue, BTreeTuple?>, BTreeMutationDeltas<ColumnValue, BTreeTuple?>)> uniqueIndexDeltas)
    {
        if (mainIndexDeltas is null)
            return;

        foreach (BTreeMvccEntry<ObjectIdValue> btreeEntry in mainIndexDeltas.MvccEntries)
            btreeEntry.CommitState = BTreeCommitState.Committed;

        await indexSaver.Persist(state.Database.BufferPool, state.Table.Rows, state.ModifiedPages, mainIndexDeltas);

        if (uniqueIndexDeltas is null)
            return;

        foreach ((BTree<ColumnValue, BTreeTuple?> index, BTreeMutationDeltas<ColumnValue, BTreeTuple?> deltas) uniqueIndex in uniqueIndexDeltas)
        {
            foreach (BTreeMvccEntry<BTreeTuple?> uniqueIndexEntry in uniqueIndex.deltas.MvccEntries)
                uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

            await indexSaver.Persist(state.Database.BufferPool, uniqueIndex.index, state.ModifiedPages, uniqueIndex.deltas);
        }
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
        machine.When(DeleteFluxSteps.DeleteRows, DeleteRowsAndIndexesFromDisk);
        machine.When(DeleteFluxSteps.ApplyPageOperations, ApplyPageOperations);        

        // machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;        

        Console.WriteLine(
            "Deleted {0} rows, Time taken: {1}",
            state.DeletedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.DeletedRows;
    }    
}
