
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// 
/// </summary>
internal sealed class RowDeleter
{
    private readonly IndexSaver indexSaver = new();
    
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
            indexes: new DeleteFluxIndexState()
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
    /// Adquire locks
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> AdquireLocks(DeleteFluxState state)
    {
        state.Locks.Add(await state.Table.ReaderWriterLock.WriterLockAsync());
        return FluxAction.Continue;
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
            database: ticket.DatabaseName,
            name: ticket.TableName,
            index: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null
        );

        state.DataCursor = await state.QueryExecutor.Query(state.Database, state.Table, queryTicket, noLocking: true);

        return FluxAction.Continue;
    }

    /// <summary>
    /// Remove the references to the row from the unique indexes
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="columnValues"></param>
    /// <param name="modifiedPages"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private async Task DeleteUniqueIndexesInternal(
        DatabaseDescriptor database,
        TableDescriptor table,
        Dictionary<string, ColumnValue> columnValues,
        List<IDisposable> locks,
        List<BufferPageOperation> modifiedPages
    )
    {
        BufferPoolHandler tablespace = database.TableSpace;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo update in parallel
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (index.Value.UniqueRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            ColumnValue? columnKey = GetColumnValue(columnValues, index.Value.Column);
            if (columnKey is null) // @todo check what to to here
                continue;

            BTree<ColumnValue, BTreeTuple?> uniqueIndex = index.Value.UniqueRows;

            RemoveUniqueIndexTicket ticket = new(
                tablespace: tablespace,
                sequence: 0,
                subSequence: 0,
                index: uniqueIndex,
                key: columnKey,
                locks: locks,
                modifiedPages: modifiedPages
            );

            await indexSaver.Remove(ticket);
        }
    }

    private async Task DeleteMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolHandler tablespace = database.TableSpace;

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
    /// All locks are released once the operation is successful
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
    /// Deletes unique indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> DeleteUniqueIndexes(DeleteFluxState state)
    {
        //await DeleteUniqueIndexesInternal(state.Database, state.Table, state.ColumnValues, state.Locks, state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Deletes multi indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> DeleteMultiIndexes(DeleteByIdFluxState state)
    {
        //await DeleteMultiIndexes(state.Database, state.Table, state.ColumnValues);        

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Deletes the row from disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteRowsFromDisk(DeleteFluxState state)
    {
        if (state.DataCursor is null)
        {
            Console.WriteLine("Invalid rows to delete");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;        
        BufferPoolHandler tablespace = state.Database.TableSpace;

        // @todo we need to take a snapshot of the data but probably need to optimize this for larger datasets
        List<QueryResultRow> rowsToDelete = await state.DataCursor.ToListAsync(); 

        foreach (QueryResultRow row in rowsToDelete)
        {           
            RemoveUniqueOffsetIndexTicket removeIndexTicket = new(
                tablespace: tablespace,
                index: table.Rows,
                key: row.Tuple.SlotOne,
                locks: state.Locks,
                modifiedPages: state.ModifiedPages
            );

            await indexSaver.Remove(removeIndexTicket);

            //await tablespace.DeletePage(row.Tuple.SlotTwo);

            state.DeletedRows++;
        }
        
        return FluxAction.Continue;
    }

    private Task<FluxAction> ApplyPageOperations(DeleteFluxState state)
    {
        if (state.ModifiedPages.Count > 0)
            state.Database.TableSpace.ApplyPageOperations(state.ModifiedPages);

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
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        DeleteTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(DeleteFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(DeleteFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteFluxSteps.DeleteUniqueIndexes, DeleteUniqueIndexes);
        //machine.When(DeleteFluxSteps.DeleteMultiIndexes, DeleteMultiIndexes);
        machine.When(DeleteFluxSteps.DeleteRow, DeleteRowsFromDisk);
        machine.When(DeleteFluxSteps.ApplyPageOperations, ApplyPageOperations);
        machine.When(DeleteFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

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
