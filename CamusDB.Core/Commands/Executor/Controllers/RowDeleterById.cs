
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
            indexes: new DeleteByIdFluxIndexState()
        );

        FluxMachine<DeleteByIdFluxSteps, DeleteByIdFluxState> machine = new(state);

        return await DeleteByIdInternal(machine, state);
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
        BufferPoolHandler tablespace = state.Database.TableSpace;
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

        if (state.RowTuple is null)
        {
            Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            return FluxAction.Abort;
        }

        (int length, List<BufferPage> pages, List<IDisposable> disposables) = await tablespace.GetPagesToWrite(state.RowTuple.SlotTwo);
        if (length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return FluxAction.Abort;
        }

        state.Locks.AddRange(disposables);

        byte[] data = tablespace.GetDataFromPageDirect(length, pages);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return FluxAction.Abort;
        }

        state.Pages = pages;
        state.ColumnValues = rowDeserializer.Deserialize(table.Schema!, data);

        Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);

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
    private Task<FluxAction> ReleaseLocks(DeleteByIdFluxState state)
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
    private async Task<FluxAction> DeleteUniqueIndexes(DeleteByIdFluxState state)
    {
        await DeleteUniqueIndexesInternal(state.Database, state.Table, state.ColumnValues, state.Locks, state.ModifiedPages);

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

    /// <summary>
    /// Deletes the row from disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteRowFromDisk(DeleteByIdFluxState state)
    {
        if (state.RowTuple is null || state.Pages is null)
        {
            Console.WriteLine("Invalid row to delete {0}", state.Ticket.Id);
            return FluxAction.Abort;
        }

        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;

        RemoveUniqueOffsetIndexTicket ticket = new(
            tablespace: tablespace,
            index: table.Rows,
            key: state.RowTuple.SlotOne,
            locks: state.Locks,
            modifiedPages: state.ModifiedPages
        );

        await indexSaver.Remove(ticket);

        foreach (BufferPage page in state.Pages)
            state.ModifiedPages.Add(new BufferPageOperation(BufferPageOperationType.Delete, page.Offset, 0, page.Buffer.Value));

        return FluxAction.Continue;
    }

    private Task<FluxAction> ApplyPageOperations(DeleteByIdFluxState state)
    {
        if (state.RowTuple is null || state.Pages is null)
        {
            Console.WriteLine("Invalid row to delete {0}", state.Ticket.Id);
            return Task.FromResult(FluxAction.Abort);
        }

        state.Database.TableSpace.ApplyPageOperations(state.ModifiedPages);

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
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        DeleteByIdTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();
        
        machine.When(DeleteByIdFluxSteps.LocateTupleToDelete, LocateTupleToDelete);
        machine.When(DeleteByIdFluxSteps.DeleteUniqueIndexes, DeleteUniqueIndexes);
        machine.When(DeleteByIdFluxSteps.DeleteMultiIndexes, DeleteMultiIndexes);
        machine.When(DeleteByIdFluxSteps.DeleteRow, DeleteRowFromDisk);
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
