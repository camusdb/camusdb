
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

public sealed class RowUpdaterById
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private readonly RowDeserializer rowDeserializer = new();

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

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
        {
            bool hasColumn = false;

            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];

                if (string.IsNullOrEmpty(columnValue.Key))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid or empty column name in values list");

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

        foreach (TableColumnSchema columnSchema in columns)
        {
            if (!columnSchema.NotNull)
                continue;

            if (!ticket.Values.TryGetValue(columnSchema.Name, out ColumnValue? columnValue))
                continue;

            if (columnValue.Value is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.NotNullViolation,
                    $"Column '{columnSchema.Name}' cannot be null"
                );
            }
        }
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
            indexes: new UpdateByIdFluxIndexState()
        );

        FluxMachine<UpdateByIdFluxSteps, UpdateByIdFluxState> machine = new(state);

        return await UpdateByIdInternal(machine, state);
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
    private async Task<FluxAction> AdquireLocks(UpdateByIdFluxState state)
    {
        state.Locks.Add(await state.Table.ReaderWriterLock.WriterLockAsync());
        return FluxAction.Continue;
    }

    /// <summary>
    /// We need to locate the row tuple to Update
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTupleToUpdate(UpdateByIdFluxState state)
    {
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        UpdateByIdTicket ticket = state.Ticket;

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

        state.RowTuple = await index.UniqueRows.Get(columnId);

        if (state.RowTuple is null)
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

        Console.WriteLine("Data Pk={0} is at page offset {1}/{2}", ticket.Id, state.RowTuple.SlotOne, state.RowTuple.SlotTwo);

        return FluxAction.Continue;
    }    

    private async Task UpdateMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
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
    private Task<FluxAction> ReleaseLocks(UpdateByIdFluxState state)
    {
        foreach (IDisposable disposable in state.Locks)
            disposable.Dispose();

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Updates unique indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> UpdateUniqueIndexes(UpdateByIdFluxState state)
    {
        //await UpdateUniqueIndexesInternal(state.Database, state.Table, state.ColumnValues, state.Locks, state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Updates multi indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> UpdateMultiIndexes(UpdateByIdFluxState state)
    {
        //await UpdateMultiIndexes(state.Database, state.Table, state.ColumnValues);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Updates the row on the disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> UpdateRowFromDisk(UpdateByIdFluxState state)
    {
        if (state.RowTuple is null)
        {
            Console.WriteLine("Invalid row to Update {0}", state.Ticket.Id);
            return Task.FromResult(FluxAction.Abort);
        }

        TableDescriptor table = state.Table;
        UpdateByIdTicket ticket = state.Ticket;

        foreach (KeyValuePair<string, ColumnValue> keyValuePair in ticket.Values)
            state.ColumnValues[keyValuePair.Key] = keyValuePair.Value;

        byte[] buffer = rowSerializer.Serialize(table, state.ColumnValues, state.RowTuple.SlotOne);

        //await tablespace.WriteDataToPage(state.RowTuple.SlotOne, 0, buffer);

        state.ModifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, state.RowTuple.SlotTwo, 0, buffer));

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
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        UpdateByIdTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(UpdateByIdFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(UpdateByIdFluxSteps.LocateTupleToUpdate, LocateTupleToUpdate);
        machine.When(UpdateByIdFluxSteps.UpdateUniqueIndexes, UpdateUniqueIndexes);
        machine.When(UpdateByIdFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(UpdateByIdFluxSteps.UpdateRow, UpdateRowFromDisk);
        machine.When(UpdateByIdFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

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

        await state.Database.TableSpace.ApplyPageOperations(state.ModifiedPages);

        Console.WriteLine(
            "Row pk {0} with id {1} updated to page {2}, Time taken: {3}",
            ticket.Id,
            state.RowTuple?.SlotOne,
            state.RowTuple?.SlotTwo,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return 1;
    }    
}
