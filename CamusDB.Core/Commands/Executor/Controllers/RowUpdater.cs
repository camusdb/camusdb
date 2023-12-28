
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
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class RowUpdater
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
    private static void Validate(TableDescriptor table, UpdateTicket ticket) // @todo optimize this
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
    /// Schedules a new Update operation by the specified filters
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> Update(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, UpdateTicket ticket)
    {
        Validate(table, ticket);

        UpdateFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: new UpdateFluxIndexState()
        );

        FluxMachine<UpdateFluxSteps, UpdateFluxState> machine = new(state);

        return await UpdateInternal(machine, state);
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
    private async Task<FluxAction> AdquireLocks(UpdateFluxState state)
    {
        state.Locks.Add(await state.Table.ReaderWriterLock.WriterLockAsync());
        return FluxAction.Continue;
    }

    /// <summary>
    /// We need to locate the row tuples to Update
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTuplesToUpdate(UpdateFluxState state)
    {
        UpdateTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            database: ticket.DatabaseName,
            name: ticket.TableName,
            index: null,
            filters: ticket.Filters,
            where: ticket.Where,
            orderBy: null
        );

        state.DataCursor = await state.QueryExecutor.Query(state.Database, state.Table, queryTicket, noLocking: true);

        //Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);*/

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
    private Task<FluxAction> ReleaseLocks(UpdateFluxState state)
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
    private Task<FluxAction> UpdateUniqueIndexes(UpdateFluxState state)
    {
        //await UpdateUniqueIndexesInternal(state.Database, state.Table, state.ColumnValues, state.Locks, state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Updates multi indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> UpdateMultiIndexes(UpdateFluxState state)
    {
        //await UpdateMultiIndexes(state.Database, state.Table, state.ColumnValues);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Updates the row on the disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> UpdateRowsFromDisk(UpdateFluxState state)
    {
        if (state.DataCursor is null)
        {
            Console.WriteLine("Invalid rows to update");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;

        await foreach (QueryResultRow row in state.DataCursor)
        {
            foreach (KeyValuePair<string, ColumnValue> keyValuePair in ticket.Values)
                row.Row[keyValuePair.Key] = keyValuePair.Value;

            byte[] buffer = rowSerializer.Serialize(table, row.Row, row.Tuple.SlotOne);

            state.ModifiedPages.Add(new InsertModifiedPage(row.Tuple.SlotTwo, 0, buffer));

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// Executes the flux state machine to update records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    internal async Task<int> UpdateInternal(FluxMachine<UpdateFluxSteps, UpdateFluxState> machine, UpdateFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        UpdateTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(UpdateFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(UpdateFluxSteps.LocateTupleToUpdate, LocateTuplesToUpdate);
        machine.When(UpdateFluxSteps.UpdateUniqueIndexes, UpdateUniqueIndexes);
        machine.When(UpdateFluxSteps.UpdateMultiIndexes, UpdateMultiIndexes);
        machine.When(UpdateFluxSteps.UpdateRow, UpdateRowsFromDisk);
        machine.When(UpdateFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;
        
        if (state.ModifiedPages.Count > 0)
            await state.Database.TableSpace.WriteDataToPages(state.ModifiedPages);

        Console.WriteLine(
            "Updated {0} rows, Time taken: {1}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
