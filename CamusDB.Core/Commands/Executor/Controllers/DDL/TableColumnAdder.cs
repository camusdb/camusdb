
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

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

public sealed class TableColumnAdder
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    /// <summary>
    /// Schedules a new AlterColumn operation by the specified filters
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> AddColumn(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterColumnTicket ticket)
    {
        //Validate(table, ticket);

        AlterColumnFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: new AlterColumnFluxIndexState()
        );

        FluxMachine<AlterColumnFluxSteps, AlterColumnFluxState> machine = new(state);

        return await AlterColumnInternal(machine, state);
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
    private async Task<FluxAction> AdquireLocks(AlterColumnFluxState state)
    {
        state.Locks.Add(await state.Table.ReaderWriterLock.WriterLockAsync());
        return FluxAction.Continue;
    }

    /// <summary>
    /// We need to locate the row tuples to AlterColumn
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTuplesToAlterColumn(AlterColumnFluxState state)
    {
        AlterColumnTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnId: ticket.TxnId,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            filters: null,
            where: null,
            orderBy: null
        );

        state.DataCursor = await state.QueryExecutor.Query(state.Database, state.Table, queryTicket, noLocking: true);

        //Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);*/

        return FluxAction.Continue;
    }

    private async Task AlterColumnMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo AlterColumn in parallel
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
    /// AlterColumns unique indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> AlterColumnUniqueIndexes(AlterColumnFluxState state)
    {
        //await AlterColumnUniqueIndexesInternal(state.Database, state.Table, state.ColumnValues, state.Locks, state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// AlterColumns multi indexes
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> AlterColumnMultiIndexes(AlterColumnFluxState state)
    {
        //await AlterColumnMultiIndexes(state.Database, state.Table, state.ColumnValues);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// AlterColumns the row on the disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> AlterColumnRowsFromDisk(AlterColumnFluxState state)
    {
        if (state.DataCursor is null)
        {
            Console.WriteLine("Invalid rows to AlterColumn");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        AlterColumnTicket ticket = state.Ticket;
        BufferPoolHandler tablespace = state.Database.TableSpace;

        await foreach (QueryResultRow row in state.DataCursor)
        {
            //foreach (KeyValuePair<string, ColumnValue> keyValuePair in ticket.Values)
            //    row.Row[keyValuePair.Key] = keyValuePair.Value;

            byte[] buffer = rowSerializer.Serialize(table, row.Row, row.Tuple.SlotOne);

            tablespace.WriteDataToPageBatch(state.ModifiedPages, row.Tuple.SlotTwo, 0, buffer);

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(AlterColumnFluxState state)
    {
        if (state.ModifiedPages.Count > 0)
            state.Database.TableSpace.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// All locks are released once the operation is successful
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ReleaseLocks(AlterColumnFluxState state)
    {
        foreach (IDisposable disposable in state.Locks)
            disposable.Dispose();

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to AlterColumn records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    internal async Task<int> AlterColumnInternal(FluxMachine<AlterColumnFluxSteps, AlterColumnFluxState> machine, AlterColumnFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolHandler tablespace = state.Database.TableSpace;
        TableDescriptor table = state.Table;
        AlterColumnTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(AlterColumnFluxSteps.AdquireLocks, AdquireLocks);
        machine.When(AlterColumnFluxSteps.LocateTupleToAlterColumn, LocateTuplesToAlterColumn);
        machine.When(AlterColumnFluxSteps.UpdateUniqueIndexes, AlterColumnUniqueIndexes);
        machine.When(AlterColumnFluxSteps.UpdateMultiIndexes, AlterColumnMultiIndexes);
        machine.When(AlterColumnFluxSteps.AlterColumnRow, AlterColumnRowsFromDisk);
        machine.When(AlterColumnFluxSteps.ApplyPageOperations, ApplyPageOperations);
        machine.When(AlterColumnFluxSteps.ReleaseLocks, ReleaseLocks);

        machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine(
            "Column drop, modified {0} rows, Time taken: {1}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
