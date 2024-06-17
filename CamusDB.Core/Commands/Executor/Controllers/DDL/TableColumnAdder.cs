
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Util.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

public sealed class TableColumnAdder
{
    private readonly ILogger<ICamusDB> logger;

    private readonly RowSerializer rowSerializer = new();

    public TableColumnAdder(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void Validate(TableDescriptor table, AlterColumnTicket ticket)
    {
        bool hasColumn = false;

        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (column.Name == ticket.Column.Name)
            {
                hasColumn = true;
                break;
            }
        }

        if (hasColumn)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Duplicate column '{ticket.Column.Name}'"
            );
    }

    /// <summary>
    /// Schedules a new AlterColumn operation by the specified filters
    /// </summary>
    /// <param name="catalogs"></param>
    /// <param name="queryExecutor"></param>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> AddColumn(
        CatalogsManager catalogs,
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterColumnTicket ticket
    )
    {
        Validate(table, ticket);

        AlterColumnFluxState state = new(
            catalogs: catalogs,
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: new AlterColumnFluxIndexState()
        );

        FluxMachine<AlterColumnFluxSteps, AlterColumnFluxState> machine = new(state);

        return await AlterColumnInternal(machine, state).ConfigureAwait(false);
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
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> AlterSchema(AlterColumnFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        CatalogsManager catalogs = state.Catalogs;
        AlterColumnTicket ticket = state.Ticket;

        await catalogs.AlterTable(database, ticket).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// We need to locate the row tuples to AlterColumn
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> LocateTuplesToAlterColumn(AlterColumnFluxState state)
    {
        AlterColumnTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
            txnType: TransactionType.Write,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        state.DataCursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        //Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);*/

        return Task.FromResult(FluxAction.Continue);
    }

    private async Task AlterColumnMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolManager tablespace = database.BufferPool;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo AlterColumn in parallel
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.BTree is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            //ColumnValue? columnValue = GetColumnValue(columnValues, index.Value.Columns);
            //if (columnValue is null) // @todo check what to to here
            //continue;

            //BTreeMulti<ColumnValue> multiIndex = index.Value.MultiRows;

            //await indexSaver.Remove(tablespace, multiIndex, columnValue);

            await Task.CompletedTask;

            throw new NotImplementedException();
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
            logger.LogWarning("Invalid rows to AlterColumn");

            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        BufferPoolManager tablespace = state.Database.BufferPool;

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
            state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to AlterColumn records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<int> AlterColumnInternal(FluxMachine<AlterColumnFluxSteps, AlterColumnFluxState> machine, AlterColumnFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(AlterColumnFluxSteps.AlterSchema, AlterSchema);
        machine.When(AlterColumnFluxSteps.LocateTupleToAlterColumn, LocateTuplesToAlterColumn);
        machine.When(AlterColumnFluxSteps.UpdateUniqueIndexes, AlterColumnUniqueIndexes);
        machine.When(AlterColumnFluxSteps.UpdateMultiIndexes, AlterColumnMultiIndexes);
        machine.When(AlterColumnFluxSteps.AlterColumnRow, AlterColumnRowsFromDisk);
        machine.When(AlterColumnFluxSteps.ApplyPageOperations, ApplyPageOperations);

        //machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogInformation(
            "Column drop, modified {ModifiedRows} rows, Time taken: {Time}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
