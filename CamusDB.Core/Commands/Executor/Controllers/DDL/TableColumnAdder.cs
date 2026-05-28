
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Flux;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

public sealed class TableColumnAdder
{
    private readonly ILogger<ICamusDB> logger;

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

    private async Task<FluxAction> AlterSchema(AlterColumnFluxState state)
    {
        await state.Catalogs.AlterTable(state.Database, state.Ticket).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    private Task<FluxAction> LocateTuplesToAlterColumn(AlterColumnFluxState state)
    {
        AlterColumnTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnState: ticket.TxnState,
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

        return Task.FromResult(FluxAction.Continue);
    }

    private Task<FluxAction> AlterColumnUniqueIndexes(AlterColumnFluxState state)
    {
        return Task.FromResult(FluxAction.Continue);
    }

    private Task<FluxAction> AlterColumnMultiIndexes(AlterColumnFluxState state)
    {
        return Task.FromResult(FluxAction.Continue);
    }

    private async Task<FluxAction> AlterColumnRowsFromDisk(AlterColumnFluxState state)
    {
        if (state.DataCursor is null)
        {
            logger.LogWarning("Invalid rows to AlterColumn");
            return FluxAction.Abort;
        }

        TableDescriptor table = state.Table;
        KvTransaction tx = state.Ticket.TxnState;

        await foreach (QueryResultRow row in state.DataCursor)
        {
            byte[] buffer = RowEncoder.Encode(table.Schema, row.Row, row.RowId);

            await table.Store.UpdateRow(tx, row.RowId, buffer).ConfigureAwait(false);

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    private async Task<int> AlterColumnInternal(FluxMachine<AlterColumnFluxSteps, AlterColumnFluxState> machine, AlterColumnFluxState state)
    {
        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(AlterColumnFluxSteps.AlterSchema, AlterSchema);
        machine.When(AlterColumnFluxSteps.LocateTupleToAlterColumn, LocateTuplesToAlterColumn);
        machine.When(AlterColumnFluxSteps.UpdateUniqueIndexes, AlterColumnUniqueIndexes);
        machine.When(AlterColumnFluxSteps.UpdateMultiIndexes, AlterColumnMultiIndexes);
        machine.When(AlterColumnFluxSteps.AlterColumnRow, AlterColumnRowsFromDisk);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogInformation(
            "Column added, modified {ModifiedRows} rows, Time taken: {Time}",
            state.ModifiedRows,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
