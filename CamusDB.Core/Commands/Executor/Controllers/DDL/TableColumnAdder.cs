
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
        KvTransaction tx,
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterColumnTicket ticket
    )
    {
        Validate(table, ticket);

        AlterColumnFluxState state = new(
            catalogs: catalogs,
            tx: tx,
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
        await state.Catalogs.AlterTable(state.Database, state.Ticket, state.Tx).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    private Task<FluxAction> LocateTuplesToAlterColumn(AlterColumnFluxState state)
    {
        AlterColumnTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnState: state.Tx,
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
        KvTransaction tx = state.Tx;

        await foreach (QueryResultRow row in state.DataCursor)
        {
            byte[] buffer = RowEncoder.Encode(table.Schema, row.Row, row.RowId);

            await table.Store.UpdateRow(tx, row.RowId, buffer).ConfigureAwait(false);

            state.ModifiedRows++;
        }

        return FluxAction.Continue;
    }

    /// <summary>
    /// Re-encodes every existing row in the table so that columns added after the row was
    /// written receive their default value.  Called by the cluster AddColumn path (D3) after
    /// the coordinator advances the column to <c>WriteOnly</c> state, at which point
    /// <see cref="RowEncoder.Encode"/> will include the column in the encoded bytes.
    /// </summary>
    internal async Task<int> BackfillColumnDefaultsAsync(
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterColumnTicket ticket,
        KvTransaction tx
    )
    {
        QueryTicket queryTicket = new(
            txnState: tx,
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

        int modifiedRows = 0;

        await foreach (QueryResultRow row in queryExecutor.Query(database, table, queryTicket))
        {
            // The query runs while the column is still WriteOnly (not yet readable), so
            // injectMissingCurrentColumns skips it. Explicitly inject the default so that
            // RowEncoder.Encode writes the value rather than TypeNull.
            if (!row.Row.ContainsKey(ticket.Column.Name))
                row.Row[ticket.Column.Name] = ticket.Column.Default ?? new(ColumnType.Null, 0L);

            byte[] buffer = RowEncoder.Encode(table.Schema, row.Row, row.RowId);
            await table.Store.UpdateRow(tx, row.RowId, buffer).ConfigureAwait(false);
            modifiedRows++;
        }

        Log.LogColumnBackfillComplete(logger, modifiedRows, ticket.Column.Name);

        return modifiedRows;
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

        Log.LogColumnAdded(logger, state.ModifiedRows, timeTaken);

        return state.ModifiedRows;
    }
}
