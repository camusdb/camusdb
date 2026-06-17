
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Flux;
using CamusDB.Core.Transactions;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class TableIndexDropper
{
    private readonly ILogger<ICamusDB> logger;

    public TableIndexDropper(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void Validate(TableDescriptor table, AlterIndexTicket ticket)
    {
        if (!table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Index '{ticket.IndexName}' does not exist in table '{table.Name}'"
            );
    }

    private async Task<FluxAction> RemoveSystemObject(DropIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;

        // Delete all KV entries for the index before removing it from the schema.
        // Both happen in state.Tx so the purge is atomic with the schema update.
        int purged = await table.Store.DropIndexEntries(state.Tx, ticket.IndexName).ConfigureAwait(false);
        Log.LogIndexEntriesPurged(logger, purged, ticket.IndexName);

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            table.Schema.Indexes?.RemoveAll(ix => ix.Name == ticket.IndexName);

            // table.Schema.Version is not bumped: indexes are not part of the row encoding.
            // On the cluster path, PersistSchemaCheckpointAsync is the authoritative write.
            if (database.OwnsKahuna)
                await state.Catalogs.PersistSchemaTableAsync(database, table.Schema, state.Tx).ConfigureAwait(false);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        table.Indexes.Remove(ticket.IndexName);

        return FluxAction.Continue;
    }

    internal async Task<int> DropIndex(CatalogsManager catalogs, KvTransaction tx, QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        Validate(table, ticket);

        DropIndexFluxState state = new(
            catalogs: catalogs,
            tx: tx,
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: new()
        );

        FluxMachine<DropIndexFluxSteps, DropIndexFluxState> machine = new(state);

        return await DropIndexInternal(machine, state).ConfigureAwait(false);
    }

    private async Task<int> DropIndexInternal(FluxMachine<DropIndexFluxSteps, DropIndexFluxState> machine, DropIndexFluxState state)
    {
        TableDescriptor table = state.Table;
        AlterIndexTicket ticket = state.Ticket;

        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(DropIndexFluxSteps.RemoveSystemObject, RemoveSystemObject);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogWarning(
            "Dropped index {IndexName} from {Name}, Time taken: {Time}",
            ticket.IndexName,
            table.Name,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
