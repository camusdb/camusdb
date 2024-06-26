﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Flux;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Flux.Models;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Serializer;
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

    /// <summary>
    /// Locate index to delete
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> LocateIndex(DropIndexFluxState state)
    {
        TableDescriptor table = state.Table;

        if (!table.Indexes.TryGetValue(state.Ticket.IndexName, out TableIndexSchema? index))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Index '{state.Ticket.IndexName}' does not exist");

        state.Btree = index.BTree;
        state.IndexOffset = index.BTree.rootOffset;

        return Task.FromResult(FluxAction.Continue);
    }       

    /// <summary>
    /// Deletes the index
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> DeleteIndexPages(DropIndexFluxState state)
    {
        if (state.Btree is null)
        {
            Console.WriteLine("Invalid btree in AlterIndex");
            return FluxAction.Abort;
        }

        BufferPoolManager tableSpace = state.Database.BufferPool;
        //using IDisposable writerLock = await state.Btree.WriterLockAsync();

        await foreach (BTreeNode<CompositeColumnValue, BTreeTuple> node in state.Btree.NodesTraverse(state.Ticket.TxnState.TxnId))
            await tableSpace.DeletePage(node.PageOffset);

        await Task.CompletedTask;

        return FluxAction.Continue;
    }    

    private async Task<FluxAction> RemoveSystemObject(DropIndexFluxState state)
    {
        if (state.Btree is null)
        {
            Console.WriteLine("Invalid btree in AlterIndex");
            return FluxAction.Abort;
        }

        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            Dictionary<string, DatabaseIndexObject> objects = database.SystemSchema.Indexes;

            foreach (KeyValuePair<string, DatabaseIndexObject> systemObject in objects)
            {
                DatabaseIndexObject databaseObject = systemObject.Value;                

                if (databaseObject.Name != ticket.IndexName)
                    continue;

                objects.Remove(databaseObject.Id);
                break;
            }

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema));
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        table.Indexes.Remove(ticket.IndexName);

        return FluxAction.Continue;
    }   

    /// <summary>
    /// Schedules a new drop index operation by the specified filters
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> DropIndex(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        Validate(table, ticket);

        DropIndexFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: new()
        );

        FluxMachine<DropIndexFluxSteps, DropIndexFluxState> machine = new(state);

        return await DropIndexInternal(machine, state).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the flux state machine to AlterIndex records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<int> DropIndexInternal(FluxMachine<DropIndexFluxSteps, DropIndexFluxState> machine, DropIndexFluxState state)
    {
        TableDescriptor table = state.Table;
        AlterIndexTicket ticket = state.Ticket;

        ValueStopwatch timer = ValueStopwatch.StartNew();

        machine.When(DropIndexFluxSteps.LocateIndex, LocateIndex);        
        machine.When(DropIndexFluxSteps.DeleteIndexPages, DeleteIndexPages);        
        machine.When(DropIndexFluxSteps.RemoveSystemObject, RemoveSystemObject);

        //machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep()).ConfigureAwait(false);

        TimeSpan timeTaken = timer.GetElapsedTime();

        logger.LogWarning(
            "Dropped index {IndexName} from {Name} at {IndexOffset}, Time taken: {Time}",
            ticket.IndexName,
            table.Name,
            state.IndexOffset,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
