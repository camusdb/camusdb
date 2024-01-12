
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
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Serializer;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class TableIndexDropper
{
    private readonly IndexSaver indexSaver = new();

    private readonly IndexReader indexReader = new();

    private void Validate(TableDescriptor table, AlterIndexTicket ticket)
    {
        if (!table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Index " + ticket.IndexName + " does not exist in table " + table.Name
            );
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
    /// Allocate a new index on disk
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> AllocateNewIndex(AddIndexFluxState state)
    {
        BufferPoolManager tablespace = state.Database.BufferPool;

        ObjectIdValue indexPageOffset = tablespace.GetNextFreeOffset();

        state.Btree = await indexReader.Read(tablespace, ObjectId.ToValue(indexPageOffset.ToString()));
        state.IndexOffset = indexPageOffset;

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
            indexes: new AlterIndexFluxIndexState()
        );

        FluxMachine<AddIndexFluxSteps, DropIndexFluxState> machine = new(state);

        return await AlterIndexInternal(machine, state);
    }

    /// <summary>
    /// We need to locate the row tuples to AlterIndex
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> LocateTuplesToFeedTheIndex(DropIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;

        QueryTicket queryTicket = new(
            txnId: ticket.TxnId,
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

    private async Task<FluxAction> FeedTheIndex(AddIndexFluxState state)
    {
        if (state.DataCursor is null)
        {
            Console.WriteLine("Invalid rows to AlterIndex");
            return FluxAction.Abort;
        }

        if (state.Btree is null)
        {
            Console.WriteLine("Invalid btree in AlterIndex");
            return FluxAction.Abort;
        }

        AlterIndexTicket ticket = state.Ticket;

        List<(BTree<CompositeColumnValue, BTreeTuple>, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>)> deltas = new();

        await foreach (QueryResultRow row in state.DataCursor)
        {
            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex = state.Btree;

            ColumnValue? uniqueKeyValue = GetColumnValue(row.Row, ticket.ColumnName);

            if (uniqueKeyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field " + ticket.ColumnName
                );

            BTreeTuple? rowTuple = await uniqueIndex.Get(TransactionType.ReadOnly, ticket.TxnId, new CompositeColumnValue(uniqueKeyValue));

            if (rowTuple is not null && !rowTuple.IsNull())
                throw new CamusDBException(
                    CamusDBErrorCodes.DuplicateUniqueKeyValue,
                    "Duplicate entry for key \"" + ticket.IndexName + "\" " + uniqueKeyValue.Type + " " + uniqueKeyValue
                );

            SaveIndexTicket saveUniqueIndexTicket = new(
                index: uniqueIndex,
                txnId: ticket.TxnId,
                commitState: BTreeCommitState.Uncommitted,
                key: new CompositeColumnValue(uniqueKeyValue),
                value: row.Tuple
            );

            deltas.Add((uniqueIndex, await indexSaver.Save(saveUniqueIndexTicket)));
        }

        state.IndexDeltas = deltas;

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> PersistIndexChanges(AddIndexFluxState state)
    {
        if (state.IndexDeltas is null)
        {
            Console.WriteLine("Invalid index deltas in AlterIndex");
            return FluxAction.Abort;
        }

        foreach ((BTree<CompositeColumnValue, BTreeTuple> index, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) uniqueIndex in state.IndexDeltas)
        {
            foreach (BTreeMvccEntry<BTreeTuple> uniqueIndexEntry in uniqueIndex.deltas.MvccEntries)
                uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

            await indexSaver.Persist(state.Database.BufferPool, uniqueIndex.index, state.ModifiedPages, uniqueIndex.deltas);
        }

        return FluxAction.Continue;
    }

    private async Task<FluxAction> AddSystemObject(AddIndexFluxState state)
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
            await database.SystemSchema.Semaphore.WaitAsync();

            Dictionary<string, DatabaseObject> objects = database.SystemSchema.Objects;

            foreach (KeyValuePair<string, DatabaseObject> systemObject in objects)
            {
                DatabaseObject databaseObject = systemObject.Value;

                if (databaseObject.Type != DatabaseObjectType.Table)
                    continue;

                if (databaseObject.Name != state.Table.Name)
                    continue;

                if (databaseObject.Indexes is null)
                    databaseObject.Indexes = new();

                databaseObject.Indexes.Add(
                    ticket.IndexName,
                    new DatabaseIndexObject(ticket.ColumnName, IndexType.Unique, state.IndexOffset.ToString())
                );
            }

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema.Objects));
        }
        finally
        {
            database.SystemSchema.Semaphore.Release();
        }

        table.Indexes.Add(
            ticket.ColumnName,
            new TableIndexSchema(ticket.ColumnName, IndexType.Unique, state.Btree)
        );

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> ApplyPageOperations(AddIndexFluxState state)
    {
        if (state.ModifiedPages.Count > 0)
            state.Database.BufferPool.ApplyPageOperations(state.ModifiedPages);

        return Task.FromResult(FluxAction.Continue);
    }

    /// <summary>
    /// Executes the flux state machine to AlterIndex records by the specified filters
    /// </summary>
    /// <param name="machine"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    internal async Task<int> AlterIndexInternal(FluxMachine<AddIndexFluxSteps, DropIndexFluxState> machine, DropIndexFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        AlterIndexTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(AddIndexFluxSteps.LocateIndex, LocateIndex);        
        machine.When(AddIndexFluxSteps.DropTheIndex, DropTheIndex);
        machine.When(AddIndexFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(AddIndexFluxSteps.ApplyPageOperations, ApplyPageOperations);
        machine.When(AddIndexFluxSteps.AddSystemObject, AddSystemObject);

        //machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine(
            "Dropped index {0} from {1} at {2}, Time taken: {3}",
            ticket.IndexName,
            table.Name,
            state.IndexOffset,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
