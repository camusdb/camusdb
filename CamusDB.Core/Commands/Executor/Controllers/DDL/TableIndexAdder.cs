
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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Serializer;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class TableIndexAdder
{
    private readonly IndexSaver indexSaver = new();

    private readonly IndexReader indexReader = new();

    private void Validate(TableDescriptor table, AlterIndexTicket ticket)
    {
        if (ticket.Operation == AlterIndexOperation.AddPrimaryKey && table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Primary key already exists on table '" + table.Name + "'"
            );

        if (table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Index '" + ticket.IndexName + "' already exists on table '" + table.Name + "'"
            );        

        bool hasColumn = false;

        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (column.Name == ticket.ColumnName)
            {
                hasColumn = true;
                break;
            }
        }

        if (!hasColumn)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Column '" + ticket.ColumnName + "' does not exist on table '" + table.Name + "'"
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
    /// Schedules a new add index operation by the specified filters
    /// </summary>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> AddIndex(QueryExecutor queryExecutor, DatabaseDescriptor database, TableDescriptor table, AlterIndexTicket ticket)
    {
        Validate(table, ticket);

        AddIndexFluxState state = new(
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor,
            indexes: new AlterIndexFluxIndexState()
        );

        FluxMachine<AddIndexFluxSteps, AddIndexFluxState> machine = new(state);

        return await AlterIndexInternal(machine, state);
    }

    /// <summary>
    /// We need to locate the row tuples to AlterColumn
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private Task<FluxAction> LocateTuplesToFeedTheIndex(AddIndexFluxState state)
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
            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> index = state.Btree;

            BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> mutations;

            if (ticket.Operation == AlterIndexOperation.AddPrimaryKey || ticket.Operation == AlterIndexOperation.AddUniqueIndex)
                mutations = await ValidateAndInsertUniqueValue(index, row, ticket);
            else
                mutations = await ValidateAndInsertMultiValue(index, row, ticket);

            deltas.Add((index, mutations));
        }

        state.IndexDeltas = deltas;

        return FluxAction.Continue;
    }

    private async Task<BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>> ValidateAndInsertUniqueValue(
        BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex,
        QueryResultRow row,
        AlterIndexTicket ticket
    )
    {
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

        return await indexSaver.Save(saveUniqueIndexTicket);
    }

    private async Task<BTreeMutationDeltas<CompositeColumnValue, BTreeTuple>> ValidateAndInsertMultiValue(
        BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex,
        QueryResultRow row,
        AlterIndexTicket ticket
    )
    {
        ColumnValue? multiKeyValue = GetColumnValue(row.Row, ticket.ColumnName);

        if (multiKeyValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A null value was found for multi key field " + ticket.ColumnName
            );

        var x = new CompositeColumnValue(new ColumnValue[] { multiKeyValue, new ColumnValue(ColumnType.Id, row.Tuple.SlotOne.ToString()) });
        //Console.WriteLine(x);

        SaveIndexTicket saveUniqueIndexTicket = new(
            index: uniqueIndex,
            txnId: ticket.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: x,
            value: row.Tuple
        );

        return await indexSaver.Save(saveUniqueIndexTicket);
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

        foreach ((BTree<CompositeColumnValue, BTreeTuple> index, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) index in state.IndexDeltas)
        {
            foreach (BTreeMvccEntry<BTreeTuple> uniqueIndexEntry in index.deltas.MvccEntries)
                uniqueIndexEntry.CommitState = BTreeCommitState.Committed;

            await indexSaver.Persist(state.Database.BufferPool, index.index, state.ModifiedPages, index.deltas);
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
        IndexType indexType = ticket.Operation == AlterIndexOperation.AddUniqueIndex || ticket.Operation == AlterIndexOperation.AddPrimaryKey ? IndexType.Unique : IndexType.Multi;

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
                    new DatabaseIndexObject(ticket.ColumnName, indexType, state.IndexOffset.ToString())
                );
            }

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema.Objects));
        }
        finally
        {
            database.SystemSchema.Semaphore.Release();
        }

        table.Indexes.Add(
            ticket.IndexName,
            new TableIndexSchema(ticket.ColumnName, indexType, state.Btree)
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
    internal async Task<int> AlterIndexInternal(FluxMachine<AddIndexFluxSteps, AddIndexFluxState> machine, AddIndexFluxState state)
    {
        DatabaseDescriptor database = state.Database;
        BufferPoolManager tablespace = state.Database.BufferPool;
        TableDescriptor table = state.Table;
        AlterIndexTicket ticket = state.Ticket;

        Stopwatch timer = Stopwatch.StartNew();

        machine.When(AddIndexFluxSteps.AllocateNewIndex, AllocateNewIndex);
        machine.When(AddIndexFluxSteps.LocateTuplesToFeedTheIndex, LocateTuplesToFeedTheIndex);
        machine.When(AddIndexFluxSteps.FeedTheIndex, FeedTheIndex);
        machine.When(AddIndexFluxSteps.PersistIndexChanges, PersistIndexChanges);
        machine.When(AddIndexFluxSteps.ApplyPageOperations, ApplyPageOperations);
        machine.When(AddIndexFluxSteps.AddSystemObject, AddSystemObject);

        //machine.WhenAbort(ReleaseLocks);

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine(
            "Added index {0} to {1} at {2}, Time taken: {3}",
            ticket.IndexName,
            table.Name,
            state.IndexOffset,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
