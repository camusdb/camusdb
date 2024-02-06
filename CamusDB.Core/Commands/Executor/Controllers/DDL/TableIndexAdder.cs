
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
using CamusDB.Core.Catalogs;
using CamusDB.Core.BufferPool.Models;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class TableIndexAdder
{
    private readonly IndexSaver indexSaver = new();

    private readonly IndexReader indexReader = new();

    private readonly ILogger<ICamusDB> logger;

    public TableIndexAdder(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    private static void Validate(TableDescriptor table, AlterIndexTicket ticket)
    {
        if (ticket.Operation == AlterIndexOperation.AddPrimaryKey && table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Primary key already exists on table '{table.Name}'"
            );

        if (table.Indexes.ContainsKey(ticket.IndexName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Index '{ticket.IndexName}' already exists on table '{table.Name}'"
            );

        foreach (ColumnIndexInfo indexColumn in ticket.Columns)
        {
            bool hasColumn = false;

            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name == indexColumn.Name)
                {
                    hasColumn = true;
                    break;
                }
            }

            if (!hasColumn)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{indexColumn.Name}' does not exist on table '{table.Name}'"
                );
        }
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
    /// Schedules a new add index operation by the specified filters
    /// </summary>
    /// <param name="catalogs"></param>
    /// <param name="queryExecutor"></param>
    /// <param name="database"></param>
    /// <param name="table"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    internal async Task<int> AddIndex(
        CatalogsManager catalogs,
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterIndexTicket ticket
    )
    {
        Validate(table, ticket);

        AddIndexFluxState state = new(
            catalogs: catalogs,
            database: database,
            table: table,
            ticket: ticket,
            queryExecutor: queryExecutor
        );

        FluxMachine<AddIndexFluxSteps, AddIndexFluxState> machine = new(state);

        return await AlterIndexInternal(machine, state).ConfigureAwait(false);
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
    /// We need to locate the row tuples to AlterColumn
    /// Perform a full scan of the table to create the initial version of the index with all the available data.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> LocateTuplesToFeedTheIndex(AddIndexFluxState state)
    {
        AlterIndexTicket ticket = state.Ticket;

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

        IAsyncEnumerable<QueryResultRow> cursor = state.QueryExecutor.Query(state.Database, state.Table, queryTicket);

        // @todo we need to take a snapshot of the data to prevent deadlocks
        // but probably need to optimize this for larger datasets
        state.RowsToFeed = await cursor.ToListAsync().ConfigureAwait(false);

        //Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, state.RowTuple.SlotTwo);*/

        return FluxAction.Continue;
    }

    /// <summary>
    /// Acquire write locks on the indices to ensure consistency in writing.
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    private async Task<FluxAction> TryAdquireLocks(AddIndexFluxState state)
    {
        await state.Ticket.TxnState.TryAdquireWriteLocks(state.Table).ConfigureAwait(false);

        return FluxAction.Continue;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="state"></param>
    /// <returns></returns>
    private async Task<FluxAction> FeedTheIndex(AddIndexFluxState state)
    {
        if (state.RowsToFeed is null)
        {
            logger.LogWarning("Invalid rows to AlterIndex");
            return FluxAction.Abort;
        }

        if (state.Btree is null)
        {
            logger.LogWarning("Invalid btree in AlterIndex");
            return FluxAction.Abort;
        }

        AlterIndexTicket ticket = state.Ticket;
        BufferPoolManager tablespace = state.Database.BufferPool;

        foreach (QueryResultRow row in state.RowsToFeed)
        {
            CompositeColumnValue indexKeyValue;
            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> index = state.Btree;

            if (ticket.Operation == AlterIndexOperation.AddPrimaryKey || ticket.Operation == AlterIndexOperation.AddUniqueIndex)
            {
                indexKeyValue = await ValidateAndInsertUniqueValue(tablespace, index, row, ticket, ticket.TxnState.ModifiedPages);
                ticket.TxnState.UniqueIndexDeltas.Add((index, indexKeyValue, row.Tuple));
            }
            else
            {
                indexKeyValue = await ValidateAndInsertMultiValue(tablespace, index, row, ticket, ticket.TxnState.ModifiedPages);
                ticket.TxnState.MultiIndexDeltas.Add((index, indexKeyValue, row.Tuple));
            }
        }        

        return FluxAction.Continue;
    }

    private async Task<CompositeColumnValue> ValidateAndInsertUniqueValue(
        BufferPoolManager tablespace,
        BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex,
        QueryResultRow row,
        AlterIndexTicket ticket,
        List<BufferPageOperation> modifiedPages
    )
    {
        int i = 0;
        ColumnValue[] columnValues = new ColumnValue[ticket.Columns.Length];

        foreach (ColumnIndexInfo columnIndex in ticket.Columns)
        {
            ColumnValue? uniqueKeyValue = GetColumnValue(row.Row, columnIndex.Name);

            if (uniqueKeyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field " + columnIndex.Name
                );

            columnValues[i++] = uniqueKeyValue;
        }

        CompositeColumnValue compositeUniqueKeyValue = new(columnValues);

        BTreeTuple? rowTuple = await uniqueIndex.Get(TransactionType.ReadOnly, ticket.TxnState.TxnId, compositeUniqueKeyValue);

        if (rowTuple is not null && !rowTuple.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                $"Duplicate entry for key '{ticket.IndexName}' {compositeUniqueKeyValue}"
            );

        SaveIndexTicket saveUniqueIndexTicket = new(
            tablespace: tablespace,
            index: uniqueIndex,
            txnId: ticket.TxnState.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: compositeUniqueKeyValue,
            value: row.Tuple,
            modifiedPages: modifiedPages
        );

        await indexSaver.Save(saveUniqueIndexTicket);

        return compositeUniqueKeyValue;
    }

    private async Task<CompositeColumnValue> ValidateAndInsertMultiValue(
        BufferPoolManager tablespace,
        BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex,
        QueryResultRow row,
        AlterIndexTicket ticket,
        List<BufferPageOperation> modifiedPages
    )
    {
        int i = 0;
        ColumnValue[] columnValues = new ColumnValue[ticket.Columns.Length + 1];

        foreach (ColumnIndexInfo columnIndex in ticket.Columns)
        {
            ColumnValue? multiKeyValue = GetColumnValue(row.Row, columnIndex.Name);

            if (multiKeyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"A null value was found for multi key field '{columnIndex.Name}'"
                );

            columnValues[i++] = multiKeyValue;
        }

        columnValues[i++] = new(ColumnType.Id, row.Tuple.SlotOne.ToString());

        CompositeColumnValue compositeIndexValue = new(columnValues);

        SaveIndexTicket saveUniqueIndexTicket = new(
            tablespace: tablespace,
            index: uniqueIndex,
            txnId: ticket.TxnState.TxnId,
            commitState: BTreeCommitState.Uncommitted,
            key: compositeIndexValue,
            value: row.Tuple,
            modifiedPages: modifiedPages
        );

        await indexSaver.Save(saveUniqueIndexTicket).ConfigureAwait(false);

        return compositeIndexValue;
    }    

    private async Task<FluxAction> AddSystemObject(AddIndexFluxState state)
    {
        if (state.Btree is null)
        {
            logger.LogWarning("Invalid btree in AlterIndex");
            return FluxAction.Abort;
        }

        AlterIndexTicket ticket = state.Ticket;
        TableDescriptor table = state.Table;
        DatabaseDescriptor database = state.Database;
        IndexType indexType = ticket.Operation == AlterIndexOperation.AddUniqueIndex || ticket.Operation == AlterIndexOperation.AddPrimaryKey ? IndexType.Unique : IndexType.Multi;

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            Dictionary<string, DatabaseIndexObject> indexes = database.SystemSchema.Indexes;

            string indexId = database.BufferPool.GetNextFreeOffset().ToString();

            indexes.Add(
                indexId,
                new DatabaseIndexObject(
                    indexId,
                    ticket.IndexName,
                    table.Id,
                    GetColumnIds(table, ticket.Columns),
                    indexType,
                    state.IndexOffset.ToString()
                )
            );

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema));
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        table.Indexes.Add(
            ticket.IndexName,
            new TableIndexSchema(ticket.Columns.Select(x => x.Name).ToArray(), indexType, state.Btree)
        );

        return FluxAction.Continue;
    }

    private static string[] GetColumnIds(TableDescriptor table, ColumnIndexInfo[] columns)
    {
        int i = 0;
        string[] columnsIds = new string[columns.Length];

        foreach (ColumnIndexInfo columnIndex in columns)
        {
            bool hasColumn = false;

            foreach (TableColumnSchema column in table.Schema.Columns!)
            {
                if (column.Name == columnIndex.Name)
                {
                    hasColumn = true;
                    columnsIds[i] = column.Id;
                    break;
                }
            }

            if (!hasColumn)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Couldn't get column id for column '{columnIndex.Name}'");
        }

        return columnsIds;
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
        machine.When(AddIndexFluxSteps.TryAdquireLocks, TryAdquireLocks);
        machine.When(AddIndexFluxSteps.LocateTuplesToFeedTheIndex, LocateTuplesToFeedTheIndex);
        machine.When(AddIndexFluxSteps.FeedTheIndex, FeedTheIndex);
        machine.When(AddIndexFluxSteps.AddSystemObject, AddSystemObject);                

        while (!machine.IsAborted)
            await machine.RunStep(machine.NextStep());

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        logger.LogInformation(
            "Added index {IndexName} to {Name} at {IndexOffset}, Time taken: {Time}",
            ticket.IndexName,
            table.Name,
            state.IndexOffset,
            timeTaken.ToString(@"m\:ss\.fff")
        );

        return state.ModifiedRows;
    }
}
