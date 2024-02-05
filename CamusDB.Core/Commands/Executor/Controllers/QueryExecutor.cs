
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly ILogger<ICamusDB> logger;

    private readonly RowDeserializer rowDeserializer = new();

    private readonly QueryPlanner queryPlanner = new();

    private readonly QueryFilterer queryFilterer = new();

    private readonly QuerySorter querySorter = new();

    private readonly QueryAggregator queryAggregator = new();

    private readonly QueryProjector queryProjector = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryScanner queryScanner = new();    

    public QueryExecutor(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    public IAsyncEnumerable<QueryResultRow> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        return ExecuteQueryPlanInternal(plan);
    }

    private IAsyncEnumerable<QueryResultRow> ExecuteQueryPlanInternal(QueryPlan plan)
    {
        foreach (QueryPlanStep step in plan.Steps)
        {
            logger.LogInformation("Executing step {Type}", step.Type);

            switch (step.Type)
            {
                case QueryPlanStepType.QueryFromIndex:
                    plan.DataCursor = QueryUsingIndex(plan.Database, plan.Table, plan.Ticket, step.Index, step.ColumnValue);
                    break;

                case QueryPlanStepType.FullScanFromIndex:
                    plan.DataCursor = queryScanner.ScanUsingIndex(plan.Database, plan.Table, plan.Ticket, queryFilterer, rowDeserializer);
                    break;

                case QueryPlanStepType.FullScanFromTableIndex:
                    plan.DataCursor = queryScanner.ScanUsingTableIndex(plan.Database, plan.Table, plan.Ticket, queryFilterer, rowDeserializer);
                    break;

                case QueryPlanStepType.SortBy:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = querySorter.SortResultset(plan.Ticket, plan.DataCursor);
                    break;

                case QueryPlanStepType.ReduceToProjections:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = queryProjector.ProjectResultset(plan.Ticket, plan.DataCursor);
                    break;

                case QueryPlanStepType.Aggregate:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = queryAggregator.AggregateResultset(plan.Ticket, plan.DataCursor);
                    break;

                case QueryPlanStepType.Limit:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = queryLimiter.LimitResultset(plan.Ticket, plan.DataCursor);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown query plan step: " + step.Type);
            }
        }

        if (plan.DataCursor is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

        return plan.DataCursor;
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingIndex(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        TableIndexSchema? index,
        ColumnValue? columnValue
    )
    {
        if (index is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Couldn't access table's unique index"
            );
        }

        if (columnValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Invalid column value"
            );

        if (index.Type == IndexType.Unique)
            return QueryUsingUniqueIndex(database, table, ticket, index, columnValue);

        return QueryUsingMultiIndex(database, table, ticket, index, columnValue);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingUniqueIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket, TableIndexSchema index, ColumnValue columnValue)
    {
        BufferPoolManager tablespace = database.BufferPool;

        using IDisposable? _ = await index.BTree.ReaderLockAsync().ConfigureAwait(false);

        BTreeTuple? pageOffset = await index.BTree.Get(
                                            TransactionType.ReadOnly,
                                            ticket.TxnState.TxnId,
                                            new CompositeColumnValue(columnValue)
                                       ).ConfigureAwait(false);

        if (pageOffset is null || pageOffset.IsNull())
        {
            //Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            yield break;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo);
        if (data.Length == 0)
        {
            //Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            yield break;
        }

        //Console.WriteLine("Got row id {0} from page data {1}", pageOffset.SlotOne, pageOffset.SlotTwo);

        Dictionary<string, ColumnValue> row = rowDeserializer.Deserialize(table.Schema, pageOffset.SlotOne, data);

        if (ticket.Filters is not null && ticket.Filters.Count > 0)
        {
            if (queryFilterer.MeetFilters(ticket.Filters, row))
                yield return new(pageOffset, row);
        }
        else
        {
            if (ticket.Where is not null)
            {
                if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                    yield return new(pageOffset, row);
            }
            else
                yield return new(pageOffset, row);
        }
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingMultiIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket, TableIndexSchema index, ColumnValue columnValue)
    {
        BufferPoolManager tablespace = database.BufferPool;

        /*await foreach (var x in index.BTree.EntriesTraverse(ticket.TxnId))
        {
            Console.WriteLine("Entry={0} Value={1}", x.Key, x.GetValue(TransactionType.ReadOnly, ticket.TxnId));
        }*/

        using IDisposable? _ = await index.BTree.ReaderLockAsync().ConfigureAwait(false);

        await foreach (BTreeTuple? pageOffset in index.BTree.GetPrefix(TransactionType.ReadOnly, ticket.TxnState.TxnId, columnValue))
        {
            if (pageOffset is null || pageOffset.IsNull())
            {
                //Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
                yield break;
            }

            byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo).ConfigureAwait(false);
            if (data.Length == 0)
            {
                //Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
                yield break;
            }

            //Console.WriteLine("Got row id {0} from page data {1}", pageOffset.SlotOne, pageOffset.SlotTwo);

            Dictionary<string, ColumnValue> row = rowDeserializer.Deserialize(table.Schema, pageOffset.SlotOne, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(pageOffset, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(pageOffset, row);
                }
                else
                    yield return new(pageOffset, row);
            }
        }
    }

    public async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        //Console.WriteLine(ticket.TxnId);

        BufferPoolManager tablespace = database.BufferPool;

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        using IDisposable? _ = await index.BTree.ReaderLockAsync().ConfigureAwait(false);

        BTreeTuple? pageOffset = await index.BTree.Get(TransactionType.ReadOnly, ticket.TxnState.TxnId, new CompositeColumnValue(columnId)).ConfigureAwait(false);

        if (pageOffset is null || pageOffset.IsNull())
        {
            //Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            yield break;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo).ConfigureAwait(false);
        if (data.Length == 0)
        {
            //Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            yield break;
        }

        //Console.WriteLine("Got row id {0} from page data {1}", pageOffset.SlotOne, pageOffset.SlotTwo);

        yield return rowDeserializer.Deserialize(table.Schema, pageOffset.SlotOne, data);
    }
}
