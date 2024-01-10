
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

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly RowDeserializer rowDeserializer = new();

    private readonly QueryPlanner queryPlanner = new();

    private readonly QueryFilterer queryFilterer = new();

    private readonly QuerySorter querySorter = new();

    private readonly QueryAggregator queryAggregator = new();

    private readonly QueryProjector queryProjector = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryScanner queryScanner = new();

    public IAsyncEnumerable<QueryResultRow> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        return ExecuteQueryPlanInternal(plan);
    }

    private IAsyncEnumerable<QueryResultRow> ExecuteQueryPlanInternal(QueryPlan plan)
    {
        foreach (QueryPlanStep step in plan.Steps)
        {
            Console.WriteLine("Executing step {0}", step.Type);

            switch (step.Type)
            {
                case QueryPlanStepType.QueryFromIndex:
                    plan.DataCursor = QueryUsingIndex(plan.Database, plan.Table, plan.Ticket, step.Index);
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

    private IAsyncEnumerable<QueryResultRow>? QueryUsingIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket, TableIndexSchema? index)
    {
        throw new NotImplementedException();
    }

    public async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolManager tablespace = database.BufferPool;

        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        if (index.UniqueRows is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        //Console.WriteLine(ticket.TxnId);

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        BTreeTuple? pageOffset = await index.UniqueRows.Get(TransactionType.ReadOnly, ticket.TxnId, columnId);

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

        yield return rowDeserializer.Deserialize(table.Schema, data);
    }
}
