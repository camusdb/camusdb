
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
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
                    plan.DataCursor = QueryUsingIndex(plan.Table, plan.Ticket, step.Index, step.ColumnValue);
                    break;

                case QueryPlanStepType.RangeScanFromIndex:
                    plan.DataCursor = QueryUsingRangeIndex(plan.Table, plan.Ticket, step.Index, step.FromBound, step.FromInclusive, step.ToBound, step.ToInclusive);
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
        TableDescriptor table,
        QueryTicket ticket,
        TableIndexSchema? index,
        ColumnValue? columnValue
    )
    {
        if (index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Couldn't access table's unique index");

        if (columnValue is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid column value");

        if (index.Type == IndexType.Unique)
            return QueryUsingUniqueIndex(table, ticket, index, columnValue);

        return QueryUsingMultiIndex(table, ticket, index, columnValue);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingUniqueIndex(
        TableDescriptor table,
        QueryTicket ticket,
        TableIndexSchema index,
        ColumnValue columnValue
    )
    {
        HLCTimestamp txId = ticket.TxnState.TransactionId;

        ObjectIdValue? rowId = await table.Store.LookupUnique(txId, index.Name, new CompositeColumnValue(new[] { columnValue })).ConfigureAwait(false);
        if (rowId is null)
            yield break;

        byte[]? data = await table.Store.GetRow(txId, rowId.Value).ConfigureAwait(false);
        if (data is null || data.Length == 0)
            yield break;

        ObjectIdValue resolvedRowId = rowId.Value;
        Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, resolvedRowId, data);

        if (ticket.Filters is not null && ticket.Filters.Count > 0)
        {
            if (queryFilterer.MeetFilters(ticket.Filters, row))
                yield return new(resolvedRowId, row);
        }
        else
        {
            if (ticket.Where is not null)
            {
                if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                    yield return new(resolvedRowId, row);
            }
            else
                yield return new(resolvedRowId, row);
        }
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingMultiIndex(
        TableDescriptor table,
        QueryTicket ticket,
        TableIndexSchema index,
        ColumnValue columnValue
    )
    {
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);

        await foreach ((CompositeColumnValue decodedKey, ObjectIdValue rowId) in table.Store.ScanIndex(txId, index.Name, keyTypes, null, null, unique: false))
        {
            // Filter: first component must match the queried value.
            if (decodedKey.Values.Length == 0 || decodedKey.Values[0].CompareTo(columnValue) != 0)
                continue;

            byte[]? data = await table.Store.GetRow(txId, rowId).ConfigureAwait(false);
            if (data is null || data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(rowId, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(rowId, row);
                }
                else
                    yield return new(rowId, row);
            }
        }
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingRangeIndex(
        TableDescriptor table,
        QueryTicket ticket,
        TableIndexSchema? index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive)
    {
        if (index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Couldn't access table's index for range scan");

        bool unique = index.Type == IndexType.Unique;
        return QueryUsingRangeIndexInternal(table, ticket, index, fromBound, fromInclusive, toBound, toInclusive, unique);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingRangeIndexInternal(
        TableDescriptor table,
        QueryTicket ticket,
        TableIndexSchema index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive,
        bool unique)
    {
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
            txId, index.Name, keyTypes, fromBound, toBound, unique, fromInclusive, toInclusive))
        {
            byte[]? data = await table.Store.GetRow(txId, rowId).ConfigureAwait(false);
            if (data is null || data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(rowId, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(rowId, row);
                }
                else
                    yield return new(rowId, row);
            }
        }
    }

    public async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryById(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryByIdTicket ticket
    )
    {
        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        ObjectIdValue? rowId = await table.Store.LookupUnique(txId, index.Name, new CompositeColumnValue(new[] { columnId })).ConfigureAwait(false);
        if (rowId is null)
            yield break;

        byte[]? data = await table.Store.GetRow(txId, rowId.Value).ConfigureAwait(false);
        if (data is null || data.Length == 0)
            yield break;

        yield return RowEncoder.Decode(table.Schema, rowId.Value, data);
    }

    private static ColumnType[] GetIndexColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
        {
            string colName = index.Columns[i];
            TableColumnSchema? col = table.Schema.Columns?.Find(c => c.Name == colName);
            types[i] = col?.Type ?? ColumnType.String;
        }

        return types;
    }
}
