
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
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

    private readonly QueryFilterer queryFilterer = new(new ExistsSubqueryExecutor());

    private readonly QuerySorter querySorter = new();

    private readonly QueryAggregator queryAggregator = new();

    private readonly QueryProjector queryProjector = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryDistincter queryDistincter = new();

    private readonly QueryJoinExecutor queryJoinExecutor;

    private readonly QueryScanner queryScanner;

    public QueryExecutor(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
        queryJoinExecutor = new QueryJoinExecutor(this);
        this.queryScanner = new(logger);
    }

    public IAsyncEnumerable<QueryResultRow> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        return ExecuteQueryPlanInternal(plan);
    }

    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket) =>
        queryJoinExecutor.ExecuteJoinQuery(database, bound, ticket);

    private IAsyncEnumerable<QueryResultRow> ExecuteQueryPlanInternal(QueryPlan plan)
    {
        foreach (QueryPlanStep step in plan.Steps)
        {
            logger.LogInformation("Executing step {Type}", step.Type);

            switch (step.Type)
            {
                case QueryPlanStepType.QueryFromIndex:
                    plan.DataCursor = QueryUsingIndex(plan, step.Index, step.LookupKey, step.ColumnValue);
                    break;

                case QueryPlanStepType.RangeScanFromIndex:
                    plan.DataCursor = QueryUsingRangeIndex(plan, step.Index, step.FromBound, step.FromInclusive, step.ToBound, step.ToInclusive);
                    break;

                case QueryPlanStepType.FullScanFromIndex:
                    plan.DataCursor = queryScanner.ScanUsingIndex(plan, queryFilterer, rowDeserializer);
                    break;

                case QueryPlanStepType.FullScanFromTableIndex:
                    plan.DataCursor = queryScanner.ScanUsingTableIndex(plan, queryFilterer, rowDeserializer);
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

                case QueryPlanStepType.Distinct:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = queryDistincter.DistinctResultset(plan.Ticket, plan.DataCursor);
                    break;

                case QueryPlanStepType.Aggregate:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = queryAggregator.AggregateResultset(plan.Ticket, plan.DataCursor);
                    break;

                case QueryPlanStepType.HavingFilter:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = queryFilterer.FilterHavingResultset(plan.Database, plan.Ticket, plan.DataCursor);
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
        QueryPlan plan,
        TableIndexSchema? index,
        CompositeColumnValue? lookupKey,
        ColumnValue? columnValue
    )
    {
        if (index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Couldn't access table's unique index");

        CompositeColumnValue key = lookupKey ?? new CompositeColumnValue(new[] { columnValue! });

        if (columnValue is null && lookupKey is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid lookup key");

        if (index.Type == IndexType.Unique)
            return QueryUsingUniqueIndex(plan, index, key);

        if (!IndexScanSelector.SupportsExactEqualityPrefixUpperBound(plan.Table, index.Columns, key.Values.Length))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Non-unique index lookup requires a bounded scan plan");

        CompositeColumnValue? upperBound = BuildPrefixScanUpperBound(plan.Table, index, key);

        return QueryUsingRangeIndex(plan, index, key, fromInclusive: true, upperBound, toInclusive: false);
    }

    private static CompositeColumnValue? BuildPrefixScanUpperBound(
        TableDescriptor table,
        TableIndexSchema index,
        CompositeColumnValue lookupKey)
    {
        if (lookupKey.Values.Length == 0)
            return null;

        ColumnValue[] upperValues = new ColumnValue[lookupKey.Values.Length];
        Array.Copy(lookupKey.Values, upperValues, lookupKey.Values.Length - 1);

        string lastColumn = index.Columns[lookupKey.Values.Length - 1];
        TableColumnSchema? column = table.Schema.Columns?.Find(c => c.Name == lastColumn);
        ColumnType columnType = column?.Type ?? ColumnType.String;
        ColumnValue lastValue = lookupKey.Values[^1];
        ColumnValue? nextValue = NextSortValue(columnType, lastValue);

        if (nextValue is null)
            return null;

        upperValues[^1] = nextValue;
        return new CompositeColumnValue(upperValues);
    }

    private static ColumnValue? NextSortValue(ColumnType columnType, ColumnValue value)
    {
        return IndexScanSelector.NextSortValue(columnType, value);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingUniqueIndex(
        QueryPlan plan,
        TableIndexSchema index,
        CompositeColumnValue lookupKey
    )
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        HLCTimestamp txId = ticket.TxnState.TransactionId;

        ObjectIdValue? rowId = await table.Store.LookupUnique(txId, index.Name, lookupKey).ConfigureAwait(false);
        if (rowId is null)
            yield break;

        byte[]? data = await table.Store.GetRow(txId, rowId.Value).ConfigureAwait(false);
        if (data is null || data.Length == 0)
            yield break;

        ObjectIdValue resolvedRowId = rowId.Value;
        Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
            table.Schema,
            txId,
            resolvedRowId,
            data,
            plan.ScanRequiredColumns).ConfigureAwait(false);

            if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                yield return new(resolvedRowId, row);
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingRangeIndex(
        QueryPlan plan,
        TableIndexSchema? index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive)
    {
        if (index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Couldn't access table's index for range scan");

        bool unique = index.Type == IndexType.Unique;
        return QueryUsingRangeIndexInternal(plan, index, fromBound, fromInclusive, toBound, toInclusive, unique);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingRangeIndexInternal(
        QueryPlan plan,
        TableIndexSchema index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive,
        bool unique)
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
            txId,
            index.Name,
            keyTypes,
            fromBound,
            toBound,
            unique,
            fromInclusive,
            toInclusive,
            maxRows: plan.ScanRowLimit))
        {
            byte[]? data = await table.Store.GetRow(txId, rowId).ConfigureAwait(false);
            if (data is null || data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                plan.ScanRequiredColumns).ConfigureAwait(false);

            if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                yield return new(rowId, row);
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

        yield return await RowEncoder.DecodeAsync(table.Schema, txId, rowId.Value, data).ConfigureAwait(false);
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
