
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes inner joins via nested-loop iteration over bound table sources (QP4.3+).
/// </summary>
internal sealed class QueryJoinExecutor
{
    private readonly QueryExecutor queryExecutor;
    private readonly DerivedTableExecutor derivedTableExecutor;
    private readonly QueryFilterer queryFilterer = new(new ExistsSubqueryExecutor());

    private readonly QuerySorter querySorter = new();

    private readonly QueryAggregator queryAggregator = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryProjector queryProjector = new();

    private readonly QueryDistincter queryDistincter = new();

    private readonly StatisticsManager? _stats;

    public QueryJoinExecutor(QueryExecutor queryExecutor, StatisticsManager? stats = null)
    {
        this.queryExecutor = queryExecutor;
        _stats = stats;
        derivedTableExecutor = new DerivedTableExecutor(queryExecutor, this);
    }

    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket)
    {
        JoinQueryPlanner planner = new(_stats);
        QueryPlan plan = planner.GetPlan(database, bound, ticket);

        IAsyncEnumerable<QueryResultRow> cursor = ExecuteJoinTree(plan.Root, plan);

        if (plan.ExecutionFilter is not null)
            cursor = ApplyWhere(cursor, plan.ExecutionFilter, plan);

        return QueryPostScanPipeline.Apply(
            plan.Database,
            ticket,
            cursor,
            queryFilterer,
            querySorter,
            queryAggregator,
            queryProjector,
            queryDistincter,
            queryLimiter);
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteJoinTree(
        PhysicalPlanNode node,
        QueryPlan plan)
    {
        switch (node)
        {
            case IndexNestedLoopJoinNode indexJoinNode:
            {
                await foreach (QueryResultRow row in ExecuteIndexNestedLoopJoin(indexJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case NestedLoopJoinNode joinNode:
            {
                await foreach (QueryResultRow row in ExecuteNestedLoopJoin(joinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case TableScanNode { BoundSource: not null } scanNode:
            {
                await foreach (QueryResultRow row in ScanBoundTable(
                    scanNode.BoundSource,
                    scanNode.ExecutionFilter,
                    plan,
                    scanNode.RequiredColumns).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
            {
                await foreach (QueryResultRow row in ScanDerivedTable(
                    derivedScanNode.BoundSource,
                    derivedScanNode.ExecutionFilter,
                    plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Unsupported join plan node: {node.GetType().Name}");
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteIndexNestedLoopJoin(
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> leftQualified = QueryRowMerger.QualifyRow(
                leftRow.Row,
                ResolveLeftAlias(joinNode.Input!, leftRow));

            if (!leftQualified.TryGetValue(joinNode.LeftLookupColumn, out ColumnValue? lookupValue) || lookupValue is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join lookup column '{joinNode.LeftLookupColumn}' is missing from left row");
            }

            CompositeColumnValue lookupKey = new(new[] { lookupValue });

            await foreach (QueryResultRow rightRow in ProbeRightIndex(
                joinNode,
                lookupKey,
                plan).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(
                    leftQualified,
                    rightRow.Row,
                    rightAlias);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteNestedLoopJoin(
        NestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.RightSource.Alias;

        await foreach (QueryResultRow leftRow in ExecuteJoinTree(joinNode.Input!, plan).ConfigureAwait(false))
        {
            Dictionary<string, ColumnValue> leftQualified = QueryRowMerger.QualifyRow(
                leftRow.Row,
                ResolveLeftAlias(joinNode.Input!, leftRow));

            await foreach (QueryResultRow rightRow in ScanJoinRightSource(
                joinNode.RightSource,
                joinNode.RightExecutionFilter,
                plan).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(
                    leftQualified,
                    rightRow.Row,
                    rightAlias);

                if (!await queryFilterer.MeetWhereAsync(joinNode.OnPredicate, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ProbeRightIndex(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan)
    {
        if (joinNode.UseUniqueLookup)
        {
            await foreach (QueryResultRow row in LookupUniqueRightRow(joinNode, lookupKey, plan).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        await foreach (QueryResultRow row in ScanMultiIndexRightRows(joinNode, lookupKey, plan).ConfigureAwait(false))
            yield return row;
    }

    private async IAsyncEnumerable<QueryResultRow> LookupUniqueRightRow(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;

        ObjectIdValue? rowId = await table.Store.LookupUnique(plan.Ticket.TxnState, joinNode.Index.Name, lookupKey).ConfigureAwait(false);

        if (rowId is null)
            yield break;

        QueryResultRow? row = await LoadRightRow(source, rowId.Value, joinNode.RightExecutionFilter, plan).ConfigureAwait(false);

        if (row is QueryResultRow loadedRow)
            yield return loadedRow;
    }

    private async IAsyncEnumerable<QueryResultRow> ScanMultiIndexRightRows(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, joinNode.Index);
        ColumnValue lookupValue = lookupKey.Values[0];

        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in table.Store.ScanIndex(
            plan.Ticket.TxnState,
            joinNode.Index.Name,
            keyTypes,
            lookupKey,
            to: null,
            unique: false).ConfigureAwait(false))
        {
            if (key.Values[0].CompareTo(lookupValue) > 0)
                break;

            if (key.Values[0].CompareTo(lookupValue) != 0)
                continue;

            QueryResultRow? row = await LoadRightRow(source, rowId, joinNode.RightExecutionFilter, plan).ConfigureAwait(false);

            if (row is QueryResultRow loadedRow)
                yield return loadedRow;
        }
    }

    private async Task<QueryResultRow?> LoadRightRow(
        BoundTableSource source,
        ObjectIdValue rowId,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        byte[]? data = await source.Table.Store.GetRow(plan.Ticket.TxnState, rowId).ConfigureAwait(false);

        if (data is null || data.Length == 0)
            return null;

        Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
            source.Table.Schema,
            plan.Ticket.TxnState.TransactionId,
            rowId,
            data,
            GetRequiredColumnsForAlias(plan, source.Alias),
            GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

        if (executionFilter is not null)
        {
            Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);

            if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                return null;
        }

        return new QueryResultRow(rowId, row);
    }

    private static string ResolveLeftAlias(PhysicalPlanNode leftNode, QueryResultRow leftRow)
    {
        switch (leftNode)
        {
            case TableScanNode { BoundSource: not null } scanNode:
                return scanNode.BoundSource.Alias;

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
                return derivedScanNode.BoundSource.Alias;

            default:
                break;
        }

        foreach (KeyValuePair<string, ColumnValue> entry in leftRow.Row)
        {
            if (!QueryRowMerger.IsQualifiedKey(entry.Key))
                continue;

            int dotIndex = entry.Key.IndexOf('.');
            return entry.Key[..dotIndex];
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Could not resolve alias for nested join left row");
    }

    private async IAsyncEnumerable<QueryResultRow> ScanJoinRightSource(
        BoundJoinRightSource source,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        if (source.Table is not null)
        {
            await foreach (QueryResultRow row in ScanBoundTable(source.Table, executionFilter, plan).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        await foreach (QueryResultRow row in ScanDerivedTable(source.Derived!, executionFilter, plan).ConfigureAwait(false))
            yield return row;
    }

    private async IAsyncEnumerable<QueryResultRow> ScanBoundTable(
        BoundTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan,
        IReadOnlySet<string>? requiredColumns = null)
    {
        TableDescriptor table = source.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        IReadOnlySet<string>? required = requiredColumns ?? GetRequiredColumnsForAlias(plan, source.Alias);

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(plan.Ticket.TxnState).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                required,
                GetTableSchemaVersionForAlias(plan, source.Alias)).ConfigureAwait(false);

            if (executionFilter is not null)
            {
                Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, source.Alias);

                if (!await queryFilterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                    continue;
            }

            yield return new QueryResultRow(rowId, row);
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ScanDerivedTable(
        BoundDerivedTableSource source,
        NodeAst? executionFilter,
        QueryPlan plan)
    {
        if (!plan.DerivedMaterializations.TryGetValue(source, out List<Dictionary<string, ColumnValue>>? rows))
        {
            rows = await derivedTableExecutor
                .MaterializeAsync(plan.Database, source, plan.Ticket, executionFilter)
                .ConfigureAwait(false);
            plan.DerivedMaterializations[source] = rows;
        }

        foreach (Dictionary<string, ColumnValue> row in rows)
            yield return new QueryResultRow(default(ObjectIdValue), row);
    }

    private async IAsyncEnumerable<QueryResultRow> ApplyWhere(
        IAsyncEnumerable<QueryResultRow> cursor,
        NodeAst where,
        QueryPlan plan)
    {
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (await queryFilterer.MeetWhereAsync(where, row.Row, plan.Ticket, plan.Database).ConfigureAwait(false))
                yield return row;
        }
    }

    private static IReadOnlySet<string>? GetRequiredColumnsForAlias(QueryPlan plan, string alias)
    {
        if (plan.RequiredColumnsByAlias?.TryGetValue(alias, out IReadOnlySet<string>? required) == true)
            return required;

        return plan.ScanRequiredColumns;
    }

    private static int GetTableSchemaVersionForAlias(QueryPlan plan, string alias)
    {
        return plan.TableSchemaVersionByAlias.TryGetValue(alias, out int version)
            ? version
            : plan.TableSchemaVersion;
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
