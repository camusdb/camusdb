
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical plan tree for single-table SELECT queries (QP2 physical planning phase).
/// </summary>
public sealed class QueryPlanner
{
    private readonly StatisticsManager? _stats;

    public QueryPlanner(StatisticsManager? stats = null)
    {
        _stats = stats;
    }

    public QueryPlan GetPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = new(database, table, ticket);

        PredicateAnalysis analysis = ticket.AnalyzedWhere is not null
            ? PredicateAnalyzer.Merge(ticket.AnalyzedWhere, PredicateAnalyzer.AnalyzeFilters(ticket.Filters))
            : PredicateAnalyzer.AnalyzeTicket(ticket);
        plan.PredicateAnalysis = analysis;

        (PhysicalPlanNode scanNode, QueryPlanStep? scanStep, NodeAst? absorbedInListConjunct) = BuildScanNode(database, table, ticket, analysis);
        plan.ExecutionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep, table, absorbedInListConjunct);

        // Populate OutputOrdering on the scan node when the chosen index scan guarantees the
        // requested ORDER BY ordering (QP6.2 / R4). The planner then uses this property to
        // decide whether to add a SortNode, keeping sort elision explicit and consistent.
        // Guard: GROUP BY and DISTINCT both destroy scan-level ordering before plan output,
        // so the scan's OutputOrdering is only meaningful in the plain (non-grouped,
        // non-distinct) path. Setting it for grouped/distinct queries would be misleading in
        // verbose EXPLAIN even though LIMIT-pushdown is already guarded independently.
        if (scanStep is not null
            && ticket.OrderBy is { Count: > 0 }
            && ticket.GroupBy is not { Count: > 0 }
            && !ticket.IsDistinct
            && IndexScanSelector.ScanSatisfiesOrderBy(table, scanStep.Value, ticket.OrderBy))
            scanNode.OutputOrdering = ticket.OrderBy;

        bool scanSatisfiesOrderBy = scanNode.OutputOrdering is not null;
        plan.ScanRowLimit = TryComputeScanRowLimit(ticket, plan.ExecutionFilter, scanSatisfiesOrderBy);

        // R12: streaming distinct detection.
        // When SELECT DISTINCT projects only simple column identifiers that are covered by
        // an index prefix, use a streaming (adjacent-key) dedup instead of a hash set,
        // reducing memory from O(distinct-count) to O(1).
        // Override the scan with a full ordered index scan when no predicate-driven scan was
        // chosen (scanStep == null). Only override when scanStep is null so predicate-driven
        // scans are not disrupted; correctness of ExecutionFilter is unaffected since a
        // FullScanFromIndex step consumes no predicates, same as a null/table-scan step.
        bool isStreamingDistinct = false;
        IReadOnlyList<QueryOrderBy>? streamingDistinctOrdering = null;
        if (ticket.IsDistinct && ticket.GroupBy is not { Count: > 0 })
        {
            List<string>? distinctCols = TryExtractDistinctColumns(ticket.Projection);
            if (distinctCols is { Count: > 0 } && AllDistinctColumnsAreNotNull(table, distinctCols))
            {
                if (scanStep is not null && IndexScanSelector.ScanStepCoversDistinctColumns(scanStep.Value, distinctCols))
                {
                    isStreamingDistinct = true;
                    streamingDistinctOrdering = BuildDistinctOrdering(scanStep.Value.Index!, distinctCols);
                }
                else if (scanStep is null || scanStep.Value.Type == QueryPlanStepType.FullScanFromTableIndex)
                {
                    TableIndexSchema? distinctIndex = IndexScanSelector.TryFindStreamingDistinctIndex(table, distinctCols);
                    if (distinctIndex is not null)
                    {
                        scanNode = new TableScanNode(TableScanSource.ForcedIndex, distinctIndex);
                        isStreamingDistinct = true;
                        streamingDistinctOrdering = BuildDistinctOrdering(distinctIndex, distinctCols);
                    }
                }
            }
        }

        PhysicalPlanNode root = scanNode;

        if (plan.ExecutionFilter is not null)
            root = new FilterNode(plan.ExecutionFilter, root);

        // R11: wrap the scan with SemiJoinNode(s) for each IN / NOT IN rewrite spec.
        if (ticket.SemiJoinSpecs is { Count: > 0 })
        {
            foreach (SemiJoinSpec spec in ticket.SemiJoinSpecs)
            {
                root = new SemiJoinNode(root, spec.Mode, spec.InnerTable,
                    spec.OuterColumn, spec.InnerColumn, spec.InnerIndex, spec.InnerFilter);
            }
        }

        bool hasGroupBy = ticket.GroupBy is { Count: > 0 };

        if (hasGroupBy)
        {
            root = new AggregateNode(root)
            {
                GroupByExpressions = ticket.GroupBy,
                AggregateProjections = ExtractAggregateProjections(ticket),
            };

            if (ticket.Having is not null)
                root = new HavingFilterNode(ticket.Having, root);

            if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };

            if (ticket.Projection is not null && ticket.Projection.Count > 0 && !QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                root = new ProjectNode(root);

            if (ticket.Limit is not null || ticket.Offset is not null)
                root = new LimitNode(root)
                {
                    LimitValue = EvalLong(ticket.Limit, ticket),
                    OffsetValue = EvalLong(ticket.Offset, ticket),
                };
        }
        else
        {
            if (ticket.IsDistinct)
            {
                if (ticket.Projection is not null && ticket.Projection.Count > 0)
                {
                    if (QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
                    {
                        root = new AggregateNode(root)
                        {
                            GroupByExpressions = ticket.GroupBy,
                            AggregateProjections = ExtractAggregateProjections(ticket),
                        };

                        if (ticket.Having is not null)
                            root = new HavingFilterNode(ticket.Having, root);
                    }

                    if (!QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                        root = new ProjectNode(root);
                }

                // R12: streaming distinct — O(1) memory dedup when the scan guarantees ordered output.
                DistinctNode distinctNode = new(root) { IsStreaming = isStreamingDistinct };
                if (isStreamingDistinct && streamingDistinctOrdering is not null)
                    distinctNode.OutputOrdering = streamingDistinctOrdering;
                root = distinctNode;

                if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                {
                    // Elide SortNode when the streaming-distinct ordering already satisfies ORDER BY.
                    bool orderElided = isStreamingDistinct
                        && streamingDistinctOrdering is not null
                        && StreamingOrderSatisfiesOrderBy(streamingDistinctOrdering, ticket.OrderBy);

                    if (!orderElided)
                        root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };
                }

                if (ticket.Limit is not null || ticket.Offset is not null)
                    root = new LimitNode(root)
                    {
                        LimitValue = EvalLong(ticket.Limit, ticket),
                        OffsetValue = EvalLong(ticket.Offset, ticket),
                    };
            }
            else
            {
                if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0 && !scanSatisfiesOrderBy)
                    root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };

                if (ticket.Limit is not null || ticket.Offset is not null)
                    root = new LimitNode(root)
                    {
                        LimitValue = EvalLong(ticket.Limit, ticket),
                        OffsetValue = EvalLong(ticket.Offset, ticket),
                    };

                if (ticket.Projection is not null && ticket.Projection.Count > 0)
                {
                    if (QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
                    {
                        root = new AggregateNode(root)
                        {
                            GroupByExpressions = ticket.GroupBy,
                            AggregateProjections = ExtractAggregateProjections(ticket),
                        };

                        if (ticket.Having is not null)
                            root = new HavingFilterNode(ticket.Having, root);
                    }

                    if (!QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                        root = new ProjectNode(root);
                }
            }
        }

        plan.Root = root;
        QueryPlanStepAdapter.PopulateLinearSteps(plan);
        ProjectionPushdownPlanner.Apply(plan);

        // R9: annotate every node with EstimatedCardinality and PlanCost using R8 statistics.
        CostEstimator.AnnotatePlan(plan.Root, database, table, _stats);

        // R10: record the plan's query-shape ID and schema-version dependencies.
        if (ticket.SelectQuery is not null)
            plan.QueryShapeId = QueryShapeComputer.Compute(ticket.SelectQuery);

        List<(string, int)> schemaDeps = [(table.Name, table.Schema.Version)];
        if (ticket.SemiJoinSpecs is { Count: > 0 })
        {
            foreach (SemiJoinSpec spec in ticket.SemiJoinSpecs)
                schemaDeps.Add((spec.InnerTable.Name, spec.InnerTable.Schema.Version));
        }
        plan.SchemaDeps = schemaDeps;

        return plan;
    }

    private static long? TryComputeScanRowLimit(
        QueryTicket ticket,
        NodeAst? executionFilter,
        bool scanSatisfiesOrderBy)
    {
        if (ticket.Limit is null)
            return null;

        if (ticket.IsDistinct)
            return null;

        if (ticket.GroupBy is { Count: > 0 } || ticket.Having is not null)
            return null;

        if (ticket.Projection is { Count: > 0 } && QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
            return null;

        if (executionFilter is not null)
            return null;

        if (ticket.OrderBy is { Count: > 0 } && !scanSatisfiesOrderBy)
            return null;

        ColumnValue limit = SqlExecutor.EvalExpr(ticket.Limit, new(), ticket.Parameters);
        if (limit.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Limit is not Integer64");

        long scanLimit = limit.LongValue;

        if (ticket.Offset is not null)
        {
            ColumnValue offset = SqlExecutor.EvalExpr(ticket.Offset, new(), ticket.Parameters);
            if (offset.Type != ColumnType.Integer64)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Offset is not Integer64");

            try
            {
                scanLimit = checked(scanLimit + offset.LongValue);
            }
            catch (OverflowException)
            {
                scanLimit = long.MaxValue;
            }
        }

        if (scanLimit <= 0)
            return 0;

        return scanLimit;
    }

    private (PhysicalPlanNode ScanNode, QueryPlanStep? ScanStep, NodeAst? AbsorbedInListConjunct) BuildScanNode(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        PredicateAnalysis analysis)
    {
        QueryPlanStep? scanStep = IndexScanSelector.TrySelectScan(table, analysis, ticket.OrderBy);

        // R9 cost-model override: if a predicate-driven range scan is selected but the cost model
        // estimates it would touch more rows than the breakeven fraction of the table, prefer a full
        // primary table scan instead. Only apply when:
        //   1. Stats are available (row count known).
        //   2. The step is a predicate-driven RangeScanFromIndex (has at least one bound).
        //      Skip ORDER BY-driven unbounded scans to preserve sort elision.
        //   3. The scan is not already a unique point lookup (those always win).
        //   4. The transaction is NOT Serializable+RW — for Serializable+RW the tightest covering
        //      range lock matters for concurrency correctness, so the cost model must not widen it
        //      to a whole-bucket lock by choosing a full table scan.
        bool isSerializableRW = ticket.TxnState.IsolationLevel == CamusIsolationLevel.Serializable
            && ticket.TxnState.TransactionMode == CamusTransactionMode.ReadWrite;
        if (scanStep is { Type: QueryPlanStepType.RangeScanFromIndex } step
            && (step.FromBound is not null || step.ToBound is not null)
            && _stats is not null
            && !isSerializableRW)
        {
            long? tableRowCount = _stats.GetRowCountEstimate(database, table);
            if (tableRowCount is { } trc && trc > 0)
            {
                var tempRangeNode = (IndexRangeScanNode)ToScanNode(scanStep.Value);
                if (CostEstimator.ShouldPreferFullScan(tempRangeNode, trc, _stats, database, table))
                    scanStep = null; // fall through to full table scan
            }
        }

        // R15b finding #1: if a predicate-driven range scan was selected, check whether a unique
        // single-column IN-list on another column would be cheaper. Unique point-lookups have a
        // deterministic cost of 2·N (N index reads + N row fetches), making them a safe bet against
        // range scans of unknown cardinality. Guard: never switch when the range scan was chosen
        // specifically to satisfy ORDER BY (that would lose sort-elision).
        if (scanStep is { Type: QueryPlanStepType.RangeScanFromIndex } rangeStep
            && (rangeStep.FromBound is not null || rangeStep.ToBound is not null)
            && analysis.InListComparisons is { Count: > 0 })
        {
            bool rangeSatisfiesOrder = ticket.OrderBy is { Count: > 0 }
                && IndexScanSelector.ScanSatisfiesOrderBy(table, rangeStep, ticket.OrderBy);
            if (!rangeSatisfiesOrder)
            {
                IndexInListScanNode? competitor = TryBuildCompetingInListScanNode(
                    database, table, analysis.InListComparisons, rangeStep);
                if (competitor is not null)
                {
                    NodeAst? absorbed = analysis.InListComparisons
                        .FirstOrDefault(il => string.Equals(il.ColumnName, competitor.ColumnName, StringComparison.Ordinal))
                        ?.Conjunct;
                    return (competitor, null, absorbed);
                }
            }
        }

        if (scanStep is not null)
            return (ToScanNode(scanStep.Value), scanStep, null);

        // R15: when no regular index scan was chosen, try an index-driven IN-list scan.
        // Only attempted when there are IN-list comparisons in the predicate analysis.
        if (analysis.InListComparisons is { Count: > 0 })
        {
            IndexInListScanNode? inListNode = TryBuildInListScanNode(database, table, analysis.InListComparisons);
            if (inListNode is not null)
            {
                NodeAst? absorbed = analysis.InListComparisons
                    .FirstOrDefault(il => string.Equals(il.ColumnName, inListNode.ColumnName, StringComparison.Ordinal))
                    ?.Conjunct;
                return (inListNode, null, absorbed);
            }
        }

        if (!string.IsNullOrEmpty(ticket.IndexName))
        {
            if (!table.Indexes.TryGetValue(ticket.IndexName, out TableIndexSchema? index))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownKey,
                    $"Key '{ticket.IndexName}' doesn't exist in table '{table.Name}'");
            }

            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownKey,
                    $"Key '{ticket.IndexName}' doesn't exist in table '{table.Name}'");
            }

            QueryPlanStep forcedIndexStep = new(QueryPlanStepType.FullScanFromIndex, index);
            return (new TableScanNode(TableScanSource.ForcedIndex, index), forcedIndexStep, null);
        }

        QueryPlanStep tableScanStep = new(QueryPlanStepType.FullScanFromTableIndex);
        return (new TableScanNode(TableScanSource.PrimaryRows), tableScanStep, null);
    }

    /// <summary>
    /// Attempts to build an <see cref="IndexInListScanNode"/> for the first IN-list comparison
    /// whose column has a usable index and whose list size passes the cost gate.
    ///
    /// Cost gate (R15):
    ///   • With stats: prefer seeks when <c>2·N &lt; tableRowCount</c>
    ///     (each seek = 1 index read + 1 row fetch).
    ///   • Without stats: allow up to <c>MaxInListSizeWithoutStats</c> values (1000).
    ///
    /// Unique indexes support all column types via <c>LookupUnique</c>.
    /// Non-unique indexes use an equality-as-range scan: types with a computable successor
    /// (Integer64, Float64, Bool) use <c>[v, successor(v))</c>; String and Id use an
    /// exact-match scan <c>[v, v]</c> inclusive — <see cref="KvTableStore.ScanIndex"/> appends
    /// a high sentinel for non-unique, so all <c>encode(v)+rowIdHex</c> entries are captured
    /// and the decoded-key bounds filter keeps only rows where key == v (R15b).
    /// </summary>
    private IndexInListScanNode? TryBuildInListScanNode(
        DatabaseDescriptor database,
        TableDescriptor table,
        IReadOnlyList<AnalyzedInList> inListComparisons)
    {
        const int MaxInListSizeWithoutStats = 1000;

        foreach (AnalyzedInList inList in inListComparisons)
        {
            if (inList.Values.Count == 0)
                continue;

            TableIndexSchema? bestIndex = null;
            bool bestIsUnique = false;

            foreach (TableIndexSchema index in table.Indexes.Values)
            {
                if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                    continue;

                if (index.Columns.Length == 0
                    || !string.Equals(index.Columns[0], inList.ColumnName, StringComparison.Ordinal))
                    continue;

                bool isUnique = index.Type == IndexType.Unique;

                // Unique index: requires all key columns for LookupUnique — only single-column unique
                // indexes are eligible (a composite unique index needs a full composite key to resolve
                // to a single row, and a partial key would silently find nothing).
                if (isUnique && index.Columns.Length != 1)
                    continue;

                // Prefer unique single-column over non-unique; among equals pick first.
                if (bestIndex is null || (isUnique && !bestIsUnique))
                {
                    bestIndex = index;
                    bestIsUnique = isUnique;
                }
            }

            if (bestIndex is null)
                continue;

            int n = inList.Values.Count;

            // Cost gate.
            if (_stats is not null)
            {
                long? tableRowCount = _stats.GetRowCountEstimate(database, table);
                if (tableRowCount is { } trc && trc > 0 && 2L * n >= trc)
                    continue; // full scan is cheaper
            }
            else if (n > MaxInListSizeWithoutStats)
            {
                continue;
            }

            return new IndexInListScanNode(bestIndex, inList.ColumnName, inList.Values);
        }

        return null;
    }

    /// <summary>
    /// Attempts to find a unique single-column IN-list that is cheaper than an already-selected
    /// range scan. Only unique single-column indexes compete here — their cost is exactly 2·N
    /// (N point lookups + N row fetches), bounded and predictable regardless of data distribution.
    ///
    /// Cost comparison:
    ///   • With stats: 2·N vs 2·estimated_range_rows.
    ///   • Without stats: compete only for N ≤ <c>MaxCompetingInListSizeWithoutStats</c> (100),
    ///     assuming N bounded point-lookups beat an unconstrained range scan.
    /// </summary>
    private IndexInListScanNode? TryBuildCompetingInListScanNode(
        DatabaseDescriptor database,
        TableDescriptor table,
        IReadOnlyList<AnalyzedInList> inListComparisons,
        QueryPlanStep rangeScanStep)
    {
        const int MaxCompetingInListSizeWithoutStats = 100;

        foreach (AnalyzedInList inList in inListComparisons)
        {
            if (inList.Values.Count == 0)
                continue;

            // Only compete for unique single-column indexes: cost is exactly 2·N.
            TableIndexSchema? uniqueIndex = null;
            foreach (TableIndexSchema index in table.Indexes.Values)
            {
                if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                    continue;
                if (index.Type != IndexType.Unique || index.Columns.Length != 1)
                    continue;
                if (!string.Equals(index.Columns[0], inList.ColumnName, StringComparison.Ordinal))
                    continue;
                uniqueIndex = index;
                break;
            }

            if (uniqueIndex is null)
                continue;

            int n = inList.Values.Count;

            if (_stats is not null)
            {
                long? tableRowCount = _stats.GetRowCountEstimate(database, table);
                if (tableRowCount is { } trc && trc > 0)
                {
                    var tempRangeNode = (IndexRangeScanNode)ToScanNode(rangeScanStep);
                    long rangeRows = CostEstimator.EstimateRangeScanRows(tempRangeNode, trc, _stats, database, table);
                    if (2L * n >= 2L * rangeRows)
                        continue; // range scan is cheaper or equal
                }
                // Stats present but no row-count estimate — fall through and compete conservatively.
            }
            else if (n > MaxCompetingInListSizeWithoutStats)
            {
                continue; // without stats, only compete for small bounded lists
            }

            return new IndexInListScanNode(uniqueIndex, inList.ColumnName, inList.Values);
        }

        return null;
    }

    private static PhysicalPlanNode ToScanNode(QueryPlanStep step)
    {
        switch (step.Type)
        {
            case QueryPlanStepType.QueryFromIndex:
            {
                CompositeColumnValue lookupKey = step.LookupKey
                    ?? new CompositeColumnValue(new[] { step.ColumnValue! });

                return new IndexLookupNode(step.Index!, lookupKey);
            }

            case QueryPlanStepType.RangeScanFromIndex:
                return new IndexRangeScanNode(
                    step.Index!,
                    step.FromBound,
                    step.FromInclusive,
                    step.ToBound,
                    step.ToInclusive);

            case QueryPlanStepType.FullScanFromIndex:
                return new TableScanNode(TableScanSource.ForcedIndex, step.Index);

            case QueryPlanStepType.FullScanFromTableIndex:
                return new TableScanNode(TableScanSource.PrimaryRows);

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Cannot convert plan step to scan node: {step.Type}");
        }
    }

    private static IReadOnlyList<NodeAst>? ExtractAggregateProjections(QueryTicket ticket)
    {
        if (ticket.Projection is not { Count: > 0 })
            return null;

        List<NodeAst>? result = null;
        foreach (NodeAst proj in ticket.Projection)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(proj))
                (result ??= new()).Add(proj);
        }
        return result;
    }

    /// <summary>
    /// Extracts a flat list of bare column names from the projection list for R12 streaming-distinct
    /// eligibility. Returns null if any projection item is a non-identifier expression (aggregate,
    /// arithmetic, alias over expression, wildcard) — those block streaming.
    /// </summary>
    // Indexes don't hold NULL-keyed rows (BackfillIndex throws on NULL), so a full index
    // scan silently omits the NULL group. Gate streaming to NOT NULL columns only.
    private static bool AllDistinctColumnsAreNotNull(TableDescriptor table, IReadOnlyList<string> distinctCols)
    {
        if (table.Schema.Columns is null)
            return false;

        foreach (string col in distinctCols)
        {
            TableColumnSchema? schema = table.Schema.Columns.Find(c => c.Name == col);
            if (schema is null || !schema.NotNull)
                return false;
        }

        return true;
    }

    private static List<string>? TryExtractDistinctColumns(List<NodeAst>? projection)
    {
        if (projection is null || projection.Count == 0)
            return null;

        List<string> cols = new(projection.Count);
        foreach (NodeAst proj in projection)
        {
            // Unwrap a simple alias (col AS alias) to get the underlying identifier.
            NodeAst expr = proj.nodeType == NodeType.ExprAlias ? proj.leftAst! : proj;

            if (expr.nodeType != NodeType.Identifier || expr.yytext is null)
                return null; // expression — streaming not possible

            cols.Add(expr.yytext);
        }

        return cols;
    }

    /// <summary>
    /// Builds an ascending OutputOrdering from the first <c>distinctCols.Count</c> columns
    /// of <paramref name="index"/>. This is the order a streaming-distinct scan guarantees.
    /// </summary>
    private static List<QueryOrderBy> BuildDistinctOrdering(TableIndexSchema index, IReadOnlyList<string> distinctCols)
    {
        List<QueryOrderBy> ordering = new(distinctCols.Count);
        for (int i = 0; i < distinctCols.Count; i++)
            ordering.Add(new QueryOrderBy(index.Columns[i], OrderType.Ascending));
        return ordering;
    }

    /// <summary>
    /// Returns true when the streaming-distinct ordering (ascending by the index prefix) is a
    /// prefix of the requested ORDER BY, allowing the SortNode to be elided (R12 sort elision).
    /// </summary>
    private static bool StreamingOrderSatisfiesOrderBy(
        IReadOnlyList<QueryOrderBy> streamingOrdering,
        IReadOnlyList<QueryOrderBy> orderBy)
    {
        if (orderBy.Count > streamingOrdering.Count)
            return false;

        for (int i = 0; i < orderBy.Count; i++)
        {
            if (orderBy[i].Type != OrderType.Ascending)
                return false; // streaming only guarantees ascending

            if (!string.Equals(orderBy[i].ColumnName, streamingOrdering[i].ColumnName, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static long? EvalLong(NodeAst? expr, QueryTicket ticket)
    {
        if (expr is null)
            return null;

        ColumnValue val = SqlExecutor.EvalExpr(expr, new(), ticket.Parameters);
        return val.Type == ColumnType.Integer64 ? val.LongValue : null;
    }

}
