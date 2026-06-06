
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

        (PhysicalPlanNode scanNode, QueryPlanStep? scanStep) = BuildScanNode(database, table, ticket, analysis);
        plan.ExecutionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep, table);

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

                root = new DistinctNode(root);

                if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                    root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };

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

    private (PhysicalPlanNode ScanNode, QueryPlanStep? ScanStep) BuildScanNode(
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
        if (scanStep is { Type: QueryPlanStepType.RangeScanFromIndex } step
            && (step.FromBound is not null || step.ToBound is not null)
            && _stats is not null)
        {
            long? tableRowCount = _stats.GetRowCountEstimate(database, table);
            if (tableRowCount is { } trc && trc > 0)
            {
                var tempRangeNode = (IndexRangeScanNode)ToScanNode(scanStep.Value);
                if (CostEstimator.ShouldPreferFullScan(tempRangeNode, trc, _stats, database, table))
                    scanStep = null; // fall through to full table scan
            }
        }

        if (scanStep is not null)
            return (ToScanNode(scanStep.Value), scanStep);

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
            return (new TableScanNode(TableScanSource.ForcedIndex, index), forcedIndexStep);
        }

        QueryPlanStep tableScanStep = new(QueryPlanStepType.FullScanFromTableIndex);
        return (new TableScanNode(TableScanSource.PrimaryRows), tableScanStep);
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

    private static long? EvalLong(NodeAst? expr, QueryTicket ticket)
    {
        if (expr is null)
            return null;

        ColumnValue val = SqlExecutor.EvalExpr(expr, new(), ticket.Parameters);
        return val.Type == ColumnType.Integer64 ? val.LongValue : null;
    }

}
