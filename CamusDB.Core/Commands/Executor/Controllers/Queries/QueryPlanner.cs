
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical plan tree for single-table SELECT queries (QP2 physical planning phase).
/// </summary>
public sealed class QueryPlanner
{
    public QueryPlanner()
    {
    }

    public QueryPlan GetPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = new(database, table, ticket);

        PredicateAnalysis analysis = ticket.AnalyzedWhere is not null
            ? PredicateAnalyzer.Merge(ticket.AnalyzedWhere, PredicateAnalyzer.AnalyzeFilters(ticket.Filters))
            : PredicateAnalyzer.AnalyzeTicket(ticket);
        plan.PredicateAnalysis = analysis;

        (PhysicalPlanNode scanNode, QueryPlanStep? scanStep) = BuildScanNode(table, ticket, analysis);
        plan.ExecutionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep, table);

        PhysicalPlanNode root = scanNode;

        if (plan.ExecutionFilter is not null)
            root = new FilterNode(plan.ExecutionFilter, root);

        bool hasGroupBy = ticket.GroupBy is { Count: > 0 };

        if (hasGroupBy)
        {
            root = new AggregateNode(root);

            if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                root = new SortNode(root);

            if (ticket.Projection is not null && ticket.Projection.Count > 0 && !IsFullProjection(ticket.Projection))
                root = new ProjectNode(root);

            if (ticket.Limit is not null || ticket.Offset is not null)
                root = new LimitNode(root);
        }
        else
        {
            if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                root = new SortNode(root);

            if (ticket.Limit is not null || ticket.Offset is not null)
                root = new LimitNode(root);

            if (ticket.Projection is not null && ticket.Projection.Count > 0)
            {
                if (HasAggregation(ticket.Projection, ticket))
                    root = new AggregateNode(root);

                if (!IsFullProjection(ticket.Projection))
                    root = new ProjectNode(root);
            }
        }

        plan.Root = root;
        QueryPlanStepAdapter.PopulateLinearSteps(plan);

        return plan;
    }

    private static (PhysicalPlanNode ScanNode, QueryPlanStep? ScanStep) BuildScanNode(
        TableDescriptor table,
        QueryTicket ticket,
        PredicateAnalysis analysis)
    {
        QueryPlanStep? scanStep = IndexScanSelector.TrySelectScan(table, analysis, ticket.OrderBy);

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

    private static bool IsFullProjection(List<NodeAst> projection)
    {
        return projection is [{ nodeType: NodeType.ExprAllFields }];
    }

    private static bool HasAggregation(List<NodeAst> projection, QueryTicket ticket)
    {
        foreach (NodeAst nodeAst in projection)
        {
            switch (nodeAst.nodeType)
            {
                case NodeType.ExprFuncCall:
                    return CheckIfSupportedAggregation(nodeAst, projection, ticket);
                
                case NodeType.ExprAlias:
                    return CheckIfSupportedAggregation(nodeAst.leftAst!, projection, ticket);
            }
        }

        return false;
    }

    private static bool CheckIfSupportedAggregation(NodeAst nodeAst, List<NodeAst> projection, QueryTicket ticket)
    {
        switch (nodeAst.leftAst!.yytext!.ToLowerInvariant())
        {
            case "count":
            case "max":
            case "min":
            case "sum":
            case "avg":
            case "distinct":

                if (projection.Count > 1 && ticket.GroupBy is not { Count: > 0 })
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Aggregations cannot be accompanied by other projections or expressions.");

                return true;
        }

        return false;
    }
}

