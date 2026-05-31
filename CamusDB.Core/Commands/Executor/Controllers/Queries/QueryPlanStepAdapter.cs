
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Bridges the physical plan tree to the legacy linear <see cref="QueryPlanStep"/> list
/// consumed by <see cref="QueryExecutor"/>.
/// </summary>
internal static class QueryPlanStepAdapter
{
    public static void PopulateLinearSteps(QueryPlan plan)
    {
        plan.Steps.Clear();

        if (plan.Root is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Query plan root is null");

        Flatten(plan.Root, plan.Steps);
    }

    private static void Flatten(PhysicalPlanNode node, List<QueryPlanStep> steps)
    {
        if (node.Input is not null)
            Flatten(node.Input, steps);

        switch (node)
        {
            case TableScanNode tableScan:
                steps.Add(ToStep(tableScan));
                return;

            case IndexLookupNode indexLookup:
                steps.Add(new QueryPlanStep(QueryPlanStepType.QueryFromIndex, indexLookup.Index, indexLookup.LookupKey));
                return;

            case IndexRangeScanNode rangeScan:
                steps.Add(new QueryPlanStep(
                    QueryPlanStepType.RangeScanFromIndex,
                    rangeScan.Index,
                    rangeScan.FromBound,
                    rangeScan.FromInclusive,
                    rangeScan.ToBound,
                    rangeScan.ToInclusive));
                return;

            case FilterNode:
                // Row filtering uses <see cref="QueryPlan.ExecutionFilter"/> during scan execution.
                return;

            case NestedLoopJoinNode:
                return;

            case IndexNestedLoopJoinNode:
                return;

            case SortNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.SortBy));
                return;

            case LimitNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.Limit));
                return;

            case AggregateNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.Aggregate));
                return;

            case ProjectNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.ReduceToProjections));
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Unknown physical plan node: {node.GetType().Name}");
        }
    }

    private static QueryPlanStep ToStep(TableScanNode tableScan)
    {
        return tableScan.Source switch
        {
            TableScanSource.PrimaryRows => new QueryPlanStep(QueryPlanStepType.FullScanFromTableIndex),
            TableScanSource.ForcedIndex => new QueryPlanStep(QueryPlanStepType.FullScanFromIndex, tableScan.Index),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Unknown table scan source: {tableScan.Source}")
        };
    }
}
