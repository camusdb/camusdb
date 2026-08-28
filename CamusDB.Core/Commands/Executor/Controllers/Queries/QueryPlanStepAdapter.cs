
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.Catalogs.Models;

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
        plan.StepNodes.Clear();

        if (plan.Root is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Query plan root is null");

        Flatten(plan.Root, plan.Steps, plan.StepNodes);
        NoteScanStrategy(plan);
    }

    /// <summary>
    /// Tells the statement's diagnostic probe, if the slow query log is on, that this plan reads a
    /// whole relation rather than seeking through an index.
    ///
    /// <para>It is answered here, once per plan, because this is where the access-path decision has
    /// just been reduced to a flat list of steps. The scan loops could each infer the same fact from
    /// their own inputs, but then every loop would have to agree with the planner about what counts
    /// as a full scan, and a new scan shape would silently report nothing.</para>
    ///
    /// <para>A statement runs more than one plan when it has a join, a subquery or a derived table.
    /// The probe belongs to the statement rather than the plan, so the flag means "some part of this
    /// statement scanned a whole relation", which is the question an operator is actually asking.</para>
    /// </summary>
    private static void NoteScanStrategy(QueryPlan plan)
    {
        if (plan.Ticket.Probe is not { } probe)
            return;

        foreach (QueryPlanStep step in plan.Steps)
        {
            if (step.Type is QueryPlanStepType.FullScanFromTableIndex or QueryPlanStepType.FullScanFromIndex)
            {
                probe.NoteFullScan();
                return;
            }
        }
    }

    private static void Flatten(PhysicalPlanNode node, List<QueryPlanStep> steps, List<PhysicalPlanNode> stepNodes)
    {
        if (node.Input is not null)
            Flatten(node.Input, steps, stepNodes);

        switch (node)
        {
            case TableScanNode tableScan:
                steps.Add(ToStep(tableScan));
                stepNodes.Add(node);
                return;

            case IndexLookupNode indexLookup:
                steps.Add(new QueryPlanStep(QueryPlanStepType.QueryFromIndex, indexLookup.Index, indexLookup.LookupKey));
                stepNodes.Add(node);
                return;

            case IndexRangeScanNode rangeScan:
                steps.Add(new QueryPlanStep(
                    QueryPlanStepType.RangeScanFromIndex,
                    rangeScan.Index,
                    rangeScan.FromBound,
                    rangeScan.FromInclusive,
                    rangeScan.ToBound,
                    rangeScan.ToInclusive));
                stepNodes.Add(node);
                return;

            case IndexInListScanNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.InListScanFromIndex));
                stepNodes.Add(node);
                return;

            case FilterNode:
                // Row filtering uses <see cref="QueryPlan.ExecutionFilter"/> during scan execution.
                return;

            case GatherNode:
                // Transparent for the legacy step list: the gather executes the same scan
                // subtree the steps describe (per span), so step-list consumers — the
                // borrowed-decode check and the planner's scan-limit/index-only pattern
                // matches — see the scan exactly as they would without fragmentation.
                return;

            case NestedLoopJoinNode:
                return;

            case IndexNestedLoopJoinNode:
                return;

            case HashJoinNode:
                return;

            case MergeJoinNode mergeJoin:
                // The right physical node (SortNode or ForcedIndex TableScanNode) is a separate
                // branch not reachable via Input — flatten it explicitly so the linear step list
                // includes the right-side sort / scan steps for EXPLAIN.
                if (mergeJoin.RightPhysicalNode is not null)
                    Flatten(mergeJoin.RightPhysicalNode, steps, stepNodes);
                return;

            case DerivedTableScanNode:
                return;

            case SortNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.SortBy));
                stepNodes.Add(node);
                return;

            case LimitNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.Limit));
                stepNodes.Add(node);
                return;

            case AggregateNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.Aggregate));
                stepNodes.Add(node);
                return;

            case HavingFilterNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.HavingFilter));
                stepNodes.Add(node);
                return;

            case ProjectNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.ReduceToProjections));
                stepNodes.Add(node);
                return;

            case DistinctNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.Distinct));
                stepNodes.Add(node);
                return;

            case SemiJoinNode:
                steps.Add(new QueryPlanStep(QueryPlanStepType.SemiJoinProbe));
                stepNodes.Add(node);
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
