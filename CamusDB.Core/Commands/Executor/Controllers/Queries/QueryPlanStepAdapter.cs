
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.Hashing;
using System.Text;
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
        NotePlanShape(plan);
    }

    /// <summary>
    /// Gives the statement's probe a hash of this plan's access paths, so statement statistics can
    /// count the executions whose plan differed from the one before.
    ///
    /// <para>The hash covers what decides how the data is read: the node kinds, the table each scan
    /// reads, whether a scan reads the rows or an index, and which index. It leaves out every bound,
    /// literal and estimate. Two executions that differ only in their literals therefore hash the
    /// same, and a new index or a fresh <c>ANALYZE</c> that flips an access path hashes differently.
    /// It is computed here, at plan time, rather than from what the execution touched: an execution
    /// whose outer side happened to be empty never probes its inner index, and counting that as a
    /// plan change would report the data, not the plan.</para>
    /// </summary>
    private static void NotePlanShape(QueryPlan plan)
    {
        if (plan.Ticket.Probe is not { } probe)
            return;

        StringBuilder shape = new(128);

        // A plan whose leaves are all functions in FROM has no base table (QueryPlan.WithoutTable):
        // INSERT ... SELECT FROM generate_series(...), or a set operation whose operands are all
        // FROM-less. Its node kinds still make a shape.
        if (plan.Table is not null)
            shape.Append(plan.Table.Id);

        shape.Append('|');
        AppendShape(plan.Root, shape);

        probe.NotePlan(XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(shape.ToString())));
    }

    private static void AppendShape(PhysicalPlanNode? node, StringBuilder shape)
    {
        for (; node is not null; node = node.Input)
        {
            shape.Append(node.GetType().Name).Append('(');

            switch (node)
            {
                case TableScanNode scan:
                    shape.Append(scan.Source).Append(',').Append(scan.Index?.KvId);
                    break;

                case IndexLookupNode lookup:
                    shape.Append(lookup.Index.KvId);
                    break;

                case IndexRangeScanNode range:
                    shape.Append(range.Index.KvId);
                    break;

                case IndexInListScanNode inList:
                    shape.Append(inList.Index.KvId);
                    break;

                case SemiJoinNode semi:
                    shape.Append(semi.InnerIndex?.KvId);
                    break;

                case IndexNestedLoopJoinNode indexJoin:
                    shape.Append(indexJoin.RightSource.Table.Id).Append(',').Append(indexJoin.Index.KvId);
                    break;

                case NestedLoopJoinNode nestedLoop:
                    shape.Append(nestedLoop.RightSource.Table?.Table.Id).Append(',');
                    AppendShape(nestedLoop.RightPhysicalNode, shape);
                    break;

                case HashJoinNode hash:
                    shape.Append(hash.BuildSource.Table?.Table.Id).Append(',');
                    AppendShape(hash.RightPhysicalNode, shape);
                    break;

                case MergeJoinNode merge:
                    shape.Append(merge.RightSource.Table?.Table.Id).Append(',');
                    AppendShape(merge.RightPhysicalNode, shape);
                    break;
            }

            shape.Append(')');
        }
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
    ///
    /// <para>The step list covers the left chain of a join plan, a merge join's ordered right input,
    /// and the index access path of a hash or nested-loop join's right table. A right table with no
    /// access path of its own is a table the join node names, not a step, and the join reads it
    /// whole, so it is checked on the tree. An index nested-loop join seeks the index per outer row
    /// and is not a full scan; a derived right side runs its own plan, which reports its own scans.</para>
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

        for (PhysicalPlanNode? node = plan.Root; node is not null; node = node.Input)
        {
            bool fullRightScan = node switch
            {
                NestedLoopJoinNode nestedLoop => nestedLoop.RightPhysicalNode is null && nestedLoop.RightSource.Table is not null,
                HashJoinNode hash => hash.RightPhysicalNode is null && hash.BuildSource.Table is not null,
                MergeJoinNode merge => merge.RightPhysicalNode is null && merge.RightSource.Table is not null,
                _ => false,
            };

            if (fullRightScan)
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
                    rangeScan.ToInclusive)
                {
                    EqualityPrefixLength = rangeScan.EqualityPrefixLength,
                });
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

            case NestedLoopJoinNode nestedLoop:
                // A right table read through its own index path is a separate branch, as for merge.
                if (nestedLoop.RightPhysicalNode is not null)
                    Flatten(nestedLoop.RightPhysicalNode, steps, stepNodes);
                return;

            case IndexNestedLoopJoinNode:
                return;

            case HashJoinNode hash:
                if (hash.RightPhysicalNode is not null)
                    Flatten(hash.RightPhysicalNode, steps, stepNodes);
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
