
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Annotates physical plan nodes with required column sets for scan-time decoding.
/// </summary>
internal static class ProjectionPushdownPlanner
{
    public static void Apply(QueryPlan plan)
    {
        if (plan.BoundQuery is not null)
            ApplyJoinPlan(plan);
        else
            ApplySingleTablePlan(plan);
    }

    private static void ApplySingleTablePlan(QueryPlan plan)
    {
        // Locate-phase tickets supply an explicit column set (WHERE + SET expressions only).
        // All other tickets derive the set from projections + WHERE via RequiredColumnAnalyzer.
        plan.ScanRequiredColumns = plan.Ticket.LocateColumns
            ?? RequiredColumnAnalyzer.ComputeSingleTable(plan.Ticket);
        AnnotateScanNodes(plan.Root, plan.ScanRequiredColumns);
    }

    private static void ApplyJoinPlan(QueryPlan plan)
    {
        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(plan.BoundQuery!, plan.Ticket.Where);
        plan.RequiredColumnsByAlias = RequiredColumnAnalyzer.ComputeJoinPlan(
            plan.BoundQuery!,
            plan.Ticket,
            pushdown.PostJoinFilter,
            pushdown.ScanFiltersByAlias);

        AnnotateJoinScanNodes(plan.Root, plan.RequiredColumnsByAlias);
    }

    private static void AnnotateScanNodes(PhysicalPlanNode node, IReadOnlySet<string>? requiredColumns)
    {
        node.RequiredColumns = requiredColumns;

        if (node.Input is not null)
            AnnotateScanNodes(node.Input, requiredColumns);
    }

    private static void AnnotateJoinScanNodes(
        PhysicalPlanNode node,
        IReadOnlyDictionary<string, IReadOnlySet<string>?> requiredByAlias)
    {
        if (node is TableScanNode { BoundSource: not null } scan)
            scan.RequiredColumns = requiredByAlias[scan.BoundSource.Alias];

        if (node.Input is not null)
            AnnotateJoinScanNodes(node.Input, requiredByAlias);
    }
}
