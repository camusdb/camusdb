
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Assigns <see cref="PlanCost"/> and <see cref="PhysicalPlanNode.EstimatedCardinality"/> to every
/// node in a physical plan tree (R9).
///
/// Selectivity assumptions (fixed defaults; R9b will supply per-column stats once implemented):
/// <list type="bullet">
///   <item>Unique index lookup → 1 row.</item>
///   <item>Range scan with both bounds → <see cref="BothBoundsSelectivity"/> (10 %) of table rows.</item>
///   <item>Range scan with one bound  → <see cref="OneBoundSelectivity"/>   (40 %) of table rows.</item>
///   <item>Range scan with no bounds  → 100 % of table rows (full index scan).</item>
///   <item>Filter node                → <see cref="FilterSelectivity"/>      (10 %) of input rows.</item>
///   <item>Aggregate (group by)       → 20 % of input rows (rough group-count estimate).</item>
///   <item>Aggregate (no group by)    → 1 row.</item>
///   <item>Distinct                   → 70 % of input rows (assumes 30 % duplicates).</item>
///   <item>Sort, Project              → same as input.</item>
///   <item>Limit                      → min(limit, input).</item>
/// </list>
///
/// Cost weights (see <see cref="PlanCost"/>):
///   Point lookup = range entry = row fetch after index = 1.0 unit each.
///   In-memory row = 0.1 unit.
///
/// Breakeven rule (used by <see cref="ShouldPreferFullScan"/>):
///   A non-covering secondary index scan plus the required primary-store row fetch costs
///   <c>2 × estimated_index_entries</c>. A full primary table scan costs <c>tableRowCount × 1</c>.
///   Prefer full scan when <c>estimated_index_entries > tableRowCount × BreakevenFraction</c>
///   (i.e. the index would touch more than half the table — primary scan is then cheaper).
/// </summary>
internal static class CostEstimator
{
    private const double BothBoundsSelectivity = 0.10;
    private const double OneBoundSelectivity   = 0.40;
    private const double FilterSelectivity     = 0.10;
    private const double AggregateSelectivity  = 0.20;
    private const double DistinctSelectivity   = 0.70;

    // Index scan becomes more expensive than full table scan when it touches this
    // fraction (or more) of the table.  At 0.5: index cost = 2×(0.5×N) = N = full-scan cost.
    // Use a slightly-below-half threshold so the breakeven flips at 40 % (one-bound default).
    private const double BreakevenFraction = 0.40;

    private const long DefaultTableRowCount = 10_000;

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Walks <paramref name="root"/> top-down, assigning <see cref="PhysicalPlanNode.EstimatedCardinality"/>
    /// and <see cref="PhysicalPlanNode.Cost"/> to every node using R8 row-count statistics.
    /// Safe to call with a null stats manager — all estimates degrade to fixed defaults.
    /// </summary>
    public static void AnnotatePlan(
        PhysicalPlanNode root,
        DatabaseDescriptor database,
        TableDescriptor? table,
        StatisticsManager? stats)
    {
        long? tableRowCount = table is not null
            ? stats?.GetRowCountEstimate(database, table)
            : null;

        AnnotateNode(root, tableRowCount);
    }

    /// <summary>
    /// Returns true when replacing a predicate-driven <see cref="IndexRangeScanNode"/> with a full
    /// primary table scan would be cheaper under the cost model.
    ///
    /// Decision rule: index cost = 2 × estimated_range_entries (index read + row fetch);
    /// full-scan cost = tableRowCount.  When index cost &gt; breakeven × tableRowCount, prefer full scan.
    ///
    /// Returns <c>false</c> whenever table stats are unavailable (preserves existing heuristic).
    /// </summary>
    public static bool ShouldPreferFullScan(IndexRangeScanNode indexNode, long tableRowCount)
    {
        long estimatedEntries = EstimateRangeScanRows(indexNode, tableRowCount);
        // Index cost = entries scanned + row fetches (one each); full scan = tableRowCount.
        // Prefer full scan when index would touch >= BreakevenFraction of the table.
        // Use ceiling so tiny tables (e.g. 3 rows with both-bounds at 10 %) don't mis-trigger:
        //   ceil(3 * 0.40) = 2, and estimated_entries = max(1, 0.30) = 1 → 1 < 2 → index wins.
        // At BreakevenFraction=0.40 with OneBoundSelectivity=0.40 and N=10000:
        //   ceil(10000 * 0.40) = 4000, estimated_entries = 4000 → 4000 >= 4000 → full scan.
        long breakeven = (long)Math.Ceiling(tableRowCount * BreakevenFraction);
        return estimatedEntries >= breakeven;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tree annotation
    // ─────────────────────────────────────────────────────────────────────────

    private static long AnnotateNode(PhysicalPlanNode node, long? tableRowCount)
    {
        long inputCardinality = node.Input is not null
            ? AnnotateNode(node.Input, tableRowCount)
            : tableRowCount ?? DefaultTableRowCount;

        (long cardinality, PlanCost cost) = EstimateNodeCost(node, inputCardinality, tableRowCount);

        node.EstimatedCardinality = cardinality;
        node.Cost = cost;
        return cardinality;
    }

    private static (long Cardinality, PlanCost Cost) EstimateNodeCost(
        PhysicalPlanNode node,
        long inputCardinality,
        long? tableRowCount)
    {
        long trc = tableRowCount ?? DefaultTableRowCount;

        switch (node)
        {
            case IndexLookupNode:
            {
                // Unique index: 1 point lookup + 1 primary row fetch.
                long rows = 1;
                return (rows, new PlanCost
                {
                    EstimatedRows       = rows,
                    KvPointLookups      = 1,
                    RowFetchesAfterIndex = 1,
                });
            }

            case IndexRangeScanNode rangeNode:
            {
                long rows = EstimateRangeScanRows(rangeNode, trc);
                return (rows, new PlanCost
                {
                    EstimatedRows        = rows,
                    KvRangeScanEntries   = rows,
                    RowFetchesAfterIndex = rows,
                });
            }

            case TableScanNode:
            {
                long rows = trc;
                return (rows, new PlanCost
                {
                    EstimatedRows      = rows,
                    KvRangeScanEntries = rows,
                });
            }

            case FilterNode:
            {
                long rows = Math.Max(1, (long)(inputCardinality * FilterSelectivity));
                return (rows, new PlanCost { EstimatedRows = rows });
            }

            case AggregateNode agg:
            {
                long rows = agg.GroupByExpressions is { Count: > 0 }
                    ? Math.Max(1, (long)(inputCardinality * AggregateSelectivity))
                    : 1;
                return (rows, new PlanCost { EstimatedRows = rows, InMemoryRows = inputCardinality });
            }

            case HavingFilterNode:
            {
                long rows = Math.Max(1, (long)(inputCardinality * FilterSelectivity));
                return (rows, new PlanCost { EstimatedRows = rows });
            }

            case SortNode:
            {
                return (inputCardinality, new PlanCost
                {
                    EstimatedRows = inputCardinality,
                    InMemoryRows  = inputCardinality,
                });
            }

            case LimitNode limit:
            {
                long rows = limit.LimitValue.HasValue
                    ? Math.Min(limit.LimitValue.Value, inputCardinality)
                    : inputCardinality;
                return (rows, new PlanCost { EstimatedRows = rows });
            }

            case DistinctNode:
            {
                long rows = Math.Max(1, (long)(inputCardinality * DistinctSelectivity));
                return (rows, new PlanCost { EstimatedRows = rows, InMemoryRows = inputCardinality });
            }

            case ProjectNode:
                return (inputCardinality, new PlanCost { EstimatedRows = inputCardinality });

            case NestedLoopJoinNode nlj:
            {
                // Cross-product upper bound, filtered by the on-predicate (10 % default).
                long innerRows = trc;
                long rows = Math.Max(1, (long)(inputCardinality * innerRows * FilterSelectivity));
                return (rows, new PlanCost
                {
                    EstimatedRows      = rows,
                    KvRangeScanEntries = inputCardinality * innerRows,
                    InMemoryRows       = 0,
                });
            }

            case IndexNestedLoopJoinNode inlj:
            {
                // Outer row drives one unique-index lookup on the inner side.
                long rows = inputCardinality;
                return (rows, new PlanCost
                {
                    EstimatedRows        = rows,
                    KvPointLookups       = inputCardinality,
                    RowFetchesAfterIndex = inputCardinality,
                });
            }

            case DerivedTableScanNode:
                return (inputCardinality, new PlanCost { EstimatedRows = inputCardinality });

            default:
                return (inputCardinality, new PlanCost { EstimatedRows = inputCardinality });
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    internal static long EstimateRangeScanRows(IndexRangeScanNode node, long tableRowCount)
    {
        bool hasFrom = node.FromBound is not null;
        bool hasTo   = node.ToBound   is not null;

        double selectivity = (hasFrom && hasTo) ? BothBoundsSelectivity
                           : (hasFrom || hasTo) ? OneBoundSelectivity
                           : 1.0; // unbounded — full index scan

        return Math.Max(1, (long)(tableRowCount * selectivity));
    }
}
