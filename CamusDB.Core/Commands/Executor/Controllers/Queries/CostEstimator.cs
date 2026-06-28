
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
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Assigns <see cref="PlanCost"/> and <see cref="PhysicalPlanNode.EstimatedCardinality"/> to every
/// node in a physical plan tree.
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
///   Prefer full scan when <c>estimated_index_entries ≥ tableRowCount × BreakevenFraction</c>.
///
/// Join cost accuracy:
///   For single-table plans, the primary table's R8 row count is resolved once and threaded
///   through the tree. For join plans (called with <paramref name="table"/> = null), each
///   <see cref="TableScanNode"/> resolves its own table's stats via
///   <see cref="TableScanNode.BoundSource"/>, and each <see cref="NestedLoopJoinNode"/> /
///   <see cref="IndexNestedLoopJoinNode"/> resolves the right-side table's stats via its
///   <see cref="NestedLoopJoinNode.RightSource"/>. This avoids the previous fallback to a
///   single global default, giving each join source a meaningful cardinality estimate.
///
/// Cost-driven decisions in R9 (scope note):
///   Only <see cref="ShouldPreferFullScan"/> drives an actual plan change (low-selectivity
///   index range → full table scan). All other annotated costs are computed and surfaced in
///   EXPLAIN output but do not yet alter plan shape — unique-index lookup vs. multi-index,
///   NLJ vs. INLJ, and sort placement remain rule-based. R9b will extend cost-based selection
///   to those choices once per-column histograms make the estimates reliable enough.
/// </summary>
internal static class CostEstimator
{
    // Fallback selectivities used when R9b min/max is unavailable for the column.
    private const double BothBoundsSelectivity = 0.10;

    // R9b: when column min/max is available, real selectivity replaces this constant.
    // Without R9b stats the pessimistic 0.40 (= BreakevenFraction) ensures every half-open
    // range flips to a full table scan, which is safe but sub-optimal.
    private const double OneBoundSelectivity   = 0.40;
    private const double FilterSelectivity     = 0.10;
    private const double AggregateSelectivity  = 0.20;
    private const double DistinctSelectivity   = 0.70;

    // Index scan breaks even with a full table scan when it touches this fraction of rows.
    // A non-covering secondary index scan costs 2 × estimated_entries (index read + row fetch);
    // a full scan costs tableRowCount × 1. Prefer full scan when entries / N ≥ 0.5, i.e.
    // BreakevenFraction = 0.50. With R9b real selectivities the false-positive flip rate drops
    // significantly compared to the pre-R9b value of 0.40.
    private const double BreakevenFraction = 0.50;

    internal const long DefaultTableRowCount = 10_000;

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Walks <paramref name="root"/> top-down, assigning <see cref="PhysicalPlanNode.EstimatedCardinality"/>
    /// and <see cref="PhysicalPlanNode.Cost"/> to every node using R8 row-count statistics.
    ///
    /// <para>
    /// For single-table plans pass <paramref name="table"/> as the primary table; the planner has
    /// already resolved it. For join plans pass <c>null</c> — each <see cref="TableScanNode"/>
    /// will independently resolve its own per-table row count via its
    /// <see cref="TableScanNode.BoundSource"/>.
    /// </para>
    ///
    /// Safe to call with a null <paramref name="stats"/> manager — all estimates degrade to the
    /// fixed <c>DefaultTableRowCount</c> default.
    /// </summary>
    public static void AnnotatePlan(
        PhysicalPlanNode root,
        DatabaseDescriptor database,
        TableDescriptor? table,
        StatisticsManager? stats)
    {
        // Pre-resolve the single-table row count; null for join plans (resolved per-node below).
        long? singleTableRowCount = table is not null
            ? stats?.GetRowCountEstimate(database, table)
            : null;

        AnnotateNode(root, database, stats, singleTableRowCount, table);
    }

    /// <summary>
    /// Returns true when replacing a predicate-driven <see cref="IndexRangeScanNode"/> with a full
    /// primary table scan would be cheaper under the cost model.
    ///
    /// <para>
    /// When R9b <paramref name="stats"/> and <paramref name="table"/> are supplied, uses real
    /// column min/max to compute selectivity (replacing the fixed fallback constants).
    /// Without stats, falls back to <see cref="OneBoundSelectivity"/> / <see cref="BothBoundsSelectivity"/>.
    /// </para>
    ///
    /// Returns <c>false</c> whenever table stats are unavailable (preserves existing heuristic).
    /// </summary>
    public static bool ShouldPreferFullScan(
        IndexRangeScanNode indexNode,
        long tableRowCount,
        StatisticsManager? stats = null,
        DatabaseDescriptor? database = null,
        TableDescriptor? table = null)
    {
        long estimatedEntries = EstimateRangeScanRows(indexNode, tableRowCount, stats, database, table);
        // Use ceiling so tiny tables don't mis-trigger on rounding (e.g. ceil(3 × 0.50) = 2,
        // so 1 estimated entry < 2 breakeven → index wins).
        long breakeven = (long)Math.Ceiling(tableRowCount * BreakevenFraction);
        return estimatedEntries >= breakeven;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tree annotation
    // ─────────────────────────────────────────────────────────────────────────

    private static long AnnotateNode(
        PhysicalPlanNode node,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        long? rowCountHint,
        TableDescriptor? primaryTable = null)
    {
        // Resolve the effective table row count for this specific node.
        // In join plans (rowCountHint == null) each TableScanNode looks up its own table's stats.
        long? effectiveRowCount = ResolveNodeRowCount(node, database, stats, rowCountHint);

        long inputCardinality = node.Input is not null
            ? AnnotateNode(node.Input, database, stats, rowCountHint, primaryTable)
            : effectiveRowCount ?? DefaultTableRowCount;

        (long cardinality, PlanCost cost) = EstimateNodeCost(node, inputCardinality, effectiveRowCount, database, stats, primaryTable);

        node.EstimatedCardinality = cardinality;
        node.Cost = cost;
        return cardinality;
    }

    /// <summary>
    /// Returns the per-node row count: uses <paramref name="rowCountHint"/> when available
    /// (single-table plans), otherwise resolves per-table stats from the node's bound source.
    /// </summary>
    private static long? ResolveNodeRowCount(
        PhysicalPlanNode node,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        long? rowCountHint)
    {
        if (rowCountHint.HasValue)
            return rowCountHint;

        // Join-plan leaf: each TableScanNode carries its own BoundSource with a TableDescriptor.
        if (node is TableScanNode { BoundSource: { } boundSource })
            return stats?.GetRowCountEstimate(database, boundSource.Table);

        return null;
    }

    /// <summary>
    /// Looks up the row count for a join's right-side table, used by
    /// <see cref="NestedLoopJoinNode"/> to estimate inner-side size.
    /// Returns null when stats are unavailable (falls back to DefaultTableRowCount in caller).
    /// </summary>
    private static long? ResolveRightTableRowCount(
        BoundTableSource? rightTable,
        DatabaseDescriptor database,
        StatisticsManager? stats)
        => rightTable is not null ? stats?.GetRowCountEstimate(database, rightTable.Table) : null;

    private static (long Cardinality, PlanCost Cost) EstimateNodeCost(
        PhysicalPlanNode node,
        long inputCardinality,
        long? tableRowCount,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        TableDescriptor? primaryTable = null)
    {
        long trc = tableRowCount ?? DefaultTableRowCount;

        switch (node)
        {
            case IndexLookupNode:
            {
                long rows = 1;
                return (rows, new PlanCost
                {
                    EstimatedRows        = rows,
                    KvPointLookups       = 1,
                    RowFetchesAfterIndex = 1,
                });
            }

            case IndexInListScanNode inListNode:
            {
                long n = inListNode.Values.Count;
                bool isUnique = inListNode.Index.Type == CamusDB.Core.Catalogs.Models.IndexType.Unique;
                return (n, new PlanCost
                {
                    EstimatedRows        = n,
                    KvPointLookups       = isUnique ? n : 0,
                    KvRangeScanEntries   = isUnique ? 0 : n,
                    RowFetchesAfterIndex = n,
                });
            }

            case IndexRangeScanNode rangeNode:
            {
                long rows = EstimateRangeScanRows(rangeNode, trc, stats, database, primaryTable);
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

            case FilterNode filterNode:
            {
                // B1: use histogram-based selectivity when stats and primary table are available.
                double filterSel = FilterSelectivity;
                if (stats is not null && primaryTable is not null)
                    filterSel = CardinalityEstimator.EstimateFilterSelectivity(filterNode.Predicate, database, primaryTable, stats);

                long rows = Math.Max(1, (long)(inputCardinality * filterSel));
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
                return (inputCardinality, new PlanCost
                {
                    EstimatedRows = inputCardinality,
                    InMemoryRows  = inputCardinality,
                });

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
                long innerRows = ResolveRightTableRowCount(nlj.RightSource.Table, database, stats)
                              ?? DefaultTableRowCount;

                // B2: extract equi-key columns from the ON predicate so NDV/FK formula applies.
                (IReadOnlyList<string> nljLeftCols, IReadOnlyList<string> nljRightCols) =
                    ExtractNljEquiKeyColumns(nlj.OnPredicate, nlj.RightSource.Alias);

                long rows = CardinalityEstimator.EstimateJoinCardinality(
                    inputCardinality, innerRows,
                    nljRightCols, nlj.RightSource.Table?.Table,
                    nljLeftCols, TryResolveLeftTable(node.Input),
                    database, stats);

                return (rows, new PlanCost
                {
                    EstimatedRows      = rows,
                    KvRangeScanEntries = inputCardinality * innerRows,
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

            case HashJoinNode hj:
            {
                // inputCardinality = left subtree cardinality (always the Input child).
                // Depending on BuildSide, left is either probe or build.
                //
                // Build side:  scan once + materialise into in-memory hash table.
                // Probe side:  scan once + O(1) hash lookup per row.
                //
                // Total KV cost ≈ buildRows + probeRows   (vs NLJ's buildRows × probeRows)
                // In-memory cost = buildRows               (hash table)
                long rightRows = ResolveRightTableRowCount(hj.BuildSource.Table, database, stats)
                                 ?? DefaultTableRowCount;

                long probeRows, buildRows;
                if (hj.BuildSide == HashJoinBuildSide.Right)
                {
                    probeRows = inputCardinality;
                    buildRows = rightRows;
                }
                else
                {
                    buildRows = inputCardinality;
                    probeRows = rightRows;
                }

                // B2: NDV/FK-aware cardinality — same formula as NLJ and merge join.
                // Use the LOGICAL join order (Input=left, BuildSource=right) for cardinality;
                // the formula is symmetric so the physical build/probe swap only affects cost.
                // Strip qualifiers from both sides: probe keys are qualified today; build keys
                // are bare today, but stripping a bare name is a no-op so both are defensive.
                IReadOnlyList<string> probeKeyCols = StripQualifiers(hj.ProbeKeyColumns);
                IReadOnlyList<string> buildKeyCols = StripQualifiers(hj.BuildKeyColumns);
                long rows = CardinalityEstimator.EstimateJoinCardinality(
                    inputCardinality, rightRows,           // logical left × right
                    buildKeyCols, hj.BuildSource.Table?.Table,
                    probeKeyCols, TryResolveLeftTable(node.Input),
                    database, stats);

                return (rows, new PlanCost
                {
                    EstimatedRows      = rows,
                    KvRangeScanEntries = buildRows + probeRows,
                    InMemoryRows       = buildRows,
                });
            }

            case MergeJoinNode mj:
            {
                // KV cost: scan both sides once (same scan cost as hash join).
                // No in-memory hash table → no build-side memory vs hash join.
                // Sort memory is charged when a side is not free-ordered (i.e. the planner
                // wrapped it in a SortNode); free-ordered sides (ForcedIndex scan) pay nothing.
                long rightRows = ResolveRightTableRowCount(mj.RightSource.Table, database, stats)
                                 ?? DefaultTableRowCount;

                long sortMemory = 0;
                if (!mj.LeftIsOrdered)  sortMemory += inputCardinality;
                if (!mj.RightIsOrdered) sortMemory += rightRows;

                // B2: NDV/FK-aware cardinality — same formula as NLJ and hash join.
                // Strip qualifiers from both sides defensively (right keys are bare today;
                // left keys are qualified; stripping a bare name is a no-op).
                IReadOnlyList<string> leftKeyCols  = StripQualifiers(mj.LeftKeyColumns);
                IReadOnlyList<string> rightKeyCols = StripQualifiers(mj.RightKeyColumns);
                long rows = CardinalityEstimator.EstimateJoinCardinality(
                    inputCardinality, rightRows,
                    rightKeyCols, mj.RightSource.Table?.Table,
                    leftKeyCols, TryResolveLeftTable(node.Input),
                    database, stats);

                return (rows, new PlanCost
                {
                    EstimatedRows      = rows,
                    KvRangeScanEntries = inputCardinality + rightRows,
                    InMemoryRows       = sortMemory,
                });
            }

            case DerivedTableScanNode:
                return (inputCardinality, new PlanCost { EstimatedRows = inputCardinality });

            case SemiJoinNode:
            {
                // Conservative estimate: semi-join passes at most inputCardinality rows (semi) or
                // fewer (anti). Use a fixed 0.5 selectivity as a rough heuristic.
                long rows = Math.Max(1, (long)(inputCardinality * FilterSelectivity));
                return (rows, new PlanCost { EstimatedRows = rows });
            }

            default:
                return (inputCardinality, new PlanCost { EstimatedRows = inputCardinality });
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // B2 join-key helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Strips "alias." qualifiers from a list of qualified column names (e.g. "a.id" → "id").
    /// Returns a new list; input is not modified.
    /// </summary>
    private static IReadOnlyList<string> StripQualifiers(IReadOnlyList<string> qualifiedColumns)
    {
        string[] result = new string[qualifiedColumns.Count];
        for (int i = 0; i < qualifiedColumns.Count; i++)
        {
            int dot = qualifiedColumns[i].IndexOf('.');
            result[i] = dot < 0 ? qualifiedColumns[i] : qualifiedColumns[i][(dot + 1)..];
        }
        return result;
    }

    /// <summary>
    /// Walks the left-input subtree looking for a leaf <see cref="TableScanNode"/>.
    /// Passes through transparent nodes (Filter, Project, Limit, Sort).
    /// Returns null when the left input is a complex subtree (e.g. a nested join).
    /// In 3+-way joins the left input is itself a join node, so leftTable is null and the
    /// FK/NDV check falls back to right-side-only NDV or <see cref="CardinalityEstimator.FallbackSelectivity"/>.
    /// Phase D (System-R join-order DP) owns multi-table cardinality; acceptable for B2.
    /// </summary>
    private static TableDescriptor? TryResolveLeftTable(PhysicalPlanNode? input)
    {
        while (input is not null)
        {
            if (input is TableScanNode { BoundSource: { } src })
                return src.Table;
            if (input is FilterNode or ProjectNode or LimitNode or SortNode)
            {
                input = input.Input;
                continue;
            }
            break;
        }
        return null;
    }

    /// <summary>
    /// Extracts unqualified column names on both sides of equi-conjuncts in an NLJ ON predicate.
    /// Only top-level AND conjuncts of the form "X.col = rightAlias.col" (or reversed) are extracted.
    /// Returns empty lists when the predicate contains no recognizable equi-keys.
    /// Non-Identifier operands (expressions, subqueries) are skipped, so a complex ON predicate
    /// yields empty lists → NLJ falls back to <see cref="CardinalityEstimator.FallbackSelectivity"/>
    /// while hash/merge join (which use explicit node key columns) do not. Spec invariant
    /// "identical cardinality across hash/merge/NLJ" holds for simple Identifier=Identifier equi-joins.
    /// </summary>
    private static (IReadOnlyList<string> LeftCols, IReadOnlyList<string> RightCols)
        ExtractNljEquiKeyColumns(NodeAst onPredicate, string rightAlias)
    {
        var conjuncts = new List<NodeAst>();
        PredicateAnalyzer.CollectAndConjuncts(onPredicate, conjuncts);

        var leftCols  = new List<string>();
        var rightCols = new List<string>();
        string prefix = rightAlias + ".";

        foreach (NodeAst c in conjuncts)
        {
            if (c.nodeType != NodeType.ExprEquals) continue;
            if (c.leftAst?.nodeType  != NodeType.Identifier) continue;
            if (c.rightAst?.nodeType != NodeType.Identifier) continue;
            string? leftText  = c.leftAst.yytext;
            string? rightText = c.rightAst.yytext;
            if (leftText is null || rightText is null) continue;

            if (rightText.StartsWith(prefix, StringComparison.Ordinal))
            {
                // "leftAlias.col = rightAlias.col" — the common orientation
                rightCols.Add(rightText[prefix.Length..]);
                int dot = leftText.IndexOf('.');
                leftCols.Add(dot < 0 ? leftText : leftText[(dot + 1)..]);
            }
            else if (leftText.StartsWith(prefix, StringComparison.Ordinal))
            {
                // "rightAlias.col = leftAlias.col" — reversed
                rightCols.Add(leftText[prefix.Length..]);
                int dot = rightText.IndexOf('.');
                leftCols.Add(dot < 0 ? rightText : rightText[(dot + 1)..]);
            }
        }
        return (leftCols, rightCols);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    internal static long EstimateRangeScanRows(
        IndexRangeScanNode node,
        long tableRowCount,
        StatisticsManager? stats = null,
        DatabaseDescriptor? database = null,
        TableDescriptor? table = null)
    {
        bool hasFrom = node.FromBound is not null;
        bool hasTo   = node.ToBound   is not null;

        // B1: attempt histogram-based selectivity (most accurate; preferred over min/max linear interpolation).
        if (stats is not null && database is not null && table is not null
            && node.Index.Columns is { Length: > 0 })
        {
            string col = node.Index.Columns[0];
            ColumnHistogram? hist = stats.GetColumnHistogram(database, table, col);
            if (hist is not null && hist.TotalRows > 0)
            {
                ScalarBound? fromBound = hasFrom ? ScalarBound.FromColumnValue(node.FromBound!.Values[0]) : null;
                ScalarBound? toBound   = hasTo   ? ScalarBound.FromColumnValue(node.ToBound!.Values[0])   : null;
                double histSel = CardinalityEstimator.EstimateRangeFraction(fromBound, toBound, col, database, table, stats);
                return Math.Max(1, (long)(tableRowCount * histSel));
            }
        }

        // R9b: attempt real selectivity from column min/max when available.
        if (stats is not null && database is not null && table is not null
            && node.Index.Columns is { Length: > 0 })
        {
            string col = node.Index.Columns[0];
            ColumnMinMax? mm = stats.GetColumnMinMax(database, table, col);
            if (mm?.Min is not null && mm.Max is not null)
            {
                double? realSel = ComputeSelectivityFromMinMax(
                    mm.Min, mm.Max,
                    hasFrom ? node.FromBound!.Values[0] : null,
                    hasTo   ? node.ToBound!.Values[0]   : null);

                if (realSel.HasValue)
                    return Math.Max(1, (long)(tableRowCount * realSel.Value));
            }
        }

        // Fallback: fixed heuristic selectivities.
        double selectivity = (hasFrom && hasTo) ? BothBoundsSelectivity
                           : (hasFrom || hasTo) ? OneBoundSelectivity
                           : 1.0;

        return Math.Max(1, (long)(tableRowCount * selectivity));
    }

    // Computes the fraction of values in [columnMin, columnMax] that satisfy the predicate
    // fromBound <= value <= toBound (null bound = open on that side).
    // Returns null when the computation is not possible (type mismatch, zero range, etc.).
    private static double? ComputeSelectivityFromMinMax(
        ScalarBound colMin,
        ScalarBound colMax,
        ColumnValue? fromBound,
        ColumnValue? toBound)
    {
        // Only numeric types support range arithmetic.
        if (colMin.Type != colMax.Type)
            return null;

        if (colMin.Type == CamusDB.Core.Catalogs.Models.ColumnType.Integer64)
        {
            long min = colMin.LongValue;
            long max = colMax.LongValue;
            if (max <= min) return null; // degenerate range

            long lo = fromBound?.Type == CamusDB.Core.Catalogs.Models.ColumnType.Integer64
                ? Math.Max(min, fromBound.LongValue)
                : min;

            long hi = toBound?.Type == CamusDB.Core.Catalogs.Models.ColumnType.Integer64
                ? Math.Min(max, toBound.LongValue)
                : max;

            if (hi < lo) return 0.0;
            return Math.Min(1.0, (double)(hi - lo) / (max - min));
        }

        if (colMin.Type == CamusDB.Core.Catalogs.Models.ColumnType.Float64)
        {
            double min = colMin.FloatValue;
            double max = colMax.FloatValue;
            if (max <= min) return null;

            double lo = fromBound?.Type == CamusDB.Core.Catalogs.Models.ColumnType.Float64
                ? Math.Max(min, fromBound.FloatValue)
                : min;

            double hi = toBound?.Type == CamusDB.Core.Catalogs.Models.ColumnType.Float64
                ? Math.Min(max, toBound.FloatValue)
                : max;

            if (hi < lo) return 0.0;
            return Math.Min(1.0, (hi - lo) / (max - min));
        }

        return null; // strings, IDs, bools: fall back to fixed heuristics
    }
}
