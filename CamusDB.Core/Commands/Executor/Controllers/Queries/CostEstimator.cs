
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
/// Selectivity assumptions (fixed defaults; per-column stats not yet available):
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
///   For single-table plans, the primary table's row count is resolved once and threaded
///   through the tree. For join plans (called with <paramref name="table"/> = null), each
///   <see cref="TableScanNode"/> resolves its own table's stats via
///   <see cref="TableScanNode.BoundSource"/>, and each <see cref="NestedLoopJoinNode"/> /
///   <see cref="IndexNestedLoopJoinNode"/> resolves the right-side table's stats via its
///   <see cref="NestedLoopJoinNode.RightSource"/>. This avoids the previous fallback to a
///   single global default, giving each join source a meaningful cardinality estimate.
///
/// Cost-driven decisions (scope note):
///   Only <see cref="ShouldPreferFullScan"/> drives an actual plan change (low-selectivity
///   index range → full table scan). All other annotated costs are computed and surfaced in
///   EXPLAIN output but do not yet alter plan shape — unique-index lookup vs. multi-index,
///   NLJ vs. INLJ, and sort placement remain rule-based. Cost-based selection will extend
///   to those choices once per-column histograms make the estimates reliable enough.
/// </summary>
internal static class CostEstimator
{
    // Fallback selectivities used when min/max is unavailable for the column.
    private const double BothBoundsSelectivity = 0.10;

    // When column min/max is available, real selectivity replaces this constant.
    // Without stats the pessimistic 0.40 (= BreakevenFraction) ensures every half-open
    // range flips to a full table scan, which is safe but sub-optimal.
    private const double OneBoundSelectivity   = 0.40;
    private const double FilterSelectivity     = 0.10;
    private const double AggregateSelectivity  = 0.20;
    private const double DistinctSelectivity   = 0.70;

    // Index scan breaks even with a full table scan when it touches this fraction of rows.
    // A non-covering secondary index scan costs 2 × estimated_entries (index read + row fetch);
    // a full scan costs tableRowCount × 1. Prefer full scan when entries / N ≥ 0.5, i.e.
    // BreakevenFraction = 0.50. With real selectivities the false-positive flip rate drops
    // significantly compared to the previous value of 0.40.
    internal const double BreakevenFraction = 0.50;

    internal const long DefaultTableRowCount = 10_000;

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Walks <paramref name="root"/> top-down, assigning <see cref="PhysicalPlanNode.EstimatedCardinality"/>
    /// and <see cref="PhysicalPlanNode.Cost"/> to every node using row-count statistics.
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
    /// Estimates the isolated scan cost of a single leaf node, used by the access-path enumerator
    /// to compare candidates before committing to one.
    ///
    /// Equivalent to one <see cref="EstimateNodeCost"/> call where the input cardinality equals
    /// the table row count (leaf nodes have no children). The result is used only for candidate
    /// comparison; nodes are annotated with full costs later via <see cref="AnnotatePlan"/>.
    /// </summary>
    public static double EstimateScanLeafCost(
        PhysicalPlanNode scanNode,
        long tableRowCount,
        StatisticsManager? stats,
        DatabaseDescriptor database,
        TableDescriptor table)
    {
        (_, PlanCost cost) = EstimateNodeCost(
            scanNode,
            inputCardinality: tableRowCount,
            tableRowCount: tableRowCount,
            database: database,
            stats: stats,
            primaryTable: table,
            resolvedTable: table);
        return cost.Total;
    }

    /// <summary>
    /// Returns true when replacing a predicate-driven <see cref="IndexRangeScanNode"/> with a full
    /// primary table scan would be cheaper under the cost model.
    ///
    /// <para>
    /// When <paramref name="stats"/> and <paramref name="table"/> are supplied, uses real
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

        TableDescriptor? resolvedTable = ResolveNodeTable(node, primaryTable);
        (long cardinality, PlanCost cost) = EstimateNodeCost(node, inputCardinality, effectiveRowCount, database, stats, primaryTable, resolvedTable);

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

        // Join-plan leaf: scan nodes carry BoundSource with a TableDescriptor.
        if (node is TableScanNode { BoundSource: { } boundSource })
            return stats?.GetRowCountEstimate(database, boundSource.Table);

        if (node is IndexRangeScanNode { BoundSource: { } rangeBoundSource })
            return stats?.GetRowCountEstimate(database, rangeBoundSource.Table);

        if (node is IndexInListScanNode { BoundSource: { } inListBoundSource })
            return stats?.GetRowCountEstimate(database, inListBoundSource.Table);

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

    /// <summary>
    /// Resolves the table descriptor for a scan node.
    /// Single-table plans pass <paramref name="primaryTable"/> directly.
    /// Join plans pass null and each leaf resolves its own table from <c>BoundSource</c>.
    /// </summary>
    private static TableDescriptor? ResolveNodeTable(PhysicalPlanNode node, TableDescriptor? primaryTable)
    {
        if (primaryTable is not null)
            return primaryTable;

        // Join-plan leaf: scan nodes carry BoundSource → Table.
        if (node is TableScanNode { BoundSource: { } tsBound })
            return tsBound.Table;
        if (node is IndexRangeScanNode { BoundSource: { } irsBound })
            return irsBound.Table;
        if (node is IndexInListScanNode { BoundSource: { } ilsBound })
            return ilsBound.Table;
        return null;
    }

    /// <summary>
    /// Computes the network shipping cost for a scan leaf.
    ///
    /// <c>NetworkFactor ≈ remoteRows × rowWidthBytes × NetWeight</c>
    ///   where <c>remoteRows = rows × remoteFraction</c>
    ///   and   <c>remoteFraction = (N-1)/N</c> when key-range sharding is on with N > 1 partitions.
    ///
    /// Only <see cref="DataDistributionKind.Partitioned"/> nodes incur remote cost.
    /// Gathered (point lookups, coordinator-local) and Replicated nodes pay 0.
    /// </summary>
    private static double ComputeNetworkFactor(PhysicalPlanNode node, long rows, TableDescriptor? resolvedTable)
    {
        if (node.Distribution is not { Kind: DataDistributionKind.Partitioned })
            return 0.0;

        double remoteFraction = PlacementReader.GetRemoteFraction();
        if (remoteFraction <= 0.0 || CamusDBConfig.NetWeight <= 0.0)
            return 0.0;

        int rowWidth = RowWidthEstimator.Estimate(resolvedTable);
        return rows * remoteFraction * rowWidth * CamusDBConfig.NetWeight;
    }

    private static (long Cardinality, PlanCost Cost) EstimateNodeCost(
        PhysicalPlanNode node,
        long inputCardinality,
        long? tableRowCount,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        TableDescriptor? primaryTable = null,
        TableDescriptor? resolvedTable = null)
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
                    NetworkFactor        = ComputeNetworkFactor(node, rows, resolvedTable),
                });
            }

            case IndexInListScanNode inListNode:
            {
                bool isUnique = inListNode.Index.Type == CamusDB.Core.Catalogs.Models.IndexType.Unique;
                // Use NDV/histogram to estimate fan-out for non-unique indexes; unique stays min(n, trc).
                long rows = CardinalityEstimator.EstimateInListRows(
                    inListNode.ColumnName, inListNode.Values, isUnique,
                    trc, database, resolvedTable ?? primaryTable, stats);
                int n = inListNode.Values.Count;
                return (rows, new PlanCost
                {
                    EstimatedRows        = rows,
                    KvPointLookups       = isUnique ? n : 0,
                    KvRangeScanEntries   = isUnique ? 0 : rows,  // non-unique: entries scanned ≈ matched rows
                    RowFetchesAfterIndex = rows,
                    NetworkFactor        = ComputeNetworkFactor(node, rows, resolvedTable),
                });
            }

            case IndexRangeScanNode rangeNode:
            {
                // In join plans primaryTable is null; resolvedTable carries the leaf's own table
                // (resolved from BoundSource by ResolveNodeTable). Pass the non-null one so
                // EstimateRangeScanRows can use histogram/min-max/NDV stats rather than the
                // fixed 10%/40% fallback.
                long rows = EstimateRangeScanRows(rangeNode, trc, stats, database, resolvedTable ?? primaryTable);
                return (rows, new PlanCost
                {
                    EstimatedRows        = rows,
                    KvRangeScanEntries   = rows,
                    RowFetchesAfterIndex = rows,
                    NetworkFactor        = ComputeNetworkFactor(node, rows, resolvedTable),
                });
            }

            case TableScanNode tsn:
            {
                long rows = trc;

                // Output cardinality reflects the pushed-down ExecutionFilter (if any).
                // The scan cost stays at the full table count — we still read every row;
                // the filter just reduces what flows to the parent node.
                long outputCard = rows;
                if (tsn.ExecutionFilter is not null && stats is not null && resolvedTable is not null)
                {
                    double sel = CardinalityEstimator.EstimateFilterSelectivity(
                        tsn.ExecutionFilter, database, resolvedTable, stats);
                    outputCard = Math.Max(1L, (long)(rows * sel));
                }

                return (outputCard, new PlanCost
                {
                    EstimatedRows      = outputCard,
                    KvRangeScanEntries = rows,
                    NetworkFactor      = ComputeNetworkFactor(node, rows, resolvedTable),
                });
            }

            case FilterNode filterNode:
            {
                // Use histogram-based selectivity when stats and primary table are available.
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

                // Extract equi-key columns from the ON predicate so NDV/FK formula applies.
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

                // NDV/FK-aware cardinality — same formula as NLJ and merge join.
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

                // NDV/FK-aware cardinality — same formula as NLJ and hash join.
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
    // Join-key helpers
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
    /// Walks the left-input subtree looking for a leaf scan node.
    /// Passes through transparent nodes (Filter, Project, Limit, Sort).
    /// Returns null when the left input is a complex subtree (e.g. a nested join).
    /// In 3+-way joins the left input is itself a join node, so leftTable is null and the
    /// FK/NDV check falls back to right-side-only NDV or <see cref="CardinalityEstimator.FallbackSelectivity"/>.
    /// System-R join-order DP owns multi-table cardinality.
    /// </summary>
    private static TableDescriptor? TryResolveLeftTable(PhysicalPlanNode? input)
    {
        while (input is not null)
        {
            if (input is TableScanNode { BoundSource: { } tsSrc })
                return tsSrc.Table;
            if (input is IndexRangeScanNode { BoundSource: { } irsSrc })
                return irsSrc.Table;
            if (input is IndexInListScanNode { BoundSource: { } ilsSrc })
                return ilsSrc.Table;
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

        if (stats is not null && database is not null && table is not null
            && node.Index.Columns is { Length: > 0 })
        {
            int fromLen = node.FromBound?.Values.Length ?? 0;
            int toLen   = node.ToBound?.Values.Length   ?? 0;

            // String/Id equality is represented as a degenerate [v, v] inclusive range, for which
            // RangeFraction(v, v) ≈ 0 → ~1 row, over-ranking these steps. Use 1/NDV instead.
            if (fromLen == 1 && toLen == 1 && node.FromInclusive && node.ToInclusive
                && IndexScanSelector.IsStringOrIdType(table, node.Index.Columns[0]))
            {
                long? colNdv = stats.GetColumnNdv(database, table, node.Index.Columns[0]);
                if (colNdv is { } ndv && ndv > 0)
                    return Math.Max(1, (long)(tableRowCount / (double)ndv));
                // Fall through to general path when NDV is absent.
            }

            // For composite-index steps (equality prefix + trailing range), price the equality
            // prefix via key NDV and the range column via its histogram / min-max. The range column
            // sits at the last position of whichever bound carries it — max(fromLen, toLen) − 1 — so
            // an upper-open range (range value only in ToBound, FromBound null/short prefix) still
            // finds the range column. The columns before it are the equality prefix. When both
            // bounds have length 1 this reduces to the single-column case.
            int rangeColIdx      = Math.Max(0, Math.Max(fromLen, toLen) - 1);
            int equalityPrefixLen = rangeColIdx; // number of columns BEFORE the range column

            double prefixSel = 1.0;
            if (equalityPrefixLen > 0 && rangeColIdx < node.Index.Columns.Length)
            {
                string[] prefixCols = node.Index.Columns[..equalityPrefixLen];
                if (equalityPrefixLen == 1)
                {
                    long? colNdv = stats.GetColumnNdv(database, table, prefixCols[0]);
                    if (colNdv is { } ndv && ndv > 0)
                        prefixSel = 1.0 / ndv;
                }
                else
                {
                    long? keyNdv = stats.GetKeyNdv(database, table, prefixCols);
                    if (keyNdv is { } ndv && ndv > 0)
                        prefixSel = 1.0 / ndv;
                    else
                    {
                        // Independence fallback: multiply per-column NDV inverses.
                        foreach (string pc in prefixCols)
                        {
                            long? pNdv = stats.GetColumnNdv(database, table, pc);
                            if (pNdv is { } pn && pn > 0)
                                prefixSel /= pn;
                        }
                    }
                }
            }

            // Range column bounds (null = open on that side).
            ColumnValue? rangeFrom = (hasFrom && rangeColIdx < fromLen) ? node.FromBound!.Values[rangeColIdx] : null;
            ColumnValue? rangeTo   = (hasTo   && rangeColIdx < toLen)   ? node.ToBound!.Values[rangeColIdx]   : null;

            if (rangeColIdx < node.Index.Columns.Length)
            {
                string rangeCol = node.Index.Columns[rangeColIdx];

                // Histogram-based (most accurate).
                ColumnHistogram? hist = stats.GetColumnHistogram(database, table, rangeCol);
                if (hist is not null && hist.TotalRows > 0)
                {
                    ScalarBound? fb = rangeFrom is not null ? ScalarBound.FromColumnValue(rangeFrom) : null;
                    ScalarBound? tb = rangeTo   is not null ? ScalarBound.FromColumnValue(rangeTo)   : null;
                    double histSel = CardinalityEstimator.EstimateRangeFraction(fb, tb, rangeCol, database, table, stats);
                    return Math.Max(1, (long)(tableRowCount * prefixSel * histSel));
                }

                // Min/max linear interpolation.
                ColumnMinMax? mm = stats.GetColumnMinMax(database, table, rangeCol);
                if (mm?.Min is not null && mm.Max is not null)
                {
                    double? realSel = ComputeSelectivityFromMinMax(mm.Min, mm.Max, rangeFrom, rangeTo);
                    if (realSel.HasValue)
                        return Math.Max(1, (long)(tableRowCount * prefixSel * realSel.Value));
                }
            }

            // If a prefix selectivity was derived but no range-column stats exist, apply prefix alone.
            if (equalityPrefixLen > 0 && prefixSel < 1.0)
            {
                double fallbackRangeSel = (hasFrom && hasTo) ? BothBoundsSelectivity
                                        : (hasFrom || hasTo) ? OneBoundSelectivity
                                        : 1.0;
                return Math.Max(1, (long)(tableRowCount * prefixSel * fallbackRangeSel));
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
