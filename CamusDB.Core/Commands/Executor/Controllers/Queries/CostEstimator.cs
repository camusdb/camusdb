
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
    // Deliberately equal to BreakevenFraction: without stats a half-open range's estimated
    // rows (computed with Math.Ceiling in the fallback) meet the breakeven exactly, so the
    // scan flips to a full table scan — safe (a non-covering index scan of half the table
    // costs ~2× a full scan) but sub-optimal. If the two constants ever diverge again, that
    // safety property silently disappears.
    private const double OneBoundSelectivity   = BreakevenFraction;
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
        StatisticsManager? stats,
        CamusDBOptions options)
    {
        // Pre-resolve the single-table row count; null for join plans (resolved per-node below).
        long? singleTableRowCount = table is not null
            ? stats?.GetRowCountEstimate(database, table)
            : null;

        AnnotateNode(root, database, stats, singleTableRowCount, options, table);
    }

    /// <summary>
    /// Estimates the isolated scan cost of a single leaf node, used by the access-path enumerator
    /// to compare candidates before committing to one.
    ///
    /// Equivalent to one <see cref="EstimateNodeCost"/> call where the input cardinality equals
    /// the table row count (leaf nodes have no children). The result is used only for candidate
    /// comparison; nodes are annotated with full costs later via <see cref="AnnotatePlan"/>.
    ///
    /// <para><paramref name="coversProjection"/>: pass <c>true</c> when the index behind
    /// <paramref name="scanNode"/> supplies every column the query needs, so the scan fetches
    /// zero primary rows. The per-node cost model prices every secondary-index scan with a
    /// primary-row fetch per matched entry (<see cref="PlanCost.RowFetchesAfterIndex"/>);
    /// without this correction a covered scan is charged double its real cost and loses to a
    /// full table scan on small tables, defeating the covering-index optimization.</para>
    /// </summary>
    public static double EstimateScanLeafCost(
        PhysicalPlanNode scanNode,
        long tableRowCount,
        StatisticsManager? stats,
        DatabaseDescriptor database,
        TableDescriptor table,
        CamusDBOptions options,
        bool coversProjection = false)
    {
        (_, PlanCost cost) = EstimateNodeCost(
            scanNode,
            inputCardinality: tableRowCount,
            tableRowCount: tableRowCount,
            database: database,
            stats: stats,
            options: options,
            primaryTable: table,
            resolvedTable: table);

        // Row fetches carry weight 1.0 in PlanCost.Total; a covering scan performs none of them.
        return coversProjection
            ? cost.Total - cost.RowFetchesAfterIndex
            : cost.Total;
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
        CamusDBOptions options,
        StatisticsManager? stats = null,
        DatabaseDescriptor? database = null,
        TableDescriptor? table = null)
    {
        long estimatedEntries = EstimateRangeScanRows(indexNode, tableRowCount, options, stats, database, table);
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
        CamusDBOptions options,
        TableDescriptor? primaryTable = null)
    {
        // Resolve the effective table row count for this specific node.
        // In join plans (rowCountHint == null) each TableScanNode looks up its own table's stats.
        long? effectiveRowCount = ResolveNodeRowCount(node, database, stats, rowCountHint);

        long inputCardinality = node.Input is not null
            ? AnnotateNode(node.Input, database, stats, rowCountHint, options, primaryTable)
            : effectiveRowCount ?? DefaultTableRowCount;

        TableDescriptor? resolvedTable = ResolveNodeTable(node, primaryTable);
        (long cardinality, PlanCost cost) = EstimateNodeCost(node, inputCardinality, effectiveRowCount, database, stats, options, primaryTable, resolvedTable);

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

        // A function in FROM: the exact count when its arguments are constants.
        if (node is DerivedTableScanNode { BoundSource: { } derivedSource })
            return TableFunctionEvaluation.TryCountRows(derivedSource);

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
    private static double ComputeNetworkFactor(PhysicalPlanNode node, long rows, TableDescriptor? resolvedTable, DatabaseDescriptor database, CamusDBOptions options)
    {
        if (node.Distribution is not { Kind: DataDistributionKind.Partitioned })
            return 0.0;

        double remoteFraction = PlacementReader.GetRemoteFraction(database, resolvedTable, node, options);
        if (remoteFraction <= 0.0 || options.NetWeight <= 0.0)
            return 0.0;

        int rowWidth = RowWidthEstimator.Estimate(resolvedTable);
        return rows * remoteFraction * rowWidth * options.NetWeight;
    }

    private static (long Cardinality, PlanCost Cost) EstimateNodeCost(
        PhysicalPlanNode node,
        long inputCardinality,
        long? tableRowCount,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        CamusDBOptions options,
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
                    NetworkFactor        = ComputeNetworkFactor(node, rows, resolvedTable, database, options),
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
                    NetworkFactor        = ComputeNetworkFactor(node, rows, resolvedTable, database, options),
                });
            }

            case IndexRangeScanNode rangeNode:
            {
                // In join plans primaryTable is null; resolvedTable carries the leaf's own table
                // (resolved from BoundSource by ResolveNodeTable). Pass the non-null one so
                // EstimateRangeScanRows can use histogram/min-max/NDV stats rather than the
                // fixed 10%/40% fallback.
                long rows = EstimateRangeScanRows(rangeNode, trc, options, stats, database, resolvedTable ?? primaryTable);
                return (rows, new PlanCost
                {
                    EstimatedRows        = rows,
                    KvRangeScanEntries   = rows,
                    RowFetchesAfterIndex = rows,
                    NetworkFactor        = ComputeNetworkFactor(node, rows, resolvedTable, database, options),
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
                    NetworkFactor      = ComputeNetworkFactor(node, rows, resolvedTable, database, options),
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

            case SortNode sort:
            {
                // A bounded sort still reads every input row — the pass over the input is what it
                // costs — but it holds and emits only the rows the LIMIT above it can reach. Pricing
                // it as a full sort would keep charging for a materialization it no longer performs,
                // and would leave every node above it estimating from the wrong cardinality.
                long retained = sort.BoundedLimit is { } bound
                    ? Math.Min(bound, inputCardinality)
                    : inputCardinality;

                return (retained, new PlanCost
                {
                    EstimatedRows = retained,
                    InMemoryRows  = retained,
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
                long innerRows = ResolveRightTableRowCount(nlj.RightSource.Table, database, stats)
                              ?? DefaultTableRowCount;

                // Extract equi-key columns from the ON predicate so NDV/FK formula applies.
                (IReadOnlyList<string> nljLeftCols, IReadOnlyList<string> nljRightCols, IReadOnlyList<string> nljLeftKeys) =
                    ExtractNljEquiKeyColumns(nlj.OnPredicate, nlj.RightSource.Alias);

                TableDescriptor? nljLeftTable = TryResolveLeftTable(node.Input, nljLeftKeys, out bool nljLeftIsJoin);
                long rows = CardinalityEstimator.EstimateJoinCardinality(
                    inputCardinality, innerRows,
                    nljRightCols, nlj.RightSource.Table?.Table,
                    nljLeftCols, nljLeftTable,
                    database, stats, nljLeftIsJoin);
                rows = ApplyOuterJoinFloor(nlj.Kind, rows, inputCardinality);

                return (rows, new PlanCost
                {
                    // The right side is read once (NestedLoopInnerSpool) and every left × right
                    // pair is compared in memory. Saturating multiply: deep join trees can each
                    // carry multi-billion-row estimates, and a raw long multiply wraps negative —
                    // poisoning the annotated cost (and any cost-driven join choice) silently.
                    KvRangeScanEntries = innerRows,
                    InMemoryRows       = SaturatingMultiply(inputCardinality, innerRows),
                    EstimatedRows      = rows,
                });
            }

            case IndexNestedLoopJoinNode inlj:
            {
                // Outer row drives one unique-index lookup on the inner side. A left outer join
                // emits at least one row per outer row, which this estimate already does.
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
                TableDescriptor? hjLeftTable = TryResolveLeftTable(node.Input, hj.ProbeKeyColumns, out bool hjLeftIsJoin);
                long rows = CardinalityEstimator.EstimateJoinCardinality(
                    inputCardinality, rightRows,           // logical left × right
                    buildKeyCols, hj.BuildSource.Table?.Table,
                    probeKeyCols, hjLeftTable,
                    database, stats, hjLeftIsJoin);
                rows = ApplyOuterJoinFloor(hj.Kind, rows, inputCardinality);

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
                TableDescriptor? mjLeftTable = TryResolveLeftTable(node.Input, mj.LeftKeyColumns, out bool mjLeftIsJoin);
                long rows = CardinalityEstimator.EstimateJoinCardinality(
                    inputCardinality, rightRows,
                    rightKeyCols, mj.RightSource.Table?.Table,
                    leftKeyCols, mjLeftTable,
                    database, stats, mjLeftIsJoin);
                rows = ApplyOuterJoinFloor(mj.Kind, rows, inputCardinality);

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
                // Semi-join passes at most inputCardinality rows (semi) or fewer (anti); with no
                // membership statistics, price it like a generic filter (FilterSelectivity).
                long rows = Math.Max(1, (long)(inputCardinality * FilterSelectivity));
                return (rows, new PlanCost { EstimatedRows = rows });
            }

            default:
                return (inputCardinality, new PlanCost { EstimatedRows = inputCardinality });
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    /// <summary>
    /// A left outer join emits every left row at least once, so its output can never be smaller
    /// than its left input: the inner estimate is raised to that floor. Nothing else in the cost
    /// model changes for the kind.
    /// </summary>
    private static long ApplyOuterJoinFloor(JoinKind kind, long innerEstimate, long leftRows) =>
        kind == JoinKind.LeftOuter ? Math.Max(innerEstimate, leftRows) : innerEstimate;

    // ─────────────────────────────────────────────────────────────────────────
    // Join step cost
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Weight of one hash-table row against one row read from KV. The in-memory build costs a
    /// tenth of a scan, the same ratio the plan annotation of <see cref="HashJoinNode"/> implies.
    /// </summary>
    internal const double HashTableRowWeight = 0.1;

    /// <summary>
    /// Weight of one row a sort keeps against one row read from KV: the in-memory weight of
    /// <see cref="PlanCost.Total"/>, the price the <see cref="SortNode"/> annotation charges.
    /// </summary>
    internal const double SortRowWeight = 0.1;

    /// <summary>
    /// Cost of one hash-join step: one scan of the right input, one probe for each left row, and a
    /// hash table on the smaller side. The <paramref name="leftRows"/> term is what makes the join
    /// order matter: the left input of a step is the intermediate result of the steps before it,
    /// so the total over a plan is the scan costs plus the size of each intermediate result.
    /// </summary>
    internal static double HashJoinStepCost(double leftRows, double rightRows, double rightScanCost) =>
        rightScanCost + leftRows + Math.Min(leftRows, rightRows) * HashTableRowWeight;

    /// <summary>
    /// Cost of one probe of a unique index, in rows read from storage, before the row it returns
    /// and beyond the unit every left row pays. Measured 2026-10-17 on the retail data set at
    /// scale 4, memory storage, Apple M3: a scanned row costs about 0.55 µs, and the join over
    /// customers (8,000 rows) timed equal to the hash join at 7,801 left rows with 2,180 distinct
    /// keys (27.9 ms against 26.6 ms) and 24 percent under it at 3,869 rows with 1,338 keys
    /// (10.3 ms against 13.6 ms). Both solve to three rows for the probe: the hash join costs
    /// 8,000 + 7,801 + 780 units, the lookup 7,801 + 2,180 · (3 + 1).
    /// </summary>
    internal const double UniqueProbeWeight = 3.0;

    /// <summary>
    /// Cost of one probe of a non-unique index, in rows read from storage, before the rows it
    /// returns and beyond the unit every left row pays. Measured as
    /// <see cref="UniqueProbeWeight"/>: a batched bounded scan costs about 10 µs for each distinct
    /// key with its 2.4 rows fetched, 16 rows for the probe alone. The break-even against a hash
    /// join solves to between 14 and 22: a probe into order_items (57,273 rows, 2.4 for each key)
    /// beat the hash join at 1,915 left rows (33 ms against 40 ms) and lost at 3,869 (55 ms against
    /// 46 ms); one into shipments (20,410 rows, 0.85 for each key) lost already at 1,915 (21 ms
    /// against 16 ms).
    /// </summary>
    internal const double RangeProbeWeight = 16.0;

    /// <summary>
    /// Cost of one index-lookup join step: one unit for each left row, as in a hash join, where a
    /// hit in the join's memo pairs the row with rows already in memory exactly as a hash probe
    /// does; plus, for each distinct key of the left input, one probe and the rows it returns. The
    /// probe is a point read (<see cref="UniqueProbeWeight"/>) or a bounded scan
    /// (<see cref="RangeProbeWeight"/>), and each row it returns costs one row fetch. The memo
    /// (<see cref="IndexNestedLoopJoinOperator.ProbeMemoMaxRows"/>) holds the rows of the probed
    /// keys; when they would not fit, a repeated key probes again, so every left row is a probe.
    /// A left row whose key is NULL probes nothing (<see cref="IndexLookupProfile.LeftNonNullFraction"/>).
    /// A memo hit priced below a hash probe made a large hash join followed by a few hundred
    /// memoized lookups look almost free, and the search moved selective tables to the end.
    /// </summary>
    internal static double IndexLookupJoinStepCost(double leftRows, IndexLookupProfile probe)
    {
        double matches = Math.Max(1.0, probe.MatchesPerProbe);
        double keyedRows = leftRows * Math.Clamp(probe.LeftNonNullFraction, 0.0, 1.0);
        double probes = Math.Min(keyedRows, probe.LeftKeyDistinct);

        if (probes * matches > IndexNestedLoopJoinOperator.ProbeMemoMaxRows)
            probes = keyedRows;

        double weight = probe.UniqueIndex ? UniqueProbeWeight : RangeProbeWeight;
        return leftRows + probes * (weight + matches);
    }

    /// <summary>
    /// The <see cref="IndexLookupProfile"/> of a lookup into <paramref name="rightTable"/> through
    /// <paramref name="index"/> on <paramref name="rightColumn"/>, driven by
    /// <paramref name="leftColumn"/> of <paramref name="leftTable"/>. A unique index returns one row
    /// for each probe; a non-unique one returns the table's rows over the column's distinct count,
    /// or one row when that count is unknown. The distinct keys of the left input are the distinct
    /// count of the left column in its table (the table's rows when the column is a unique key),
    /// unbounded when the column's table or its count is unknown. The left table is the table of
    /// the column, not of the whole left input: a join before this step can only reduce the
    /// distinct values the column still has. When the left column's values are known
    /// (<paramref name="leftKeys"/>, read at plan time by <see cref="PlanTimeLeafLookup"/>), a
    /// non-unique probe returns the mean of those keys' shares of the right column, from its
    /// histogram, and the distinct keys are the known keys.
    /// </summary>
    internal static IndexLookupProfile EstimateIndexLookupProfile(
        TableDescriptor rightTable,
        TableIndexSchema index,
        string rightColumn,
        TableDescriptor? leftTable,
        string leftColumn,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        IReadOnlyList<ColumnValue>? leftKeys = null)
    {
        bool unique = index.Type == IndexType.Unique;
        long rightRows = stats?.GetRowCountEstimate(database, rightTable) ?? DefaultTableRowCount;

        double matches = 1.0;
        if (!unique)
        {
            long? distinct = CardinalityEstimator.EstimateKeyDistinctCount(rightTable, [rightColumn], rightRows, database, stats);
            if (distinct is > 0)
                matches = Math.Max(1.0, (double)rightRows / distinct.Value);

            if (leftKeys is { Count: > 0 }
                && stats?.GetColumnHistogram(database, rightTable, rightColumn) is { } histogram
                && CardinalityEstimator.KeyShareSelectivity(histogram, leftKeys) is { } share)
            {
                matches = Math.Max(1.0, rightRows * share);
            }
        }

        double leftDistinct = double.PositiveInfinity;
        double leftNonNull = 1.0;
        if (leftKeys is { Count: > 0 })
        {
            leftDistinct = leftKeys.Count;
        }
        else if (leftTable is not null)
        {
            long leftRows = stats?.GetRowCountEstimate(database, leftTable) ?? DefaultTableRowCount;
            long? distinct = CardinalityEstimator.EstimateKeyDistinctCount(leftTable, [leftColumn], leftRows, database, stats);
            if (distinct is > 0)
                leftDistinct = distinct.Value;

            leftNonNull = CardinalityEstimator.NonNullFraction(leftTable, [leftColumn], database, stats);
        }

        return new IndexLookupProfile(unique, matches, leftDistinct, leftNonNull);
    }

    /// <summary>
    /// Weight of one left × right pair a nested-loop join compares in memory against one row read
    /// from KV: the same in-memory weight as a hash-table row (<see cref="HashTableRowWeight"/>).
    /// </summary>
    internal const double NestedLoopPairWeight = 0.1;

    /// <summary>
    /// Cost of one nested-loop join step: one read of the right input, then one in-memory
    /// comparison for each left × right pair. The operator reads a base-table right side from
    /// storage once and replays it for the other left rows (<see cref="NestedLoopInnerSpool"/>),
    /// so the right read is not multiplied by the left rows. <paramref name="rightRows"/> is the
    /// right input after its pushed filter, which is what the spool holds. The builder uses this
    /// step when no conjunct of the <c>ON</c> is an equi-key, so it decides the join order, not the
    /// operator. A right filter with a volatile function still re-reads per left row; that case
    /// is rare and is not priced separately.
    /// </summary>
    internal static double NestedLoopJoinStepCost(double leftRows, double rightRows, double rightScanCost) =>
        rightScanCost + Math.Max(1.0, leftRows) * Math.Max(1.0, rightRows) * NestedLoopPairWeight;

    /// <summary>
    /// The rule that selects a hash join over an index-lookup join. <see cref="JoinEnumerator"/>
    /// and <see cref="JoinQueryPlanner"/> both call it, so the join order is costed with the
    /// operator that the builder then builds. Hash wins only when it is strictly cheaper.
    /// </summary>
    internal static bool PrefersHashOverIndexLookup(double leftRows, double rightRows, double rightScanCost, IndexLookupProfile probe) =>
        HashJoinStepCost(leftRows, rightRows, rightScanCost) < IndexLookupJoinStepCost(leftRows, probe);

    /// <summary>
    /// The operator that the builder selects for one join step: an index-lookup join when the right
    /// table has an index on a key column (<paramref name="indexLookup"/> is its profile) and hash
    /// is not strictly cheaper (<see cref="PrefersHashOverIndexLookup"/>), else a hash join when a
    /// conjunct is an equi-key, else a nested loop. This is the order of the checks in
    /// <see cref="JoinQueryPlanner"/>, so with the same inputs the enumerator and the builder select
    /// the same operator.
    /// </summary>
    internal static JoinStepOperator SelectJoinStepOperator(
        double leftRows,
        double rightRows,
        double rightScanCost,
        bool hasEquiKey,
        IndexLookupProfile? indexLookup)
    {
        if (indexLookup is { } probe && !(hasEquiKey && PrefersHashOverIndexLookup(leftRows, rightRows, rightScanCost, probe)))
            return JoinStepOperator.IndexLookup;

        return hasEquiKey ? JoinStepOperator.Hash : JoinStepOperator.NestedLoop;
    }

    /// <summary>
    /// Cost of joining a right input to a left input of <paramref name="leftRows"/> rows, with the
    /// operator that <see cref="SelectJoinStepOperator"/> selects.
    /// </summary>
    internal static double JoinStepCost(
        double leftRows,
        double rightRows,
        double rightScanCost,
        bool hasEquiKey,
        IndexLookupProfile? indexLookup) =>
        SelectJoinStepOperator(leftRows, rightRows, rightScanCost, hasEquiKey, indexLookup) switch
        {
            JoinStepOperator.IndexLookup => IndexLookupJoinStepCost(leftRows, indexLookup!.Value),
            JoinStepOperator.Hash => HashJoinStepCost(leftRows, rightRows, rightScanCost),
            _ => NestedLoopJoinStepCost(leftRows, rightRows, rightScanCost),
        };

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
    /// Resolves the table of the left join key, for the unique-key and NDV rules of
    /// <see cref="CardinalityEstimator.EstimateJoinCardinality"/>.
    ///
    /// <para>A scan input (through Filter, Project, Limit and Sort) has one table, and that is the
    /// answer. A join input holds several tables; there the qualifier of the left key names the
    /// one it comes from, when every key names the same alias, and <paramref name="isJoinResult"/>
    /// is true. Such a table gives the key's distinct count, never the unique-key shortcut: an
    /// earlier join can repeat each row of the table, so a key that is unique in the table is not
    /// unique in the join input. Without the alias rule a join over a join had no left table at
    /// all and fell back to the fixed selectivity. The join-order enumerator estimates each set of
    /// tables from the tables alone (<see cref="JoinEnumerator.EstimateSubsetRows"/>); this
    /// annotation still comes from different code, so the two estimates can differ.</para>
    /// </summary>
    internal static TableDescriptor? TryResolveLeftTable(
        PhysicalPlanNode? input,
        IReadOnlyList<string> leftKeys,
        out bool isJoinResult)
    {
        isJoinResult = false;
        PhysicalPlanNode? current = input;

        while (current is not null)
        {
            if (current is TableScanNode { BoundSource: { } tsSrc })
                return tsSrc.Table;
            if (current is IndexRangeScanNode { BoundSource: { } irsSrc })
                return irsSrc.Table;
            if (current is IndexInListScanNode { BoundSource: { } ilsSrc })
                return ilsSrc.Table;
            if (current is FilterNode or ProjectNode or LimitNode or SortNode)
            {
                current = current.Input;
                continue;
            }
            break;
        }

        string? alias = CommonQualifier(leftKeys);
        TableDescriptor? table = alias is null ? null : FindTableByAlias(input, alias);
        isJoinResult = table is not null;
        return table;
    }

    /// <summary>The alias that qualifies every key, or null when a key is bare or the keys name two aliases.</summary>
    private static string? CommonQualifier(IReadOnlyList<string> keys)
    {
        string? alias = null;

        foreach (string key in keys)
        {
            int dot = key.IndexOf('.');
            if (dot <= 0 || dot >= key.Length - 1)
                return null;

            string qualifier = key[..dot];

            if (alias is null)
                alias = qualifier;
            else if (!string.Equals(alias, qualifier, StringComparison.OrdinalIgnoreCase))
                return null;
        }

        return alias;
    }

    /// <summary>
    /// Finds the table that <paramref name="alias"/> names inside a left-deep join input: a scan
    /// leaf on the left spine, or the right input of a join on it. Null for a derived source.
    /// </summary>
    private static TableDescriptor? FindTableByAlias(PhysicalPlanNode? input, string alias)
    {
        PhysicalPlanNode? current = input;

        while (current is not null)
        {
            BoundTableSource? candidate = current switch
            {
                TableScanNode { BoundSource: { } source } => source,
                IndexRangeScanNode { BoundSource: { } source } => source,
                IndexInListScanNode { BoundSource: { } source } => source,
                NestedLoopJoinNode nlj => nlj.RightSource.Table,
                IndexNestedLoopJoinNode inlj => inlj.RightSource,
                HashJoinNode hj => hj.BuildSource.Table,
                MergeJoinNode mj => mj.RightSource.Table,
                _ => null,
            };

            if (candidate is not null && string.Equals(candidate.Alias, alias, StringComparison.OrdinalIgnoreCase))
                return candidate.Table;

            current = current.Input;
        }

        return null;
    }

    /// <summary>
    /// Extracts unqualified column names on both sides of equi-conjuncts in an NLJ ON predicate,
    /// and the left keys as written (with their qualifier, which names the left table).
    /// Only top-level AND conjuncts of the form "X.col = rightAlias.col" (or reversed) are extracted.
    /// Returns empty lists when the predicate contains no recognizable equi-keys.
    /// Non-Identifier operands (expressions, subqueries) are skipped, so a complex ON predicate
    /// yields empty lists → NLJ falls back to <see cref="CardinalityEstimator.FallbackSelectivity"/>
    /// while hash/merge join (which use explicit node key columns) do not. Spec invariant
    /// "identical cardinality across hash/merge/NLJ" holds for simple Identifier=Identifier equi-joins.
    /// </summary>
    private static (IReadOnlyList<string> LeftCols, IReadOnlyList<string> RightCols, IReadOnlyList<string> LeftKeys)
        ExtractNljEquiKeyColumns(NodeAst onPredicate, string rightAlias)
    {
        var conjuncts = new List<NodeAst>();
        PredicateAnalyzer.CollectAndConjuncts(onPredicate, conjuncts);

        var leftCols  = new List<string>();
        var rightCols = new List<string>();
        var leftKeys  = new List<string>();
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
                leftKeys.Add(leftText);
            }
            else if (leftText.StartsWith(prefix, StringComparison.Ordinal))
            {
                // "rightAlias.col = leftAlias.col" — reversed
                rightCols.Add(leftText[prefix.Length..]);
                int dot = rightText.IndexOf('.');
                leftCols.Add(dot < 0 ? rightText : rightText[(dot + 1)..]);
                leftKeys.Add(rightText);
            }
        }
        return (leftCols, rightCols, leftKeys);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    internal static long EstimateRangeScanRows(
        IndexRangeScanNode node,
        long tableRowCount,
        CamusDBOptions options,
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

            // A degenerate equality range is one where every bound position has from==to and both
            // sides are inclusive — e.g. a single-column String/Id equality or a composite equality
            // such as (tenant, status) = (v1, v2). RangeFraction(v, v) ≈ 0 → 1 row for numeric
            // types, and the histogram/min-max paths both collapse to 0 for any type, so all such
            // lookups must be priced via NDV rather than the range path.
            //
            // Detect: same bound length, both inclusive, and every value pair compares equal.
            bool isFullEquality = fromLen > 0 && fromLen == toLen
                && node.FromInclusive && node.ToInclusive;
            if (isFullEquality)
            {
                for (int i = 0; i < fromLen; i++)
                {
                    if (node.FromBound!.Values[i].CompareTo(node.ToBound!.Values[i]) != 0)
                    {
                        isFullEquality = false;
                        break;
                    }
                }
            }

            if (isFullEquality)
            {
                // Price via composite KeyNdv for the full prefix, then single-column NDV, then
                // NDV product as successive fallbacks.
                string[] eqCols = node.Index.Columns[..Math.Min(fromLen, node.Index.Columns.Length)];

                if (eqCols.Length > 1)
                {
                    long? keyNdv = stats.GetKeyNdv(database, table, eqCols);
                    if (keyNdv is { } kn && kn > 0)
                        return Math.Max(1, (long)(tableRowCount / (double)kn));
                }

                if (eqCols.Length == 1)
                {
                    long? colNdv = stats.GetColumnNdv(database, table, eqCols[0]);
                    if (colNdv is { } cn && cn > 0)
                        return Math.Max(1, (long)(tableRowCount / (double)cn));
                }
                else
                {
                    // NDV product fallback for composite when KeyNdv is absent.
                    double sel = 1.0;
                    bool hasSel = false;
                    foreach (string col in eqCols)
                    {
                        long? colNdv = stats.GetColumnNdv(database, table, col);
                        if (colNdv is { } cn && cn > 0) { sel /= cn; hasSel = true; }
                    }
                    if (hasSel)
                        return Math.Max(1, (long)(tableRowCount * sel));
                }
                // Fall through to general path when no NDV stats are available.
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
                    // The range column is the terminal component of its bound, so the scan's
                    // composite inclusivity flags apply to it directly; an inclusive lower
                    // bound keeps the equality mass at the bound value.
                    double histSel = CardinalityEstimator.EstimateRangeFraction(
                        fb, node.FromInclusive, tb, node.ToInclusive, rangeCol, database, table, stats);
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

        // Fallback: fixed heuristic selectivities. Ceiling so that a stats-free half-open
        // range (OneBoundSelectivity == BreakevenFraction) meets ShouldPreferFullScan's
        // ceiling-based breakeven on every table size, including odd row counts — truncation
        // would land one row short and keep the index.
        double selectivity = (hasFrom && hasTo) ? BothBoundsSelectivity
                           : (hasFrom || hasTo) ? OneBoundSelectivity
                           : 1.0;

        return Math.Max(1, (long)Math.Ceiling(tableRowCount * selectivity));
    }

    // Multiplies two non-negative row counts, saturating at long.MaxValue instead of wrapping.
    private static long SaturatingMultiply(long a, long b)
    {
        double product = (double)a * b;
        return product >= long.MaxValue ? long.MaxValue : (long)product;
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
            // Inclusive integer ranges span (hi − lo + 1) of (max − min + 1) values — without
            // the +1 a single-value range on a 10-value domain estimates 0 instead of 0.1.
            // Compute in double so extreme values near long.Min/MaxValue cannot overflow the
            // subtraction into a negative selectivity.
            return Math.Min(1.0, ((double)hi - lo + 1) / ((double)max - min + 1));
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

        // NUMERIC (and FLOAT32) interpolate as doubles; a bound of another numeric type converts.
        if (colMin.TryToDouble(out double dMin) && colMax.TryToDouble(out double dMax))
        {
            if (dMax <= dMin) return null;

            double lo = fromBound is not null && ScalarBound.FromColumnValue(fromBound).TryToDouble(out double f)
                ? Math.Max(dMin, f)
                : dMin;

            double hi = toBound is not null && ScalarBound.FromColumnValue(toBound).TryToDouble(out double t)
                ? Math.Min(dMax, t)
                : dMax;

            if (hi < lo) return 0.0;
            return Math.Min(1.0, (hi - lo) / (dMax - dMin));
        }

        return null; // strings, IDs, bools: fall back to fixed heuristics
    }
}
