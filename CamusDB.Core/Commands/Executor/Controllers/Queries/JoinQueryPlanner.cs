
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical join plan tree for bound multi-source SELECT queries.
///
/// R4 LIMITATION — distributed-ready properties are single-table only:
/// The R4 distributed-ready properties (<see cref="PhysicalPlanNode.OutputOrdering"/>,
/// <see cref="PhysicalPlanNode.CanDecomposeToLocalPlusMerge"/>, <see cref="PhysicalPlanNode.EstimatedCardinality"/>,
/// <see cref="PhysicalPlanNode.PartitionLocality"/>) are populated by
/// <see cref="QueryPlanner.GetPlan"/> only. This planner does not set <c>OutputOrdering</c>
/// on child scan nodes (join-side scans are never index-selected for ORDER BY here), so join
/// plans always have <c>null</c> <c>OutputOrdering</c> on their leaves.
/// <para>
/// As a result, any distributed-execution or sort-elision logic that reads
/// <c>OutputOrdering</c> from a join plan will see <c>null</c> and must treat the ordering as
/// undefined — which is the correct conservative assumption for the current single-partition
/// implementation.
/// </para>
/// <para>
/// This is intentional for the current single-partition deployment. The R7 join-order
/// heuristics pass (<see cref="JoinOrderOptimizer"/>) reorders sources but does not yet
/// populate these distributed properties on join-side scan nodes. Any distributed sharding
/// work must extend this planner to set them when join-side scans are index-selected.
/// </para>
/// </summary>
internal sealed class JoinQueryPlanner
{
    private readonly StatisticsManager? _stats;

    public JoinQueryPlanner(StatisticsManager? stats = null)
    {
        _stats = stats;
    }

    public QueryPlan GetPlan(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)
    {
        if (!bound.IsMultiSource)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Join planning requires a multi-source query");
        }

        if (bound.Sources.Count == 0 && bound.DerivedSources.Count == 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Join planning requires at least one bound source");
        }

        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

        // Apply heuristic join-order rewriting before building the physical plan tree.
        QuerySource orderedSource = JoinOrderOptimizer.Reorder(bound.Query.Source, bound, pushdown);

        QueryPlan plan = new(database, ResolvePlanTable(bound), ticket)
        {
            BoundQuery = bound,
            PredicateAnalysis = PredicateAnalyzer.Analyze(ticket.Where, ticket.Parameters),
            ExecutionFilter = pushdown.PostJoinFilter,
            Root = BuildJoinTree(orderedSource, bound, pushdown, database, _stats),
        };

        foreach (BoundTableSource source in bound.Sources)
            plan.TableSchemaVersionByAlias[source.Alias] = source.Table.Schema.Version;

        QueryPlanStepAdapter.PopulateLinearSteps(plan);
        ProjectionPushdownPlanner.Apply(plan);

        // Annotate the join plan tree with cardinality and cost estimates.
        // Join plans use null for the primary table (multi-source); each scan node is
        // independently costed inside CostEstimator.AnnotatePlan.
        CostEstimator.AnnotatePlan(plan.Root, database, table: null, _stats);

        // Record the plan's query-shape ID and schema-version dependencies.
        // CollectSchemaDeps walks the full BoundSelectQuery tree recursively so that tables
        // referenced only inside derived-table subqueries are included — a schema change to
        // any of them must invalidate a future cached plan.
        plan.QueryShapeId = QueryShapeComputer.Compute(bound.Query);
        plan.SchemaDeps = CollectSchemaDeps(bound);

        return plan;
    }

    // Recursively collects (TableName, SchemaVersion) pairs from every table referenced in
    // the bound query, including tables that appear only inside derived-table subqueries.
    // Without the recursion, a schema change to such a table would not invalidate a cached plan.
    private static List<(string TableName, int SchemaVersion)> CollectSchemaDeps(BoundSelectQuery bound)
    {
        var deps = new List<(string, int)>();
        CollectSchemaDepsInto(bound, deps);
        return deps;
    }

    private static void CollectSchemaDepsInto(
        BoundSelectQuery bound,
        List<(string TableName, int SchemaVersion)> deps)
    {
        foreach (BoundTableSource s in bound.Sources)
            deps.Add((s.Table.Name, s.Table.Schema.Version));

        foreach (BoundDerivedTableSource d in bound.DerivedSources)
            CollectSchemaDepsInto(d.InnerBound, deps);
    }

    private static PhysicalPlanNode BuildJoinTree(
        QuerySource source,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                BoundTableSource boundSource = BoundSourceCatalog.FindTableSource(bound, tableSource);
                pushdown.ScanFiltersByAlias.TryGetValue(boundSource.Alias, out NodeAst? scanFilter);

                return new TableScanNode(TableScanSource.PrimaryRows)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = scanFilter,
                };
            }

            case DerivedTableSource derivedSource:
            {
                BoundDerivedTableSource boundDerived = BoundSourceCatalog.FindDerivedSource(bound, derivedSource);
                pushdown.ScanFiltersByAlias.TryGetValue(boundDerived.Alias, out NodeAst? scanFilter);

                return new DerivedTableScanNode
                {
                    BoundSource = boundDerived,
                    ExecutionFilter = scanFilter,
                };
            }

            case JoinSource joinSource:
            {
                PhysicalPlanNode left = BuildJoinTree(joinSource.Left, bound, pushdown, database, stats);
                BoundJoinRightSource right = BoundSourceCatalog.FindJoinRightSource(bound, joinSource.Right);
                pushdown.ScanFiltersByAlias.TryGetValue(right.Alias, out NodeAst? rightFilter);

                // Pre-extract equi-keys (no index required) — shared by both hash-join paths below.
                IReadOnlyList<JoinEquiKeyPair>? equiKeys = null;
                bool hasEquiKeys = joinSource.Kind == JoinKind.Inner
                    && JoinEquiJoinAnalyzer.TryExtractEquiKeys(right, joinSource.OnPredicate, bound, out equiKeys);

                // Try to find an index match on the right side (INLJ candidate).
                JoinEquiJoinIndexMatch? indexMatch = null;
                bool hasIndexMatch = right.Table is not null
                    && JoinEquiJoinAnalyzer.TryMatch(right.Table, joinSource.OnPredicate, bound, out indexMatch);

                if (hasIndexMatch)
                {
                    // Cost-based selection between INLJ and hash join.
                    //
                    // When both operators are viable (equi-keys exist + index exists) and stats are
                    // available, pick whichever has lower estimated cost:
                    //   INLJ  ≈ 2 × leftRows   (one point-lookup + one row-fetch per outer row)
                    //   Hash  ≈ leftRows + rightRows + min(L,R) × 0.1
                    //
                    // Without stats we always choose INLJ (same behaviour when stats are absent — no regression).
                    // INLJ wins when the outer side is small (fewest lookups); hash wins when the outer
                    // side is large and both sides can be scanned once instead of n×m.
                    if (hasEquiKeys
                        && stats is not null
                        && ShouldPreferHashOverIndexNestedLoop(left, right, database, stats))
                    {
                        return BuildHashJoinNode(left, right, joinSource.OnPredicate, equiKeys!, rightFilter, database, stats);
                    }

                    return new IndexNestedLoopJoinNode(
                        left,
                        right.Table!,
                        joinSource.OnPredicate,
                        indexMatch!.Index,
                        indexMatch.LeftLookupColumn,
                        indexMatch.RightIndexColumn)
                    {
                        RightExecutionFilter = rightFilter,
                    };
                }

                // Hash join for unindexed equi-joins (inner only — outer join variants are not yet implemented).
                if (hasEquiKeys)
                {
                    return BuildHashJoinNode(left, right, joinSource.OnPredicate, equiKeys!, rightFilter, database, stats);
                }

                return new NestedLoopJoinNode(left, right, joinSource.OnPredicate)
                {
                    RightExecutionFilter = rightFilter,
                };
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported join source: {source.GetType().Name}");
        }
    }

    /// <summary>
    /// Constructs a <see cref="HashJoinNode"/> from pre-extracted equi-keys, choosing the
    /// optimal build side. Shared by the indexed and unindexed hash-join selection paths.
    /// </summary>
    private static HashJoinNode BuildHashJoinNode(
        PhysicalPlanNode left,
        BoundJoinRightSource right,
        NodeAst? onPredicate,
        IReadOnlyList<JoinEquiKeyPair> equiKeys,
        NodeAst? rightFilter,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        string[] probeKeys = new string[equiKeys.Count];
        string[] buildKeys = new string[equiKeys.Count];

        for (int i = 0; i < equiKeys.Count; i++)
        {
            probeKeys[i] = equiKeys[i].LeftLookupColumn;
            buildKeys[i] = equiKeys[i].RightColumnName;
        }

        HashJoinBuildSide buildSide = ChooseBuildSide(left, right, database, stats);

        return new HashJoinNode(left, right, onPredicate, probeKeys, buildKeys)
        {
            BuildExecutionFilter = rightFilter,
            BuildSide = buildSide,
        };
    }

    /// <summary>
    /// Returns <see langword="true"/> when the estimated hash-join cost is strictly below
    /// the estimated index-nested-loop cost, using the same weights as
    /// <see cref="CostEstimator.AnnotatePlan"/> for both operators:
    /// <list type="bullet">
    ///   <item>INLJ  ≈ 2 × leftRows  (KvPointLookups + RowFetchesAfterIndex)</item>
    ///   <item>Hash  ≈ (leftRows + rightRows) + min(L,R) × 0.1  (KvRangeScanEntries + InMemoryRows)</item>
    /// </list>
    /// Hash beats INLJ when the outer (left) side is large relative to the indexed right side
    /// — roughly when leftRows &gt; 1.1 × rightRows. For equal-sized inputs INLJ wins by a
    /// small margin, matching the "prefer INLJ for equal/unknown cardinality" guidance in the spec.
    /// </summary>
    private static bool ShouldPreferHashOverIndexNestedLoop(
        PhysicalPlanNode leftNode,
        BoundJoinRightSource rightSource,
        DatabaseDescriptor database,
        StatisticsManager stats)
    {
        long leftRows  = EstimatePhysicalNodeRows(leftNode, database, stats);
        long rightRows = stats.GetRowCountEstimate(database, rightSource.Table!.Table)
                         ?? CostEstimator.DefaultTableRowCount;

        double inljCost = leftRows * 2.0;
        long   buildRows = Math.Min(leftRows, rightRows);
        double hashCost  = (leftRows + rightRows) + buildRows * 0.1;

        return hashCost < inljCost;
    }

    /// <summary>
    /// Chooses which side to materialise as the hash-table build input.
    ///
    /// Uses <see cref="StatisticsManager.GetRowCountEstimate"/> directly on both sides
    /// so the decision can be made during tree construction (before
    /// <see cref="CostEstimator.AnnotatePlan"/> runs). When stats are unavailable or the
    /// right source is a derived subquery (no table descriptor), defaults to
    /// <see cref="HashJoinBuildSide.Right"/> (declared right = build), which matches the
    /// prior convention and is always safe.
    ///
    /// The rule: if the left-side estimated row count is strictly less than the right-side
    /// row count, choose left as the build side (smaller hash table, smaller memory
    /// footprint). Otherwise choose right.
    /// </summary>
    private static HashJoinBuildSide ChooseBuildSide(
        PhysicalPlanNode leftNode,
        BoundJoinRightSource rightSource,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        if (stats is null || rightSource.Table is null)
            return HashJoinBuildSide.Right;

        long rightRows = stats.GetRowCountEstimate(database, rightSource.Table.Table)
                         ?? CostEstimator.DefaultTableRowCount;

        long leftRows = EstimatePhysicalNodeRows(leftNode, database, stats);

        return leftRows < rightRows ? HashJoinBuildSide.Left : HashJoinBuildSide.Right;
    }

    /// <summary>
    /// Returns a row-count estimate for <paramref name="node"/> without running a full
    /// <see cref="CostEstimator.AnnotatePlan"/> pass. For leaf scan nodes the per-table stats
    /// are available immediately; for interior nodes (joins, filters, sorts that sit on top of
    /// the left subtree) we fall back to the engine default to avoid a recursive annotation
    /// that would mutate the partially-built tree.
    /// </summary>
    // Internal for direct unit testing of the IndexRangeScanNode/IndexLookupNode arms, which
    // BuildJoinTree does not currently produce in a join subtree (join leaves are TableScanNode)
    // — they are defensive and otherwise unreachable through GetPlan.
    internal static long EstimatePhysicalNodeRows(
        PhysicalPlanNode node,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        switch (node)
        {
            case TableScanNode { BoundSource: { } boundSource }:
                return stats?.GetRowCountEstimate(database, boundSource.Table)
                       ?? CostEstimator.DefaultTableRowCount;

            case IndexLookupNode:
                return 1;

            case IndexRangeScanNode:
                // Range scans are typically selective; use the default. MUST NOT recurse here:
                // IndexRangeScanNode.Input is always null, so a `node.Input ?? node` fallback would
                // re-enter this case and infinite-loop → StackOverflow (uncatchable). Defensive —
                // BuildJoinTree never emits an IndexRangeScanNode into a join subtree today.
                return CostEstimator.DefaultTableRowCount;

            default:
                // For joined subtrees (IndexNestedLoopJoinNode, HashJoinNode, etc.) and any
                // unknown node, fall back to the engine default.  The caller will then default
                // to build=right, which is always correct.
                return CostEstimator.DefaultTableRowCount;
        }
    }

    private static TableDescriptor ResolvePlanTable(BoundSelectQuery bound)
    {
        if (bound.Sources.Count > 0)
            return bound.Sources[0].Table;

        foreach (BoundDerivedTableSource derived in bound.DerivedSources)
        {
            if (derived.InnerBound.Sources.Count > 0)
                return derived.InnerBound.Sources[0].Table;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Could not resolve plan table metadata for derived-only query");
    }
}
