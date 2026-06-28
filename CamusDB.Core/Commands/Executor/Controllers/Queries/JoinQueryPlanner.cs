
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical join plan tree for bound multi-source SELECT queries.
///
/// Distributed-ready properties on join leaves:
/// The distributed-ready properties (<see cref="PhysicalPlanNode.OutputOrdering"/>,
/// <see cref="PhysicalPlanNode.CanDecomposeToLocalPlusMerge"/>, <see cref="PhysicalPlanNode.EstimatedCardinality"/>)
/// and the <see cref="PhysicalPlanNode.Distribution"/> property are set on scan leaves
/// by this planner. However, this planner does not set <c>OutputOrdering</c>
/// on child scan nodes (join-side scans are never index-selected for ORDER BY here), so join
/// plans always have <c>null</c> <c>OutputOrdering</c> on their leaves.
/// <para>
/// As a result, any distributed-execution or sort-elision logic that reads
/// <c>OutputOrdering</c> from a join plan will see <c>null</c> and must treat the ordering as
/// undefined — which is the correct conservative assumption for the current single-partition
/// implementation.
/// </para>
/// <para>
/// This is intentional for the current single-partition deployment. The join-order
/// heuristics pass (<see cref="JoinOrderOptimizer"/>) reorders sources but does not yet
/// populate these distributed properties on join-side scan nodes. Any distributed sharding
/// work must extend this planner to set them when join-side scans are index-selected.
/// </para>
/// </summary>
internal sealed class JoinQueryPlanner
{
    // Outer-side row count below which INLJ is preferred over merge join even when both sides
    // have free index ordering (small outer → few point lookups wins over full-scan merge).
    private const long MergeJoinPreferenceThreshold = 100;

    private readonly StatisticsManager? _stats;
    private readonly PlanCache? _cache;

    public JoinQueryPlanner(StatisticsManager? stats = null) : this(stats, null) { }

    internal JoinQueryPlanner(StatisticsManager? stats, PlanCache? cache)
    {
        _stats = stats;
        _cache = cache;
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

        // Compute shape ID and schema deps early so the cache can be checked before
        // the potentially-expensive join-order enumeration / DP.
        string shapeId = QueryShapeComputer.Compute(bound.Query);
        IReadOnlyList<(string, int)> schemaDeps = CollectSchemaDeps(bound);

        QuerySource orderedSource;
        bool fromCache = false;

        if (_cache is not null
            && CamusDBConfig.PlanCacheEnabled
            && _cache.TryGet(database.Id, shapeId, schemaDeps, out PlanCacheEntry? cached)
            && cached!.JoinAliasOrder is { } cachedAliasOrder)
        {
            // Re-apply the cached ordering to the CURRENT bound.Query.Source so that ON-predicate
            // literals from this query are used (not frozen from the first query of this shape).
            orderedSource = JoinOrderOptimizer.ReorderByAliases(bound.Query.Source, cachedAliasOrder, bound);
            fromCache = true;
        }
        else
        {
            orderedSource = CamusDBConfig.CostBasedJoinOrderEnabled && _stats is not null
                ? JoinEnumerator.Enumerate(bound.Query.Source, bound, pushdown, database, _stats)
                : JoinOrderOptimizer.Reorder(bound.Query.Source, bound, pushdown);
        }

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
        CostEstimator.AnnotatePlan(plan.Root, database, table: null, _stats);

        // Record the plan's query-shape ID and schema-version dependencies.
        plan.QueryShapeId = shapeId;
        plan.SchemaDeps = schemaDeps;

        // On a cache miss, store the optimized alias ordering (not the source AST) so that
        // subsequent queries with the same shape re-apply the order to their own source tree,
        // preserving their current ON-predicate literals.
        if (!fromCache && _cache is not null && CamusDBConfig.PlanCacheEnabled)
            _cache.Put(database.Id, shapeId,
                new PlanCacheEntry(schemaDeps, SingleTable: null,
                    JoinAliasOrder: CollectAliasOrder(orderedSource)));

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

                // When CBO access-path selection is enabled, attempt an index scan on this join leaf.
                if (CamusDBConfig.CostBasedAccessPathEnabled
                    && stats is not null
                    && scanFilter is not null)
                {
                    PhysicalPlanNode? indexLeaf = TryBuildIndexLeaf(
                        scanFilter, boundSource, database, stats);
                    if (indexLeaf is not null)
                        return indexLeaf;
                }

                return new TableScanNode(TableScanSource.PrimaryRows)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = scanFilter,
                    Distribution = PlacementReader.Instance.GetPrimaryRowScanDistribution(boundSource.Table),
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

                // Test-only overrides — checked before any algorithm selection.
                if (stats?.ForceNestedLoopForTesting == true)
                    return new NestedLoopJoinNode(left, right, joinSource.OnPredicate)
                        { RightExecutionFilter = rightFilter };

                if (hasEquiKeys && stats?.ForceHashJoinForTesting == true)
                    return BuildHashJoinNode(left, right, joinSource.OnPredicate, equiKeys!, rightFilter, database, stats);

                if (hasEquiKeys && stats?.ForceMergeJoinForTesting == true)
                    return BuildMergeJoinNode(left, right, joinSource.OnPredicate!, equiKeys!, rightFilter);

                // Cost-based merge-join selection: check whether both sides have free index
                // ordering before entering the index/hash branches; if yes, merge join beats
                // both INLJ and hash for large inputs.
                (bool leftFree, bool rightFree) = hasEquiKeys
                    ? CheckFreeOrdering(left, right, equiKeys!)
                    : (false, false);

                if (hasIndexMatch)
                {
                    // Three-way selection: merge / hash / INLJ.
                    //   Merge  — both sides have a secondary index covering the join key AND the outer
                    //            side is large (scanning in index order wins over per-row lookups).
                    //   Hash   — outer side large relative to inner indexed side.
                    //   INLJ   — outer side small (few point lookups; default when stats absent).
                    if (hasEquiKeys && stats is not null)
                    {
                        long leftRows = EstimatePhysicalNodeRows(left, database, stats);

                        if (leftFree && rightFree && leftRows > MergeJoinPreferenceThreshold)
                            return BuildMergeJoinNode(left, right, joinSource.OnPredicate!, equiKeys!, rightFilter);

                        if (ShouldPreferHashOverIndexNestedLoop(left, right, database, stats))
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

                // No right-side INLJ index (right has a composite or multi-column index whose
                // leading prefix covers the join key, so TryMatch missed it but FindIndexForJoinKey
                // found it): apply the same threshold guard as the indexed branch to avoid
                // materialising both sides for small joins.
                if (hasEquiKeys)
                {
                    if (leftFree && rightFree && stats is not null)
                    {
                        long leftRows = EstimatePhysicalNodeRows(left, database, stats);
                        if (leftRows > MergeJoinPreferenceThreshold)
                            return BuildMergeJoinNode(left, right, joinSource.OnPredicate!, equiKeys!, rightFilter);
                    }

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
    /// Constructs a <see cref="MergeJoinNode"/> from pre-extracted equi-keys, wiring in
    /// sort-elision:
    /// <list type="bullet">
    ///   <item>Left side: if <c>left.OutputOrdering</c> already covers the join keys (free
    ///     ordering from a prior index scan or sort), the left input is used as-is and
    ///     <see cref="MergeJoinNode.LeftIsOrdered"/> is set. Otherwise the left input is
    ///     wrapped in a <see cref="SortNode"/>.</item>
    ///   <item>Right side: if the right table has an index whose leading columns match the
    ///     join key columns, a <c>ForcedIndex</c> <see cref="TableScanNode"/> is created
    ///     for free ordering. Otherwise a <see cref="SortNode"/> is inserted above a
    ///     full-scan node. In either case the result is stored in
    ///     <see cref="MergeJoinNode.RightPhysicalNode"/>.</item>
    /// </list>
    /// Also sets <see cref="PhysicalPlanNode.OutputOrdering"/> to ascending on the left key
    /// columns so a downstream ORDER BY on those columns can be elided by the planner.
    /// </summary>
    private static MergeJoinNode BuildMergeJoinNode(
        PhysicalPlanNode left,
        BoundJoinRightSource right,
        NodeAst onPredicate,
        IReadOnlyList<JoinEquiKeyPair> equiKeys,
        NodeAst? rightFilter)
    {
        string[] leftKeys  = new string[equiKeys.Count];
        string[] rightKeys = new string[equiKeys.Count];
        string[] bareLeftKeys = new string[equiKeys.Count];

        for (int i = 0; i < equiKeys.Count; i++)
        {
            leftKeys[i]  = equiKeys[i].LeftLookupColumn;   // qualified: "o.id"
            rightKeys[i] = equiKeys[i].RightColumnName;    // unqualified: "order_id"
            bareLeftKeys[i] = BareColumnName(leftKeys[i]); // bare: "id"
        }

        List<QueryOrderBy> leftKeyOrdering  = BuildKeyOrdering(leftKeys);
        List<QueryOrderBy> rightKeyOrdering = BuildKeyOrdering(rightKeys);

        // ── Left side ──────────────────────────────────────────────────────────
        // Free ordering: OutputOrdering already covers the join keys (ascending prefix match).
        PhysicalPlanNode leftInput;

        if (OutputOrderingCoversKeys(left.OutputOrdering, leftKeyOrdering))
        {
            leftInput = left;
        }
        else if (left is TableScanNode { BoundSource: { } ls, Source: TableScanSource.PrimaryRows }
                 && FindIndexForJoinKey(ls.Table, bareLeftKeys) is { } leftIndex)
        {
            // Flip to an index-ordered scan on the left table.
            leftInput = new TableScanNode(TableScanSource.ForcedIndex, leftIndex)
            {
                BoundSource = ls,
                ExecutionFilter = left is TableScanNode lsn ? lsn.ExecutionFilter : null,
                OutputOrdering = leftKeyOrdering,
                Distribution = PlacementReader.Instance.GetIndexScanDistribution(leftIndex),
            };
        }
        else
        {
            // Explicit sort: wrap left in a SortNode.
            leftInput = new SortNode(left) { OrderBy = leftKeyOrdering, OutputOrdering = leftKeyOrdering };
        }

        // ── Right side ─────────────────────────────────────────────────────────
        PhysicalPlanNode? rightPhysicalNode = null;
        bool rightIsOrdered = false;

        if (right.Table is { } rightBound)
        {
            if (FindIndexForJoinKey(rightBound.Table, rightKeys) is { } rightIndex)
            {
                // Free ordering via index scan on the right table.
                rightPhysicalNode = new TableScanNode(TableScanSource.ForcedIndex, rightIndex)
                {
                    BoundSource = rightBound,
                    ExecutionFilter = rightFilter,
                    OutputOrdering = rightKeyOrdering,
                    Distribution = PlacementReader.Instance.GetIndexScanDistribution(rightIndex),
                };
                rightIsOrdered = true;
            }
            else
            {
                // Explicit sort: full primary-rows scan wrapped in a SortNode.
                TableScanNode rightScan = new(TableScanSource.PrimaryRows)
                {
                    BoundSource = rightBound,
                    ExecutionFilter = rightFilter,
                    Distribution = PlacementReader.Instance.GetPrimaryRowScanDistribution(rightBound.Table),
                };
                rightPhysicalNode = new SortNode(rightScan)
                {
                    OrderBy = rightKeyOrdering,
                    OutputOrdering = rightKeyOrdering,
                };
                rightIsOrdered = true;
            }
        }
        // else: derived table — RightPhysicalNode stays null; executor falls back to
        // ScanJoinRightSource + internal sort (RightIsOrdered = false).

        MergeJoinNode node = new(leftInput, right, onPredicate, leftKeys, rightKeys)
        {
            RightExecutionFilter = rightFilter,
            LeftIsOrdered  = true,  // left is always ordered: either free-ordering or SortNode
            RightIsOrdered = rightIsOrdered,
            RightPhysicalNode = rightPhysicalNode,
        };

        // Advertise output ordering on the join key for downstream sort-elision.
        node.OutputOrdering = leftKeyOrdering;

        return node;
    }

    private static List<QueryOrderBy> BuildKeyOrdering(string[] keys)
    {
        List<QueryOrderBy> ordering = new(keys.Length);
        foreach (string col in keys)
            ordering.Add(new QueryOrderBy(col, OrderType.Ascending));
        return ordering;
    }

    /// <summary>
    /// Returns true when <paramref name="outputOrdering"/> already covers
    /// <paramref name="keyOrdering"/> as an ascending leading prefix.
    /// </summary>
    private static bool OutputOrderingCoversKeys(
        IReadOnlyList<QueryOrderBy>? outputOrdering,
        IReadOnlyList<QueryOrderBy> keyOrdering)
    {
        if (outputOrdering is null || outputOrdering.Count < keyOrdering.Count)
            return false;

        for (int i = 0; i < keyOrdering.Count; i++)
        {
            if (outputOrdering[i].Type != OrderType.Ascending)
                return false;
            if (!string.Equals(BareColumnName(outputOrdering[i].ColumnName),
                               BareColumnName(keyOrdering[i].ColumnName),
                               StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Searches <paramref name="table"/>'s public in-memory indexes (column-name-resolved) for
    /// one whose leading columns match <paramref name="bareKeyColumns"/> exactly (ordinal).
    /// Uses <see cref="TableDescriptor.Indexes"/> (the projection populated by
    /// <see cref="TableOpener"/> with bare column names) rather than
    /// <see cref="CamusDB.Core.Catalogs.Models.TableSchema.Indexes"/> (which stores
    /// raw column IDs before name resolution). Returns the first match or null.
    /// </summary>
    private static TableIndexSchema? FindIndexForJoinKey(
        TableDescriptor table,
        IReadOnlyList<string> bareKeyColumns)
    {
        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            // Skip the synthetic primary-key index: ~pk is equivalent to ScanRows and
            // requires a different unique-key parse mode — only secondary indexes here.
            if (index.Name == "~pk")
                continue;

            if (index.State != SchemaElementState.Public)
                continue;

            if (index.Columns.Length < bareKeyColumns.Count)
                continue;

            bool match = true;
            for (int i = 0; i < bareKeyColumns.Count; i++)
            {
                if (!string.Equals(index.Columns[i], bareKeyColumns[i], StringComparison.Ordinal))
                { match = false; break; }
            }

            if (match)
                return index;
        }

        return null;
    }

    private static string BareColumnName(string columnName)
    {
        int dot = columnName.LastIndexOf('.');
        return dot >= 0 && dot < columnName.Length - 1 ? columnName[(dot + 1)..] : columnName;
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
    /// Checks whether both sides of an equi-join have free index ordering on the join key
    /// columns: left via <see cref="OutputOrderingCoversKeys"/> or a secondary index found
    /// by <see cref="FindIndexForJoinKey"/>, right via <see cref="FindIndexForJoinKey"/>.
    /// Used by cost-based merge-join selection to detect the "both free → merge beats hash
    /// and INLJ" case without calling <see cref="BuildMergeJoinNode"/> speculatively.
    /// </summary>
    private static (bool LeftFree, bool RightFree) CheckFreeOrdering(
        PhysicalPlanNode left,
        BoundJoinRightSource right,
        IReadOnlyList<JoinEquiKeyPair> equiKeys)
    {
        string[] leftBareKeys  = equiKeys.Select(k => BareColumnName(k.LeftLookupColumn)).ToArray();
        string[] rightBareKeys = equiKeys.Select(k => k.RightColumnName).ToArray();

        List<QueryOrderBy> leftKeyOrdering  = BuildKeyOrdering(leftBareKeys);
        List<QueryOrderBy> rightKeyOrdering = BuildKeyOrdering(rightBareKeys);

        bool leftFree = OutputOrderingCoversKeys(left.OutputOrdering, leftKeyOrdering);
        if (!leftFree && left is TableScanNode { Source: TableScanSource.PrimaryRows, BoundSource: not null } leftScan)
            leftFree = FindIndexForJoinKey(leftScan.BoundSource.Table, leftBareKeys) is not null;

        bool rightFree = right.Table is not null
                         && FindIndexForJoinKey(right.Table.Table, rightBareKeys) is not null;

        return (leftFree, rightFree);
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
            case TableScanNode { BoundSource: { } boundSource } tsn:
            {
                long tableRows = stats?.GetRowCountEstimate(database, boundSource.Table)
                                 ?? CostEstimator.DefaultTableRowCount;
                if (tsn.ExecutionFilter is not null && stats is not null)
                {
                    double sel = CardinalityEstimator.EstimateFilterSelectivity(
                        tsn.ExecutionFilter, database, boundSource.Table, stats);
                    return Math.Max(1L, (long)(tableRows * sel));
                }
                return tableRows;
            }

            case IndexLookupNode:
                return 1;

            case IndexRangeScanNode { BoundSource: { } rangeBound } rsn:
            {
                long tableRows = stats?.GetRowCountEstimate(database, rangeBound.Table)
                                 ?? CostEstimator.DefaultTableRowCount;
                // Estimate index range selectivity via the cost model.
                long rangeRows = CostEstimator.EstimateRangeScanRows(rsn, tableRows, stats, database, rangeBound.Table);
                // Add residual filter on top if present.
                if (rsn.ExecutionFilter is not null && stats is not null)
                {
                    double sel = CardinalityEstimator.EstimateFilterSelectivity(
                        rsn.ExecutionFilter, database, rangeBound.Table, stats);
                    rangeRows = Math.Max(1L, (long)(rangeRows * sel));
                }
                return rangeRows;
            }

            case IndexRangeScanNode:
                // Defensive: IndexRangeScanNode without BoundSource is a single-table leaf —
                // BuildJoinTree does not emit these; use the default rather than recursing.
                return CostEstimator.DefaultTableRowCount;

            default:
                // For joined subtrees (IndexNestedLoopJoinNode, HashJoinNode, etc.) and any
                // unknown node, fall back to the engine default.  The caller will then default
                // to build=right, which is always correct.
                return CostEstimator.DefaultTableRowCount;
        }
    }

    /// <summary>
    /// Returns the table/derived-table aliases from <paramref name="source"/> in left-deep order:
    /// leftmost leaf first, then each right-side leaf in join depth order.
    /// Used to extract the ordering fingerprint after enumeration, so the cache stores
    /// an alias list rather than a frozen AST.
    /// </summary>
    private static List<string> CollectAliasOrder(QuerySource source)
    {
        List<string> aliases = [];
        CollectAliasOrderInto(source, aliases);
        return aliases;
    }

    private static void CollectAliasOrderInto(QuerySource source, List<string> aliases)
    {
        switch (source)
        {
            case TableSource ts:
                aliases.Add(ts.Alias ?? ts.TableName);
                break;
            case DerivedTableSource ds:
                aliases.Add(ds.Alias!);
                break;
            case JoinSource js:
                CollectAliasOrderInto(js.Left, aliases);
                CollectAliasOrderInto(js.Right, aliases);
                break;
        }
    }

    /// <summary>
    /// Attempts to build an index-range-scan leaf for a join table source that has a
    /// pushed-down filter, replacing the default full primary-row scan.
    ///
    /// Gated on <see cref="CamusDBConfig.CostBasedAccessPathEnabled"/>. Falls back
    /// (returns null) when no usable index is found, the chosen step is a full scan,
    /// or the range-scan cost exceeds the full-scan breakeven threshold.
    /// </summary>
    private static PhysicalPlanNode? TryBuildIndexLeaf(
        NodeAst scanFilter,
        BoundTableSource boundSource,
        DatabaseDescriptor database,
        StatisticsManager stats)
    {
        PredicateAnalysis raw = PredicateAnalyzer.Analyze(scanFilter, null);
        // Strip "alias." prefix so bare index column names match (e.g. "o.status" → "status").
        PredicateAnalysis analysis = JoinEnumerator.StripAliasPrefix(raw, boundSource.Alias);

        if (analysis.IndexableComparisons.Count == 0 && analysis.InListComparisons.Count == 0)
            return null;

        QueryPlanStep? step = IndexScanSelector.TrySelectScan(boundSource.Table, analysis);
        if (step is null || step.Value.Type == QueryPlanStepType.FullScanFromTableIndex)
            return null;

        // Breakeven veto: fall back to full scan when the range scan touches too many rows.
        if (step.Value.Type == QueryPlanStepType.RangeScanFromIndex
            && (step.Value.FromBound is not null || step.Value.ToBound is not null))
        {
            long? tableRows = stats.GetRowCountEstimate(database, boundSource.Table);
            if (tableRows is { } trc && trc > 0)
            {
                var candidate = (IndexRangeScanNode)QueryPlanner.ToScanNode(step.Value);
                if (CostEstimator.ShouldPreferFullScan(candidate, trc, stats, database, boundSource.Table))
                    return null;
            }
        }

        PhysicalPlanNode indexNode = QueryPlanner.ToScanNode(step.Value);

        // Residual conjuncts not absorbed by the index step become the ExecutionFilter.
        // These ASTs still use qualified names (e.g. "o.status") — the executor qualifies
        // rows before evaluation, so qualified residual filters are correct.
        NodeAst? residual = PredicateAnalyzer.BuildExecutionFilter(analysis, step, boundSource.Table, null);

        // Wire BoundSource and residual filter onto the node.
        switch (indexNode)
        {
            case IndexRangeScanNode rsn:
                indexNode = new IndexRangeScanNode(
                    rsn.Index, rsn.FromBound, rsn.FromInclusive, rsn.ToBound, rsn.ToInclusive)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = residual,
                };
                indexNode.Distribution = PlacementReader.Instance.GetIndexScanDistribution(rsn.Index);
                break;

            case IndexLookupNode:
                // Unique point lookup in a join leaf: fall back to full scan for now.
                // (The executor's INLJ path already handles indexed right sides; the left
                // side as a lookup node is unusual and not worth a separate path.)
                return null;

            default:
                return null;
        }

        return indexNode;
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
