
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Functions;
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

    /// <summary>
    /// Configuration of the engine this planner belongs to. Held here rather than read from the
    /// descriptor passed to each call, so one planner cannot silently plan under two different
    /// configurations.
    /// </summary>
    private readonly CamusDBOptions _options;

    private readonly PlanCache? _cache;

    public JoinQueryPlanner(CamusDBOptions options, StatisticsManager? stats = null) : this(stats, null, options) { }

    internal JoinQueryPlanner(StatisticsManager? stats, PlanCache? cache, CamusDBOptions options)
    {
        _stats = stats;
        _cache = cache;
        _options = options;
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
        // Read the config flag once per plan: it is a mutable static, and reading it again at
        // the Put below could observe a mid-plan flip (Put into a cache the lookup said was off).
        bool planCacheEnabled = _cache is not null && _options.PlanCacheEnabled;

        string shapeId = QueryShapeComputer.Compute(bound.Query);
        IReadOnlyList<(string, int)> schemaDeps = CollectSchemaDeps(bound);

        // Plan-cache-only: nothing outside the cache reads these descriptors, so a deployment with the
        // cache off walks the bound query once instead of twice. The shape id and schema deps stay
        // unconditional — EXPLAIN renders both on every plan.
        IReadOnlyList<PlanCacheDep>? cacheDeps = planCacheEnabled ? CollectCacheDeps(database, bound) : null;

        // The leaf analysis below resolves no user parameter, but a clock call (now()) must resolve to
        // the statement's time: an index bound taken from the wall clock at plan time would be later
        // than the time the row filter reads, and the scan would miss rows the filter accepts.
        Dictionary<string, ColumnValue>? clockValues = ClockScalarFunctions.ClockValuesOnly(ticket.Parameters);

        JoinOrderChoice? choice = null;
        bool fromCache = false;

        if (planCacheEnabled
            && _cache!.TryGet(database.Id, shapeId, cacheDeps!, out PlanCacheEntry? cached)
            && cached!.JoinAliasOrder is { } cachedAliasOrder)
        {
            // Re-apply the cached ordering to the CURRENT bound.Query.Source so that ON-predicate
            // literals from this query are used (not frozen from the first query of this shape).
            choice = ReplayCachedOrder(database, bound, pushdown, cachedAliasOrder, cached.JoinBuild, clockValues);

            if (choice is not null)
                fromCache = true;
            else
                _cache.RecordReplayFailure();
        }

        // A miss, or a hit whose order cannot be rebuilt over the current query: plan from the start.
        choice ??= _options.CostBasedJoinOrderEnabled && _stats is not null
                && AllBaseTableEstimatesLoaded(database, bound)
            ? JoinEnumerator.Enumerate(bound.Query.Source, bound, pushdown, database, _stats, _options, clockValues)
            : JoinOrderOptimizer.Choose(bound.Query.Source, bound, pushdown);

        QuerySource orderedSource = choice.Source;

        // A query over functions only (FROM generate_series(1, 3)) has no base table at any depth.
        QueryPlan plan = TryResolvePlanTable(bound) is { } planTable
            ? new QueryPlan(database, planTable, ticket)
            : QueryPlan.WithoutTable(database, ticket);

        plan.BoundQuery = bound;
        plan.PredicateAnalysis = PredicateAnalyzer.Analyze(ticket.Where, ticket.Parameters);
        // The filter that goes with the tree: a conjunct-mode tree already evaluates the WHERE
        // conjuncts it moved into its joins.
        plan.ExecutionFilter = choice.PostJoinFilter;
        plan.Root = BuildJoinTree(orderedSource, bound, pushdown, database, _stats, choice.StepEstimates, clockValues);

        plan.JoinOrderSearch = new JoinOrderSearchSummary(
            fromCache ? "cached" : choice.Mode == JoinBuildMode.CostBased ? "cost-based" : "heuristic",
            choice.CostedJoinSteps);

        foreach (BoundTableSource source in bound.Sources)
            plan.TableSchemaVersionByAlias[source.Alias] = source.Table.Schema.Version;

        QueryPlanStepAdapter.PopulateLinearSteps(plan);
        ProjectionPushdownPlanner.Apply(plan);

        // Annotate the join plan tree with cardinality and cost estimates.
        CostEstimator.AnnotatePlan(plan.Root, database, table: null, _stats, _options);

        // Record the plan's query-shape ID and schema-version dependencies.
        plan.QueryShapeId = shapeId;
        plan.SchemaDeps = schemaDeps;

        // On a cache miss, store the optimized alias ordering (not the source AST) so that
        // subsequent queries with the same shape re-apply the order to their own source tree,
        // preserving their current ON-predicate literals.
        if (!fromCache && planCacheEnabled)
            _cache!.Put(database.Id, shapeId,
                new PlanCacheEntry(cacheDeps!, SingleTable: null,
                    JoinAliasOrder: CollectAliasOrder(orderedSource),
                    JoinBuild: choice.Mode));

        return plan;
    }

    /// <summary>
    /// Rebuilds a cached join order over the current query, in the mode the miss used. A
    /// whole-predicate entry replays through <see cref="JoinOrderOptimizer.ReorderByAliases"/>, as
    /// the heuristic built it. A conjunct or cost-based entry rebuilds through
    /// <see cref="JoinGraph"/>, also when the order equals the declared order: the miss can keep
    /// the declared order and still move a conjunct to an earlier join. A cost-based entry also
    /// recomputes the estimates of each join, so the builder selects the operators of the miss.
    /// Returns null when the entry cannot be rebuilt (the graph refuses the current tree, the order
    /// is not a valid permutation of its leaves, or a cost-based entry finds a row count not
    /// loaded); the caller then plans from the start, as on a miss.
    /// </summary>
    private JoinOrderChoice? ReplayCachedOrder(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        IReadOnlyList<string> cachedAliasOrder,
        JoinBuildMode mode,
        Dictionary<string, ColumnValue>? clockValues)
    {
        if (mode == JoinBuildMode.WholePredicate)
            return JoinOrderChoice.WholePredicate(
                JoinOrderOptimizer.ReorderByAliases(bound.Query.Source, cachedAliasOrder, bound), pushdown);

        JoinGraph? graph = JoinGraph.TryBuild(bound.Query.Source, bound, pushdown.PostJoinConjuncts);
        if (graph is null)
            return null;

        int[]? order = graph.MapAliases(cachedAliasOrder);
        if (order is null || !graph.IsConnectedOrder(order))
            return null;

        QuerySource? tree = graph.BuildTree(order);
        if (tree is null)
            return null;

        if (mode == JoinBuildMode.Conjunct)
            return JoinOrderChoice.Conjunct(tree, graph);

        if (_stats is null || !AllBaseTableEstimatesLoaded(database, bound))
            return null;

        // A replay runs no search, so it costs no step.
        return JoinOrderChoice.CostBased(tree, graph,
            JoinEnumerator.EstimateSteps(graph, order, tree, bound, pushdown, database, _stats, _options, clockValues),
            costedJoinSteps: 0);
    }

    /// <summary>
    /// True when every base table of the query has a loaded row-count estimate. Cost-based
    /// join ordering and hash-join build-side selection compare per-table estimates against
    /// each other; while a table's statistics are still unloaded, its estimate is the fixed
    /// default row count, and comparing a real count against that default systematically
    /// mis-sizes the unloaded side (a tiny table impersonates 10,000 rows, so the genuinely
    /// small analyzed side becomes the build/probe backwards). Until every estimate is real,
    /// those comparisons degrade to the declared-order defaults. Calling
    /// <see cref="StatisticsManager.GetRowCountEstimate"/> here also fires the background
    /// stats load for every table (no early exit, so the loads run in parallel), and a
    /// completed load bumps the table's statistics generation, which invalidates plans cached
    /// during the window — the degradation heals itself.
    /// </summary>
    private bool AllBaseTableEstimatesLoaded(DatabaseDescriptor database, BoundSelectQuery bound)
    {
        bool all = true;

        foreach (BoundTableSource s in bound.Sources)
        {
            if (_stats!.GetRowCountEstimate(database, s.Table) is null)
                all = false;
        }

        return all;
    }

    // Recursively collects (TableId, SchemaVersion) pairs from every table referenced in
    // the bound query, including tables that appear only inside derived-table subqueries.
    // Without the recursion, a schema change to such a table would not invalidate a cached plan.
    // Uses the immutable table id (not the name) so a DROP TABLE t + CREATE TABLE t at the same
    // schema version produces a different fingerprint and does not produce a false cache hit.
    private static List<(string TableId, int SchemaVersion)> CollectSchemaDeps(BoundSelectQuery bound)
    {
        var deps = new List<(string, int)>();
        CollectSchemaDepsInto(bound, deps);
        return deps;
    }

    private static void CollectSchemaDepsInto(
        BoundSelectQuery bound,
        List<(string TableId, int SchemaVersion)> deps)
    {
        foreach (BoundTableSource s in bound.Sources)
            deps.Add((s.Table.Id, s.Table.Schema.Version));

        foreach (BoundDerivedTableSource d in bound.DerivedSources)
            CollectSchemaDepsInto(d.InnerBound, deps);
    }

    /// <summary>
    /// Same traversal as <see cref="CollectSchemaDeps"/> but produces the plan-cache dependency
    /// fingerprint, which additionally carries each table's index-set and statistics generations
    /// so index DDL and ANALYZE invalidate cached join orderings (see <see cref="PlanCacheDep"/>).
    /// </summary>
    private List<PlanCacheDep> CollectCacheDeps(DatabaseDescriptor database, BoundSelectQuery bound)
    {
        var deps = new List<PlanCacheDep>();
        CollectCacheDepsInto(database, bound, deps);
        return deps;
    }

    private void CollectCacheDepsInto(DatabaseDescriptor database, BoundSelectQuery bound, List<PlanCacheDep> deps)
    {
        foreach (BoundTableSource s in bound.Sources)
            deps.Add(new PlanCacheDep(
                s.Table.Id,
                s.Table.Schema.Version,
                s.Table.IndexSetGeneration,
                _stats?.GetAnalyzeGeneration(database, s.Table) ?? 0));

        foreach (BoundDerivedTableSource d in bound.DerivedSources)
            CollectCacheDepsInto(database, d.InnerBound, deps);
    }

    /// <summary>
    /// Builds the physical tree for <paramref name="source"/>. When <paramref name="steps"/> has an
    /// entry for a join (a tree from the cost-based enumerator), the operator choice of that join
    /// (index lookup against hash, the merge-join size threshold, and the hash build side) uses the
    /// enumerator's estimates instead of <see cref="EstimatePhysicalNodeRows"/>, which knows a join
    /// input only as a fixed default, and <see cref="EstimateRightRows"/>, which counts a filtered
    /// right input by its output rows although the operator reads all of it. Without estimates
    /// (the heuristic order), the choice is the same as before the enumerator shared its numbers.
    /// </summary>
    private PhysicalPlanNode BuildJoinTree(
        QuerySource source,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        IReadOnlyDictionary<JoinSource, JoinStepEstimate>? steps = null,
        Dictionary<string, ColumnValue>? clockValues = null)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                BoundTableSource boundSource = BoundSourceCatalog.FindTableSource(bound, tableSource);
                pushdown.ScanFiltersByAlias.TryGetValue(boundSource.Alias, out NodeAst? scanFilter);

                // When CBO access-path selection is enabled, attempt an index scan on this join leaf.
                if (_options.CostBasedAccessPathEnabled
                    && stats is not null
                    && scanFilter is not null)
                {
                    PhysicalPlanNode? indexLeaf = TryBuildIndexLeaf(
                        scanFilter, boundSource, database, stats, clockValues);
                    if (indexLeaf is not null)
                        return indexLeaf;
                }

                return new TableScanNode(TableScanSource.PrimaryRows)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = scanFilter,
                    Distribution = PlacementReader.Instance.GetPrimaryRowScanDistribution(boundSource.Table, _options),
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
                PhysicalPlanNode left = BuildJoinTree(joinSource.Left, bound, pushdown, database, stats, steps, clockValues);
                JoinStepEstimate? step = steps is not null && steps.TryGetValue(joinSource, out JoinStepEstimate found)
                    ? found
                    : null;

                PhysicalPlanNode joinNode = BuildJoinNode(joinSource, left, step, bound, pushdown, database, stats);
                AttachRightAccessPath(joinNode, database, stats, clockValues);

                // Kept beside the annotated estimate, not in place of it: EXPLAIN (ANALYZE) prints both,
                // so a reader can see which of the two models was closer to the rows the join produced.
                if (step is { } estimate)
                    joinNode.JoinOrderEstimatedRows = estimate.OutputRows;

                return joinNode;
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported join source: {source.GetType().Name}");
        }
    }

    /// <summary>
    /// Gives the right table of a hash or nested-loop join its own access path, when its pushed
    /// filter drives an index: the same index range or IN-list leaf (<see cref="TryBuildIndexLeaf"/>,
    /// with its breakeven veto) the first table of the order would get. Without it the operator read
    /// the whole right table and filtered it in memory, so <c>b.category = 'rare'</c> used an index
    /// only when <c>b</c> came first. The join-order search prices the right read with the same leaf
    /// (<see cref="JoinEnumerator"/>), so the order and the operators are chosen for the reads the
    /// plan makes. Index lookup and merge joins read the right side their own way and are left alone.
    /// </summary>
    private void AttachRightAccessPath(
        PhysicalPlanNode joinNode,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        Dictionary<string, ColumnValue>? clockValues)
    {
        if (!_options.CostBasedAccessPathEnabled || stats is null)
            return;

        switch (joinNode)
        {
            case HashJoinNode { BuildSource.Table: { } table, BuildExecutionFilter: { } filter } hash:
                hash.RightPhysicalNode = TryBuildIndexLeaf(filter, table, database, stats, clockValues);
                break;

            case NestedLoopJoinNode { RightSource.Table: { } table, RightExecutionFilter: { } filter } nestedLoop:
                nestedLoop.RightPhysicalNode = TryBuildIndexLeaf(filter, table, database, stats, clockValues);
                break;
        }
    }

    /// <summary>
    /// Chooses and builds the operator of one join over an already built left input: index
    /// nested-loop, hash, merge or nested-loop, with the test-only overrides first. Split from
    /// <see cref="BuildJoinTree"/> so that every operator the choice can return passes through one
    /// place on its way out, where the caller records the join-order search estimate.
    /// </summary>
    private PhysicalPlanNode BuildJoinNode(
        JoinSource joinSource,
        PhysicalPlanNode left,
        JoinStepEstimate? step,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        BoundJoinRightSource right = BoundSourceCatalog.FindJoinRightSource(bound, joinSource.Right);
        pushdown.ScanFiltersByAlias.TryGetValue(right.Alias, out NodeAst? rightFilter);

        // Pre-extract equi-keys (no index required) — shared by both hash-join paths below.
        // A left outer join keys the same way as an inner join: every operator keeps the
        // left input as the preserved side, so index, hash and merge are all valid for it.
        JoinKind kind = joinSource.Kind;
        IReadOnlyList<JoinEquiKeyPair>? equiKeys = null;
        bool hasEquiKeys = kind is JoinKind.Inner or JoinKind.LeftOuter
            && JoinEquiJoinAnalyzer.TryExtractEquiKeys(right, joinSource.OnPredicate, bound, out equiKeys);

        // Try to find an index match on the right side (INLJ candidate).
        JoinEquiJoinIndexMatch? indexMatch = null;
        bool hasIndexMatch = right.Table is not null
            && JoinEquiJoinAnalyzer.TryMatch(right.Table, joinSource.OnPredicate, bound, out indexMatch);

        // Test-only overrides — checked before any algorithm selection.
        if (stats?.ForceNestedLoopForTesting == true)
            return new NestedLoopJoinNode(left, right, joinSource.OnPredicate)
                { RightExecutionFilter = rightFilter, Kind = kind };

        if (hasEquiKeys && stats?.ForceHashJoinForTesting == true)
            return BuildHashJoinNode(left, right, joinSource.OnPredicate, equiKeys!, rightFilter, database, stats, kind, step);

        if (hasEquiKeys && stats?.ForceMergeJoinForTesting == true)
            return BuildMergeJoinNode(left, right, joinSource.OnPredicate!, equiKeys!, rightFilter, kind);

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
            if (hasEquiKeys && stats is not null && !stats.ForceIndexNestedLoopForTesting)
            {
                double leftRows = step?.LeftRows ?? EstimatePhysicalNodeRows(left, database, stats);

                if (leftFree && rightFree && leftRows > MergeJoinPreferenceThreshold)
                    return BuildMergeJoinNode(left, right, joinSource.OnPredicate!, equiKeys!, rightFilter, kind);

                if (ShouldPreferHashOverIndexNestedLoop(left, right, database, stats, rightFilter, step))
                    return BuildHashJoinNode(left, right, joinSource.OnPredicate, equiKeys!, rightFilter, database, stats, kind, step);
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
                Kind = kind,
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
                double leftRows = step?.LeftRows ?? EstimatePhysicalNodeRows(left, database, stats);
                if (leftRows > MergeJoinPreferenceThreshold)
                    return BuildMergeJoinNode(left, right, joinSource.OnPredicate!, equiKeys!, rightFilter, kind);
            }

            return BuildHashJoinNode(left, right, joinSource.OnPredicate, equiKeys!, rightFilter, database, stats, kind, step);
        }

        return new NestedLoopJoinNode(left, right, joinSource.OnPredicate)
        {
            RightExecutionFilter = rightFilter,
            Kind = kind,
        };
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
    private MergeJoinNode BuildMergeJoinNode(
        PhysicalPlanNode left,
        BoundJoinRightSource right,
        NodeAst onPredicate,
        IReadOnlyList<JoinEquiKeyPair> equiKeys,
        NodeAst? rightFilter,
        JoinKind kind = JoinKind.Inner)
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
                Distribution = PlacementReader.Instance.GetIndexScanDistribution(leftIndex, _options),
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
                    Distribution = PlacementReader.Instance.GetIndexScanDistribution(rightIndex, _options),
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
                    Distribution = PlacementReader.Instance.GetPrimaryRowScanDistribution(rightBound.Table, _options),
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
            Kind = kind,
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
    ///
    /// <para>A leaf scan advertises bare column names, and its alias is the only alias the key can
    /// carry, so a bare entry compares by bare name. An interior join advertises alias-qualified
    /// names, and there the alias matters: an ordering on <c>a.id</c> does not cover a key on
    /// <c>b.id</c>, even though both are called <c>id</c>. Comparing bare names there skipped the
    /// sort a chained merge join needed, and the merge then walked an unsorted left side and
    /// silently dropped (inner) or padded (outer) rows that had a match.</para>
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

            string ordered = outputOrdering[i].ColumnName;
            string key = keyOrdering[i].ColumnName;

            bool covers = ordered.Contains('.') && key.Contains('.')
                ? string.Equals(ordered, key, StringComparison.OrdinalIgnoreCase)
                : string.Equals(BareColumnName(ordered), BareColumnName(key), StringComparison.Ordinal);

            if (!covers)
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
    private static TableIndexSchema? FindIndexForJoinKey(TableDescriptor table, IReadOnlyList<string> bareKeyColumns)
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

            // The merge join walks the chosen index from start to end as the join input. A unique
            // index omits every row with a NULL in any key column — a NULL join key never matches,
            // but a NULL in a trailing non-key column drops a row that would have joined — so it
            // may serve only when every column is NOT NULL. A non-unique index holds every row.
            if (index.Type == IndexType.Unique && !IndexScanSelector.AllColumnsNotNull(table, index))
                continue;

            bool match = true;

            for (int i = 0; i < bareKeyColumns.Count; i++)
            {
                if (!string.Equals(index.Columns[i], bareKeyColumns[i], StringComparison.Ordinal))
                {
                    match = false;
                    break;
                }

                // The merge join consumes a strictly ascending stream and the key ordering
                // built for it is all-ascending; a descending index column would deliver
                // complemented (descending) key order and silently drop join matches.
                if (index.DirectionAt(i) != OrderType.Ascending)
                {
                    match = false;
                    break;
                }
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
    /// A left outer join pins the build side to the right input and never consults
    /// <see cref="ChooseBuildSide"/>: the preserved side must be the probe so an unmatched
    /// probe row can be padded in stream order (see <see cref="HashJoinNode"/>).
    /// </summary>
    private HashJoinNode BuildHashJoinNode(
        PhysicalPlanNode left,
        BoundJoinRightSource right,
        NodeAst? onPredicate,
        IReadOnlyList<JoinEquiKeyPair> equiKeys,
        NodeAst? rightFilter,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        JoinKind kind = JoinKind.Inner,
        JoinStepEstimate? step = null)
    {
        string[] probeKeys = new string[equiKeys.Count];
        string[] buildKeys = new string[equiKeys.Count];

        for (int i = 0; i < equiKeys.Count; i++)
        {
            probeKeys[i] = equiKeys[i].LeftLookupColumn;
            buildKeys[i] = equiKeys[i].RightColumnName;
        }

        HashJoinBuildSide buildSide = kind == JoinKind.LeftOuter
            ? HashJoinBuildSide.Right
            : step is { } estimate
                // The enumerator costed the hash table on the smaller input; build that one.
                ? estimate.LeftRows < estimate.RightRows ? HashJoinBuildSide.Left : HashJoinBuildSide.Right
                : ChooseBuildSide(left, right, database, stats, rightFilter);

        return new HashJoinNode(left, right, onPredicate, probeKeys, buildKeys)
        {
            Kind = kind,
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
    ///
    /// <para>The output-ordering check uses the same alias-qualified left keys that
    /// <see cref="BuildMergeJoinNode"/> uses, so selection and construction agree. With bare
    /// keys an interior join ordered on <c>a.x</c> counted as free ordering for a key on
    /// <c>b.x</c>; merge was then selected without costing, and construction, which does
    /// compare the aliases, inserted a sort of the whole intermediate result. Bare names are
    /// used only to look up an index on a base-table leaf, whose alias is the only one the
    /// key can carry.</para>
    /// </summary>
    private static (bool LeftFree, bool RightFree) CheckFreeOrdering(
        PhysicalPlanNode left,
        BoundJoinRightSource right,
        IReadOnlyList<JoinEquiKeyPair> equiKeys)
    {
        string[] leftKeys      = equiKeys.Select(k => k.LeftLookupColumn).ToArray();
        string[] leftBareKeys  = equiKeys.Select(k => BareColumnName(k.LeftLookupColumn)).ToArray();
        string[] rightBareKeys = equiKeys.Select(k => k.RightColumnName).ToArray();

        List<QueryOrderBy> leftKeyOrdering = BuildKeyOrdering(leftKeys);

        bool leftFree = OutputOrderingCoversKeys(left.OutputOrdering, leftKeyOrdering);
        if (!leftFree && left is TableScanNode { Source: TableScanSource.PrimaryRows, BoundSource: not null } leftScan)
            leftFree = FindIndexForJoinKey(leftScan.BoundSource.Table, leftBareKeys) is not null;

        bool rightFree = right.Table is not null
                         && FindIndexForJoinKey(right.Table.Table, rightBareKeys) is not null;

        return (leftFree, rightFree);
    }

    /// <summary>
    /// Returns <see langword="true"/> when the estimated hash-join cost is strictly below
    /// the estimated index-nested-loop cost, through the one rule the join-order enumerator also
    /// uses (<see cref="CostEstimator.PrefersHashOverIndexLookup"/>):
    /// <list type="bullet">
    ///   <item>INLJ  ≈ 2 × leftRows  (KvPointLookups + RowFetchesAfterIndex)</item>
    ///   <item>Hash  ≈ rightRows + leftRows + min(L,R) × 0.1  (KvRangeScanEntries + InMemoryRows)</item>
    /// </list>
    /// The right row estimate stands in for the right scan cost here. Hash beats INLJ when the
    /// outer (left) side is large relative to the indexed right side — roughly when
    /// leftRows &gt; 1.1 × rightRows. For equal-sized inputs INLJ wins by a small margin, so equal
    /// or unknown cardinalities keep the index lookup.
    /// </summary>
    private bool ShouldPreferHashOverIndexNestedLoop(
        PhysicalPlanNode leftNode,
        BoundJoinRightSource rightSource,
        DatabaseDescriptor database,
        StatisticsManager stats,
        NodeAst? rightScanFilter = null,
        JoinStepEstimate? step = null)
    {
        // A join from the cost-based enumerator decides with the numbers it was costed with.
        if (step is { } estimate)
            return CostEstimator.PrefersHashOverIndexLookup(estimate.LeftRows, estimate.RightRows, estimate.RightScanCost);

        long leftRows  = EstimatePhysicalNodeRows(leftNode, database, stats);
        long rightRows = EstimateRightRows(rightSource, database, stats, rightScanFilter);

        return CostEstimator.PrefersHashOverIndexLookup(leftRows, rightRows, rightScanCost: rightRows);
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
    /// The comparison runs only when both sides' leaf estimates are real. While a leaf
    /// table's statistics are still unloaded its estimate is the fixed default row count, and
    /// a real count on the other side then always "wins" the small-side contest — the join
    /// builds the genuinely large side and probes the small one, which also starves the
    /// broadcast probe (the probe must be the large, span-split side). Interior nodes keep
    /// their fixed-constant estimates by design.
    ///
    /// The rule: if the left-side estimated row count is strictly less than the right-side
    /// row count, choose left as the build side (smaller hash table, smaller memory
    /// footprint). Otherwise choose right.
    /// </summary>
    private HashJoinBuildSide ChooseBuildSide(
        PhysicalPlanNode leftNode,
        BoundJoinRightSource rightSource,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        NodeAst? rightScanFilter = null)
    {
        if (stats is null || rightSource.Table is null)
            return HashJoinBuildSide.Right;

        if (stats.GetRowCountEstimate(database, rightSource.Table.Table) is null
            || !LeafEstimateGrounded(leftNode, database, stats))
            return HashJoinBuildSide.Right;

        long rightRows = EstimateRightRows(rightSource, database, stats, rightScanFilter);
        long leftRows  = EstimatePhysicalNodeRows(leftNode, database, stats);

        return leftRows < rightRows ? HashJoinBuildSide.Left : HashJoinBuildSide.Right;
    }

    /// <summary>
    /// True when <paramref name="node"/>'s row estimate is not the unloaded-statistics
    /// default: a leaf scan whose table has a loaded row count, a point lookup (exactly one
    /// row by construction), or an interior/derived node, whose estimates are fixed constants
    /// by design and are not affected by statistics loads. Used by
    /// <see cref="ChooseBuildSide"/> to avoid comparing a real count against the default.
    /// </summary>
    private static bool LeafEstimateGrounded(
        PhysicalPlanNode node,
        DatabaseDescriptor database,
        StatisticsManager stats) => node switch
    {
        TableScanNode { BoundSource: { } bound }      => stats.GetRowCountEstimate(database, bound.Table) is not null,
        IndexRangeScanNode { BoundSource: { } bound } => stats.GetRowCountEstimate(database, bound.Table) is not null,
        IndexLookupNode                               => true,
        _                                             => true,
    };

    /// <summary>
    /// Returns the estimated row count for the right side of a join, applying the pushed-down
    /// scan filter when present. Without the filter the raw table row count is used, which
    /// over-sizes the right side for selective predicates and skews both build-side selection
    /// and hash-vs-INLJ threshold decisions.
    /// </summary>
    private static long EstimateRightRows(
        BoundJoinRightSource rightSource,
        DatabaseDescriptor database,
        StatisticsManager stats,
        NodeAst? rightScanFilter)
    {
        if (rightSource.Table is null)
            return CostEstimator.DefaultTableRowCount;

        long tableRows = stats.GetRowCountEstimate(database, rightSource.Table.Table)
                         ?? CostEstimator.DefaultTableRowCount;

        if (rightScanFilter is not null)
        {
            double sel = CardinalityEstimator.EstimateFilterSelectivity(
                rightScanFilter, 
                database, 
                rightSource.Table.Table, 
                stats
            );

            return Math.Max(1L, (long)(tableRows * sel));
        }

        return tableRows;
    }

    /// <summary>
    /// Returns a row-count estimate for <paramref name="node"/> without running a full
    /// <see cref="CostEstimator.AnnotatePlan"/> pass. For leaf scan nodes the per-table stats
    /// are available immediately; for interior nodes (joins, filters, sorts that sit on top of
    /// the left subtree) we fall back to the engine default to avoid a recursive annotation
    /// that would mutate the partially-built tree.
    /// </summary>
    // Internal for unit testing. With CostBasedAccessPathEnabled=true, BuildJoinTree emits
    // IndexRangeScanNode join leaves when a selective index is available; the IndexRangeScanNode
    // arm is therefore reachable through GetPlan in that mode.
    internal long EstimatePhysicalNodeRows(
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
                        tsn.ExecutionFilter, 
                        database, 
                        boundSource.Table, 
                        stats
                    );

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
                long rangeRows = CostEstimator.EstimateRangeScanRows(rsn, tableRows, _options, stats, database, rangeBound.Table);

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
                // Defensive: IndexRangeScanNode without BoundSource is a single-table leaf;
                // use the default rather than recursing.
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
    /// Gated on <see cref="CamusDBOptions.CostBasedAccessPathEnabled"/>. Falls back
    /// (returns null) when no usable index is found, the chosen step is a full scan,
    /// or the range-scan cost exceeds the full-scan breakeven threshold.
    /// </summary>
    private PhysicalPlanNode? TryBuildIndexLeaf(
        NodeAst scanFilter,
        BoundTableSource boundSource,
        DatabaseDescriptor database,
        StatisticsManager stats,
        Dictionary<string, ColumnValue>? clockValues)
    {
        // Clock values only: a clock call becomes a bound at the statement's time, which equals what
        // the residual filter reads; a placeholder stays unresolved, as it always has here.
        PredicateAnalysis raw = PredicateAnalyzer.Analyze(scanFilter, clockValues);

        // Strip "alias." prefix so bare index column names match (e.g. "o.status" → "status").
        PredicateAnalysis analysis = JoinEnumerator.StripAliasPrefix(raw, boundSource.Alias);

        // Same constant coercion the single-table planner applies: a literal typed differently
        // from the indexed column (1.0 on INT, '…' on Uuid) would build a key that addresses no
        // entry, and the join leaf would silently lose rows the table scan returns.
        analysis = PredicateAnalyzer.CoerceConstantsForColumns(analysis, boundSource.Table);

        if (analysis.IndexableComparisons.Count == 0 && analysis.InListComparisons.Count == 0)
            return null;

        // ── Range-scan path ───────────────────────────────────────────────────
        QueryPlanStep? step = IndexScanSelector.TrySelectScan(boundSource.Table, analysis);

        if (step is not null && step.Value.Type != QueryPlanStepType.FullScanFromTableIndex)
        {
            if (step.Value.Type == QueryPlanStepType.RangeScanFromIndex
                && (step.Value.FromBound is not null || step.Value.ToBound is not null))
            {
                long? tableRows = stats.GetRowCountEstimate(database, boundSource.Table);
                bool breakeven = tableRows is { } trc && trc > 0
                    && CostEstimator.ShouldPreferFullScan(
                        (IndexRangeScanNode)QueryPlanner.ToScanNode(step.Value),
                        trc, _options, stats, database, boundSource.Table);

                if (!breakeven)
                {
                    PhysicalPlanNode indexNode = QueryPlanner.ToScanNode(step.Value);
                    NodeAst? residual = PredicateAnalyzer.BuildExecutionFilter(
                        analysis, step, boundSource.Table, null);

                    if (indexNode is IndexRangeScanNode rsn)
                    {
                        indexNode = new IndexRangeScanNode(
                            rsn.Index, rsn.FromBound, rsn.FromInclusive, rsn.ToBound, rsn.ToInclusive)
                        {
                            BoundSource = boundSource,
                            ExecutionFilter = residual,
                            Distribution = PlacementReader.Instance.GetIndexScanDistribution(rsn.Index, _options)
                        };
                        return indexNode;
                    }
                }
            }
            // Unique point-lookup (QueryFromIndex): unusual join-leaf shape; skip.
        }

        // ── IN-list path ──────────────────────────────────────────────────────
        // Tried when the range-scan path failed, produced no usable step, or was vetoed.
        if (analysis.InListComparisons.Count > 0)
        {
            long trc = stats.GetRowCountEstimate(database, boundSource.Table)
                       ?? CostEstimator.DefaultTableRowCount;

            foreach (AnalyzedInList inList in analysis.InListComparisons)
            {
                if (inList.Values.Count == 0) continue;

                TableIndexSchema? bestIndex = null;
                bool bestIsUnique = false;
                foreach (TableIndexSchema idx in boundSource.Table.Indexes.Values)
                {
                    if (!SchemaElementStateRules.IsReadableIndex(boundSource.Table.Schema, idx)) 
                        continue;

                    if (idx.Columns.Length == 0 || !string.Equals(idx.Columns[0], inList.ColumnName, StringComparison.Ordinal))
                        continue;
                        
                    bool isUnique = idx.Type == IndexType.Unique;
                    if (isUnique && idx.Columns.Length != 1) 
                        continue;

                    if (bestIndex is null || (isUnique && !bestIsUnique))
                    {
                        bestIndex = idx;
                        bestIsUnique = isUnique;
                    }
                }

                if (bestIndex is null) 
                    continue;

                long inListRows = CardinalityEstimator.EstimateInListRows(
                    inList.ColumnName, inList.Values, bestIsUnique,
                    trc, database, boundSource.Table, stats);

                long breakeven = (long)Math.Ceiling(trc * CostEstimator.BreakevenFraction);
                if (inListRows >= breakeven) 
                    continue;

                NodeAst? residual = PredicateAnalyzer.BuildExecutionFilter(
                    analysis, scanStep: null, boundSource.Table, inList.Conjunct);

                var inListNode = new IndexInListScanNode(bestIndex, inList.ColumnName, inList.Values)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = residual,
                    Distribution = PlacementReader.GetLookupDistribution()
                };
                return inListNode;
            }
        }

        return null;
    }

    /// <summary>
    /// Picks a representative real table for the plan's metadata, or null when no leaf at any depth
    /// reads a table (every leaf is a function in <c>FROM</c>).
    /// </summary>
    /// <remarks>
    /// The descent is recursive because derived tables nest: a query whose only source is a derived
    /// table over another derived table has no real table at either of the top two levels, and the
    /// real one may sit arbitrarily deep. That shape is ordinary SQL
    /// (<c>FROM (SELECT … FROM (SELECT … FROM t) a) b</c>) and it is also exactly what a view over a
    /// view expands to, so stopping at one level rejected valid queries. Recursing changes nothing
    /// for the shallow cases — the first real table found is the same one — it only stops giving up
    /// early on the deep ones.
    /// </remarks>
    private static TableDescriptor? TryResolvePlanTable(BoundSelectQuery bound)
    {
        if (bound.Sources.Count > 0)
            return bound.Sources[0].Table;

        foreach (BoundDerivedTableSource derived in bound.DerivedSources)
        {
            TableDescriptor? nested = TryResolvePlanTable(derived.InnerBound);
            if (nested is not null)
                return nested;
        }

        return null;
    }
}
