
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical plan tree for single-table SELECT queries.
/// </summary>
public sealed class QueryPlanner
{
    private readonly StatisticsManager? _stats;

    /// <summary>
    /// Configuration of the engine this planner belongs to. Held here rather than read from the
    /// descriptor passed to each call, so one planner cannot silently plan under two different
    /// configurations.
    /// </summary>
    private readonly CamusDBOptions _options;
    private readonly PlanCache? _cache;

    public QueryPlanner(CamusDBOptions options, StatisticsManager? stats = null) : this(stats, null, options) { }

    internal QueryPlanner(StatisticsManager? stats, PlanCache? cache, CamusDBOptions options)
    {
        _stats = stats;
        _cache = cache;
        _options = options;
    }

    // Tag a scan leaf with its DataDistribution and return it (fluent helper).
    private static PhysicalPlanNode WithDistribution(PhysicalPlanNode node, TableDescriptor table)
    {
        node.Distribution = node switch
        {
            IndexLookupNode                                                   => PlacementReader.GetLookupDistribution(),
            IndexInListScanNode                                               => PlacementReader.GetLookupDistribution(),
            IndexRangeScanNode rangeScan                                      => PlacementReader.Instance.GetIndexScanDistribution(rangeScan.Index),
            TableScanNode { Source: TableScanSource.ForcedIndex, Index: { } forcedIdx } => PlacementReader.Instance.GetIndexScanDistribution(forcedIdx),
            TableScanNode                                                     => PlacementReader.Instance.GetPrimaryRowScanDistribution(table),
            _                                                                 => DataDistribution.Gathered,
        };
        return node;
    }

    public QueryPlan GetPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = new(database, table, ticket);

        PredicateAnalysis analysis = ticket.AnalyzedWhere is not null
            ? PredicateAnalyzer.Merge(ticket.AnalyzedWhere, PredicateAnalyzer.AnalyzeFilters(ticket.Filters))
            : PredicateAnalyzer.AnalyzeTicket(ticket);
        // Turn a boolean column used directly as a predicate (WHERE enabled / WHERE NOT enabled)
        // into an explicit equality. It carries no equality bound otherwise, which makes any index
        // it leads unusable and silently forces a full table scan. Must precede the qualified-name
        // pass below so a promoted alias-qualified reference still gets reduced to its bare name.
        analysis = PredicateAnalyzer.NormalizeBooleanColumnPredicates(analysis, table);
        // Resolve alias-qualified column references (u.usersId → usersId) before access-path
        // selection: index columns are stored unqualified, so without this every aliased
        // single-table predicate fails to match any index and degrades to a full table scan.
        analysis = PredicateAnalyzer.ResolveQualifiedColumnNames(analysis, ticket.RowNameResolver);
        // Reconcile bare string literals against Uuid columns before access-path selection so the
        // index selector and the bound-absorption check both see a Uuid constant, not a String.
        analysis = PredicateAnalyzer.CoerceConstantsForColumns(analysis, table);
        plan.PredicateAnalysis = analysis;

        // Compute the shape ID and schema deps up-front so the cache can be checked
        // before the (potentially expensive) cost-based access-path selection.
        string? shapeId = ticket.SelectQuery is not null
            ? QueryShapeComputer.Compute(ticket.SelectQuery)
            : null;

        List<(string, int)> schemaDeps = [(table.Id, table.Schema.Version)];
        List<PlanCacheDep> cacheDeps = [MakeCacheDep(database, table)];

        if (ticket.SemiJoinSpecs is { Count: > 0 })
        {
            foreach (SemiJoinSpec spec in ticket.SemiJoinSpecs)
            {
                schemaDeps.Add((spec.InnerTable.Id, spec.InnerTable.Schema.Version));
                cacheDeps.Add(MakeCacheDep(database, spec.InnerTable));
            }
        }

        plan.QueryShapeId = shapeId;
        plan.SchemaDeps = schemaDeps;

        // Cache lookup: skip access-path selection on a valid hit.
        bool fromCache = false;
        PhysicalPlanNode scanNode;
        QueryPlanStep? scanStep;
        NodeAst? absorbedInListConjunct;

        if (shapeId is not null
            && _cache is not null
            && _options.PlanCacheEnabled
            && _cache.TryGet(database.Id, shapeId, cacheDeps, out PlanCacheEntry? cached)
            && cached!.SingleTable is { } cachedDecision)
        {
            (scanNode, scanStep, absorbedInListConjunct) =
                BuildScanNodeFromCachedDecision(database, table, ticket, analysis, cachedDecision, out bool replayFailed);

            // A failed replay (cached index no longer usable) fell back to a full replan; keep
            // fromCache=false so the fresh decision REPLACES the stale entry below. Leaving it
            // true would skip the Put while every lookup keeps hitting (and LRU-promoting) the
            // dead entry, paying cache-lookup + full replan on every same-shape query forever.
            fromCache = !replayFailed;
        }
        else
        {
            (scanNode, scanStep, absorbedInListConjunct) = BuildScanNode(database, table, ticket, analysis);
        }
        plan.ExecutionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep, table, absorbedInListConjunct);

        // Populate OutputOrdering on the scan node when the chosen index scan guarantees the
        // requested ORDER BY ordering. The planner then uses this property to
        // decide whether to add a SortNode, keeping sort elision explicit and consistent.
        // Guard: GROUP BY and DISTINCT both destroy scan-level ordering before plan output,
        // so the scan's OutputOrdering is only meaningful in the plain (non-grouped,
        // non-distinct) path. Setting it for grouped/distinct queries would be misleading in
        // verbose EXPLAIN even though LIMIT-pushdown is already guarded independently.
        if (scanStep is not null
            && ticket.OrderBy is { Count: > 0 }
            && ticket.GroupBy is not { Count: > 0 }
            && !ticket.IsDistinct
            && IndexScanSelector.ScanSatisfiesOrderBy(table, scanStep.Value, ticket.OrderBy))
            scanNode.OutputOrdering = ticket.OrderBy;

        bool scanSatisfiesOrderBy = scanNode.OutputOrdering is not null;
        plan.ScanRowLimit = TryComputeScanRowLimit(ticket, plan.ExecutionFilter, scanSatisfiesOrderBy);

        // Streaming distinct detection.
        // When SELECT DISTINCT projects only simple column identifiers that are covered by
        // an index prefix, use a streaming (adjacent-key) dedup instead of a hash set,
        // reducing memory from O(distinct-count) to O(1).
        // Override the scan with a full ordered index scan when no predicate-driven scan was
        // chosen (scanStep == null). Only override when scanStep is null so predicate-driven
        // scans are not disrupted; correctness of ExecutionFilter is unaffected since a
        // FullScanFromIndex step consumes no predicates, same as a null/table-scan step.
        bool isStreamingDistinct = false;
        IReadOnlyList<QueryOrderBy>? streamingDistinctOrdering = null;
        if (ticket.IsDistinct && ticket.GroupBy is not { Count: > 0 })
        {
            List<string>? distinctCols = TryExtractDistinctColumns(ticket.Projection);
            if (distinctCols is { Count: > 0 } && AllDistinctColumnsAreNotNull(table, distinctCols))
            {
                if (scanStep is not null && IndexScanSelector.ScanStepCoversDistinctColumns(scanStep.Value, distinctCols))
                {
                    isStreamingDistinct = true;
                    streamingDistinctOrdering = BuildDistinctOrdering(scanStep.Value.Index!, distinctCols);
                }
                else if (scanStep is null || scanStep.Value.Type == QueryPlanStepType.FullScanFromTableIndex)
                {
                    TableIndexSchema? distinctIndex = IndexScanSelector.TryFindStreamingDistinctIndex(table, distinctCols);
                    if (distinctIndex is not null)
                    {
                        scanNode = WithDistribution(new TableScanNode(TableScanSource.ForcedIndex, distinctIndex), table);
                        isStreamingDistinct = true;
                        streamingDistinctOrdering = BuildDistinctOrdering(distinctIndex, distinctCols);
                    }
                }
            }
        }

        // Streaming GROUP BY detection.
        // When all GROUP BY expressions are simple column identifiers, every column is NOT NULL,
        // and the scan delivers rows ordered so equal group keys are adjacent, the O(1)-memory
        // streaming path can replace the hash dictionary.
        //
        // Two cases, mirroring the streaming DISTINCT detection above:
        //   1. The predicate-driven scan already uses an index that covers all GROUP BY columns.
        //   2. No predicate-driven scan was chosen (full table-index scan), but an index covers all
        //      GROUP BY columns — force a full ordered index scan the same way streaming DISTINCT does.
        //
        // Only applies to the plan-based path; the join path (QueryPostScanPipeline) always
        // uses the hash path. Nullable columns are excluded because index scans omit NULL rows,
        // which would silently drop the NULL group.
        // Must run before `root = scanNode` so that scan-node overrides propagate into the plan tree.
        bool hasGroupBy = ticket.GroupBy is { Count: > 0 };
        bool isStreamingGroupBy = false;
        if (hasGroupBy && !ticket.IsDistinct)
        {
            List<string>? groupByCols = TryExtractGroupByColumns(ticket.GroupBy!);
            if (groupByCols is { Count: > 0 } && AllDistinctColumnsAreNotNull(table, groupByCols))
            {
                if (scanStep is not null
                    && IndexScanSelector.ScanStepCoversDistinctColumns(scanStep.Value, groupByCols))
                {
                    // Case 1: predicate-driven scan already covers the GROUP BY columns.
                    isStreamingGroupBy = true;
                }
                else if (scanStep is null
                    || scanStep.Value.Type == QueryPlanStepType.FullScanFromTableIndex)
                {
                    // Case 2: no predicate-driven scan; force a full ordered index scan if an
                    // index covers the GROUP BY columns, exactly as streaming DISTINCT does.
                    TableIndexSchema? groupByIndex =
                        IndexScanSelector.TryFindStreamingDistinctIndex(table, groupByCols);
                    if (groupByIndex is not null)
                    {
                        scanNode = WithDistribution(
                            new TableScanNode(TableScanSource.ForcedIndex, groupByIndex), table);
                        isStreamingGroupBy = true;
                    }
                }
            }
        }

        PhysicalPlanNode root = scanNode;

        if (plan.ExecutionFilter is not null)
            root = new FilterNode(plan.ExecutionFilter, root);

        // Wrap the scan with SemiJoinNode(s) for each IN / NOT IN rewrite spec.
        if (ticket.SemiJoinSpecs is { Count: > 0 })
        {
            foreach (SemiJoinSpec spec in ticket.SemiJoinSpecs)
            {
                root = new SemiJoinNode(root, spec.Mode, spec.InnerTable,
                    spec.OuterColumn, spec.InnerColumn, spec.InnerIndex, spec.InnerFilter);
            }
        }

        if (hasGroupBy)
        {
            root = new AggregateNode(root)
            {
                GroupByExpressions = ticket.GroupBy,
                AggregateProjections = ExtractAggregateProjections(ticket),
                IsStreamingGroupBy = isStreamingGroupBy,
            };

            if (ticket.Having is not null)
                root = new HavingFilterNode(ticket.Having, root);

            if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };

            if (ticket.Projection is not null && ticket.Projection.Count > 0 && !QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                root = new ProjectNode(root);

            if (ticket.Limit is not null || ticket.Offset is not null)
                root = new LimitNode(root)
                {
                    LimitValue = EvalLong(ticket.Limit, ticket),
                    OffsetValue = EvalLong(ticket.Offset, ticket),
                };
        }
        else
        {
            if (ticket.IsDistinct)
            {
                if (ticket.Projection is not null && ticket.Projection.Count > 0)
                {
                    if (QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
                    {
                        root = new AggregateNode(root)
                        {
                            GroupByExpressions = ticket.GroupBy,
                            AggregateProjections = ExtractAggregateProjections(ticket),
                        };

                        if (ticket.Having is not null)
                            root = new HavingFilterNode(ticket.Having, root);
                    }

                    if (!QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                        root = new ProjectNode(root);
                }

                // Streaming distinct — O(1) memory dedup when the scan guarantees ordered output.
                DistinctNode distinctNode = new(root) { IsStreaming = isStreamingDistinct };
                if (isStreamingDistinct && streamingDistinctOrdering is not null)
                    distinctNode.OutputOrdering = streamingDistinctOrdering;
                root = distinctNode;

                if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
                {
                    // Elide SortNode when the streaming-distinct ordering already satisfies ORDER BY.
                    bool orderElided = isStreamingDistinct
                        && streamingDistinctOrdering is not null
                        && StreamingOrderSatisfiesOrderBy(streamingDistinctOrdering, ticket.OrderBy);

                    if (!orderElided)
                        root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };
                }

                if (ticket.Limit is not null || ticket.Offset is not null)
                    root = new LimitNode(root)
                    {
                        LimitValue = EvalLong(ticket.Limit, ticket),
                        OffsetValue = EvalLong(ticket.Offset, ticket),
                    };
            }
            else
            {
                if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0 && !scanSatisfiesOrderBy)
                    root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };

                if (ticket.Projection is not null && ticket.Projection.Count > 0)
                {
                    if (QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
                    {
                        root = new AggregateNode(root)
                        {
                            GroupByExpressions = ticket.GroupBy,
                            AggregateProjections = ExtractAggregateProjections(ticket),
                        };

                        if (ticket.Having is not null)
                            root = new HavingFilterNode(ticket.Having, root);
                    }

                    if (!QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                        root = new ProjectNode(root);
                }

                // Limit must sit above the aggregate: LIMIT/OFFSET restricts the query's
                // output rows, not the aggregate's input (SELECT COUNT(*) … LIMIT 1 counts
                // every row and then limits the single result row).
                if (ticket.Limit is not null || ticket.Offset is not null)
                    root = new LimitNode(root)
                    {
                        LimitValue = EvalLong(ticket.Limit, ticket),
                        OffsetValue = EvalLong(ticket.Offset, ticket),
                    };
            }
        }

        plan.Root = root;
        QueryPlanStepAdapter.PopulateLinearSteps(plan);
        ProjectionPushdownPlanner.Apply(plan);
        TryMarkIndexOnly(plan, table);

        // Annotate every node with EstimatedCardinality and PlanCost.
        CostEstimator.AnnotatePlan(plan.Root, database, table, _stats);

        // On a cache miss, store the access-path decision so the next identical-shape query
        // can skip the expensive cost enumeration.  In-list scans are excluded: they have no
        // QueryPlanStep (the step slot is null) and their choice is IN-list-value-count-dependent.
        if (!fromCache && _cache is not null && _options.PlanCacheEnabled
            && shapeId is not null
            && plan.Steps is [{ } s0, ..]
            && s0.Type != QueryPlanStepType.InListScanFromIndex)
        {
            _cache.Put(database.Id, shapeId,
                new PlanCacheEntry(cacheDeps,
                    SingleTable: new SingleTableDecision(s0.Index?.Name),
                    JoinAliasOrder: null));
        }

        return plan;
    }

    private static long? TryComputeScanRowLimit(
        QueryTicket ticket,
        NodeAst? executionFilter,
        bool scanSatisfiesOrderBy)
    {
        if (ticket.Limit is null)
            return null;

        if (ticket.IsDistinct)
            return null;

        if (ticket.GroupBy is { Count: > 0 } || ticket.Having is not null)
            return null;

        if (ticket.Projection is { Count: > 0 } && QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
            return null;

        if (executionFilter is not null)
            return null;

        // IN / NOT IN rewrites filter rows in SemiJoinNode *above* the scan, and the rewrite
        // removed the predicate from WHERE — so ExecutionFilter is null even though rows can
        // still be rejected. Pushing the limit into the scan would truncate before the probe
        // and return fewer rows than actually match.
        if (ticket.SemiJoinSpecs is { Count: > 0 })
            return null;

        if (ticket.OrderBy is { Count: > 0 } && !scanSatisfiesOrderBy)
            return null;

        ColumnValue limit = SqlExecutor.EvalExpr(ticket.Limit, new Dictionary<string, ColumnValue>(), ticket.Parameters);
        if (limit.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Limit is not Integer64");

        long scanLimit = limit.LongValue;

        if (ticket.Offset is not null)
        {
            ColumnValue offset = SqlExecutor.EvalExpr(ticket.Offset, new Dictionary<string, ColumnValue>(), ticket.Parameters);
            if (offset.Type != ColumnType.Integer64)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Offset is not Integer64");

            try
            {
                scanLimit = checked(scanLimit + offset.LongValue);
            }
            catch (OverflowException)
            {
                scanLimit = long.MaxValue;
            }
        }

        if (scanLimit <= 0)
            return 0;

        return scanLimit;
    }

    /// <summary>
    /// Enumerates all viable predicate-driven index steps, costs each against the full-scan
    /// baseline, and returns the cheapest. When all index candidates are more expensive than a
    /// full table scan, returns the full table scan node (step = null).
    ///
    /// Called only when per-table stats are available. The full-scan cost is:
    ///   tableRowCount × 1 unit (sequential KV scan, no row fetch overhead).
    /// An index scan costs:
    ///   estimatedRows × 2 units (index entry + primary-store row fetch, non-covering).
    /// Unique-index lookups always cost 2 units regardless of table size.
    /// NetworkFactor is included when key-range sharding is on.
    /// </summary>
    private (PhysicalPlanNode Node, QueryPlanStep? Step) PickCheapestIndexOrFullScan(
        DatabaseDescriptor database,
        TableDescriptor table,
        PredicateAnalysis analysis,
        long tableRowCount)
    {
        PhysicalPlanNode fullScanNode = WithDistribution(new TableScanNode(TableScanSource.PrimaryRows), table);
        double bestCost = CostEstimator.EstimateScanLeafCost(fullScanNode, tableRowCount, _stats, database, table);
        PhysicalPlanNode bestNode = fullScanNode;
        QueryPlanStep? bestStep = null;

        foreach (QueryPlanStep step in IndexScanSelector.EnumerateViableSteps(table, analysis))
        {
            PhysicalPlanNode candidateNode = WithDistribution(ToScanNode(step), table);
            double cost = CostEstimator.EstimateScanLeafCost(candidateNode, tableRowCount, _stats, database, table);

            // Deterministic tie-break by index name: candidates arrive in Dictionary iteration
            // order, which can differ across descriptor rebuilds (and therefore across nodes) —
            // a strict `<` would then pick different equal-cost plans on different nodes.
            bool wins = cost < bestCost
                || (cost == bestCost
                    && bestStep is { Index: { } bestIdx }
                    && step.Index is { } candIdx
                    && string.CompareOrdinal(candIdx.Name, bestIdx.Name) < 0);

            if (wins)
            {
                bestCost = cost;
                bestNode = candidateNode;
                bestStep = step;
            }
        }

        return (bestNode, bestStep);
    }

    /// <summary>
    /// Cache-hit path: re-binds the current query's predicate literals into the pre-chosen
    /// access path recorded in <paramref name="decision"/>, skipping cost enumeration entirely.
    ///
    /// Chosen-index replay policy: only the index name is honored from the cache entry;
    /// the scan type (range vs full) is re-derived from the current predicates by
    /// <see cref="IndexScanSelector.TrySelectScanForForcedIndex"/>.  A query that cached a
    /// full-index scan on its first run will therefore replay as a predicate-bounded range scan
    /// on any subsequent run that has matching predicates — always correct, often more selective.
    ///
    /// Falls back to <see cref="BuildScanNode"/> if the cached index name is no longer usable
    /// (defensive; should not occur when <see cref="PlanCacheEntry.SchemaDeps"/> are valid).
    /// </summary>
    private (PhysicalPlanNode ScanNode, QueryPlanStep? ScanStep, NodeAst? AbsorbedInListConjunct)
    BuildScanNodeFromCachedDecision(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        PredicateAnalysis analysis,
        SingleTableDecision decision,
        out bool replayFailed)
    {
        replayFailed = false;

        if (decision.IndexName is null)
        {
            return (
                WithDistribution(new TableScanNode(TableScanSource.PrimaryRows), table),
                new QueryPlanStep(QueryPlanStepType.FullScanFromTableIndex),
                null);
        }

        QueryPlanStep? step = IndexScanSelector.TrySelectScanForForcedIndex(table, analysis, decision.IndexName);
        if (step is null)
        {
            // Cached index no longer usable: full replan. The caller must treat this as a cache
            // miss so the fresh decision replaces the stale entry.
            replayFailed = true;
            return BuildScanNode(database, table, ticket, analysis);
        }

        return (WithDistribution(ToScanNode(step.Value), table), step, null);
    }

    /// <summary>
    /// Builds one table's plan-cache dependency fingerprint: schema version plus the index-set
    /// and statistics generations, so index DDL (which doesn't bump the schema version) and
    /// ANALYZE both invalidate cached decisions. See <see cref="PlanCacheDep"/>.
    /// </summary>
    private PlanCacheDep MakeCacheDep(DatabaseDescriptor database, TableDescriptor table) =>
        new(table.Id,
            table.Schema.Version,
            table.IndexSetGeneration,
            _stats?.GetAnalyzeGeneration(database, table) ?? 0);

    private (PhysicalPlanNode ScanNode, QueryPlanStep? ScanStep, NodeAst? AbsorbedInListConjunct) BuildScanNode(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        PredicateAnalysis analysis)
    {
        // Cost-based access-path selection: when the feature flag is on AND stats are available
        // and the predicate drives an index, enumerate all viable steps, cost each against the
        // full-scan baseline, and pick the cheapest rather than the heuristic-best.
        // Falls back to the rule-based path when the flag is off, stats are absent, or no
        // row-count estimate is available — plans are byte-identical to the rule-based path.
        // Skipped for UPDATE/DELETE paths (ExclusivePredicateLocks) to avoid widening exclusive
        // row-range locks from a narrow range to the full table.
        // ORDER BY needs no special handling here: sort-elision is applied downstream from the
        // chosen scan node's own ordering, so a cost-picked order-satisfying index still elides
        // the sort, and a non-satisfying pick correctly retains it. (Folding sort cost into the
        // candidate comparison — "interesting orders" — is a future enhancement, not a correctness need.)
        if (_options.CostBasedAccessPathEnabled
            && _stats is not null
            && analysis.IndexableComparisons.Count > 0
            && !ticket.ExclusivePredicateLocks)
        {
            long? trc = _stats.GetRowCountEstimate(database, table);
            if (trc is { } tableRowCount && tableRowCount > 0)
            {
                (PhysicalPlanNode bestNode, QueryPlanStep? bestStep) =
                    PickCheapestIndexOrFullScan(database, table, analysis, tableRowCount);

                // If the chosen path is a predicate-driven range scan, still check whether an
                // IN-list scan would be even cheaper (same competition as the rule-based path).
                if (bestStep is { Type: QueryPlanStepType.RangeScanFromIndex } chosenRange
                    && (chosenRange.FromBound is not null || chosenRange.ToBound is not null)
                    && analysis.InListComparisons is { Count: > 0 })
                {
                    bool rangeSatisfiesOrder = ticket.OrderBy is { Count: > 0 }
                        && IndexScanSelector.ScanSatisfiesOrderBy(table, chosenRange, ticket.OrderBy);
                    if (!rangeSatisfiesOrder)
                    {
                        IndexInListScanNode? competitor = TryBuildCompetingInListScanNode(
                            database, table, analysis.InListComparisons, chosenRange);
                        if (competitor is not null)
                        {
                            NodeAst? absorbed = analysis.InListComparisons
                                .FirstOrDefault(il => string.Equals(il.ColumnName, competitor.ColumnName, StringComparison.OrdinalIgnoreCase))
                                ?.Conjunct;
                            return (WithDistribution(competitor, table), null, absorbed);
                        }
                    }
                }

                // If cost-based chose the full scan, check whether an IN-list is even cheaper.
                if (bestStep is null && analysis.InListComparisons is { Count: > 0 })
                {
                    IndexInListScanNode? inListNode = TryBuildInListScanNode(database, table, analysis.InListComparisons);
                    if (inListNode is not null)
                    {
                        NodeAst? absorbed = analysis.InListComparisons
                            .FirstOrDefault(il => string.Equals(il.ColumnName, inListNode.ColumnName, StringComparison.OrdinalIgnoreCase))
                            ?.Conjunct;
                        return (WithDistribution(inListNode, table), null, absorbed);
                    }
                }

                return (bestNode, bestStep, null);
            }
        }

        QueryPlanStep? scanStep = IndexScanSelector.TrySelectScan(table, analysis, ticket.OrderBy);

        // Cost-model override: if a predicate-driven range scan is selected but the cost model
        // estimates it would touch more rows than the breakeven fraction of the table, prefer a full
        // primary table scan instead. Only apply when:
        //   1. Stats are available (row count known).
        //   2. The step is a predicate-driven RangeScanFromIndex (has at least one bound).
        //      Skip ORDER BY-driven unbounded scans to preserve sort elision.
        //   3. The scan is not already a unique point lookup (those always win).
        //   4. The scan does not hold exclusive predicate locks (UPDATE/DELETE path) — widening to a
        //      full-table exclusive row range lock would block all concurrent writes on disjoint ranges,
        //      defeating key-range concurrency. Shared-lock SELECTs are safe to promote to table scan.
        //   5. The scan is not covering. A covered scan (index key + INCLUDE columns supply every needed
        //      column) fetches zero primary rows, so it is at least as cheap as a full table scan for
        //      any table size; downgrading it would defeat the covering-index optimization — most
        //      visibly on tiny tables, where the breakeven fraction rounds down to the matched rows.
        if (scanStep is { Type: QueryPlanStepType.RangeScanFromIndex } step
            && (step.FromBound is not null || step.ToBound is not null)
            && _stats is not null
            && !ticket.ExclusivePredicateLocks
            && !ScanCoversProjection(step, ticket))
        {
            long? tableRowCount = _stats.GetRowCountEstimate(database, table);
            if (tableRowCount is { } trc && trc > 0)
            {
                var tempRangeNode = (IndexRangeScanNode)ToScanNode(scanStep.Value);
                if (CostEstimator.ShouldPreferFullScan(tempRangeNode, trc, _stats, database, table))
                    scanStep = null; // fall through to full table scan
            }
        }

        // If a predicate-driven range scan was selected, check whether a unique
        // single-column IN-list on another column would be cheaper. Unique point-lookups have a
        // deterministic cost of 2·N (N index reads + N row fetches), making them a safe bet against
        // range scans of unknown cardinality. Guard: never switch when the range scan was chosen
        // specifically to satisfy ORDER BY (that would lose sort-elision).
        if (scanStep is { Type: QueryPlanStepType.RangeScanFromIndex } rangeStep
            && (rangeStep.FromBound is not null || rangeStep.ToBound is not null)
            && analysis.InListComparisons is { Count: > 0 })
        {
            bool rangeSatisfiesOrder = ticket.OrderBy is { Count: > 0 }
                && IndexScanSelector.ScanSatisfiesOrderBy(table, rangeStep, ticket.OrderBy);
            if (!rangeSatisfiesOrder)
            {
                IndexInListScanNode? competitor = TryBuildCompetingInListScanNode(
                    database, table, analysis.InListComparisons, rangeStep);
                if (competitor is not null)
                {
                    NodeAst? absorbed = analysis.InListComparisons
                        .FirstOrDefault(il => string.Equals(il.ColumnName, competitor.ColumnName, StringComparison.OrdinalIgnoreCase))
                        ?.Conjunct;
                    return (WithDistribution(competitor, table), null, absorbed);
                }
            }
        }

        if (scanStep is not null)
            return (WithDistribution(ToScanNode(scanStep.Value), table), scanStep, null);

        // When no regular index scan was chosen, try an index-driven IN-list scan.
        // Only attempted when there are IN-list comparisons in the predicate analysis.
        if (analysis.InListComparisons is { Count: > 0 })
        {
            IndexInListScanNode? inListNode = TryBuildInListScanNode(database, table, analysis.InListComparisons);
            if (inListNode is not null)
            {
                NodeAst? absorbed = analysis.InListComparisons
                    .FirstOrDefault(il => string.Equals(il.ColumnName, inListNode.ColumnName, StringComparison.OrdinalIgnoreCase))
                    ?.Conjunct;
                return (WithDistribution(inListNode, table), null, absorbed);
            }
        }

        if (!string.IsNullOrEmpty(ticket.IndexName))
        {
            if (!table.Indexes.TryGetValue(ticket.IndexName, out TableIndexSchema? index))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownKey,
                    $"Key '{ticket.IndexName}' doesn't exist in table '{table.Name}'");
            }

            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownKey,
                    $"Key '{ticket.IndexName}' doesn't exist in table '{table.Name}'");
            }

            // A unique index omits entries for rows with a NULL in any indexed column, so an
            // unbounded full scan over it would silently drop those rows from the result. The
            // automatic ORDER BY path already refuses this (TrySelectOrderByScan); the user-forced
            // path must not bypass that correctness guard — fall back to the primary-row scan.
            if (index.Type == IndexType.Unique && !IndexScanSelector.AllColumnsNotNull(table, index))
            {
                QueryPlanStep nullSafeScanStep = new(QueryPlanStepType.FullScanFromTableIndex);
                return (WithDistribution(new TableScanNode(TableScanSource.PrimaryRows), table), nullSafeScanStep, null);
            }

            QueryPlanStep forcedIndexStep = new(QueryPlanStepType.FullScanFromIndex, index);
            return (WithDistribution(new TableScanNode(TableScanSource.ForcedIndex, index), table), forcedIndexStep, null);
        }

        QueryPlanStep tableScanStep = new(QueryPlanStepType.FullScanFromTableIndex);
        return (WithDistribution(new TableScanNode(TableScanSource.PrimaryRows), table), tableScanStep, null);
    }

    /// <summary>
    /// Attempts to build an <see cref="IndexInListScanNode"/> for the first IN-list comparison
    /// whose column has a usable index and whose list size passes the cost gate.
    ///
    /// Cost gate:
    ///   • With stats: prefer seeks when <c>2·N &lt; tableRowCount</c>
    ///     (each seek = 1 index read + 1 row fetch).
    ///   • Without stats: allow up to <c>MaxInListSizeWithoutStats</c> values (1000).
    ///
    /// Unique indexes support all column types via <c>LookupUnique</c>.
    /// Non-unique indexes use an equality-as-range scan: types with a computable successor
    /// (Integer64, Float64, Bool) use <c>[v, successor(v))</c>; String and Id use an
    /// exact-match scan <c>[v, v]</c> inclusive — <see cref="KvTableStore.ScanIndex"/> appends
    /// a high sentinel for non-unique, so all <c>encode(v)+rowIdHex</c> entries are captured
    /// and the decoded-key bounds filter keeps only rows where key == v.
    /// </summary>
    private IndexInListScanNode? TryBuildInListScanNode(
        DatabaseDescriptor database,
        TableDescriptor table,
        IReadOnlyList<AnalyzedInList> inListComparisons)
    {
        const int MaxInListSizeWithoutStats = 1000;

        foreach (AnalyzedInList inList in inListComparisons)
        {
            if (inList.Values.Count == 0)
                continue;

            TableIndexSchema? bestIndex = null;
            bool bestIsUnique = false;

            foreach (TableIndexSchema index in table.Indexes.Values)
            {
                if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                    continue;

                if (index.Columns.Length == 0
                    || !string.Equals(index.Columns[0], inList.ColumnName, StringComparison.OrdinalIgnoreCase))
                    continue;

                bool isUnique = index.Type == IndexType.Unique;

                // Unique index: requires all key columns for LookupUnique — only single-column unique
                // indexes are eligible (a composite unique index needs a full composite key to resolve
                // to a single row, and a partial key would silently find nothing).
                if (isUnique && index.Columns.Length != 1)
                    continue;

                // Prefer unique single-column over non-unique; among equals pick first.
                if (bestIndex is null || (isUnique && !bestIsUnique))
                {
                    bestIndex = index;
                    bestIsUnique = isUnique;
                }
            }

            if (bestIndex is null)
                continue;

            int n = inList.Values.Count;

            // Cost gate: for non-unique indexes use estimated fan-out (NDV/histogram), not n.
            if (_stats is not null)
            {
                long? tableRowCount = _stats.GetRowCountEstimate(database, table);
                if (tableRowCount is { } trc && trc > 0)
                {
                    long estimatedRows = CardinalityEstimator.EstimateInListRows(
                        inList.ColumnName, inList.Values, bestIsUnique, trc, database, table, _stats);
                    if (2L * estimatedRows >= trc)
                        continue; // full scan is cheaper
                }
            }
            else if (n > MaxInListSizeWithoutStats)
            {
                continue;
            }

            return new IndexInListScanNode(bestIndex, inList.ColumnName, inList.Values);
        }

        return null;
    }

    /// <summary>
    /// Attempts to find an IN-list scan that is cheaper than an already-selected range scan.
    ///
    /// With stats: both unique and non-unique single-column indexes compete, priced via
    /// <see cref="CardinalityEstimator.EstimateInListRows"/> (NDV/histogram fan-out for
    /// non-unique; min(n, trc) for unique).
    ///
    /// Without stats: unique single-column only (cost exactly 2·N; bounded and predictable),
    /// compete only when N ≤ <c>MaxCompetingInListSizeWithoutStats</c>.
    /// </summary>
    private IndexInListScanNode? TryBuildCompetingInListScanNode(
        DatabaseDescriptor database,
        TableDescriptor table,
        IReadOnlyList<AnalyzedInList> inListComparisons,
        QueryPlanStep rangeScanStep)
    {
        const int MaxCompetingInListSizeWithoutStats = 100;

        foreach (AnalyzedInList inList in inListComparisons)
        {
            if (inList.Values.Count == 0)
                continue;

            int n = inList.Values.Count;

            if (_stats is not null)
            {
                // With stats: unique and non-unique may both compete; price via EstimateInListRows.
                TableIndexSchema? bestIndex = null;
                bool bestIsUnique = false;
                foreach (TableIndexSchema index in table.Indexes.Values)
                {
                    if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                        continue;
                    if (index.Columns.Length == 0 ||
                        !string.Equals(index.Columns[0], inList.ColumnName, StringComparison.OrdinalIgnoreCase))
                        continue;
                    bool isUnique = index.Type == IndexType.Unique;
                    if (isUnique && index.Columns.Length != 1)
                        continue;
                    if (bestIndex is null || (isUnique && !bestIsUnique))
                    {
                        bestIndex = index;
                        bestIsUnique = isUnique;
                    }
                }

                if (bestIndex is null)
                    continue;

                long? tableRowCount = _stats.GetRowCountEstimate(database, table);
                if (tableRowCount is { } trc && trc > 0)
                {
                    var tempRangeNode = (IndexRangeScanNode)ToScanNode(rangeScanStep);
                    long rangeRows = CostEstimator.EstimateRangeScanRows(tempRangeNode, trc, _stats, database, table);
                    long inListRows = CardinalityEstimator.EstimateInListRows(
                        inList.ColumnName, inList.Values, bestIsUnique, trc, database, table, _stats);
                    if (2L * inListRows >= 2L * rangeRows)
                        continue; // range scan is cheaper or equal
                }
                // No row-count estimate — fall through and compete conservatively.

                return new IndexInListScanNode(bestIndex, inList.ColumnName, inList.Values);
            }
            else
            {
                // Without stats: unique single-column only.
                if (n > MaxCompetingInListSizeWithoutStats)
                    continue;

                TableIndexSchema? uniqueIndex = null;
                foreach (TableIndexSchema index in table.Indexes.Values)
                {
                    if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                        continue;
                    if (index.Type != IndexType.Unique || index.Columns.Length != 1)
                        continue;
                    if (!string.Equals(index.Columns[0], inList.ColumnName, StringComparison.OrdinalIgnoreCase))
                        continue;
                    uniqueIndex = index;
                    break;
                }

                if (uniqueIndex is null)
                    continue;

                return new IndexInListScanNode(uniqueIndex, inList.ColumnName, inList.Values);
            }
        }

        return null;
    }

    internal static PhysicalPlanNode ToScanNode(QueryPlanStep step)
    {
        switch (step.Type)
        {
            case QueryPlanStepType.QueryFromIndex:
            {
                CompositeColumnValue lookupKey = step.LookupKey
                    ?? new CompositeColumnValue(new[] { step.ColumnValue! });

                return new IndexLookupNode(step.Index!, lookupKey);
            }

            case QueryPlanStepType.RangeScanFromIndex:
                return new IndexRangeScanNode(
                    step.Index!,
                    step.FromBound,
                    step.FromInclusive,
                    step.ToBound,
                    step.ToInclusive);

            case QueryPlanStepType.FullScanFromIndex:
                return new TableScanNode(TableScanSource.ForcedIndex, step.Index);

            case QueryPlanStepType.FullScanFromTableIndex:
                return new TableScanNode(TableScanSource.PrimaryRows);

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Cannot convert plan step to scan node: {step.Type}");
        }
    }

    private static IReadOnlyList<NodeAst>? ExtractAggregateProjections(QueryTicket ticket)
    {
        if (ticket.Projection is not { Count: > 0 })
            return null;

        List<NodeAst>? result = null;
        foreach (NodeAst proj in ticket.Projection)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(proj)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(proj))
                (result ??= new()).Add(proj);
        }
        return result;
    }

    /// <summary>
    /// Extracts a flat list of bare column names from the projection list for streaming-distinct
    /// eligibility. Returns null if any projection item is a non-identifier expression (aggregate,
    /// arithmetic, alias over expression, wildcard) — those block streaming.
    /// </summary>
    // Unique indexes omit NULL-keyed rows entirely, and even Multi indexes (which do carry
    // NULL entries) place the NULL group at one end of the scan rather than adjacent to a
    // hypothetical "NULL value" ordering the sorter agrees on — so gate streaming to
    // NOT NULL columns, which sidesteps both hazards.
    private static bool AllDistinctColumnsAreNotNull(TableDescriptor table, IReadOnlyList<string> distinctCols)
    {
        if (table.Schema.Columns is null)
            return false;

        foreach (string col in distinctCols)
        {
            TableColumnSchema? schema = table.Schema.Columns.Find(c => string.Equals(c.Name, col, StringComparison.OrdinalIgnoreCase));
            if (schema is null || !schema.NotNull)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Extracts simple bare column names from GROUP BY expressions. Returns null if any expression
    /// is non-trivial (computed, aggregate, alias over expression) — those block streaming.
    /// </summary>
    private static List<string>? TryExtractGroupByColumns(IReadOnlyList<NodeAst> groupBy)
    {
        List<string> cols = new(groupBy.Count);
        foreach (NodeAst expr in groupBy)
        {
            NodeAst bare = expr.nodeType == NodeType.ExprAlias ? expr.leftAst! : expr;
            if (bare.nodeType != NodeType.Identifier || bare.yytext is null)
                return null;
            cols.Add(bare.yytext);
        }
        return cols;
    }

    private static List<string>? TryExtractDistinctColumns(List<NodeAst>? projection)
    {
        if (projection is null || projection.Count == 0)
            return null;

        List<string> cols = new(projection.Count);
        foreach (NodeAst proj in projection)
        {
            // Unwrap a simple alias (col AS alias) to get the underlying identifier.
            NodeAst expr = proj.nodeType == NodeType.ExprAlias ? proj.leftAst! : proj;

            if (expr.nodeType != NodeType.Identifier || expr.yytext is null)
                return null; // expression — streaming not possible

            cols.Add(expr.yytext);
        }

        return cols;
    }

    /// <summary>
    /// Builds the OutputOrdering a streaming-distinct scan actually delivers: the first
    /// <c>distinctCols.Count</c> columns of <paramref name="index"/> with each column's real
    /// sort direction. Descending index columns are stored complemented, so a forward scan
    /// yields them in descending order — claiming Ascending here would let sort elision emit
    /// rows in the wrong order.
    /// </summary>
    private static List<QueryOrderBy> BuildDistinctOrdering(TableIndexSchema index, IReadOnlyList<string> distinctCols)
    {
        List<QueryOrderBy> ordering = new(distinctCols.Count);
        for (int i = 0; i < distinctCols.Count; i++)
            ordering.Add(new QueryOrderBy(index.Columns[i], index.DirectionAt(i)));
        return ordering;
    }

    /// <summary>
    /// Returns true when the streaming-distinct scan's delivered ordering (index prefix with
    /// per-column directions) is a prefix of the requested ORDER BY — same column, same
    /// direction — allowing the SortNode to be elided.
    /// </summary>
    private static bool StreamingOrderSatisfiesOrderBy(
        IReadOnlyList<QueryOrderBy> streamingOrdering,
        IReadOnlyList<QueryOrderBy> orderBy)
    {
        if (orderBy.Count > streamingOrdering.Count)
            return false;

        for (int i = 0; i < orderBy.Count; i++)
        {
            if (orderBy[i].Type != streamingOrdering[i].Type)
                return false;

            if (!string.Equals(orderBy[i].ColumnName, streamingOrdering[i].ColumnName, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static long? EvalLong(NodeAst? expr, QueryTicket ticket)
    {
        if (expr is null)
            return null;

        ColumnValue val = SqlExecutor.EvalExpr(expr, new Dictionary<string, ColumnValue>(), ticket.Parameters);
        return val.Type == ColumnType.Integer64 ? val.LongValue : null;
    }

    /// <summary>
    /// Sets <see cref="QueryPlan.IndexOnly"/> when the chosen index scan covers every column
    /// the query requires, making a primary-row fetch unnecessary.
    ///
    /// Covering holds when <see cref="QueryPlan.ScanRequiredColumns"/> (the finalized needed
    /// column set from projection pushdown) is non-null AND every column it names is present in
    /// the chosen index's key columns — available directly from the decoded index entry without
    /// reading the primary row.
    ///
    /// <c>ColumnType.Id</c> columns are NOT treated as universally available from the index
    /// entry. Although the index entry embeds the KV row key (ObjectId), that is the internally
    /// generated storage identity, not the value stored in the <c>id</c> column — the column
    /// holds the user-provided value, which differs from the KV key. An <c>id</c> column can
    /// only be covered when it is explicitly part of the index's key columns.
    ///
    /// An empty <see cref="QueryPlan.ScanRequiredColumns"/> (row-id-only query) is also covered:
    /// the row id is always decoded from the index entry.
    ///
    /// Must be called after <see cref="ProjectionPushdownPlanner.Apply"/>, which is where
    /// <see cref="QueryPlan.ScanRequiredColumns"/> is finalized.
    ///
    /// Not applied to locate-phase plans (UPDATE / DELETE), which set
    /// <see cref="QueryTicket.LocateColumns"/> to a WHERE-only subset. Even when that subset
    /// fits in the index, the DML executor still needs the full primary row to re-encode the
    /// updated row and to remove stale secondary-index entries, so the flag must not be set.
    /// </summary>
    private static void TryMarkIndexOnly(QueryPlan plan, TableDescriptor table)
    {
        // Locate-phase plans (UPDATE/DELETE) set LocateColumns to a WHERE-only subset; the DML
        // executor always needs the primary row to re-encode or remove stale index entries.
        if (plan.Ticket.LocateColumns is not null)
            return;

        // null ScanRequiredColumns means "decode all columns" (SELECT * or no pushdown) — can't cover.
        if (plan.ScanRequiredColumns is not { } required)
            return;

        // Covering only applies to index-based scan steps.
        if (plan.Steps is not [{ Index: { } index, Type: var stepType }, ..])
            return;

        // A full table scan — even one driven off an index — fetches the primary row.
        if (stepType is QueryPlanStepType.FullScanFromTableIndex)
            return;

        // IN-list scans have no covering path in QueryUsingInListIndexInternal; exclude them
        // explicitly so that if the step ever gains a non-null Index the flag stays honest.
        if (stepType is QueryPlanStepType.InListScanFromIndex)
            return;

        // Empty required set → only the row id is needed; always present in the index entry.
        if (required.Count == 0)
        {
            plan.IndexOnly = true;
            plan.IndexOnlyColumns = [];
            return;
        }

        // Columns available without a primary-row fetch: the index key columns plus any stored/payload
        // (INCLUDE) columns materialized into the entry value.
        HashSet<string> available = new(index.Columns, StringComparer.OrdinalIgnoreCase);
        foreach (string includeColumn in index.IncludeColumns)
            available.Add(includeColumn);

        List<string> covered = [];
        foreach (string col in required)
        {
            if (!available.Contains(col))
                return; // a needed column is absent from the index — not covering
            covered.Add(col);
        }

        plan.IndexOnly = true;
        plan.IndexOnlyColumns = covered;
    }

    /// <summary>
    /// True when the index behind <paramref name="step"/> supplies every column the query needs (its
    /// key columns plus INCLUDE columns cover the projection), so the scan reads zero primary rows.
    /// Mirrors <see cref="TryMarkIndexOnly"/>'s covering rule but is evaluated at scan-selection time —
    /// before projection pushdown finalizes <see cref="QueryPlan.ScanRequiredColumns"/> — so the
    /// small-table cost override does not downgrade a covering scan to a full table scan.
    /// </summary>
    private static bool ScanCoversProjection(QueryPlanStep step, QueryTicket ticket)
    {
        if (step.Index is not { } index)
            return false;

        // UPDATE/DELETE locate phase always needs the primary row (re-encode / stale-entry removal).
        if (ticket.LocateColumns is not null)
            return false;

        IReadOnlySet<string>? required = RequiredColumnAnalyzer.ComputeSingleTable(ticket);
        if (required is null)
            return false;   // SELECT * / no pushdown — not covering
        if (required.Count == 0)
            return true;    // row-id only — always available from the index entry

        HashSet<string> available = new(index.Columns, StringComparer.OrdinalIgnoreCase);
        foreach (string includeColumn in index.IncludeColumns)
            available.Add(includeColumn);

        foreach (string col in required)
        {
            if (!available.Contains(col))
                return false;
        }

        return true;
    }

}
