
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
    private CamusDBOptions _options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => _options = next;

    private readonly PlanCache? _cache;

    /// <summary>
    /// True for the planner of the <c>EXPLAIN (ANALYZE)</c> path: every estimate records how it was
    /// made (<see cref="PhysicalPlanNode.EstimateTrace"/>). False for the planner a statement runs
    /// through, which then allocates nothing for traces.
    /// </summary>
    private readonly bool _traceEstimates;

    public QueryPlanner(CamusDBOptions options, StatisticsManager? stats = null, bool traceEstimates = false) : this(stats, null, options, traceEstimates) { }

    internal QueryPlanner(StatisticsManager? stats, PlanCache? cache, CamusDBOptions options, bool traceEstimates = false)
    {
        _stats = stats;
        _cache = cache;
        _options = options;
        _traceEstimates = traceEstimates;
    }

    // Tag a scan leaf with its DataDistribution and return it (fluent helper).
    private PhysicalPlanNode WithDistribution(PhysicalPlanNode node, TableDescriptor table)
    {
        node.Distribution = node switch
        {
            IndexLookupNode                                                   => PlacementReader.GetLookupDistribution(),
            IndexInListScanNode                                               => PlacementReader.GetLookupDistribution(),
            IndexRangeScanNode rangeScan                                      => PlacementReader.Instance.GetIndexScanDistribution(rangeScan.Index, _options),
            TableScanNode { Source: TableScanSource.ForcedIndex, Index: { } forcedIdx } => PlacementReader.Instance.GetIndexScanDistribution(forcedIdx, _options),
            TableScanNode                                                     => PlacementReader.Instance.GetPrimaryRowScanDistribution(table, _options),
            _                                                                 => DataDistribution.Gathered,
        };
        return node;
    }

    /// <summary>
    /// Wraps a full primary-row scan in a <see cref="GatherNode"/> when the statement and the
    /// table's live placement make span-fragmented execution both safe and useful. Runs on
    /// every plan build — including plan-cache replays, which rebuild the scan node fresh — so
    /// the gather always reflects current placement and nothing placement-shaped is ever
    /// cached.
    ///
    /// Eligibility, all required:
    /// distribution enabled, key-range sharding on, cluster mode (a standalone node has one
    /// span by construction), a plain primary-row table scan (index scans, forced-index scans
    /// and point lookups keep today's plan), a transaction that neither folds reads (an
    /// optimistic read-set must be built by one session stream) nor takes exclusive predicate
    /// locks (the DML write path), and a placement with more than one span whose interior
    /// boundaries all parse as row ids. Anything else returns the scan unchanged — with the
    /// flag off the plan is byte-identical to the non-distributed engine.
    /// </summary>
    private PhysicalPlanNode TryWrapScanInGather(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryPlan plan,
        PhysicalPlanNode scanNode)
    {
        if (!_options.DistributedQueryExecutionEnabled)
            return scanNode;

        QueryTicket ticket = plan.Ticket;
        string? reason = null;

        if (!_options.KeyRangeShardingEnabled)
            reason = "key-range sharding is off";
        else if (scanNode is not TableScanNode { Source: TableScanSource.PrimaryRows })
            reason = "not a primary-row full scan";
        else if (!database.Kahuna.IsClusterMode)
            reason = "standalone node";
        else if (ticket.TxnState.FoldReads)
            reason = "transaction folds reads (optimistic read-set needs one session stream)";
        else if (ticket.ExclusivePredicateLocks)
            reason = "exclusive predicate locks (DML write path)";

        if (reason is not null)
        {
            plan.DistributionSkipReason = reason;
            return scanNode;
        }

        Storage.Kv.TablePlacement placement = database.Kahuna.GetPlacement(table.Store.RowKeySpace);

        if (!placement.IsKeyRange || placement.Spans.Count <= 1)
        {
            plan.DistributionSkipReason = "placement has a single span";
            return scanNode;
        }

        foreach (Storage.Kv.PlacementSpan span in placement.Spans)
        {
            if (!GatherNode.TryParseRowIdBound(span.StartKey, out _)
                || !GatherNode.TryParseRowIdBound(span.EndKey, out _))
            {
                plan.DistributionSkipReason = "placement span boundary is not a row-id key";
                return scanNode;
            }
        }

        return new GatherNode(placement)
        {
            Input = scanNode,
            // Ordered concatenation of contiguous ascending spans preserves whatever ordering
            // the underlying scan guarantees.
            OutputOrdering = scanNode.OutputOrdering,
            // A remote fragment applies the residual filter BEFORE this cap, so — unlike the
            // scan-level ScanRowLimit — a filter does not disqualify it: each span may stop
            // after limit+offset SURVIVORS, because span-order concatenation means the global
            // LIMIT cut can never need more than that many rows from any single span.
            MaxSurvivors = ComputeFragmentSurvivorCap(ticket, scanNode.OutputOrdering is not null),
        };
    }

    /// <summary>
    /// The per-span survivor cap for fragmented scans: <c>limit + offset</c> under the same
    /// shape preconditions as <see cref="TryComputeScanRowLimit"/> minus the residual-filter
    /// clause (fragments filter before counting). Null = no cap (no LIMIT, or a shape where a
    /// downstream operator consumes unbounded rows: DISTINCT, grouping, aggregation,
    /// semi-join probes, or an ORDER BY the scan does not satisfy).
    /// </summary>
    private static long? ComputeFragmentSurvivorCap(QueryTicket ticket, bool scanSatisfiesOrderBy)
    {
        if (ticket.Limit is null
            || ticket.RowLocking is not null
            || ticket.IsDistinct
            || ticket.GroupBy is { Count: > 0 }
            || ticket.Having is not null
            || (ticket.Projection is { Count: > 0 } && QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
            || ticket.SemiJoinSpecs is { Count: > 0 }
            || (ticket.OrderBy is { Count: > 0 } && !scanSatisfiesOrderBy))
            return null;

        ColumnValue limit = SqlExecutor.EvalExpr(ticket.Limit, new Dictionary<string, ColumnValue>(), ticket.Parameters);
        if (limit.Type != ColumnType.Integer64)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Limit is not Integer64");

        long cap = limit.LongValue;

        if (ticket.Offset is not null)
        {
            ColumnValue offset = SqlExecutor.EvalExpr(ticket.Offset, new Dictionary<string, ColumnValue>(), ticket.Parameters);
            if (offset.Type != ColumnType.Integer64)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Offset is not Integer64");

            try
            {
                cap = checked(cap + offset.LongValue);
            }
            catch (OverflowException)
            {
                return null;
            }
        }

        return cap <= 0 ? 0 : cap;
    }

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst AliasedCall(string function, NodeAst? argument, string alias) =>
        new(NodeType.ExprAlias,
            new NodeAst(NodeType.ExprFuncCall, Identifier(function), argument, null, null, null, null, null, null),
            Identifier(alias),
            null, null, null, null, null, null);

    /// <summary>
    /// Builds the partial split for an aggregate directly above a gather, or null when the
    /// shape disqualifies it — see <see cref="PartialAggregatePlan"/> for what qualifies and
    /// why. Global shapes are limited to a single non-AVG aggregate. That limit is a coverage
    /// choice, not a correctness one: the aggregator computes several global aggregates in one
    /// pass, but the multi-aggregate global split (and the global AVG sum/count pair) has no
    /// cluster-level parity test yet. A global shape that does not qualify still returns the right
    /// answer, because its aggregate then runs above the gather over the gathered rows.
    /// Grouped shapes take full projection lists including AVG, which decomposes into
    /// internal SUM/COUNT pair columns finalized after the merge.
    /// </summary>
    private static PartialAggregatePlan? TryBuildPartialAggregatePlan(QueryTicket ticket, TableDescriptor table, PhysicalPlanNode input)
    {
        // The residual filter rides between the aggregate and the gather as a FilterNode
        // (it contributes no operator of its own — fragments evaluate it), so unwrap it.
        if (input is FilterNode)
            input = input.Input!;

        if (input is not GatherNode)
            return null;

        if (ticket.Projection is not { Count: > 0 })
            return null;

        // HAVING's hidden-aggregate expansion evaluates over the original rows, which
        // partials no longer have.
        if (ticket.Having is not null)
            return null;

        bool grouped = ticket.GroupBy is { Count: > 0 };

        List<string>? groupCols = null;
        if (grouped)
        {
            groupCols = TryExtractGroupByColumns(ticket.GroupBy!);
            if (groupCols is null)
                return null;
        }
        else if (ticket.Projection.Count != 1)
        {
            return null;
        }

        List<NodeAst> ship = new(ticket.Projection.Count + 1);
        List<NodeAst> merge = new(ticket.Projection.Count + 1);
        List<PartialAvgFinalizer> avgFinalizers = [];
        HashSet<string> projectedGroupCols = new(StringComparer.OrdinalIgnoreCase);

        foreach (NodeAst projection in ticket.Projection)
        {
            // Bare group-key column: passes through both halves under its bare name.
            if (grouped && projection.nodeType == NodeType.Identifier && projection.yytext is { } columnName)
            {
                string bare = columnName.LastIndexOf('.') >= 0 ? columnName[(columnName.LastIndexOf('.') + 1)..] : columnName;

                if (!groupCols!.Contains(bare, StringComparer.OrdinalIgnoreCase))
                    return null;

                projectedGroupCols.Add(bare);
                ship.Add(projection);
                merge.Add(Identifier(bare));
                continue;
            }

            if (projection.nodeType != NodeType.ExprAlias
                || projection.leftAst is not { nodeType: NodeType.ExprFuncCall } funcCall
                || projection.rightAst?.yytext is not { } alias)
                return null;

            // Built-ins only (AggregateFunctionCatalog): an extension aggregate runs above the gather.
            string? funcName = funcCall.leftAst?.yytext;
            if (funcName is null || !AggregateFunctionCatalog.IsPartialMergeable(funcName))
                return null;

            // The argument runs on remote nodes: it must be a pure function of the row.
            // COUNT(*) has the all-fields sentinel argument, which is trivially pure.
            if (funcCall.rightAst is { } argument
                && argument.nodeType != NodeType.ExprAllFields
                && !FragmentFilterShippability.IsShippable(argument))
                return null;

            // A partial SUM is finalized per span into a range-checked NUMERIC cell, so a span total
            // past the NUMERIC range fails the query although the exact total, or the average, is in
            // range. The single-node accumulator keeps an exact total and checks only the result, so a
            // SUM or AVG that can see a NUMERIC value runs above the gather instead.
            if ((funcName.Equals("sum", StringComparison.OrdinalIgnoreCase) || funcName.Equals("avg", StringComparison.OrdinalIgnoreCase))
                && funcCall.rightAst is { } summed
                && MayProduceNumeric(summed, table, ticket))
                return null;

            if (funcName.Equals("avg", StringComparison.OrdinalIgnoreCase))
            {
                // AVG only decomposes on the grouped path (see the method summary for why the
                // global split is not taken) and needs a real column argument.
                if (!grouped || funcCall.rightAst is null || funcCall.rightAst.nodeType == NodeType.ExprAllFields)
                    return null;

                string sumName = $"__partial_{alias}_sum";
                string countName = $"__partial_{alias}_cnt";

                ship.Add(AliasedCall("sum", funcCall.rightAst, sumName));
                ship.Add(AliasedCall("count", funcCall.rightAst, countName));
                merge.Add(AliasedCall("sum", Identifier(sumName), sumName));
                merge.Add(AliasedCall("sum", Identifier(countName), countName));
                avgFinalizers.Add(new PartialAvgFinalizer(alias, sumName, countName));
                continue;
            }

            string mergeFunction = funcName.Equals("count", StringComparison.OrdinalIgnoreCase) ? "sum" : funcName;
            ship.Add(projection);
            merge.Add(AliasedCall(mergeFunction, Identifier(alias), alias));
        }

        // Every group key must reach the partial rows, or the coordinator cannot re-group.
        if (grouped && groupCols!.Any(c => !projectedGroupCols.Contains(c)))
            return null;

        return new PartialAggregatePlan
        {
            ShipProjections = ship,
            ShipGroupBy = grouped ? groupCols!.Select(Identifier).ToArray() : null,
            MergeProjections = merge,
            MergeGroupBy = grouped ? groupCols!.Select(Identifier).ToArray() : null,
            AvgFinalizers = avgFinalizers,
        };
    }

    /// <summary>
    /// True when <paramref name="argument"/> can evaluate to a NUMERIC value. A NUMERIC value can only
    /// come from a NUMERIC column, a NUMERIC literal or cast (a <see cref="NodeType.TypeNumeric"/>
    /// node), or a NUMERIC parameter, so the walk looks for those. An identifier that names no column
    /// of <paramref name="table"/>, or a parameter with no value, counts as NUMERIC: the answer must
    /// never be a false "no". The walk uses a stack, because an expression chain can be deep.
    /// </summary>
    private static bool MayProduceNumeric(NodeAst argument, TableDescriptor table, QueryTicket ticket)
    {
        Stack<NodeAst> pending = new();
        pending.Push(argument);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.TypeNumeric:
                    return true;

                case NodeType.Identifier:
                    if (ColumnTypeOf(table, node.yytext) is not { } type || type == ColumnType.Numeric)
                        return true;
                    continue;

                case NodeType.Placeholder:
                    if (ticket.Parameters is null
                        || node.yytext is null
                        || !ticket.Parameters.TryGetValue(node.yytext, out ColumnValue? parameter)
                        || parameter.Type == ColumnType.Numeric)
                        return true;
                    continue;

                case NodeType.ExprFuncCall:
                    // leftAst is the function name, not a column.
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;
            }

            if (node.leftAst is not null) pending.Push(node.leftAst);
            if (node.rightAst is not null) pending.Push(node.rightAst);
            if (node.extendedOne is not null) pending.Push(node.extendedOne);
            if (node.extendedTwo is not null) pending.Push(node.extendedTwo);
            if (node.extendedThree is not null) pending.Push(node.extendedThree);
            if (node.extendedFour is not null) pending.Push(node.extendedFour);
            if (node.extendedFive is not null) pending.Push(node.extendedFive);
            if (node.extendedSix is not null) pending.Push(node.extendedSix);
            if (node.extendedSeven is not null) pending.Push(node.extendedSeven);
        }

        return false;
    }

    /// <summary>The declared type of a bare or qualified column name in <paramref name="table"/>, or null.</summary>
    private static ColumnType? ColumnTypeOf(TableDescriptor table, string? name)
    {
        if (name is null)
            return null;

        int dot = name.LastIndexOf('.');
        string bare = dot >= 0 ? name[(dot + 1)..] : name;

        foreach (TableColumnSchema column in table.Schema.Columns ?? [])
        {
            if (string.Equals(column.Name, bare, StringComparison.OrdinalIgnoreCase))
                return column.Type;
        }

        return null;
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

        // Read the config flag once per plan: it is mutable at runtime, and reading it again at the
        // Put below could observe a mid-plan flip (a Put into a cache the lookup said was off).
        bool planCacheEnabled = _cache is not null && _options.PlanCacheEnabled;

        // The plan-cache dependency descriptors feed nothing but the plan cache, so a deployment with
        // the cache off builds neither the list nor a descriptor per table. The shape id and the schema
        // deps above are NOT gated: the query result cache builds its fingerprint from them
        // (QueryExecutor.QueryWithCache) and EXPLAIN renders them, both independently of this flag.
        List<(string, int)> schemaDeps = [(table.Id, table.Schema.Version)];
        List<PlanCacheDep>? cacheDeps = planCacheEnabled ? [MakeCacheDep(database, table)] : null;

        if (ticket.SemiJoinSpecs is { Count: > 0 })
        {
            foreach (SemiJoinSpec spec in ticket.SemiJoinSpecs)
            {
                schemaDeps.Add((spec.InnerTable.Id, spec.InnerTable.Schema.Version));
                cacheDeps?.Add(MakeCacheDep(database, spec.InnerTable));
            }
        }

        plan.QueryShapeId = shapeId;
        plan.SchemaDeps = schemaDeps;

        // Cache lookup: skip access-path selection on a valid hit.
        // Read once, before the lookup, and used again for the store: a plan built while an
        // extension install or update lands is then filed under the stamp it was built with.
        long extensionStamp = database.Schema.ExtensionCatalog.Stamp;

        bool fromCache = false;
        PhysicalPlanNode scanNode;
        QueryPlanStep? scanStep;
        NodeAst? absorbedInListConjunct;

        if (shapeId is not null
            && planCacheEnabled
            && _cache!.TryGet(database.Id, shapeId, cacheDeps!, out PlanCacheEntry? cached, extensionStamp)
            && cached!.SingleTable is { } cachedDecision
            && !EstimateMovedFromCachedDecision(database, table, ticket, analysis, cachedDecision))
        {
            (scanNode, scanStep, absorbedInListConjunct) =
                BuildScanNodeFromCachedDecision(database, table, ticket, analysis, cachedDecision, out bool replayFailed);

            if (replayFailed)
                _cache.RecordReplayFailure();

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
            && IndexScanSelector.ScanSatisfiesOrderBy(table, scanStep.Value, ticket.OrderBy, IndexScanSelector.ConstantColumns(analysis)))
            scanNode.OutputOrdering = ticket.OrderBy;

        bool scanSatisfiesOrderBy = scanNode.OutputOrdering is not null;
        // A row-locking read must not cut its scan by the LIMIT: a row the lock step skips or re-checks
        // away does not use up the LIMIT, so the scan has to be able to supply the next one.
        plan.ScanRowLimit = ticket.RowLocking is null
            ? TryComputeScanRowLimit(ticket, plan.ExecutionFilter, scanSatisfiesOrderBy)
            : null;

        // The streaming DISTINCT / GROUP BY overrides below may swap the scan for a full ordered index
        // scan only when the scan they replace reads every row and applies no predicate itself. A null
        // scan step alone does not prove that: an IN-list index scan also has no step, and it applies
        // the IN conjunct it took out of the execution filter. Swapping it would leave nothing to
        // apply that conjunct, and the query would return rows outside the list.
        bool scanIsUnfilteredFullRead = absorbedInListConjunct is null
            && (scanStep is null || scanStep.Value.Type == QueryPlanStepType.FullScanFromTableIndex);

        // Streaming distinct detection.
        // When SELECT DISTINCT projects only simple column identifiers that are covered by
        // an index prefix, use a streaming (adjacent-key) dedup instead of a hash set,
        // reducing memory from O(distinct-count) to O(1).
        // Override the scan with a full ordered index scan when no predicate-driven scan was
        // chosen (scanIsUnfilteredFullRead). Only override then, so predicate-driven scans —
        // including an IN-list scan, which has no step — are not disrupted. ExecutionFilter is
        // unaffected (a FullScanFromIndex step consumes no predicates, same as a null/table-scan
        // step), but the override is only safe when the
        // forced index holds every row: a unique index omits rows with a NULL in any key column,
        // so TryFindStreamingDistinctIndex accepts a unique candidate only when all of its columns
        // are NOT NULL. The DISTINCT-column check below is not sufficient on its own.
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
                else if (scanIsUnfilteredFullRead)
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
        // uses the hash path. Nullable GROUP BY columns are excluded because a unique index omits
        // NULL rows, which would silently drop the NULL group; and the forced index itself must
        // hold every row (TryFindStreamingDistinctIndex rejects a unique index with any nullable
        // key column), or a group loses the rows whose trailing key is NULL and undercounts.
        // Must run before `root = scanNode` so that scan-node overrides propagate into the plan tree.
        bool hasGroupBy = ticket.GroupBy is { Count: > 0 };
        bool isStreamingGroupBy = false;

        // The order streaming aggregation emits its groups in: the order its input scan delivers the
        // grouping columns, which is the index column order and direction (as for streaming
        // DISTINCT). Null when grouping is not streaming.
        IReadOnlyList<QueryOrderBy>? streamingGroupByOrdering = null;
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
                    streamingGroupByOrdering = BuildDistinctOrdering(scanStep.Value.Index!, groupByCols);
                }
                else if (scanIsUnfilteredFullRead)
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
                        streamingGroupByOrdering = BuildDistinctOrdering(groupByIndex, groupByCols);
                    }
                }
            }
        }

        PhysicalPlanNode root = TryWrapScanInGather(database, table, plan, scanNode);

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
                PartialPlan = isStreamingGroupBy ? null : TryBuildPartialAggregatePlan(ticket, table, root),
                GroupByExpressions = ticket.GroupBy,
                AggregateProjections = ExtractAggregateProjections(ticket),
                IsStreamingGroupBy = isStreamingGroupBy,
                OutputOrdering = streamingGroupByOrdering,
            };

            if (ticket.Having is not null)
                root = new HavingFilterNode(ticket.Having, root);

            if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
            {
                // Streaming aggregation already emits its groups in index order. When ORDER BY asks
                // for that order (or a prefix of it), the sort is dropped, and a LIMIT then stops the
                // scan after enough groups instead of after every group. HAVING only removes groups,
                // so it keeps the order.
                bool orderElided = streamingGroupByOrdering is not null
                    && TryResolveGroupedOrderBy(ticket.OrderBy, ticket.Projection, out List<QueryOrderBy>? resolvedOrderBy)
                    && StreamingOrderSatisfiesOrderBy(streamingGroupByOrdering, resolvedOrderBy);

                if (!orderElided)
                    root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };
            }

            if (ticket.Projection is not null && ticket.Projection.Count > 0 && !QueryPostScanPipeline.IsFullProjection(ticket.Projection))
                root = new ProjectNode(root);

            if (ticket.Limit is not null || ticket.Offset is not null)
            {
                LimitNode limitNode = new(root)
                {
                    LimitValue = EvalLong(ticket.Limit, ticket),
                    OffsetValue = EvalLong(ticket.Offset, ticket),
                };

                ApplyTopKBound(limitNode, ticket);
                root = limitNode;
            }
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
                {
                    LimitNode limitNode = new(root)
                    {
                        LimitValue = EvalLong(ticket.Limit, ticket),
                        OffsetValue = EvalLong(ticket.Offset, ticket),
                    };

                    ApplyTopKBound(limitNode, ticket);
                    root = limitNode;
                }
            }
            else
            {
                if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0 && !scanSatisfiesOrderBy)
                    root = new SortNode(root) { OrderBy = ticket.OrderBy, OutputOrdering = ticket.OrderBy };

                // SELECT … FOR UPDATE: lock after the sort, so rows are locked in the order they are
                // returned, and before the projection, so the step re-reads and re-checks whole rows.
                // The executor admits only this plain shape (no grouping, DISTINCT or aggregate). The
                // node also stops ApplyTopKBound below, which walks only through projections, from
                // bounding the sort to the LIMIT.
                if (ticket.RowLocking is { } waitPolicy)
                {
                    root = new LockRowsNode(root)
                    {
                        WaitPolicy = waitPolicy,
                        RowBudget = ComputeLockRowBudget(ticket),
                        OutputOrdering = root.OutputOrdering,
                    };
                }

                if (ticket.Projection is not null && ticket.Projection.Count > 0)
                {
                    if (QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
                    {
                        root = new AggregateNode(root)
                        {
                            PartialPlan = TryBuildPartialAggregatePlan(ticket, table, root),
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
                {
                    LimitNode limitNode = new(root)
                    {
                        LimitValue = EvalLong(ticket.Limit, ticket),
                        OffsetValue = EvalLong(ticket.Offset, ticket),
                    };

                    ApplyTopKBound(limitNode, ticket);
                    root = limitNode;
                }
            }
        }

        plan.Root = root;
        QueryPlanStepAdapter.PopulateLinearSteps(plan);
        ProjectionPushdownPlanner.Apply(plan);
        TryMarkIndexOnly(plan, table);

        // Annotate every node with EstimatedCardinality and PlanCost.
        CostEstimator.AnnotatePlan(plan.Root, database, table, _stats, _options, _traceEstimates);

        // On a cache miss, store the access-path decision so the next identical-shape query
        // can skip the expensive cost enumeration.  In-list scans are excluded: they have no
        // QueryPlanStep (the step slot is null) and their choice is IN-list-value-count-dependent.
        if (!fromCache && planCacheEnabled
            && shapeId is not null
            && plan.Steps is [{ } s0, ..]
            && s0.Type != QueryPlanStepType.InListScanFromIndex)
        {
            bool guarded = s0.Type != QueryPlanStepType.QueryFromIndex;

            _cache!.Put(database.Id, shapeId,
                new PlanCacheEntry(cacheDeps!,
                    SingleTable: new SingleTableDecision(
                        s0.Index?.Name,
                        ClassifyDecision(s0),
                        guarded ? TryEstimateSelectivityForCache(database, table, analysis) : null,
                        guarded ? TryComputeRowDemand(ticket) : null),
                    JoinAliasOrder: null),
                extensionStamp);
        }

        return plan;
    }

    /// <summary>
    /// The number of rows the lock step of a <c>SELECT … FOR UPDATE</c> may emit: <c>limit + offset</c>
    /// when the LIMIT is a known value, or null to emit every row. The rows OFFSET skips count, because
    /// they pass through the lock step before the LIMIT drops them.
    /// </summary>
    private static long? ComputeLockRowBudget(QueryTicket ticket)
    {
        if (EvalLong(ticket.Limit, ticket) is not { } limit || limit < 0)
            return null;

        long offset = EvalLong(ticket.Offset, ticket) ?? 0;
        if (offset < 0)
            return null;

        try
        {
            return checked(limit + offset);
        }
        catch (OverflowException)
        {
            return null;
        }
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

        // A scan-level cap truncates BEFORE filtering, so any residual filter disqualifies it.
        // (The fragment survivor cap below has no such clause: a fragment filters first.)
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
    ///
    /// <para><b>ORDER BY and LIMIT.</b> The cheapest scan is not always the cheapest result. With an
    /// ORDER BY, a candidate that does not deliver that order also pays for the sort of its output
    /// (<see cref="SortCost"/>, priced as the plan annotation prices a sort). With a LIMIT on a
    /// row-preserving query, the index that delivers the ORDER BY order joins the competition even
    /// though no predicate drives it: it stops once the LIMIT has enough surviving rows, so it reads
    /// about <c>(limit + offset) / selectivity</c> entries, never more than the table holds. Without a
    /// LIMIT that candidate reads every entry plus a row fetch each and loses to the full scan, as
    /// before.</para>
    /// </summary>
    private (PhysicalPlanNode Node, QueryPlanStep? Step) PickCheapestIndexOrFullScan(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        PredicateAnalysis analysis,
        long tableRowCount)
    {
        bool hasOrderBy = ticket.OrderBy is { Count: > 0 };
        IReadOnlySet<string>? constantColumns = hasOrderBy ? IndexScanSelector.ConstantColumns(analysis) : null;
        long? rowDemand = hasOrderBy ? TryComputeRowDemand(ticket) : null;
        double selectivity = hasOrderBy ? EstimateWhereSelectivity(database, table, analysis) : 1.0;
        double sortCost = hasOrderBy ? SortCost(tableRowCount * selectivity, rowDemand) : 0.0;

        PhysicalPlanNode fullScanNode = WithDistribution(new TableScanNode(TableScanSource.PrimaryRows), table);
        double bestCost = CostEstimator.EstimateScanLeafCost(fullScanNode, tableRowCount, _stats, database, table, _options) + sortCost;
        PhysicalPlanNode bestNode = fullScanNode;
        QueryPlanStep? bestStep = null;
        bool bestCovers = false;
        int bestBoundColumns = 0;

        foreach (QueryPlanStep step in IndexScanSelector.EnumerateViableSteps(table, analysis))
        {
            // A covering candidate (index key + INCLUDE columns supply every needed column) fetches
            // zero primary rows, so it is costed without the per-entry row fetch. This mirrors the
            // rule-based path's refusal to downgrade a covered scan to a full table scan: a covered
            // scan reads at most as many index entries as the full scan reads rows, for any table size.
            bool covers = ScanCoversProjection(step, ticket);
            PhysicalPlanNode candidateNode = WithDistribution(ToScanNode(step), table);
            double cost = CostEstimator.EstimateScanLeafCost(candidateNode, tableRowCount, _stats, database, table, _options, covers);

            if (hasOrderBy && !IndexScanSelector.ScanSatisfiesOrderBy(table, step, ticket.OrderBy, constantColumns))
                cost += sortCost;

            // Ties resolve in three layers, all required for determinism and plan quality:
            //   1. A covering candidate beats a non-covering equal-cost best (including the
            //      full-scan baseline: an unbounded covered scan costs exactly the table row
            //      count, tying the full scan it must not lose to).
            //   2. Among candidates alike in coverage, the step that binds more key columns wins:
            //      it reads a subset of what the shorter step reads. Two steps tie in cost when
            //      the statistics cannot tell them apart (no distinct count for the prefix column,
            //      no histogram for the range column); an equality prefix plus a range must then
            //      beat the same prefix alone on another index.
            //   3. Among those, break by ordinal index name: candidates arrive in Dictionary
            //      iteration order, which can differ across descriptor rebuilds (and therefore
            //      across nodes) — a strict `<` would then pick different equal-cost plans on
            //      different nodes.
            int boundColumns = IndexScanSelector.BoundColumnCount(step);
            bool wins = cost < bestCost
                || (cost == bestCost
                    && (covers && !bestCovers
                        || covers == bestCovers
                            && bestStep is { Index: { } bestIdx }
                            && step.Index is { } candIdx
                            && (boundColumns > bestBoundColumns
                                || boundColumns == bestBoundColumns
                                    && string.CompareOrdinal(candIdx.Name, bestIdx.Name) < 0)));

            if (wins)
            {
                bestCost = cost;
                bestNode = candidateNode;
                bestStep = step;
                bestCovers = covers;
                bestBoundColumns = boundColumns;
            }
        }

        // The index that delivers the ORDER BY order, costed for the rows the LIMIT needs. It must
        // satisfy the whole ORDER BY, or a sort would still read everything it returns.
        if (rowDemand is { } demand
            && IndexScanSelector.TrySelectOrderByScan(table, ticket.OrderBy) is { } orderStep
            && IndexScanSelector.ScanSatisfiesOrderBy(table, orderStep, ticket.OrderBy, constantColumns))
        {
            bool covers = ScanCoversProjection(orderStep, ticket);
            double entries = Math.Min(tableRowCount, demand / Math.Max(selectivity, MinimumSelectivity));
            double cost = entries * (covers ? 1.0 : 2.0);

            if (cost < bestCost)
                return (WithDistribution(ToScanNode(orderStep), table), orderStep);
        }

        return (bestNode, bestStep);
    }

    /// <summary>Lower bound on an estimated selectivity, so a division by it stays finite.</summary>
    private const double MinimumSelectivity = 1e-9;

    /// <summary>
    /// How far a cached single-table decision's estimate may move, in either direction, before a hit
    /// is re-planned. Four times keeps a hit for literals of the same regime and re-plans across the
    /// regimes that change the cheapest path (a few rows against half the table).
    /// </summary>
    private const double CachedEstimateReplanFactor = 4.0;

    /// <summary>
    /// The WHERE selectivity a cached decision is guarded with, or null when the table has no
    /// analyzed statistics: then the estimate is a fixed default for every literal and cannot move.
    /// </summary>
    private double? TryEstimateSelectivityForCache(DatabaseDescriptor database, TableDescriptor table, PredicateAnalysis analysis)
    {
        if (_stats is null || !_stats.HasAnalyzedStatistics(database, table))
            return null;

        return EstimateWhereSelectivity(database, table, analysis);
    }

    /// <summary>
    /// True when the current literals moved the estimate far from the one <paramref name="decision"/>
    /// was made for, so the hit must not be replayed: the selectivity or the LIMIT demand changed by
    /// more than <see cref="CachedEstimateReplanFactor"/>, or a demand appeared or disappeared. The
    /// planner then plans from the start and the fresh decision replaces the entry. Costs one
    /// selectivity estimate per guarded hit (statistics lookups, no storage read).
    /// </summary>
    private bool EstimateMovedFromCachedDecision(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        PredicateAnalysis analysis,
        SingleTableDecision decision)
    {
        bool moved = false;

        if (decision.Selectivity is { } cachedSelectivity
            && TryEstimateSelectivityForCache(database, table, analysis) is { } currentSelectivity)
        {
            moved = MovedByFactor(cachedSelectivity, currentSelectivity);
        }

        // The demand changes the choice only through ORDER BY: it prices the sort and the index
        // that delivers the order (PickCheapestIndexOrFullScan).
        if (!moved && ticket.OrderBy is { Count: > 0 })
        {
            long? currentDemand = TryComputeRowDemand(ticket);
            moved = (decision.RowDemand, currentDemand) switch
            {
                (null, null) => false,
                ({ } cachedDemand, { } demand) => MovedByFactor(Math.Max(1, cachedDemand), Math.Max(1, demand)),
                _ => true,
            };
        }

        if (moved)
            _cache!.RecordEstimateReplan();

        return moved;

        static bool MovedByFactor(double cached, double current)
        {
            double low = Math.Max(Math.Min(cached, current), MinimumSelectivity);
            return Math.Max(cached, current) / low > CachedEstimateReplanFactor;
        }
    }

    /// <summary>
    /// The cost of sorting <paramref name="rows"/> rows, as <see cref="CostEstimator"/> prices a
    /// <see cref="SortNode"/>: one in-memory unit (0.1) for each row the sort keeps, which is all
    /// of them, or only the rows a LIMIT can reach when the sort is bounded.
    /// </summary>
    private static double SortCost(double rows, long? rowDemand)
    {
        double retained = rowDemand is { } demand ? Math.Min(demand, rows) : rows;
        return Math.Max(0.0, retained) * CostEstimator.SortRowWeight;
    }

    /// <summary>
    /// The estimated fraction of the table's rows the whole WHERE keeps. Comparisons, IN lists and
    /// pattern predicates use the statistics; each other conjunct counts at the fallback
    /// selectivity, which can only make an early-stopping ordered scan look more expensive, never
    /// cheaper.
    /// </summary>
    private double EstimateWhereSelectivity(DatabaseDescriptor database, TableDescriptor table, PredicateAnalysis analysis)
    {
        double selectivity = CardinalityEstimator.EstimatePredicateSelectivity(
            analysis.IndexableComparisons, analysis.InListComparisons, database, table, _stats!);

        selectivity *= CardinalityEstimator.PatternConjunctsSelectivity(
            analysis.ResidualConjuncts, database, table, _stats!, out int unrecognized);

        for (int i = 0; i < unrecognized + analysis.ColumnComparisons.Count; i++)
            selectivity *= CardinalityEstimator.FallbackSelectivity;

        return Math.Clamp(selectivity, MinimumSelectivity, 1.0);
    }

    /// <summary>
    /// The number of scanned rows that survive to the LIMIT, <c>limit + offset</c>, when every
    /// surviving scan row is one result row: no DISTINCT, grouping, HAVING, aggregate or semi-join
    /// sits between them. Null otherwise, or when the values are not integers (the executor
    /// reports that error, not the planner).
    /// </summary>
    private static long? TryComputeRowDemand(QueryTicket ticket)
    {
        if (ticket.Limit is null || ticket.IsDistinct || ticket.GroupBy is { Count: > 0 } || ticket.Having is not null)
            return null;

        if (ticket.SemiJoinSpecs is { Count: > 0 })
            return null;

        if (ticket.Projection is { Count: > 0 } && QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
            return null;

        ColumnValue limit = SqlExecutor.EvalExpr(ticket.Limit, new Dictionary<string, ColumnValue>(), ticket.Parameters);
        if (limit.Type != ColumnType.Integer64 || limit.LongValue < 0)
            return null;

        long demand = limit.LongValue;

        if (ticket.Offset is not null)
        {
            ColumnValue offset = SqlExecutor.EvalExpr(ticket.Offset, new Dictionary<string, ColumnValue>(), ticket.Parameters);
            if (offset.Type != ColumnType.Integer64 || offset.LongValue < 0)
                return null;

            demand = offset.LongValue > long.MaxValue - demand ? long.MaxValue : demand + offset.LongValue;
        }

        return demand;
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

        if (decision.IndexName is null || decision.Kind == ScanDecisionKind.TableScan)
        {
            return (
                WithDistribution(new TableScanNode(TableScanSource.PrimaryRows), table),
                new QueryPlanStep(QueryPlanStepType.FullScanFromTableIndex),
                null);
        }

        if (decision.Kind is ScanDecisionKind.OrderByIndexScan or ScanDecisionKind.FullIndexScan)
        {
            // A predicate-free decision has nothing to re-bind. Rebuild the unbounded step after
            // the same checks the cold path made: the index must still be readable, and a unique
            // index must still hold every row (it omits rows with a NULL key column). Running the
            // predicate matcher here would find no comparisons and fail every replay.
            if (table.Indexes.TryGetValue(decision.IndexName, out TableIndexSchema? unboundedIndex)
                && SchemaElementStateRules.IsReadableIndex(table.Schema, unboundedIndex)
                && (unboundedIndex.Type != IndexType.Unique || IndexScanSelector.AllColumnsNotNull(table, unboundedIndex)))
            {
                if (decision.Kind == ScanDecisionKind.OrderByIndexScan)
                {
                    QueryPlanStep orderByStep = new(
                        QueryPlanStepType.RangeScanFromIndex,
                        unboundedIndex,
                        fromBound: null,
                        fromInclusive: true,
                        toBound: null,
                        toInclusive: true);
                    return (WithDistribution(ToScanNode(orderByStep), table), orderByStep, null);
                }

                QueryPlanStep fullStep = new(QueryPlanStepType.FullScanFromIndex, unboundedIndex);
                return (WithDistribution(new TableScanNode(TableScanSource.ForcedIndex, unboundedIndex), table), fullStep, null);
            }

            replayFailed = true;
            return BuildScanNode(database, table, ticket, analysis);
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
    /// Names how the first linear step used its index, so the cache entry can be replayed the
    /// same way. An unbounded range scan is the ORDER BY choice; a full index scan is the
    /// streaming override or a user-forced index; anything with a lookup key or a bound came
    /// from the predicates.
    /// </summary>
    private static ScanDecisionKind ClassifyDecision(QueryPlanStep step) => step.Type switch
    {
        QueryPlanStepType.FullScanFromTableIndex => ScanDecisionKind.TableScan,
        QueryPlanStepType.FullScanFromIndex => ScanDecisionKind.FullIndexScan,
        QueryPlanStepType.RangeScanFromIndex when step.FromBound is null && step.ToBound is null => ScanDecisionKind.OrderByIndexScan,
        _ => step.Index is null ? ScanDecisionKind.TableScan : ScanDecisionKind.PredicateScan,
    };

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
        // A FORCE_INDEX hint names the index the scan must use. When the predicate can bound that
        // index, the bounded scan is the plan: it does not compete with the other indexes on cost
        // or score, and the range-vs-full-scan veto does not apply, because the user asked for the
        // index. A hint whose index the predicate cannot bound goes through the automatic choice
        // below, and becomes an end-to-end scan of that index only when no automatic scan is chosen.
        if (!string.IsNullOrEmpty(ticket.IndexName))
        {
            TableIndexSchema forcedIndex = ResolveForcedIndex(table, ticket.IndexName);
            QueryPlanStep? forcedStep = IndexScanSelector.TrySelectScanForForcedIndex(table, analysis, forcedIndex.Name);
            if (forcedStep is not null)
                return (WithDistribution(ToScanNode(forcedStep.Value), table), forcedStep, null);
        }

        // Cost-based access-path selection: when the feature flag is on AND the table has been
        // ANALYZEd and the predicate drives an index, enumerate all viable steps, cost each
        // against the full-scan baseline, and pick the cheapest rather than the heuristic-best.
        // Falls back to the rule-based path when the flag is off, stats are absent, no row-count
        // estimate is available, or the table was never ANALYZEd — plans are then byte-identical
        // to the rule-based path. The ANALYZE requirement is load-bearing: row counts are
        // DML-maintained and exist for any table with data, so without it the cost model engages
        // with fixed fallback selectivities and abandons correct index paths on small tables that
        // will never cross the auto-analyze staleness threshold.
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
            if (trc is { } tableRowCount && tableRowCount > 0
                && _stats.HasAnalyzedStatistics(database, table))
            {
                (PhysicalPlanNode bestNode, QueryPlanStep? bestStep) =
                    PickCheapestIndexOrFullScan(database, table, ticket, analysis, tableRowCount);

                // If the chosen path is a predicate-driven range scan, still check whether an
                // IN-list scan would be even cheaper (same competition as the rule-based path).
                if (bestStep is { Type: QueryPlanStepType.RangeScanFromIndex } chosenRange
                    && (chosenRange.FromBound is not null || chosenRange.ToBound is not null)
                    && analysis.InListComparisons is { Count: > 0 })
                {
                    bool rangeSatisfiesOrder = ticket.OrderBy is { Count: > 0 }
                        && IndexScanSelector.ScanSatisfiesOrderBy(table, chosenRange, ticket.OrderBy, IndexScanSelector.ConstantColumns(analysis));
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
                if (CostEstimator.ShouldPreferFullScan(tempRangeNode, trc, _options, _stats, database, table))
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
                && IndexScanSelector.ScanSatisfiesOrderBy(table, rangeStep, ticket.OrderBy, IndexScanSelector.ConstantColumns(analysis));
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
            TableIndexSchema index = ResolveForcedIndex(table, ticket.IndexName);

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
    /// The index a <c>FORCE_INDEX</c> hint names. A name that is not an index of the table, or an
    /// index that is not readable yet (a backfill in progress), is an unknown key to the user, as
    /// it was before the hint could bound a scan.
    /// </summary>
    private static TableIndexSchema ResolveForcedIndex(TableDescriptor table, string indexName)
    {
        if (!table.Indexes.TryGetValue(indexName, out TableIndexSchema? index)
            || !SchemaElementStateRules.IsReadableIndex(table.Schema, index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                $"Key '{indexName}' doesn't exist in table '{table.Name}'");
        }

        return index;
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
                    long rangeRows = CostEstimator.EstimateRangeScanRows(tempRangeNode, trc, _options, _stats, database, table);
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
                    step.ToInclusive)
                {
                    EqualityPrefixLength = step.EqualityPrefixLength,
                };

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
    /// Translates the ORDER BY keys of a grouped query into the source columns they order by, so
    /// they can be compared with the order streaming aggregation emits. A key that names an output
    /// alias resolves to the expression behind it, which must be a bare column; a key that names no
    /// output is a source column already. Returns false — keep the sort — for a computed key, an
    /// alias over an aggregate or any other expression, or a name more than one output carries.
    /// </summary>
    private static bool TryResolveGroupedOrderBy(
        IReadOnlyList<QueryOrderBy> orderBy,
        List<NodeAst>? projection,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out List<QueryOrderBy>? resolved)
    {
        resolved = new List<QueryOrderBy>(orderBy.Count);

        foreach (QueryOrderBy key in orderBy)
        {
            if (key.IsExpression)
            {
                resolved = null;
                return false;
            }

            string column = key.ColumnName;
            int matches = 0;

            if (projection is not null)
            {
                for (int i = 0; i < projection.Count; i++)
                {
                    if (!string.Equals(QueryProjectionResolver.GetOutputNameFromProjectionExpression(projection[i], i), key.ColumnName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    matches++;
                    NodeAst expr = projection[i].nodeType == NodeType.ExprAlias ? projection[i].leftAst! : projection[i];

                    if (expr.nodeType != NodeType.Identifier || expr.yytext is null)
                    {
                        resolved = null;
                        return false;
                    }

                    column = expr.yytext;
                }
            }

            if (matches > 1)
            {
                resolved = null;
                return false;
            }

            resolved.Add(new QueryOrderBy(column, key.Type));
        }

        return true;
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
            // A computed key is not a column, so no index order can satisfy it. Its label is only a
            // diagnostic string; matching it against an index column name would elide the sort and
            // return rows in index order while the query asked for something else.
            if (orderBy[i].IsExpression)
                return false;

            if (orderBy[i].Type != streamingOrdering[i].Type)
                return false;

            if (!string.Equals(orderBy[i].ColumnName, streamingOrdering[i].ColumnName, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Marks the sort below <paramref name="limit"/> as bounded when the LIMIT makes all but the
    /// first <c>offset + limit</c> rows unreachable.
    ///
    /// <para><b>Only when nothing between the two changes the row count.</b> A projection is
    /// row-preserving, so a bound above it is still the same set of rows. An aggregate is not:
    /// <c>SELECT count(*) FROM t ORDER BY x LIMIT 1</c> must count every row and then return one, so
    /// bounding its input to a single row would answer with a count of 1. The walk stops at anything
    /// that is not row-preserving rather than reasoning about which plan shape produced it.</para>
    ///
    /// <para>The bound is refused when spill is enabled and <c>k</c> exceeds the spill threshold: at
    /// that size the external sort is the operator that is actually designed to hold the rows. That
    /// reuses the existing threshold rather than adding a second knob to keep in agreement.</para>
    /// </summary>
    private void ApplyTopKBound(LimitNode limit, QueryTicket ticket)
    {
        if (limit.LimitValue is not { } limitValue || limitValue < 0)
            return;

        long offsetValue = limit.OffsetValue ?? 0;

        if (offsetValue < 0)
            return;

        long bound;

        try
        {
            bound = checked(offsetValue + limitValue);
        }
        catch (OverflowException)
        {
            // A caller can write LIMIT 9223372036854775807 OFFSET 9223372036854775807. There is no
            // bound to apply, and wrapping would produce a small one — silently dropping rows.
            return;
        }

        if (bound > int.MaxValue)
            return;

        if (_options.SpillEnabled && bound > _options.SpillEffectiveThreshold)
            return;

        PhysicalPlanNode? node = limit.Input;

        while (node is ProjectNode)
            node = node.Input;

        if (node is SortNode sort)
            sort.BoundedLimit = bound;
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
