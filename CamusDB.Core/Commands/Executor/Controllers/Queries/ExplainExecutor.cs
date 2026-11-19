
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes EXPLAIN statements by running parse→bind→plan (without execution) and
/// returning one result row per physical plan node.
///
/// KNOWN LIMITATION — subquery materialization during EXPLAIN:
/// The prepare pipeline calls SubqueryRewriter.RewriteSelectQueryAsync (which executes the
/// uncorrelated subqueries of the WHERE, the select list, the HAVING and the JOIN ON predicates
/// against storage to fold them into literals) and ExistsSubqueryPreparer.PrepareAsync (which
/// may run uncorrelated EXISTS checks of the WHERE). As a result, EXPLAIN SELECT ... WHERE x IN
/// (SELECT ...) performs real storage reads for the inner subquery, even though the outer table
/// is never scanned.
///
/// This is reads-only (no mutation) and affects only queries that contain subqueries.
/// A plan-only mode that leaves subquery nodes symbolic is deferred until the subquery
/// infrastructure grows a dry-run flag (candidate for a future task).
/// </summary>
internal sealed class ExplainExecutor
{
    private readonly SelectQueryCreator selectQueryCreator = new();

    private readonly SelectBindPipeline bindPipeline;

    private readonly QueryPlanner queryPlanner;

    private readonly JoinQueryPlanner joinQueryPlanner;

    /// <summary>
    /// The planners of the <c>ANALYZE</c> path. They are built to trace estimates
    /// (<see cref="PhysicalPlanNode.EstimateTrace"/>), which plain <c>EXPLAIN</c> and a statement do
    /// not do, so the trace costs nothing outside <c>EXPLAIN (ANALYZE)</c>.
    /// </summary>
    private readonly QueryPlanner tracingQueryPlanner;

    private readonly JoinQueryPlanner tracingJoinQueryPlanner;

    private readonly QueryExecutor queryExecutor;

    /// <summary>Configuration of the engine this executor belongs to.</summary>
    private readonly CamusDBOptions options;

    public ExplainExecutor(
        SelectBindPipeline bindPipeline,
        QueryExecutor queryExecutor,
        CamusDBOptions options,
        StatisticsManager? stats = null)
    {
        this.bindPipeline = bindPipeline;
        this.queryExecutor = queryExecutor;
        this.options = options;
        queryPlanner = new QueryPlanner(options, stats);
        joinQueryPlanner = new JoinQueryPlanner(options, stats);
        tracingQueryPlanner = new QueryPlanner(options, stats, traceEstimates: true);
        tracingJoinQueryPlanner = new JoinQueryPlanner(options, stats, traceEstimates: true);
    }

    /// <summary>
    /// Builds the query plan for the given SELECT AST and returns one result row per node.
    ///
    /// <para><b>The plan is built before this method returns, and that is an authorization
    /// requirement, not a style choice.</b> Building the plan resolves every table the query reads,
    /// and each resolution is where the caller's privilege on that table is checked. The statement's
    /// authorization scope exists only in the flow that runs the statement; a cursor enumerated later
    /// by the transport runs outside it. A plan built lazily inside the returned cursor was therefore
    /// never checked against the caller — and it also executes scalar subqueries, whose results the
    /// rendered plan prints. Only rendering is deferred.</para>
    /// </summary>
    /// <param name="database">Open database descriptor.</param>
    /// <param name="selectAst">The inner SELECT AST wrapped by the EXPLAIN node.</param>
    /// <param name="ticket">The originating execution ticket (provides parameters and txn).</param>
    /// <param name="stage">The stage label to emit in each row ("physical" or "logical").</param>
    /// <param name="lockingClause">
    /// The locking clause of <c>EXPLAIN SELECT … FOR UPDATE</c>, or null. The plan then shows the lock
    /// step, and no lock is taken. As in PostgreSQL, the query and privilege checks of the locking read
    /// apply (UPDATE is needed too), and the transaction rules do not: plain EXPLAIN of a locking read
    /// works in a read-only or optimistic transaction.
    /// </param>
    public async Task<IAsyncEnumerable<QueryResultRow>> ExplainQueryAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket,
        string stage,
        NodeAst? lockingClause = null)
    {
        if (lockingClause is not null)
            RowLockingRules.RequireLockableSelect(selectAst);

        selectAst = SelectQueryCreator.BindFromlessWhere(selectAst);

        // A FROM-less SELECT (null table node) never reaches the planner: it projects a single
        // synthetic row directly, so there is no scan/join/plan tree to walk. Render a fixed
        // constant-source → project shape instead. Projection subqueries are left symbolic here
        // (their names only) — unlike a real query, EXPLAIN does not pre-materialize them.
        if (selectAst.rightAst is null)
            return RenderFromlessPlan(selectAst, stage).ToAsyncEnumerable();

        QueryPlan plan = await BuildPlanAsync(database, selectAst, ticket, lockingClause: lockingClause).ConfigureAwait(false);
        AttachSetOperationOperands(plan, ticket, traceEstimates: false, analyze: false);
        await AttachDerivedPlansAsync(plan, ticket, traceEstimates: false, analyze: false, new ExplainedDerivedTables()).ConfigureAwait(false);

        return RenderPlan(plan, stage).ToAsyncEnumerable();
    }

    /// <summary>Renders one result row per plan node, plus the trailing plan-info, distribution and cache rows.</summary>
    private IEnumerable<QueryResultRow> RenderPlan(QueryPlan plan, string stage)
    {
        // Zip rendered (name, detail, depth) lines with plan nodes so we can emit cost estimates.
        List<PhysicalPlanNode> orderedNodes = WalkNodesOrdered(plan.Root);
        List<(string Name, string Detail, int Depth)> rendered = PlanRenderer.WalkNodes(plan.Root, plan).ToList();

        for (int i = 0; i < rendered.Count; i++)
        {
            (string name, string detail, int depth) = rendered[i];
            PhysicalPlanNode? physNode = i < orderedNodes.Count ? orderedNodes[i] : null;

            long? estRows = physNode?.EstimatedCardinality;
            double? estCost = physNode?.Cost?.Total;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "stage",          new ColumnValue(ColumnType.String, stage) },
                { "node",           new ColumnValue(ColumnType.String, name) },
                { "detail",         new ColumnValue(ColumnType.String, detail) },
                { "estimated_rows", estRows is not null
                    ? new ColumnValue(ColumnType.Integer64, estRows.Value)
                    : ColumnValue.Null },
                { "estimated_cost", estCost is not null
                    ? new ColumnValue(ColumnType.Float64, estCost.Value)
                    : ColumnValue.Null },
                { "depth",          new ColumnValue(ColumnType.Integer64, depth) },
            });
        }

        // Emit one trailing plan-info row with shape and schema-dep metadata when available.
        if (plan.QueryShapeId is not null)
        {
            string schemaDepsStr = plan.SchemaDeps is { Count: > 0 }
                ? "[" + string.Join(", ", plan.SchemaDeps.Select(d => $"{d.TableId}@{d.SchemaVersion}")) + "]"
                : "[]";

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "stage",          new ColumnValue(ColumnType.String, stage) },
                { "node",           new ColumnValue(ColumnType.String, "plan-info") },
                { "detail",         new ColumnValue(ColumnType.String, $"shape={plan.QueryShapeId}, schema-deps={schemaDepsStr}") },
                { "estimated_rows", ColumnValue.Null },
                { "estimated_cost", ColumnValue.Null },
            });
        }

        // Emit a distribution row when distributed execution is enabled, so eligibility is
        // observable: either the gather that was planned or the reason the plan stayed local.
        if (plan.Database.Options.DistributedQueryExecutionEnabled)
        {
            string distributionDetail = plan.Root is not null && ContainsGather(plan.Root)
                ? "distributed=yes"
                : $"distributed=no ({plan.DistributionSkipReason ?? "not eligible"})";

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "stage",          new ColumnValue(ColumnType.String, stage) },
                { "node",           new ColumnValue(ColumnType.String, "distribution") },
                { "detail",         new ColumnValue(ColumnType.String, distributionDetail) },
                { "estimated_rows", ColumnValue.Null },
                { "estimated_cost", ColumnValue.Null },
            });
        }

        // Emit a cache-eligibility row when the query carries a {cache=...} hint, so EXPLAIN
        // surfaces whether the result will be cached — and, when a hint is present but ignored
        // (a join, or the cache disabled), why. This is a static plan property with no side
        // effects: it never probes or populates the cache.
        if (plan.Ticket.CacheHint is { } cacheHint)
        {
            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "stage",          new ColumnValue(ColumnType.String, stage) },
                { "node",           new ColumnValue(ColumnType.String, "cache") },
                { "detail",         new ColumnValue(ColumnType.String, DescribeCacheEligibility(plan, cacheHint)) },
                { "estimated_rows", ColumnValue.Null },
                { "estimated_cost", ColumnValue.Null },
            });
        }
    }

    /// <summary>
    /// Describes, at plan time, whether this query's result is eligible for the result cache.
    /// A pure plan property with no side effects — it does not probe the cache or reveal whether
    /// a live entry currently exists (that is inherently racy; the authoritative run-time outcome
    /// is reported in the query response's <c>cacheStatus</c> instead).
    ///
    /// <para>A hint that is present but will be ignored is reported as <c>eligible=false</c> with
    /// the reason, so a silently-dropped hint is visible: joins bypass the cache (they set
    /// <see cref="QueryPlan.BoundQuery"/>), and a disabled cache ignores every hint. Eligibility
    /// here is the plan-level view; at run time the cache additionally applies only to autocommit
    /// reads (an explicit transaction reads live storage).</para>
    /// </summary>
    private string DescribeCacheEligibility(QueryPlan plan, CacheHintOptions hint)
    {
        string opts =
            $"ttl={(hint.TtlMs is { } ms ? ms + "ms" : "default")}, " +
            $"strict={hint.IsStrict.ToString().ToLowerInvariant()}";

        if (plan.BoundQuery is not null)
            return $"family={hint.CacheName}, eligible=false, reason=join, {opts}";

        if (!options.QueryResultCacheEnabled)
            return $"family={hint.CacheName}, eligible=false, reason=cache-disabled, {opts}";

        return $"family={hint.CacheName}, eligible=true, {opts}";
    }

    /// <summary>
    /// Executes the query for real (draining the full cursor), collects runtime counters per
    /// physical plan node, then yields one result row per node with actual stats.
    ///
    /// The result row schema extends the plain EXPLAIN schema with five actual columns:
    /// <c>actual_rows INT64</c>, <c>rows_read INT64</c>, <c>actual_time_ms FLOAT64</c>,
    /// <c>kv_lookups INT64</c>, <c>kv_scan_entries INT64</c>. <c>actual_time_ms</c> is the inclusive time
    /// of the node: on the root, the time to drain the whole plan; on another node, the time spent in
    /// its row cursor, inputs included (see <see cref="PlanNodeCursor"/>). A nested-loop join with a
    /// <c>LATERAL</c> right side adds <c>lateral-runs=</c>, <c>lateral-kept-reads=</c> and
    /// <c>lateral-ms=</c> to its detail.
    ///
    /// <para><b>Join plans</b> run through <see cref="QueryJoinExecutor.ExecuteJoinPlan"/>. A join
    /// node's storage columns count the reads of its right side, which has no row of its own (see
    /// <see cref="PlanNodeStats"/>); a merge join's ordered right input is added to its row. A join
    /// row's detail also shows what the plan cannot: <c>ran=</c> when a hash join changed strategy at
    /// run time, and <c>join-order-est-rows=</c>, the join-order search's estimate, next to the plan
    /// annotation's <c>estimated_rows</c>. A broadcast probe adds one <c>broadcast-span</c> row per
    /// span. The work after the last join (the residual filter, grouping, ordering, limit) has no
    /// node in a join plan, so the root row is the last join and not the final result.</para>
    ///
    /// <para>Every check and the plan itself are done before this method returns, for the reason given
    /// on <see cref="ExplainQueryAsync"/>: the tables are resolved, and so authorized, in the flow that
    /// runs the statement. Only the execution and the rendering are deferred to the cursor, and neither
    /// resolves a table.</para>
    ///
    /// <para><b>A locking read</b> (<paramref name="lockingClause"/> set) runs as the statement runs: every
    /// check of <c>SELECT … FOR UPDATE</c> applies, and the rows it returns stay locked until the
    /// transaction ends, as in PostgreSQL. NOWAIT fails the statement as the query fails. The lock step's
    /// row adds three counters to its detail, which PostgreSQL does not have: <c>locked=</c>,
    /// <c>skipped-held=</c> and <c>recheck-dropped=</c> (see <see cref="PlanNodeStats.RowsLocked"/>).</para>
    /// </summary>
    public async Task<IAsyncEnumerable<QueryResultRow>> ExplainAnalyzeQueryAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket,
        NodeAst? lockingClause = null)
    {
        if (lockingClause is not null)
            RowLockingRules.RequireLockableStatement(selectAst, ticket.TxnState, RowLockingRules.PolicyOf(lockingClause));

        selectAst = SelectQueryCreator.BindFromlessWhere(selectAst);

        // A FROM-less SELECT has no plan tree to instrument (see ExplainQuery). There are no
        // storage reads to measure, so ANALYZE would add nothing over plain EXPLAIN; reject it
        // clearly rather than fabricate zeroed runtime stats.
        if (selectAst.rightAst is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "EXPLAIN (ANALYZE) is not supported for a FROM-less SELECT (there is no table access to measure). " +
                "Use plain EXPLAIN to inspect the plan.");

        // Bind and plan, timed together: the planning time a client waits for before the first row.
        Stopwatch planning = Stopwatch.StartNew();
        QueryPlan plan = await BuildPlanAsync(database, selectAst, ticket, traceEstimates: true, lockingClause).ConfigureAwait(false);
        AttachSetOperationOperands(plan, ticket, traceEstimates: true, analyze: true);
        await AttachDerivedPlansAsync(plan, ticket, traceEstimates: true, analyze: true, new ExplainedDerivedTables()).ConfigureAwait(false);
        planning.Stop();

        return RunAnalyzedPlan(plan, planning.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    /// Executes an already-built plan to completion with runtime stats on, then yields one row per
    /// node. The plan carries its resolved tables, so nothing here resolves one.
    /// </summary>
    /// <param name="plan">The plan to run.</param>
    /// <param name="planningMs">
    /// The time of the bind and plan stages, printed on the plan-info row. It includes the subqueries
    /// that the bind stage runs to fold them into literals (see the class summary), because a client
    /// waits for them before the first row too.
    /// </param>
    private async IAsyncEnumerable<QueryResultRow> RunAnalyzedPlan(QueryPlan plan, double planningMs)
    {
        plan.CollectRuntimeStats = true;

        // A join plan (BoundQuery set) runs through the join executor, which does not allocate
        // counters: they are attached here, before the cursor opens, to every node that will run.
        bool isJoin = plan.BoundQuery is not null;
        if (isJoin)
            AttachJoinStats(plan.Root);

        IAsyncEnumerable<QueryResultRow> cursor = isJoin
            ? queryExecutor.ExecuteJoinPlan(plan)
            : queryExecutor.ExecuteQueryPlan(plan);

        Stopwatch sw = Stopwatch.StartNew();
        await foreach (QueryResultRow _ in cursor.ConfigureAwait(false)) { }
        sw.Stop();

        // Assign total elapsed time to the root node's stats.
        if (plan.Root.Stats is null)
            plan.Root.Stats = new();
        plan.Root.Stats.ElapsedMs = sw.Elapsed.TotalMilliseconds;

        // Zip the rendered (name, detail, depth) lines with the node objects in the same DFS order.
        List<PhysicalPlanNode> orderedNodes = WalkNodesOrdered(plan.Root);
        List<(string Name, string Detail, int Depth)> rendered = PlanRenderer.WalkNodes(plan.Root, plan).ToList();

        for (int i = 0; i < rendered.Count; i++)
        {
            (string name, string detail, int depth) = rendered[i];
            PhysicalPlanNode? physNode = i < orderedNodes.Count ? orderedNodes[i] : null;

            // FilterNode is folded into the scan (ExecutionFilter); it never appears in StepNodes
            // and has no counters of its own. Attribute its actual_rows from the child scan's
            // RowsEmitted — that count is already post-filter (the scan only yields passing rows).
            // KV counters are 0: predicate evaluation adds no storage reads. A filter can be the
            // root, and the root alone carries the statement's time, so the time is kept.
            PlanNodeStats? stats;
            if (physNode is FilterNode)
                stats = physNode.Input?.Stats is { } childStats
                    ? new PlanNodeStats { RowsEmitted = childStats.RowsEmitted, ElapsedMs = physNode.Stats?.ElapsedMs ?? childStats.ReportedMs }
                    : null;
            else if (physNode is not null && RightInputOf(physNode) is { } rightInput)
                stats = WithRightInputReads(physNode.Stats, rightInput);
            else
                stats = physNode?.Stats;

            // A derived table whose query EXPLAIN planned gets the time of that query's run, as the root of
            // the plan under it does, so the line and its child agree. Another reader of a CTE computed once
            // reads the kept rows, so it keeps the time of its own cursor.
            if (physNode is DerivedTableScanNode { Explained: { } explained } derivedScan
                && ReferenceEquals(explained.RenderedAt, derivedScan.BoundSource)
                && stats is { ElapsedMs: null })
                stats.ElapsedMs = explained.ScanStats.ElapsedMs;

            // An operand of a set operation counts its rows on its own line; its time is that of its plan.
            if (physNode is SetOperationOperandNode operand && stats is { ElapsedMs: null })
                stats.ElapsedMs = TimeOf(operand.Plan.Root);

            if (physNode is not null)
                detail = AppendRuntimeDetail(detail, physNode, stats);

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "stage",             new ColumnValue(ColumnType.String, "analyze") },
                { "node",              new ColumnValue(ColumnType.String, name) },
                { "detail",            new ColumnValue(ColumnType.String, detail) },
                { "estimated_rows",    physNode?.EstimatedCardinality is { } er
                    ? new ColumnValue(ColumnType.Integer64, er)
                    : ColumnValue.Null },
                { "estimated_cost",    physNode?.Cost?.Total is { } ec
                    ? new ColumnValue(ColumnType.Float64, ec)
                    : ColumnValue.Null },
                { "actual_rows",       stats is not null ? new ColumnValue(ColumnType.Integer64, stats.RowsEmitted) : ColumnValue.Null },
                { "rows_read",         stats is not null ? new ColumnValue(ColumnType.Integer64, stats.RowsRead) : ColumnValue.Null },
                { "actual_time_ms",    stats?.ReportedMs is { } ms ? new ColumnValue(ColumnType.Float64, ms) : ColumnValue.Null },
                { "kv_lookups",        stats is not null ? new ColumnValue(ColumnType.Integer64, stats.KvPointLookups) : ColumnValue.Null },
                { "kv_scan_entries",   stats is not null ? new ColumnValue(ColumnType.Integer64, stats.KvScanEntries) : ColumnValue.Null },
                { "estimate_trace",    physNode?.EstimateTrace is { } estimateTrace
                    ? new ColumnValue(ColumnType.String, EstimateTraceJson.Render(estimateTrace))
                    : ColumnValue.Null },
                { "depth",             new ColumnValue(ColumnType.Integer64, depth) },
            });

            // A gather that ran carries per-span actuals: one gather-span row per placement
            // span, so ANALYZE attributes rows to the span (and node) that produced them
            // rather than reporting only the merged stream. A broadcast probe does the same
            // for the spans it probed, under the join that ran it.
            GatherSpanExecution[]? spanExecutions = physNode switch
            {
                GatherNode gather => gather.SpanExecutions,
                _ => physNode?.Stats?.BroadcastSpans,
            };

            if (spanExecutions is not null)
            {
                string spanNode = physNode is GatherNode ? "gather-span" : "broadcast-span";

                foreach (GatherSpanExecution span in spanExecutions)
                {
                    yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
                    {
                        { "stage",           new ColumnValue(ColumnType.String, "analyze") },
                        { "node",            new ColumnValue(ColumnType.String, spanNode) },
                        { "detail",          new ColumnValue(ColumnType.String, DescribeGatherSpan(span)) },
                        { "estimated_rows",  ColumnValue.Null },
                        { "estimated_cost",  ColumnValue.Null },
                        { "actual_rows",     new ColumnValue(ColumnType.Integer64, span.RowsDelivered) },
                        { "rows_read",       span.RemoteRowsScanned is { } remoteScanned
                            ? new ColumnValue(ColumnType.Integer64, remoteScanned)
                            : ColumnValue.Null },
                        { "actual_time_ms",  ColumnValue.Null },
                        { "kv_lookups",      ColumnValue.Null },
                        { "kv_scan_entries", ColumnValue.Null },
                    });
                }
            }
        }

        // Emit one trailing plan-info row with shape and schema-dep metadata when available.
        if (plan.QueryShapeId is not null)
        {
            string schemaDepsStr = plan.SchemaDeps is { Count: > 0 }
                ? "[" + string.Join(", ", plan.SchemaDeps.Select(d => $"{d.TableId}@{d.SchemaVersion}")) + "]"
                : "[]";

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "stage",           new ColumnValue(ColumnType.String, "analyze") },
                { "node",            new ColumnValue(ColumnType.String, "plan-info") },
                { "detail",          new ColumnValue(ColumnType.String,
                    $"shape={plan.QueryShapeId}, schema-deps={schemaDepsStr}" + DescribePlanning(plan, planningMs)) },
                { "estimated_rows",  ColumnValue.Null },
                { "estimated_cost",  ColumnValue.Null },
                { "actual_rows",     ColumnValue.Null },
                { "rows_read",       ColumnValue.Null },
                { "actual_time_ms",  ColumnValue.Null },
                { "kv_lookups",      ColumnValue.Null },
                { "kv_scan_entries", ColumnValue.Null },
            });
        }
    }

    /// <summary>
    /// The planning facts that only EXPLAIN (ANALYZE) appends to the plan-info row:
    /// <c>planning-ms=</c>, the bind and plan time, and on a join plan <c>join-order=</c> (how the
    /// order was chosen, see <see cref="JoinOrderSearchSummary"/>) and <c>join-steps-costed=</c>.
    /// The step count repeats between runs of the same query on the same statistics, so a
    /// regression check can compare it where it cannot compare a time. Plain EXPLAIN output stays
    /// unchanged.
    /// </summary>
    private static string DescribePlanning(QueryPlan plan, double planningMs)
    {
        string detail = ", planning-ms=" + planningMs.ToString("0.###", System.Globalization.CultureInfo.InvariantCulture);

        if (plan.JoinOrderSearch is { } search)
            detail += ", join-order=" + search.Path
                + ", join-steps-costed=" + search.CostedJoinSteps.ToString(System.Globalization.CultureInfo.InvariantCulture);

        return detail;
    }

    /// <summary>
    /// The time printed for <paramref name="node"/>: its own, or for a filter, which runs inside the scan
    /// below it, the time of that scan. Null when the node ran without a timer.
    /// </summary>
    private static double? TimeOf(PhysicalPlanNode? node)
    {
        for (; node is not null; node = node.Input)
        {
            if (node.Stats?.ReportedMs is { } ms)
                return ms;

            if (node is not FilterNode)
                return null;
        }

        return null;
    }

    /// <summary>
    /// Attaches a fresh <see cref="PlanNodeStats"/> to every node of a join plan that can run: the
    /// left chain through <see cref="PhysicalPlanNode.Input"/>, and each right input that is a plan
    /// node of its own (<see cref="RightInputOf"/>), a subtree off that chain. The plan tree is
    /// rebuilt for every execution, so the counters belong to this statement only.
    /// </summary>
    private static void AttachJoinStats(PhysicalPlanNode root)
    {
        for (PhysicalPlanNode? node = root; node is not null; node = node.Input)
        {
            node.Stats = new();

            if (RightInputOf(node) is { } rightInput)
            {
                for (PhysicalPlanNode? right = rightInput; right is not null; right = right.Input)
                    right.Stats = new();
            }
        }
    }

    /// <summary>
    /// The right input of a join that reads its right table through a plan node of its own (a merge
    /// join's ordered input, or the index access path of a hash or nested-loop join); null otherwise.
    /// </summary>
    private static PhysicalPlanNode? RightInputOf(PhysicalPlanNode node) => node switch
    {
        MergeJoinNode merge => merge.RightPhysicalNode,
        HashJoinNode hash => hash.RightPhysicalNode,
        NestedLoopJoinNode nestedLoop => nestedLoop.RightPhysicalNode,
        _ => null,
    };

    /// <summary>
    /// The counters printed on a join row whose right table is read through a node of its own (a
    /// merge join's ordered input, or the index access path of a hash or nested-loop join): the
    /// join's own, plus the storage reads of that input. The plan walk does not render the input,
    /// and a join without one charges its right-side reads to the join row, so these do the same.
    /// Returns a copy; the nodes keep their own counters.
    /// </summary>
    private static PlanNodeStats? WithRightInputReads(PlanNodeStats? joinStats, PhysicalPlanNode rightInput)
    {
        if (joinStats is null)
            return null;

        PlanNodeStats combined = new()
        {
            RowsEmitted = joinStats.RowsEmitted,
            RowsRead = joinStats.RowsRead,
            KvPointLookups = joinStats.KvPointLookups,
            KvScanEntries = joinStats.KvScanEntries,
            ProbeMemoHits = joinStats.ProbeMemoHits,
            ProbeBatches = joinStats.ProbeBatches,
            ElapsedMs = joinStats.ElapsedMs,
            CursorTicks = joinStats.CursorTicks,
            CursorTimed = joinStats.CursorTimed,
            LateralRuns = joinStats.LateralRuns,
            LateralKeptReads = joinStats.LateralKeptReads,
            LateralTicks = joinStats.LateralTicks,
            RanAs = joinStats.RanAs,
            BroadcastSpans = joinStats.BroadcastSpans,
        };

        for (PhysicalPlanNode? right = rightInput; right is not null; right = right.Input)
        {
            if (right.Stats is not { } rightStats)
                continue;

            combined.RowsRead += rightStats.RowsRead;
            combined.KvPointLookups += rightStats.KvPointLookups;
            combined.KvScanEntries += rightStats.KvScanEntries;
        }

        return combined;
    }

    /// <summary>
    /// Appends what only execution knows to a node's detail: <c>ran=</c> when the operator ran a
    /// different strategy than the plan names, <c>probe-memo-hits=</c> when an index nested-loop
    /// join answered probes from its memo, <c>probe-batches=</c> when it probed a unique index in
    /// batches, the three lock counters on the lock step of a locking read, and, on a join the
    /// join-order search costed,
    /// <c>join-order-est-rows=</c>, the search's estimate of the join's output. The last is a plan
    /// fact, but it is shown only here: it is useful only beside <c>actual_rows</c>, and plain
    /// EXPLAIN output stays unchanged.
    ///
    /// <para>A nested-loop join with a <c>LATERAL</c> right side also shows its body:
    /// <c>lateral-runs=</c>, the left rows that ran the body; <c>lateral-kept-reads=</c>, when above zero,
    /// the left rows that read the kept rows of an earlier run with the same left values; and
    /// <c>lateral-ms=</c>, the time of all runs and reads, the right filter included. That time is part of
    /// the join's <c>actual_time_ms</c>.</para>
    /// </summary>
    private static string AppendRuntimeDetail(string detail, PhysicalPlanNode node, PlanNodeStats? stats)
    {
        if (stats?.RanAs is { } ranAs)
            detail += ", ran=" + ranAs;

        if (stats is { ProbeMemoHits: > 0 } memo)
            detail += ", probe-memo-hits=" + memo.ProbeMemoHits.ToString(System.Globalization.CultureInfo.InvariantCulture);

        if (stats is { ProbeBatches: > 0 } batches)
            detail += ", probe-batches=" + batches.ProbeBatches.ToString(System.Globalization.CultureInfo.InvariantCulture);

        if (node is NestedLoopJoinNode { RightSource.Derived.Lateral: not null } && stats is not null)
        {
            detail += ", lateral-runs=" + stats.LateralRuns.ToString(System.Globalization.CultureInfo.InvariantCulture);

            if (stats.LateralKeptReads > 0)
                detail += ", lateral-kept-reads=" + stats.LateralKeptReads.ToString(System.Globalization.CultureInfo.InvariantCulture);

            detail += ", lateral-ms=" + PlanNodeStats.TicksToMs(stats.LateralTicks).ToString("0.###", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (node.JoinOrderEstimatedRows is { } searchRows)
            detail += ", join-order-est-rows=" + searchRows.ToString("0", System.Globalization.CultureInfo.InvariantCulture);

        // Printed also when zero: on the lock step a zero says something (no row was held).
        if (node is LockRowsNode && stats is not null)
            detail += string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $", locked={stats.RowsLocked}, skipped-held={stats.RowsSkippedHeld}, recheck-dropped={stats.RowsDroppedByRecheck}");

        return detail;
    }

    /// <summary>
    /// Renders one span's execution actuals for the ANALYZE <c>gather-span</c> and
    /// <c>broadcast-span</c> rows. The mode
    /// distinguishes the three ways a span can run — local from the start, remote to
    /// completion, remote that failed and resumed locally — because "rows_delivered" alone
    /// cannot: after a fallback the delivered count mixes shipped rows with locally resumed
    /// ones, and <c>rows_shipped</c> is what says how far the remote got.
    /// </summary>
    private static string DescribeGatherSpan(GatherSpanExecution span)
    {
        System.Text.StringBuilder detail = new();
        detail.Append(System.Globalization.CultureInfo.InvariantCulture, $"partition {span.PartitionId}: mode=");

        if (!span.DispatchedRemotely)
            detail.Append("local");
        else if (span.FellBackToLocal)
            detail.Append("remote-fallback");
        else
            detail.Append("remote");

        if (span.RemoteEndpoint is not null)
            detail.Append(System.Globalization.CultureInfo.InvariantCulture, $", node={span.RemoteEndpoint}");

        detail.Append(System.Globalization.CultureInfo.InvariantCulture, $", rows_delivered={span.RowsDelivered}");

        if (span.DispatchedRemotely)
            detail.Append(System.Globalization.CultureInfo.InvariantCulture, $", rows_shipped={span.RemoteRowsShipped}");

        if (span.RemoteRowsScanned is { } remoteScanned)
            detail.Append(System.Globalization.CultureInfo.InvariantCulture, $", remote_rows_scanned={remoteScanned}");

        return detail.ToString();
    }

    private static bool ContainsGather(PhysicalPlanNode node)
    {
        for (PhysicalPlanNode? current = node; current is not null; current = current.Input)
        {
            if (current is GatherNode)
                return true;
        }

        return false;
    }

    /// <param name="traceEstimates">
    /// True to plan with the tracing planners, so every node records how its estimate was made.
    /// Only the <c>ANALYZE</c> path asks for it: the trace is useful next to the actual rows.
    /// </param>
    /// <param name="lockingClause">
    /// The locking clause of a locking read, or null. The bound query gets the checks and the UPDATE
    /// privilege check of <c>SELECT … FOR UPDATE</c>, and the plan gets its lock step, with the same
    /// planner changes the statement gets, so the plan shown is the plan the statement runs.
    /// </param>
    private async Task<QueryPlan> BuildPlanAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket,
        bool traceEstimates = false,
        NodeAst? lockingClause = null)
    {
        SelectQuery selectQuery = selectQueryCreator.CreateSelectQuery(selectAst);

        // The same stages, in the same order, as an executed SELECT — so the plan EXPLAIN shows is
        // the plan the statement would run.
        SelectBindResult bound = await bindPipeline.BindAsync(database, selectQuery, ticket).ConfigureAwait(false);
        BoundSelectQuery boundQuery = bound.Bound;

        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(
            boundQuery, ticket, bound.ExistsRegistry, bound.SemiJoinSpecsOrNull);

        if (lockingClause is not null)
        {
            RowLockingRules.RequireLockableQuery(boundQuery, queryTicket, lockingClause);
            RowLockingRules.RequireUpdatePrivilege(database, boundQuery.PrimaryTable);
            queryTicket.RowLocking = RowLockingRules.PolicyOf(lockingClause);
        }

        if (boundQuery.IsMultiSource)
        {
            IReadOnlyDictionary<string, PlanTimeLeafKeys>? leafKeys =
                await queryExecutor.JoinExecutor.ResolvePlanTimeLeafKeysAsync(database, boundQuery, queryTicket).ConfigureAwait(false);
            return (traceEstimates ? tracingJoinQueryPlanner : joinQueryPlanner).GetPlan(database, boundQuery, queryTicket, leafKeys);
        }

        TableDescriptor table = boundQuery.PrimaryTable;
        return (traceEstimates ? tracingQueryPlanner : queryPlanner).GetPlan(database, table, queryTicket);
    }

    /// <summary>
    /// Plans the operands of every set operation on the main path of <paramref name="plan"/> and hangs
    /// them under its scan node, so EXPLAIN shows what each operand reads
    /// (<see cref="SetOperationOperandNode"/>). Nested set operations get theirs too, because an
    /// operand plan goes through this step as well.
    ///
    /// <para>With <paramref name="analyze"/>, the operand plans also get counters and are handed to the
    /// set operation (<see cref="SetOperationStats"/>), which then runs them instead of planning its
    /// operands again. The counters under each operand are therefore the counters of the rows that
    /// really ran, and the operand row reports how many rows that operand produced.</para>
    ///
    /// <para>Only the chain of <see cref="PhysicalPlanNode.Input"/> is walked, because only that
    /// chain renders one row per node. A set operation on the inner side of a join still runs, but
    /// EXPLAIN shows it as its scan line only.</para>
    /// </summary>
    private void AttachSetOperationOperands(QueryPlan plan, ExecuteSQLTicket ticket, bool traceEstimates, bool analyze)
    {
        for (PhysicalPlanNode? node = plan.Root; node is not null; node = node.Input)
        {
            // A windowed SELECT's derived table gets the window operator's counters.
            if (analyze && node is DerivedTableScanNode { BoundSource.Source.Windows: not null } windowScan)
                windowScan.WindowStats = new WindowStats();

            // A reader of a recursive CTE shows the plans of the two terms, under the recursive UNION that
            // its query passes through. They run inside the reader's own query, which EXPLAIN (ANALYZE)
            // does not count, so they get no counters: a count of zero would be false.
            if (node is DerivedTableScanNode { BoundSource: { SetOperation: null } reader } readerScan
                && BoundSetOperation.PassedThrough(reader.InnerBound) is { Source.WorkTable: not null } recursion)
            {
                QueryPlan nonRecursive = PlanOperand(plan, recursion.Left, recursion.LeftSubqueries, ticket, traceEstimates, analyze: false);
                QueryPlan recursive = PlanOperand(plan, recursion.Right, recursion.RightSubqueries, ticket, traceEstimates, analyze: false);

                readerScan.SetOperationOperands =
                [
                    new SetOperationOperandNode { Side = "left", Plan = nonRecursive, Input = nonRecursive.Root },
                    new SetOperationOperandNode { Side = "right", Plan = recursive, Input = recursive.Root },
                ];
                continue;
            }

            if (node is not DerivedTableScanNode { BoundSource.SetOperation: { } setOperation } scan)
                continue;

            QueryPlan left = PlanOperand(plan, setOperation.Left, setOperation.LeftSubqueries, ticket, traceEstimates, analyze);
            QueryPlan right = PlanOperand(plan, setOperation.Right, setOperation.RightSubqueries, ticket, traceEstimates, analyze);

            SetOperationOperandNode leftNode = new() { Side = "left", Plan = left, Input = left.Root };
            SetOperationOperandNode rightNode = new() { Side = "right", Plan = right, Input = right.Root };

            scan.SetOperationOperands = [leftNode, rightNode];

            if (!analyze)
                continue;

            SetOperationStats stats = new() { LeftPlan = left, RightPlan = right };
            scan.SetOperationStats = stats;
            leftNode.Stats = stats.Left;
            rightNode.Stats = stats.Right;
        }
    }

    /// <summary>
    /// Plans the query of each derived table that <paramref name="plan"/> reads, and of each derived table
    /// those queries read, so EXPLAIN shows their operators (<see cref="ExplainedDerivedTable"/>). A derived
    /// table on the <see cref="PhysicalPlanNode.Input"/> chain shows the plan under its own line; one on the
    /// right side of a join gets a line of its own under the join (<see cref="PhysicalPlanNode.ExplainedRightScan"/>).
    ///
    /// <para>Each query is planned as the derived-table executor plans it when it runs, with the plan-time
    /// lookup of small dimension leaves, so the plan shown is the plan that runs. With
    /// <paramref name="analyze"/> the plans get counters, and the executor runs them in place of a new plan.</para>
    ///
    /// <para>Not planned here: a LATERAL body (it binds and runs for each left row), a set operation (see
    /// <see cref="AttachSetOperationOperands"/>), the reader of a recursive CTE, a work table and a function in
    /// FROM, which have no query of their own. A derived table inside a set-operation operand or inside a
    /// subquery keeps its single line.</para>
    /// </summary>
    private async Task AttachDerivedPlansAsync(
        QueryPlan plan, ExecuteSQLTicket ticket, bool traceEstimates, bool analyze, ExplainedDerivedTables registry)
    {
        plan.ExplainedDerivedTables = registry;

        for (PhysicalPlanNode? node = plan.Root; node is not null; node = node.Input)
        {
            if (node is DerivedTableScanNode scan && HasQueryToExplain(scan.BoundSource))
                scan.Explained = await ExplainDerivedAsync(plan, scan.BoundSource, ticket, traceEstimates, analyze, registry).ConfigureAwait(false);

            if (RightDerivedSourceOf(node) is { } right && HasQueryToExplain(right))
            {
                ExplainedDerivedTable explained = await ExplainDerivedAsync(plan, right, ticket, traceEstimates, analyze, registry).ConfigureAwait(false);

                node.ExplainedRightScan = new DerivedTableScanNode
                {
                    BoundSource = right,
                    Explained = explained,
                    Stats = analyze ? explained.ScanStats : null,
                };
            }
        }
    }

    /// <summary>The derived table on the right side of a join node, or null for any other node.</summary>
    private static BoundDerivedTableSource? RightDerivedSourceOf(PhysicalPlanNode node) => node switch
    {
        NestedLoopJoinNode nestedLoop => nestedLoop.RightSource.Derived,
        HashJoinNode hash => hash.BuildSource.Derived,
        MergeJoinNode merge => merge.RightSource.Derived,
        _ => null,
    };

    /// <summary>
    /// True when <paramref name="source"/> runs a query of its own through the derived-table executor, which
    /// can then run a plan that EXPLAIN built. See <see cref="AttachDerivedPlansAsync"/> for the kinds left out.
    /// </summary>
    private static bool HasQueryToExplain(BoundDerivedTableSource source) =>
        source.Lateral is null
        && source.SetOperation is null
        && source.Source.Function is null
        && source.Source.WorkTable is null
        && BoundSetOperation.PassedThrough(source.InnerBound) is not { Source.WorkTable: not null };

    /// <summary>
    /// The entry of <paramref name="source"/>: a new plan of its query, or, for a reader of a CTE that the
    /// statement computes once, the entry of the first reader, which alone shows the plan.
    /// </summary>
    private async Task<ExplainedDerivedTable> ExplainDerivedAsync(
        QueryPlan parent,
        BoundDerivedTableSource source,
        ExecuteSQLTicket ticket,
        bool traceEstimates,
        bool analyze,
        ExplainedDerivedTables registry)
    {
        if (registry.For(source) is { } known)
            return known;

        if (source.Source.ComputedOnce is { } marker && registry.ForComputedOnce(marker) is { } shared)
        {
            registry.Add(source, shared);
            return shared;
        }

        BoundSelectQuery innerBound = source.InnerBound;
        QueryTicket outer = parent.Ticket;

        // The ticket the derived-table executor gives the query when it runs.
        ExecuteSQLTicket executeTicket = new(
            txnState: ticket.TxnState,
            database: outer.DatabaseName,
            sql: "",
            parameters: outer.Parameters,
            cancellationToken: outer.CancellationToken,
            probe: outer.Probe);

        QueryTicket innerTicket = QueryTicketAdapter.ToQueryTicket(
            innerBound, executeTicket, source.InnerSubqueries.ExistsRegistry, source.InnerSubqueries.SemiJoinSpecs);

        QueryPlan innerPlan;

        if (innerBound.IsMultiSource)
        {
            IReadOnlyDictionary<string, PlanTimeLeafKeys>? leafKeys = await queryExecutor.JoinExecutor
                .ResolvePlanTimeLeafKeysAsync(parent.Database, innerBound, innerTicket).ConfigureAwait(false);
            innerPlan = (traceEstimates ? tracingJoinQueryPlanner : joinQueryPlanner).GetPlan(parent.Database, innerBound, innerTicket, leafKeys);
        }
        else
        {
            innerPlan = (traceEstimates ? tracingQueryPlanner : queryPlanner).GetPlan(parent.Database, innerBound.PrimaryTable, innerTicket);
        }

        ExplainedDerivedTable explained = new(innerPlan, source, analyze);
        registry.Add(source, explained);

        AttachSetOperationOperands(innerPlan, ticket, traceEstimates, analyze);
        await AttachDerivedPlansAsync(innerPlan, ticket, traceEstimates, analyze, registry).ConfigureAwait(false);

        if (analyze)
        {
            innerPlan.CollectRuntimeStats = true;

            if (innerPlan.BoundQuery is not null)
                AttachJoinStats(innerPlan.Root);
        }

        return explained;
    }

    /// <summary>
    /// Plans one operand with the ticket the set operation would give it when it runs
    /// (<see cref="SetOperationExecutor"/>). The plan-time leaf lookup is skipped: it reads rows, and
    /// a plan built without it is still the plan of a valid execution.
    /// </summary>
    private QueryPlan PlanOperand(
        QueryPlan parent,
        BoundSelectQuery operand,
        SubqueryBindProducts subqueries,
        ExecuteSQLTicket ticket,
        bool traceEstimates,
        bool analyze)
    {
        QueryTicket outer = parent.Ticket;

        ExecuteSQLTicket executeTicket = new(
            txnState: ticket.TxnState,
            database: outer.DatabaseName,
            sql: "",
            parameters: outer.Parameters,
            cancellationToken: outer.CancellationToken,
            probe: outer.Probe);

        QueryTicket operandTicket = QueryTicketAdapter.ToQueryTicket(
            operand, executeTicket, subqueries.ExistsRegistry, subqueries.SemiJoinSpecs,
            exclusivePredicateLocks: outer.ExclusivePredicateLocks);

        QueryPlan plan = operand.IsMultiSource
            ? (traceEstimates ? tracingJoinQueryPlanner : joinQueryPlanner).GetPlan(parent.Database, operand, operandTicket, null)
            : (traceEstimates ? tracingQueryPlanner : queryPlanner).GetPlan(parent.Database, operand.PrimaryTable, operandTicket);

        AttachSetOperationOperands(plan, ticket, traceEstimates, analyze);

        if (analyze)
        {
            plan.CollectRuntimeStats = true;

            if (plan.BoundQuery is not null)
                AttachJoinStats(plan.Root);
        }

        return plan;
    }

    /// <summary>
    /// Returns the plan nodes in the same depth-first order that <see cref="PlanRenderer.WalkNodes"/>
    /// visits them (parent before children), so they can be zipped with the rendered output.
    /// </summary>
    private static List<PhysicalPlanNode> WalkNodesOrdered(PhysicalPlanNode root)
    {
        var result = new List<PhysicalPlanNode>();
        WalkNodesOrderedInner(root, result);
        return result;
    }

    private static void WalkNodesOrderedInner(PhysicalPlanNode node, List<PhysicalPlanNode> result)
    {
        result.Add(node);

        // The same children, in the same order, as the rendered lines (PlanRenderer.ExtraChildren). The
        // plan of each child is not needed here: only the nodes are zipped with the lines.
        foreach ((PhysicalPlanNode child, QueryPlan _) in PlanRenderer.ExtraChildren(node, null!))
            WalkNodesOrderedInner(child, result);

        if (node.Input is not null)
            WalkNodesOrderedInner(node.Input, result);
    }

    /// <summary>
    /// Renders the fixed plan shape for a FROM-less SELECT: a single-row <c>constant-source</c>
    /// feeding a <c>project</c> that lists the output column names, plus a <c>limit</c> row
    /// when <c>LIMIT</c>/<c>OFFSET</c> is present. There is no cost model here — the source always
    /// produces exactly one row — so <c>estimated_rows</c> is a static 1/0 and cost is null.
    /// As for every other plan, the rows come parent first: <c>limit</c>, <c>project</c>, then
    /// <c>constant-source</c>, each one level deeper than the row before it.
    /// </summary>
    private static IEnumerable<QueryResultRow> RenderFromlessPlan(NodeAst selectAst, string stage)
    {
        List<NodeAst> projections = new();
        FlattenProjectionList(selectAst.leftAst!, projections);

        List<string> names = new(projections.Count);
        for (int i = 0; i < projections.Count; i++)
            names.Add(QueryProjectionResolver.GetOutputNameFromProjectionExpression(projections[i], i));

        int depth = 0;

        if (selectAst.extendedThree is not null || selectAst.extendedFour is not null)
            yield return FromlessRow(stage, "limit", DescribeLimitOffset(selectAst), null, depth++);

        yield return FromlessRow(stage, "project", "columns: " + string.Join(", ", names), 1, depth++);
        yield return FromlessRow(stage, "constant-source", "1 row", 1, depth);
    }

    private static QueryResultRow FromlessRow(string stage, string node, string detail, long? estimatedRows, int depth)
    {
        return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "stage",          new ColumnValue(ColumnType.String, stage) },
            { "node",           new ColumnValue(ColumnType.String, node) },
            { "detail",         new ColumnValue(ColumnType.String, detail) },
            { "estimated_rows", estimatedRows is not null
                ? new ColumnValue(ColumnType.Integer64, estimatedRows.Value)
                : ColumnValue.Null },
            { "estimated_cost", ColumnValue.Null },
            { "depth",          new ColumnValue(ColumnType.Integer64, depth) },
        });
    }

    private static string DescribeLimitOffset(NodeAst selectAst)
    {
        string limit = selectAst.extendedThree is not null ? (selectAst.extendedThree.yytext ?? "?") : "none";
        string offset = selectAst.extendedFour is not null ? (selectAst.extendedFour.yytext ?? "?") : "0";
        return $"limit={limit}, offset={offset}";
    }

    /// <summary>Flattens the left-recursive <see cref="NodeType.IdentifierList"/> projection chain in order.</summary>
    private static void FlattenProjectionList(NodeAst ast, List<NodeAst> projections)
    {
        if (ast.nodeType == NodeType.IdentifierList)
        {
            if (ast.leftAst is not null)
                FlattenProjectionList(ast.leftAst, projections);
            if (ast.rightAst is not null)
                FlattenProjectionList(ast.rightAst, projections);
            return;
        }

        projections.Add(ast);
    }
}
