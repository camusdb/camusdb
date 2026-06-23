
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
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
/// KNOWN LIMITATION — subquery materialization during EXPLAIN (R3 conscious decision):
/// The prepare pipeline calls SubqueryRewriter.RewriteSelectQueryAsync (which executes
/// scalar and IN/NOT-IN subqueries against storage to fold them into literals) and
/// ExistsSubqueryPreparer.PrepareAsync (which may run uncorrelated EXISTS checks).
/// As a result, EXPLAIN SELECT ... WHERE x IN (SELECT ...) performs real storage reads
/// for the inner subquery, even though the outer table is never scanned.
///
/// This is reads-only (no mutation) and affects only queries that contain subqueries.
/// A plan-only mode that leaves subquery nodes symbolic is deferred until the subquery
/// infrastructure grows a dry-run flag (candidate for R6 documentation and a future task).
/// </summary>
internal sealed class ExplainExecutor
{
    private readonly SelectQueryCreator selectQueryCreator = new();

    private readonly SubqueryRewriter subqueryRewriter;

    private readonly SemiJoinAnalyzer? semiJoinAnalyzer;

    private readonly QueryBinder queryBinder;

    private readonly ExistsSubqueryPreparer existsSubqueryPreparer;

    private readonly QueryPlanner queryPlanner;

    private readonly JoinQueryPlanner joinQueryPlanner;

    private readonly QueryExecutor queryExecutor;

    public ExplainExecutor(
        SubqueryRewriter subqueryRewriter,
        QueryBinder queryBinder,
        ExistsSubqueryPreparer existsSubqueryPreparer,
        QueryExecutor queryExecutor,
        StatisticsManager? stats = null,
        SemiJoinAnalyzer? semiJoinAnalyzer = null)
    {
        this.subqueryRewriter = subqueryRewriter;
        this.semiJoinAnalyzer = semiJoinAnalyzer;
        this.queryBinder = queryBinder;
        this.existsSubqueryPreparer = existsSubqueryPreparer;
        this.queryExecutor = queryExecutor;
        queryPlanner = new QueryPlanner(stats);
        joinQueryPlanner = new JoinQueryPlanner(stats);
    }

    /// <summary>
    /// Builds the query plan for the given SELECT AST and yields one result row per node.
    /// </summary>
    /// <param name="database">Open database descriptor.</param>
    /// <param name="selectAst">The inner SELECT AST wrapped by the EXPLAIN node.</param>
    /// <param name="ticket">The originating execution ticket (provides parameters and txn).</param>
    /// <param name="stage">The stage label to emit in each row ("physical" or "logical").</param>
    public async IAsyncEnumerable<QueryResultRow> ExplainQuery(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket,
        string stage)
    {
        QueryPlan plan = await BuildPlanAsync(database, selectAst, ticket).ConfigureAwait(false);

        // Zip rendered (name, detail) pairs with plan nodes so we can emit R9 cost estimates.
        List<PhysicalPlanNode> orderedNodes = WalkNodesOrdered(plan.Root);
        List<(string Name, string Detail)> rendered = PlanRenderer.WalkNodes(plan.Root, plan).ToList();

        for (int i = 0; i < rendered.Count; i++)
        {
            (string name, string detail) = rendered[i];
            PhysicalPlanNode? physNode = i < orderedNodes.Count ? orderedNodes[i] : null;

            long? estRows = physNode?.EstimatedCardinality;
            double? estCost = physNode?.Cost?.Total;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>
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
            });
        }

        // Emit one trailing plan-info row with shape and schema-dep metadata when available.
        if (plan.QueryShapeId is not null)
        {
            string schemaDepsStr = plan.SchemaDeps is { Count: > 0 }
                ? "[" + string.Join(", ", plan.SchemaDeps.Select(d => $"{d.TableName}@{d.SchemaVersion}")) + "]"
                : "[]";

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>
            {
                { "stage",          new ColumnValue(ColumnType.String, stage) },
                { "node",           new ColumnValue(ColumnType.String, "plan-info") },
                { "detail",         new ColumnValue(ColumnType.String, $"shape={plan.QueryShapeId}, schema-deps={schemaDepsStr}") },
                { "estimated_rows", ColumnValue.Null },
                { "estimated_cost", ColumnValue.Null },
            });
        }
    }

    /// <summary>
    /// Executes the query for real (draining the full cursor), collects runtime counters per
    /// physical plan node, then yields one result row per node with actual stats.
    ///
    /// The result row schema extends the plain EXPLAIN schema with four actual columns:
    /// <c>actual_rows INT64</c>, <c>actual_time_ms FLOAT64</c>, <c>kv_lookups INT64</c>,
    /// <c>kv_scan_entries INT64</c>. <c>actual_time_ms</c> is the total wall-clock milliseconds
    /// for the whole plan (reported on the root node only; zero for inner nodes).
    ///
    /// KNOWN LIMITATION — join plans are not supported (R5 conscious decision):
    /// <see cref="QueryExecutor.ExecuteQueryPlan"/> runs only the linear <see cref="QueryPlan.Steps"/>
    /// pipeline. <see cref="QueryPlanStepAdapter"/> emits no step for join nodes
    /// (NestedLoopJoinNode, IndexNestedLoopJoinNode, DerivedTableScanNode), so executing a join
    /// plan through this path silently runs only the left-side scan and reports wrong counts.
    /// Rather than produce misleading stats, this method throws for join queries.
    /// Full join instrumentation requires carrying stats through QueryJoinExecutor (deferred to R7).
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> ExplainAnalyzeQuery(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket)
    {
        QueryPlan plan = await BuildPlanAsync(database, selectAst, ticket).ConfigureAwait(false);

        if (plan.BoundQuery is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "EXPLAIN (ANALYZE) is not yet supported for JOIN queries. " +
                "Use plain EXPLAIN to inspect the join plan (R7 will add join instrumentation).");

        plan.CollectRuntimeStats = true;

        IAsyncEnumerable<QueryResultRow> cursor = queryExecutor.ExecuteQueryPlan(plan);

        Stopwatch sw = Stopwatch.StartNew();
        await foreach (QueryResultRow _ in cursor.ConfigureAwait(false)) { }
        sw.Stop();

        // Assign total elapsed time to the root node's stats.
        if (plan.Root.Stats is null)
            plan.Root.Stats = new();
        plan.Root.Stats.ElapsedMs = sw.Elapsed.TotalMilliseconds;

        // Zip the rendered (name, detail) pairs with the node objects in the same DFS order.
        List<PhysicalPlanNode> orderedNodes = WalkNodesOrdered(plan.Root);
        List<(string Name, string Detail)> rendered = PlanRenderer.WalkNodes(plan.Root, plan).ToList();

        for (int i = 0; i < rendered.Count; i++)
        {
            (string name, string detail) = rendered[i];
            PhysicalPlanNode? physNode = i < orderedNodes.Count ? orderedNodes[i] : null;

            // FilterNode is folded into the scan (ExecutionFilter); it never appears in StepNodes
            // and has no Stats of its own. Attribute its actual_rows from the child scan's
            // RowsEmitted — that count is already post-filter (the scan only yields passing rows).
            // KV counters are 0: predicate evaluation adds no storage reads.
            PlanNodeStats? stats;
            bool isFilter = physNode is FilterNode;
            if (isFilter)
                stats = physNode?.Input?.Stats is { } childStats
                    ? new PlanNodeStats { RowsEmitted = childStats.RowsEmitted }
                    : null;
            else
                stats = physNode?.Stats;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>
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
                { "actual_time_ms",    stats?.ElapsedMs is { } ms ? new ColumnValue(ColumnType.Float64, ms) : ColumnValue.Null },
                { "kv_lookups",        stats is not null ? new ColumnValue(ColumnType.Integer64, stats.KvPointLookups) : ColumnValue.Null },
                { "kv_scan_entries",   stats is not null ? new ColumnValue(ColumnType.Integer64, stats.KvScanEntries) : ColumnValue.Null },
            });
        }

        // Emit one trailing plan-info row with shape and schema-dep metadata when available.
        if (plan.QueryShapeId is not null)
        {
            string schemaDepsStr = plan.SchemaDeps is { Count: > 0 }
                ? "[" + string.Join(", ", plan.SchemaDeps.Select(d => $"{d.TableName}@{d.SchemaVersion}")) + "]"
                : "[]";

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>
            {
                { "stage",           new ColumnValue(ColumnType.String, "analyze") },
                { "node",            new ColumnValue(ColumnType.String, "plan-info") },
                { "detail",          new ColumnValue(ColumnType.String, $"shape={plan.QueryShapeId}, schema-deps={schemaDepsStr}") },
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

    private async Task<QueryPlan> BuildPlanAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        ExecuteSQLTicket ticket)
    {
        SelectQuery selectQuery = selectQueryCreator.CreateSelectQuery(selectAst);

        // Extract semi/anti-join specs before SubqueryRewriter materialises IN/NOT IN.
        List<SemiJoinSpec> semiJoinSpecs = [];
        if (semiJoinAnalyzer is not null)
        {
            (selectQuery, semiJoinSpecs) = await semiJoinAnalyzer
                .AnalyzeAsync(database, selectQuery, ticket)
                .ConfigureAwait(false);
        }

        selectQuery = await subqueryRewriter
            .RewriteSelectQueryAsync(database, selectQuery, ticket)
            .ConfigureAwait(false);

        BoundSelectQuery boundQuery = await queryBinder
            .BindAsync(database, selectQuery)
            .ConfigureAwait(false);

        (selectQuery, ExistsSubqueryRegistry? existsRegistry) = await existsSubqueryPreparer
            .PrepareAsync(
                database,
                selectQuery,
                boundQuery.Sources,
                boundQuery.DerivedSources,
                ticket)
            .ConfigureAwait(false);

        boundQuery = new BoundSelectQuery(
            selectQuery,
            boundQuery.Sources,
            boundQuery.RowNames,
            boundQuery.DerivedSources);

        IReadOnlyList<SemiJoinSpec>? specs = semiJoinSpecs.Count > 0 ? semiJoinSpecs : null;
        QueryTicket queryTicket = QueryTicketAdapter.ToQueryTicket(boundQuery, ticket, existsRegistry, specs);

        if (boundQuery.IsMultiSource)
            return joinQueryPlanner.GetPlan(database, boundQuery, queryTicket);

        TableDescriptor table = boundQuery.PrimaryTable;
        return queryPlanner.GetPlan(database, table, queryTicket);
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
        if (node.Input is not null)
            WalkNodesOrderedInner(node.Input, result);
    }
}
