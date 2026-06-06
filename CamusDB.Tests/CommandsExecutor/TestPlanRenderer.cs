
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="PlanRenderer"/> — verifies exact plan-string output for all acceptance
/// criteria specified in QUERY_PLANNER_REMAINING.md §R1.
///
/// Scope note: these tests exercise SelectQueryCreator → QueryTicketAdapter → QueryPlanner.GetPlan →
/// PlanRenderer only. That path intentionally skips the QueryBinder's column-existence validation, so
/// queries that reference columns absent from the fixture schema (e.g. "role" in GROUP BY / ORDER BY
/// tests — the robots table has no such column) are accepted here but would be rejected in a fully
/// bound end-to-end flow. Do not treat these as end-to-end coverage of column resolution or schema
/// validation; see TestExecuteSqlSelect for that.
/// </summary>
[NonParallelizable]
public sealed class TestPlanRenderer
{
    private static QueryPlannerTestContext? context;
    private readonly QueryPlanner queryPlanner = new();

    [OneTimeSetUp]
    public void OneTimeSetUp() => context = QueryPlannerTestContext.Create();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (context is not null)
        {
            await context.DisposeAsync().ConfigureAwait(false);
            context = null;
        }
    }

    private static QueryTicket Ticket(string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(ast);

        ExecuteSQLTicket executeTicket = new(
            txnState: context!.Txn,
            database: QueryPlannerTestContext.DatabaseName,
            sql: sql,
            parameters: parameters);

        return QueryTicketAdapter.ToQueryTicket(query, executeTicket);
    }

    private QueryPlan Plan(string sql, Dictionary<string, ColumnValue>? parameters = null) =>
        queryPlanner.GetPlan(context!.Database, context.Table, Ticket(sql, parameters));

    // ── full table scan ────────────────────────────────────────────────────

    [Test]
    public void Render_FullTableScan()
    {
        QueryPlan plan = Plan("SELECT * FROM robots");
        string rendered = PlanRenderer.Render(plan);
        Assert.AreEqual("table-scan(table=robots)", rendered);
    }

    // ── forced-index full scan ─────────────────────────────────────────────

    [Test]
    public void Render_ForcedIndexScan()
    {
        QueryPlan plan = Plan("SELECT id FROM robots@{FORCE_INDEX=year_idx}");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("table-scan(table=robots, forced-index=year_idx)"));
        Assert.That(rendered, Does.Contain("project"));
    }

    // ── equality index lookup ──────────────────────────────────────────────

    [Test]
    public void Render_IndexLookup()
    {
        QueryPlan plan = Plan(
            "SELECT * FROM robots WHERE id = @id",
            new() { { "@id", new ColumnValue(ColumnType.Id, QueryPlannerTestContext.SampleRowId) } });

        string rendered = PlanRenderer.Render(plan);
        Assert.AreEqual($"index-lookup(index={CamusDBConfig.PrimaryKeyInternalName}, key={QueryPlannerTestContext.SampleRowId})", rendered);
    }

    // ── range index scan ───────────────────────────────────────────────────

    [Test]
    public void Render_IndexRangeScan_BoundedRange()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year >= 2001 AND year < 2005");
        string rendered = PlanRenderer.Render(plan);
        Assert.AreEqual("index-range-scan(index=year_idx, from>=2001, to<2005)", rendered);
    }

    [Test]
    public void Render_IndexRangeScan_EqualityAsSingletonRange()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year = 2000");
        string rendered = PlanRenderer.Render(plan);
        // Equality on a non-unique index becomes [2000, 2001)
        Assert.That(rendered, Does.StartWith("index-range-scan(index=year_idx,"));
        Assert.That(rendered, Does.Contain("from>=2000"));
        Assert.That(rendered, Does.Contain("to<2001"));
    }

    [Test]
    public void Render_IndexRangeScan_LowerBoundOnly()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year > 2020");
        string rendered = PlanRenderer.Render(plan);
        Assert.AreEqual("index-range-scan(index=year_idx, from>2020)", rendered);
    }

    // ── scan + filter ──────────────────────────────────────────────────────

    [Test]
    public void Render_ScanPlusFilter()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year = 2000 OR year = 2001");
        string rendered = PlanRenderer.Render(plan);
        StringAssert.StartsWith("filter(", rendered);
        Assert.That(rendered, Does.Contain("table-scan(table=robots)"));
    }

    // ── aggregate ─────────────────────────────────────────────────────────

    [Test]
    public void Render_GlobalAggregate_CountStar()
    {
        QueryPlan plan = Plan("SELECT COUNT(*) FROM robots");
        string rendered = PlanRenderer.Render(plan);
        // plan = project > aggregate > table-scan
        Assert.That(rendered, Does.Contain("aggregate(aggs=[count(*)])"));
        Assert.That(rendered, Does.Contain("table-scan(table=robots)"));
    }

    [Test]
    public void Render_GroupedAggregate()
    {
        QueryPlan plan = Plan("SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role ORDER BY role");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("aggregate(group=[role], aggs=[count(*)])"));
        Assert.That(rendered, Does.Contain("sort(role ASC)"));
    }

    // ── sort ───────────────────────────────────────────────────────────────

    [Test]
    public void Render_SortNode_MultiColumn()
    {
        // year, name ORDER BY: year_idx is used but name requires a separate sort
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year, name");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("sort(year ASC, name ASC)"));
    }

    [Test]
    public void Render_SortNodeDesc()
    {
        // Descending ORDER BY cannot be satisfied by an ascending index, so SortNode is emitted.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year DESC");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("sort(year DESC)"));
    }

    // ── limit ─────────────────────────────────────────────────────────────

    [Test]
    public void Render_LimitOnly()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year = 2000 OR year = 2001 ORDER BY name LIMIT 10");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("limit(10)"));
    }

    [Test]
    public void Render_LimitWithOffset()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year = 2000 OR year = 2001 ORDER BY name LIMIT 10 OFFSET 5");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("limit(10 offset 5)"));
    }

    // ── distinct ──────────────────────────────────────────────────────────

    [Test]
    public void Render_Distinct()
    {
        QueryPlan plan = Plan("SELECT DISTINCT enabled FROM robots ORDER BY enabled LIMIT 2");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("distinct"));
    }

    // ── having-filter ─────────────────────────────────────────────────────

    [Test]
    public void Render_HavingFilter()
    {
        QueryPlan plan = Plan(
            "SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role HAVING cnt > 0 ORDER BY role");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("having-filter("));
    }

    // ── project ───────────────────────────────────────────────────────────

    [Test]
    public void Render_ProjectNode()
    {
        QueryPlan plan = Plan("SELECT id, name FROM robots WHERE year = 2000");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("project"));
        Assert.That(rendered, Does.Contain("index-range-scan(index=year_idx,"));
    }

    // ── sort elision (no SortNode emitted) ────────────────────────────────

    [Test]
    public void Render_SortElision_NoSortNodeInString()
    {
        // ORDER BY year on year_idx should elide the sort
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year >= 2020 ORDER BY year LIMIT 5");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Not.Contain("sort("));
        Assert.That(rendered, Does.Contain("index-range-scan(index=year_idx,"));
        Assert.That(rendered, Does.Contain("limit(5)"));
    }

    // ── includeRequiredColumns flag ────────────────────────────────────────

    [Test]
    public void Render_IncludeRequiredColumns()
    {
        QueryPlan plan = Plan("SELECT id FROM robots");
        string rendered = PlanRenderer.Render(plan, includeRequiredColumns: true);
        Assert.That(rendered, Does.Contain("cols=[id]"));
    }

    [Test]
    public void Render_ExcludeRequiredColumnsByDefault()
    {
        QueryPlan plan = Plan("SELECT id FROM robots");
        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Not.Contain("cols="));
    }

    // ── determinism ───────────────────────────────────────────────────────

    [Test]
    public void Render_IsDeterministic()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year >= 2001 AND year < 2005");
        string first = PlanRenderer.Render(plan);
        string second = PlanRenderer.Render(plan);
        Assert.AreEqual(first, second);
    }

    // ── no execution-behavior change: existing planner tests ──────────────

    [Test]
    public void Render_DoesNotAlterPlanSteps()
    {
        QueryPlan plan = Plan("SELECT COUNT(*) FROM robots");
        _ = PlanRenderer.Render(plan);
        // Steps must still be intact
        Assert.IsTrue(plan.Steps.Count > 0);
        Assert.IsNotNull(plan.Root);
    }

    // ── join plans (hand-constructed) ─────────────────────────────────────

    /// <summary>
    /// Verifies NestedLoopJoinNode rendering: join node appears first (parent), left scan below it,
    /// and the right alias is embedded in the join detail. The child wiring is:
    ///   NestedLoopJoinNode.Input  = left PhysicalPlanNode
    ///   NestedLoopJoinNode.RightSource = alias/table for the right side (scanned inline at runtime)
    /// </summary>
    [Test]
    public void Render_NestedLoopJoin_ParentBeforeChild()
    {
        // Build two minimal table sources reusing the robots descriptor with different aliases.
        TableDescriptor leftTable = context!.Table;
        TableDescriptor rightTable = context.Table;   // same schema, aliased differently

        BoundTableSource leftSource = new(
            new TableSource("robots", "r"),
            leftTable,
            "r");

        BoundTableSource rightTableSource = new(
            new TableSource("posts", "p"),
            rightTable,
            "p");

        BoundJoinRightSource rightSource = BoundJoinRightSource.FromTable(rightTableSource);

        // ON p.user_id = r.id
        NodeAst onPredicate = new(
            NodeType.ExprEquals,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "p.user_id"),
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "r.id"),
            null, null, null, null, null, null);

        PhysicalPlanNode leftScan = new TableScanNode(TableScanSource.PrimaryRows)
        {
            BoundSource = leftSource,
        };

        NestedLoopJoinNode joinNode = new(leftScan, rightSource, onPredicate);

        QueryPlan plan = BuildJoinPlan(joinNode, leftTable);
        string rendered = PlanRenderer.Render(plan);

        // Parent (join) line must come BEFORE child (scan) line.
        int joinPos = rendered.IndexOf("nested-loop-join(", StringComparison.Ordinal);
        int scanPos = rendered.IndexOf("table-scan(", StringComparison.Ordinal);
        Assert.Greater(joinPos, -1, "Expected nested-loop-join in output");
        Assert.Greater(scanPos, -1, "Expected table-scan in output");
        Assert.Less(joinPos, scanPos, "Join node must appear before its left child");

        // Join detail includes the ON predicate and right alias.
        Assert.That(rendered, Does.Contain("on=p.user_id = r.id"));
        Assert.That(rendered, Does.Contain("right=p"));

        // Left child is the left table scan.
        Assert.That(rendered, Does.Contain("table-scan(table=robots)"));
    }

    [Test]
    public void Render_NestedLoopJoin_ExactString()
    {
        BoundTableSource rightTableSource = new(
            new TableSource("posts", "p"),
            context!.Table,
            "p");

        BoundJoinRightSource rightSource = BoundJoinRightSource.FromTable(rightTableSource);

        NodeAst onPredicate = new(
            NodeType.ExprEquals,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "p.user_id"),
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "r.id"),
            null, null, null, null, null, null);

        BoundTableSource leftSource = new(
            new TableSource("robots", "r"),
            context.Table,
            "r");

        PhysicalPlanNode leftScan = new TableScanNode(TableScanSource.PrimaryRows)
        {
            BoundSource = leftSource,
        };

        NestedLoopJoinNode joinNode = new(leftScan, rightSource, onPredicate);
        QueryPlan plan = BuildJoinPlan(joinNode, context.Table);

        string rendered = PlanRenderer.Render(plan);
        string expected =
            "nested-loop-join(on=p.user_id = r.id, right=p)\n" +
            "  table-scan(table=robots)";

        Assert.AreEqual(expected, rendered);
    }

    /// <summary>
    /// Verifies IndexNestedLoopJoinNode rendering: the index name, left lookup column,
    /// and right index column all appear in the detail. The child wiring is:
    ///   IndexNestedLoopJoinNode.Input = left PhysicalPlanNode
    ///   Index / LeftLookupColumn / RightIndexColumn describe the index probe on the right side.
    /// </summary>
    [Test]
    public void Render_IndexNestedLoopJoin_ExactString()
    {
        TableIndexSchema probeIndex = new("user_id_idx", ["user_id"], IndexType.Multi);

        BoundTableSource rightTableSource = new(
            new TableSource("posts", "p"),
            context!.Table,
            "p");

        NodeAst onPredicate = new(
            NodeType.ExprEquals,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "p.user_id"),
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "r.id"),
            null, null, null, null, null, null);

        BoundTableSource leftSource = new(
            new TableSource("robots", "r"),
            context.Table,
            "r");

        PhysicalPlanNode leftScan = new TableScanNode(TableScanSource.PrimaryRows)
        {
            BoundSource = leftSource,
        };

        IndexNestedLoopJoinNode joinNode = new(
            leftScan,
            rightTableSource,
            onPredicate,
            probeIndex,
            leftLookupColumn: "r.id",
            rightIndexColumn: "p.user_id");

        QueryPlan plan = BuildJoinPlan(joinNode, context.Table);

        string rendered = PlanRenderer.Render(plan);
        string expected =
            "index-nested-loop-join(on=p.user_id = r.id, index=user_id_idx, left=r.id, right=p.user_id)\n" +
            "  table-scan(table=robots)";

        Assert.AreEqual(expected, rendered);
    }

    [Test]
    public void Render_IndexNestedLoopJoin_ParentBeforeChild()
    {
        TableIndexSchema probeIndex = new("year_idx", ["year"], IndexType.Multi);

        BoundTableSource rightTableSource = new(
            new TableSource("posts", "p"),
            context!.Table,
            "p");

        NodeAst onPredicate = new(
            NodeType.ExprEquals,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "p.year"),
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "r.year"),
            null, null, null, null, null, null);

        BoundTableSource leftSource = new(
            new TableSource("robots", "r"),
            context.Table,
            "r");

        PhysicalPlanNode leftScan = new TableScanNode(TableScanSource.PrimaryRows)
        {
            BoundSource = leftSource,
        };

        IndexNestedLoopJoinNode joinNode = new(
            leftScan,
            rightTableSource,
            onPredicate,
            probeIndex,
            leftLookupColumn: "r.year",
            rightIndexColumn: "p.year");

        QueryPlan plan = BuildJoinPlan(joinNode, context.Table);
        string rendered = PlanRenderer.Render(plan);

        int joinPos = rendered.IndexOf("index-nested-loop-join(", StringComparison.Ordinal);
        int scanPos = rendered.IndexOf("table-scan(", StringComparison.Ordinal);
        Assert.Greater(joinPos, -1, "Expected index-nested-loop-join in output");
        Assert.Greater(scanPos, -1, "Expected table-scan in output");
        Assert.Less(joinPos, scanPos, "Join node must appear before its left child");
    }

    [Test]
    public void Render_NestedLoopJoin_WithRightExecutionFilter()
    {
        BoundTableSource rightTableSource = new(
            new TableSource("posts", "p"),
            context!.Table,
            "p");

        BoundJoinRightSource rightSource = BoundJoinRightSource.FromTable(rightTableSource);

        NodeAst onPredicate = new(
            NodeType.ExprEquals,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "p.user_id"),
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "r.id"),
            null, null, null, null, null, null);

        // pushed-down right-side predicate: p.published = true
        NodeAst rightFilter = new(
            NodeType.ExprEquals,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "p.published"),
            new NodeAst(NodeType.Bool, null, null, null, null, null, null, null, "true"),
            null, null, null, null, null, null);

        BoundTableSource leftSource = new(
            new TableSource("robots", "r"),
            context.Table,
            "r");

        PhysicalPlanNode leftScan = new TableScanNode(TableScanSource.PrimaryRows)
        {
            BoundSource = leftSource,
        };

        NestedLoopJoinNode joinNode = new(leftScan, rightSource, onPredicate)
        {
            RightExecutionFilter = rightFilter,
        };

        QueryPlan plan = BuildJoinPlan(joinNode, context.Table);
        string rendered = PlanRenderer.Render(plan);

        string expected =
            "nested-loop-join(on=p.user_id = r.id, right=p, right-filter=p.published = true)\n" +
            "  table-scan(table=robots)";

        Assert.AreEqual(expected, rendered);
    }

    // ── helper ────────────────────────────────────────────────────────────

    private QueryPlan BuildJoinPlan(PhysicalPlanNode root, TableDescriptor table)
    {
        // Build a minimal ticket (no WHERE / GROUP BY / ORDER BY) so the renderer
        // context is valid without needing a full SQL parse.
        QueryTicket ticket = new(
            txnState: context!.Txn,
            databaseName: QueryPlannerTestContext.DatabaseName,
            tableName: table.Name,
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null);

        QueryPlan plan = new(context.Database, table, ticket)
        {
            Root = root,
        };

        return plan;
    }
}
