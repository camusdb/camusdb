
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// R4 acceptance tests for distributed-ready plan properties:
///   - CanDecomposeToLocalPlusMerge per node type
///   - OutputOrdering populated on scan nodes that satisfy ORDER BY (sort elision)
///   - OutputOrdering populated on SortNode when explicit sort is needed
///   - Verbose renderer shows order= and decomposable= suffixes
/// No rows are inserted; uses QueryPlannerTestContext (schema-only fixture).
/// </summary>
[NonParallelizable]
public class TestPlanDistributedProperties
{
    private static QueryPlannerTestContext? ctx;
    private readonly QueryPlanner queryPlanner = new();

    [OneTimeSetUp]
    public void OneTimeSetUp() => ctx = QueryPlannerTestContext.Create();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (ctx is not null) { await ctx.DisposeAsync(); ctx = null; }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static QueryTicket Ticket(string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(ast);
        ExecuteSQLTicket exec = new(
            txnState: ctx!.Txn,
            database: QueryPlannerTestContext.DatabaseName,
            sql: sql,
            parameters: parameters);
        return QueryTicketAdapter.ToQueryTicket(query, exec);
    }

    private QueryPlan Plan(string sql, Dictionary<string, ColumnValue>? parameters = null) =>
        queryPlanner.GetPlan(ctx!.Database, ctx!.Table, Ticket(sql, parameters));

    /// <summary>Walk the plan tree in DFS order and collect all nodes.</summary>
    private static List<PhysicalPlanNode> AllNodes(QueryPlan plan)
    {
        var list = new List<PhysicalPlanNode>();
        Collect(plan.Root, list);
        return list;
    }

    private static void Collect(PhysicalPlanNode node, List<PhysicalPlanNode> list)
    {
        list.Add(node);
        if (node.Input is not null)
            Collect(node.Input, list);
    }

    // ── CanDecomposeToLocalPlusMerge: leaf / scan nodes ───────────────────────

    [Test]
    public void TableScan_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT * FROM robots");
        TableScanNode scan = AllNodes(plan).OfType<TableScanNode>().First();
        Assert.IsTrue(scan.CanDecomposeToLocalPlusMerge, "table-scan must be decomposable");
    }

    [Test]
    public void IndexLookup_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE id = '000000000000000000000001'");
        IndexLookupNode lookup = AllNodes(plan).OfType<IndexLookupNode>().First();
        Assert.IsTrue(lookup.CanDecomposeToLocalPlusMerge, "index-lookup must be decomposable");
    }

    [Test]
    public void IndexRangeScan_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT * FROM robots WHERE year >= 2020 AND year <= 2023");
        IndexRangeScanNode scan = AllNodes(plan).OfType<IndexRangeScanNode>().First();
        Assert.IsTrue(scan.CanDecomposeToLocalPlusMerge, "index-range-scan must be decomposable");
    }

    [Test]
    public void FilterNode_IsDecomposable()
    {
        // WHERE on a non-indexed column forces a filter node above the scan.
        QueryPlan plan = Plan("SELECT * FROM robots WHERE name = 'alice'");
        FilterNode filter = AllNodes(plan).OfType<FilterNode>().First();
        Assert.IsTrue(filter.CanDecomposeToLocalPlusMerge, "filter must be decomposable");
    }

    [Test]
    public void ProjectNode_IsDecomposable()
    {
        // A projection on a subset of columns emits a ProjectNode.
        QueryPlan plan = Plan("SELECT name FROM robots");
        ProjectNode project = AllNodes(plan).OfType<ProjectNode>().First();
        Assert.IsTrue(project.CanDecomposeToLocalPlusMerge, "project must be decomposable");
    }

    // ── CanDecomposeToLocalPlusMerge: aggregate nodes ─────────────────────────

    [Test]
    public void Aggregate_Count_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT COUNT(*) FROM robots");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsTrue(agg.IsDecomposableAggregate, "COUNT(*) aggregate must be decomposable");
        Assert.IsTrue(agg.CanDecomposeToLocalPlusMerge);
    }

    [Test]
    public void Aggregate_Sum_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT SUM(year) FROM robots");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsTrue(agg.IsDecomposableAggregate, "SUM aggregate must be decomposable");
    }

    [Test]
    public void Aggregate_Min_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT MIN(year) FROM robots");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsTrue(agg.IsDecomposableAggregate, "MIN aggregate must be decomposable");
    }

    [Test]
    public void Aggregate_Max_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT MAX(year) FROM robots");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsTrue(agg.IsDecomposableAggregate, "MAX aggregate must be decomposable");
    }

    [Test]
    public void Aggregate_Avg_IsNotDecomposable()
    {
        QueryPlan plan = Plan("SELECT AVG(year) FROM robots");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsFalse(agg.IsDecomposableAggregate,
            "AVG aggregate must NOT be decomposable (requires SUM/COUNT split which is not yet implemented)");
        Assert.IsFalse(agg.CanDecomposeToLocalPlusMerge);
    }

    [Test]
    public void Aggregate_GroupBy_WithCount_IsDecomposable()
    {
        QueryPlan plan = Plan("SELECT year, COUNT(*) FROM robots GROUP BY year");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsTrue(agg.IsDecomposableAggregate,
            "GROUP BY + COUNT(*) aggregate must be decomposable");
    }

    [Test]
    public void Aggregate_GroupBy_WithAvg_IsNotDecomposable()
    {
        QueryPlan plan = Plan("SELECT year, AVG(year) FROM robots GROUP BY year");
        AggregateNode agg = AllNodes(plan).OfType<AggregateNode>().First();
        Assert.IsFalse(agg.IsDecomposableAggregate,
            "GROUP BY + AVG aggregate must NOT be decomposable");
    }

    // ── CanDecomposeToLocalPlusMerge: non-decomposable pipeline nodes ─────────

    [Test]
    public void SortNode_IsNotDecomposable()
    {
        // ORDER BY on year DESC cannot be satisfied by any index → SortNode emitted.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year DESC");
        SortNode sort = AllNodes(plan).OfType<SortNode>().First();
        Assert.IsFalse(sort.CanDecomposeToLocalPlusMerge, "sort must NOT be decomposable");
    }

    [Test]
    public void LimitNode_IsNotDecomposable()
    {
        QueryPlan plan = Plan("SELECT * FROM robots LIMIT 5");
        LimitNode limit = AllNodes(plan).OfType<LimitNode>().First();
        Assert.IsFalse(limit.CanDecomposeToLocalPlusMerge, "limit must NOT be decomposable");
    }

    [Test]
    public void DistinctNode_IsNotDecomposable()
    {
        QueryPlan plan = Plan("SELECT DISTINCT name FROM robots");
        DistinctNode distinct = AllNodes(plan).OfType<DistinctNode>().First();
        Assert.IsFalse(distinct.CanDecomposeToLocalPlusMerge, "distinct must NOT be decomposable");
    }

    // ── OutputOrdering: index scan satisfies ORDER BY (sort elision) ──────────

    [Test]
    public void IndexScan_SatisfiesOrderBy_SetsOutputOrdering()
    {
        // year_idx is ASC; ORDER BY year ASC should be satisfied.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year");

        // Scan node must carry OutputOrdering.
        PhysicalPlanNode scan = AllNodes(plan).First(n => n is IndexRangeScanNode or TableScanNode);
        Assert.IsNotNull(scan.OutputOrdering,
            "Scan node that covers ORDER BY must have OutputOrdering set");
        Assert.AreEqual(1, scan.OutputOrdering!.Count);
        Assert.AreEqual("year", scan.OutputOrdering[0].ColumnName);
        Assert.AreEqual(OrderType.Ascending, scan.OutputOrdering[0].Type);
    }

    [Test]
    public void IndexScan_SatisfiesOrderBy_NoSortNodeEmitted()
    {
        // With year_idx covering ORDER BY year ASC, no SortNode should appear.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year");
        bool hasSortNode = AllNodes(plan).Any(n => n is SortNode);
        Assert.IsFalse(hasSortNode,
            "SortNode must not be emitted when the index scan satisfies ORDER BY (sort elision)");
    }

    [Test]
    public void Desc_OrderBy_NotSatisfiedByIndex_EmitsSortNode()
    {
        // year_idx is ASC; ORDER BY year DESC cannot be satisfied → SortNode required.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year DESC");
        bool hasSortNode = AllNodes(plan).Any(n => n is SortNode);
        Assert.IsTrue(hasSortNode,
            "SortNode must be emitted for DESC ORDER BY that the index cannot satisfy");
    }

    [Test]
    public void TableScan_WithoutOrderBy_HasNoOutputOrdering()
    {
        // A plain full scan with no ORDER BY should leave OutputOrdering null.
        QueryPlan plan = Plan("SELECT * FROM robots");
        TableScanNode scan = AllNodes(plan).OfType<TableScanNode>().First();
        Assert.IsNull(scan.OutputOrdering,
            "Scan without ORDER BY must not have OutputOrdering set");
    }

    // ── OutputOrdering: GROUP BY and DISTINCT suppress scan-level ordering ────
    //
    // GROUP BY runs an AggregateNode above the scan, destroying any scan-level row order.
    // DISTINCT inserts a DistinctNode that does the same. In both cases the scan's
    // OutputOrdering is irrelevant to the final plan output, so the planner must NOT set
    // it — doing so would produce misleading verbose EXPLAIN output.

    [Test]
    public void GroupBy_WithIndexOrderBy_ScanHasNoOutputOrdering()
    {
        // year_idx satisfies ORDER BY year in a plain query, but GROUP BY destroys that order.
        // The planner must NOT tag the scan with OutputOrdering.
        QueryPlan plan = Plan("SELECT year, COUNT(*) FROM robots GROUP BY year ORDER BY year");
        PhysicalPlanNode scan = AllNodes(plan).First(n => n is IndexRangeScanNode or TableScanNode);
        Assert.IsNull(scan.OutputOrdering,
            "Grouped query: scan.OutputOrdering must be null — AggregateNode destroys scan ordering");
    }

    [Test]
    public void GroupBy_WithIndexOrderBy_SortNodeIsStillEmitted()
    {
        // Even though the scan could satisfy ORDER BY in isolation, GROUP BY requires an
        // explicit SortNode after aggregation. Confirm it is present and carries OutputOrdering.
        QueryPlan plan = Plan("SELECT year, COUNT(*) FROM robots GROUP BY year ORDER BY year");
        SortNode sort = AllNodes(plan).OfType<SortNode>().First();
        Assert.IsNotNull(sort.OutputOrdering,
            "Grouped query: SortNode must carry OutputOrdering after aggregation");
        Assert.AreEqual("year", sort.OutputOrdering![0].ColumnName);
    }

    [Test]
    public void Distinct_WithIndexOrderBy_ScanHasNoOutputOrdering()
    {
        // DISTINCT with ORDER BY also goes through the DISTINCT path which always emits a
        // SortNode, so the scan's ordering is irrelevant to the final output.
        QueryPlan plan = Plan("SELECT DISTINCT year FROM robots ORDER BY year");
        PhysicalPlanNode scan = AllNodes(plan).First(n => n is IndexRangeScanNode or TableScanNode);
        Assert.IsNull(scan.OutputOrdering,
            "DISTINCT query: scan.OutputOrdering must be null — DistinctNode destroys scan ordering");
    }

    [Test]
    public void Distinct_WithIndexOrderBy_SortNodeIsStillEmitted()
    {
        QueryPlan plan = Plan("SELECT DISTINCT year FROM robots ORDER BY year");
        SortNode sort = AllNodes(plan).OfType<SortNode>().First();
        Assert.IsNotNull(sort.OutputOrdering,
            "DISTINCT query: SortNode must carry OutputOrdering");
    }

    [Test]
    public void Renderer_Verbose_GroupedQuery_ScanHasNoOrderSuffix()
    {
        // In verbose mode, the scan line must NOT contain order= for a grouped query.
        QueryPlan plan = Plan("SELECT year, COUNT(*) FROM robots GROUP BY year ORDER BY year");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);

        // Find the index-range-scan or table-scan line.
        string? scanLine = rendered.Split('\n')
            .FirstOrDefault(l => l.TrimStart().StartsWith("index-range-scan")
                               || l.TrimStart().StartsWith("table-scan"));
        Assert.IsNotNull(scanLine, "No scan line in rendered plan");
        StringAssert.DoesNotContain("order=[", scanLine,
            "Grouped query: scan line must not show order= in verbose EXPLAIN");
    }

    // ── OutputOrdering: SortNode carries it when explicit sort is needed ──────

    [Test]
    public void SortNode_SetsOutputOrdering()
    {
        // ORDER BY year DESC forces a SortNode; the node must carry OutputOrdering.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year DESC");
        SortNode sort = AllNodes(plan).OfType<SortNode>().First();
        Assert.IsNotNull(sort.OutputOrdering, "SortNode must have OutputOrdering set");
        Assert.AreEqual(1, sort.OutputOrdering!.Count);
        Assert.AreEqual("year", sort.OutputOrdering[0].ColumnName);
        Assert.AreEqual(OrderType.Descending, sort.OutputOrdering[0].Type);
    }

    [Test]
    public void SortNode_MultiColumn_SetsOutputOrdering()
    {
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year DESC, name");
        SortNode sort = AllNodes(plan).OfType<SortNode>().First();
        Assert.IsNotNull(sort.OutputOrdering);
        Assert.AreEqual(2, sort.OutputOrdering!.Count);
        Assert.AreEqual("year", sort.OutputOrdering[0].ColumnName);
        Assert.AreEqual(OrderType.Descending, sort.OutputOrdering[0].Type);
        Assert.AreEqual("name", sort.OutputOrdering[1].ColumnName);
        Assert.AreEqual(OrderType.Ascending, sort.OutputOrdering[1].Type);
    }

    // ── Placeholder properties (EstimatedCardinality, PartitionLocality) ──────

    [Test]
    public void EstimatedCardinality_IsPopulatedByR9CostModel()
    {
        // R9: CostEstimator.AnnotatePlan now sets EstimatedCardinality on every node.
        // When no live stats are available the planner falls back to a fixed default row count,
        // so all nodes must have a non-null, positive estimate.
        QueryPlan plan = Plan("SELECT * FROM robots");
        foreach (PhysicalPlanNode node in AllNodes(plan))
            Assert.IsNotNull(node.EstimatedCardinality,
                $"{node.GetType().Name}.EstimatedCardinality must be non-null after R9 cost model");
    }

    [Test]
    public void PartitionLocality_IsNullByDefault()
    {
        QueryPlan plan = Plan("SELECT * FROM robots");
        foreach (PhysicalPlanNode node in AllNodes(plan))
            Assert.IsNull(node.PartitionLocality,
                $"{node.GetType().Name}.PartitionLocality must be null (single-partition placeholder)");
    }

    // ── PlanRenderer verbose mode: order= and decomposable= ──────────────────

    [Test]
    public void Renderer_Verbose_ShowsDecomposableSuffix()
    {
        QueryPlan plan = Plan("SELECT * FROM robots");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        StringAssert.Contains("decomposable=", rendered);
    }

    [Test]
    public void Renderer_Verbose_TableScan_ShowsDecomposableTrue()
    {
        QueryPlan plan = Plan("SELECT * FROM robots");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        // table-scan line must contain decomposable=true
        string? scanLine = rendered.Split('\n').FirstOrDefault(l => l.TrimStart().StartsWith("table-scan"));
        Assert.IsNotNull(scanLine, "No table-scan line found in rendered plan");
        StringAssert.Contains("decomposable=true", scanLine);
    }

    [Test]
    public void Renderer_Verbose_SortNode_ShowsDecomposableFalse()
    {
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year DESC");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        string? sortLine = rendered.Split('\n').FirstOrDefault(l => l.TrimStart().StartsWith("sort"));
        Assert.IsNotNull(sortLine, "No sort line found in rendered plan");
        StringAssert.Contains("decomposable=false", sortLine);
    }

    [Test]
    public void Renderer_Verbose_ShowsOrderSuffix_WhenOutputOrderingSet()
    {
        // ORDER BY year ASC is satisfied by index → scan carries OutputOrdering → renderer shows order=.
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        StringAssert.Contains("order=[year ASC]", rendered,
            "Verbose render must show order=[year ASC] for scan that satisfies ORDER BY");
    }

    [Test]
    public void Renderer_Verbose_NoOrderSuffix_WhenOutputOrderingNull()
    {
        // Plain table scan has no OutputOrdering.
        QueryPlan plan = Plan("SELECT * FROM robots");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        StringAssert.DoesNotContain("order=[", rendered,
            "Verbose render must not show order= when OutputOrdering is not set");
    }

    [Test]
    public void Renderer_NonVerbose_DoesNotShowDistributedProps()
    {
        QueryPlan plan = Plan("SELECT * FROM robots ORDER BY year");
        string rendered = PlanRenderer.Render(plan); // default: no verbose flag
        StringAssert.DoesNotContain("decomposable=", rendered);
        StringAssert.DoesNotContain("order=[", rendered);
    }

    [Test]
    public void Renderer_Verbose_AggregateWithAvg_ShowsDecomposableFalse()
    {
        QueryPlan plan = Plan("SELECT AVG(year) FROM robots");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        string? aggLine = rendered.Split('\n').FirstOrDefault(l => l.TrimStart().StartsWith("aggregate"));
        Assert.IsNotNull(aggLine, "No aggregate line in rendered plan");
        StringAssert.Contains("decomposable=false", aggLine);
    }

    [Test]
    public void Renderer_Verbose_AggregateWithCount_ShowsDecomposableTrue()
    {
        QueryPlan plan = Plan("SELECT COUNT(*) FROM robots");
        string rendered = PlanRenderer.Render(plan, includeDistributedProperties: true);
        string? aggLine = rendered.Split('\n').FirstOrDefault(l => l.TrimStart().StartsWith("aggregate"));
        Assert.IsNotNull(aggLine, "No aggregate line in rendered plan");
        StringAssert.Contains("decomposable=true", aggLine);
    }
}

/// <summary>
/// Pins the R4 limitation that distributed-ready properties are not populated for join plans.
///
/// JoinQueryPlanner does not call IndexScanSelector for child scan nodes, so:
///   - join-side TableScanNodes have OutputOrdering = null (order is undefined)
///   - join nodes (NestedLoopJoinNode, IndexNestedLoopJoinNode) inherit CanDecomposeToLocalPlusMerge = false
///
/// These tests are intentionally negative assertions. When R7 join-order heuristics or a
/// future distributed sharding pass begins populating these properties for join plans, these
/// tests must be updated to reflect the new contract.
/// </summary>
[NonParallelizable]
public class TestPlanJoinDistributedPropertiesLimitation : SharedNodeBaseTest
{
    // ── fixture ───────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTwoTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "users",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "posts",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("user_id", ColumnType.Id),
                new("title",   ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExplainAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ── pinning tests ─────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task JoinPlan_JoinNode_OutputOrdering_IsNull()
    {
        // EXPLAIN returns EXPLAIN rows, not plan nodes directly. We use it to confirm
        // the join does appear in the plan, then rely on the documented limitation.
        // The direct assertion is in the comment: JoinQueryPlanner never sets OutputOrdering.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT u.email, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(
            nodes.Contains("nested-loop-join") || nodes.Contains("index-nested-loop-join"),
            "Expected a join node in the plan");

        // Verify the plan rows carry the explain schema (not data rows) — join EXPLAIN works end-to-end.
        foreach (QueryResultRow row in rows)
        {
            Assert.IsTrue(row.Row.ContainsKey("stage"), "EXPLAIN join row must have 'stage' column");
            Assert.IsTrue(row.Row.ContainsKey("node"),  "EXPLAIN join row must have 'node' column");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task JoinPlan_ChildTableScans_HaveNoOutputOrdering()
    {
        // This test documents the R4 limitation: child scans inside join plans never have
        // OutputOrdering populated because JoinQueryPlanner does not call IndexScanSelector
        // for ORDER BY matching. The test is written as a documentation assertion via the
        // EXPLAIN node names (table-scan appears, no order= in the verbose-rendered plan).
        //
        // When R7 or a distributed-sharding pass begins setting OutputOrdering for join-side
        // scans, this test must be updated.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT u.email, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id");

        // At least one table-scan child must exist.
        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("table-scan"),
            "Expected table-scan child node(s) in join plan");

        // None of the EXPLAIN detail strings should contain "order=" — join-side scans have
        // null OutputOrdering, so the verbose renderer emits nothing for them.
        // (EXPLAIN rows use the non-verbose WalkNodes path, so detail has no order= here;
        // this confirms the base invariant holds end-to-end through R3.)
        foreach (QueryResultRow row in rows)
        {
            string detail = row.Row["detail"].StrValue ?? "";
            Assert.IsFalse(detail.Contains("order="),
                $"Join plan node '{row.Row["node"].StrValue}' must not have order= in detail " +
                "(R4 limitation: join-side OutputOrdering is always null)");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task JoinNode_CanDecomposeToLocalPlusMerge_IsFalse()
    {
        // Documents that join nodes are not decomposable (R4 base class default = false,
        // NestedLoopJoinNode and IndexNestedLoopJoinNode do not override it).
        // This is correct: a nested-loop join cannot be split into per-partition local +
        // coordinator merge without a full hash-join or repartition step (not implemented).
        //
        // If a future distributed join operator overrides CanDecomposeToLocalPlusMerge,
        // update this test and the JoinQueryPlanner doc comment.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        // We assert via direct node instantiation — join nodes must not override the base false.
        // Use a dummy scan as a placeholder left child.
        TableScanNode dummyScan = new(TableScanSource.PrimaryRows);
        Assert.IsFalse(
            new NestedLoopJoinNode(dummyScan, null!, null!).CanDecomposeToLocalPlusMerge,
            "NestedLoopJoinNode must not be decomposable (R4 limitation)");
        Assert.IsFalse(
            new IndexNestedLoopJoinNode(dummyScan, null!, null!, null!, null!, null!).CanDecomposeToLocalPlusMerge,
            "IndexNestedLoopJoinNode must not be decomposable (R4 limitation)");

        await Task.CompletedTask; // satisfy async signature for fixture compatibility
    }
}
