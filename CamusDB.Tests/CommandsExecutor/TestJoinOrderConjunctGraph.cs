/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Plan-quality tests for the cost-based join order: the conjunct graph, the step cost that counts
/// intermediate results, the row estimate of each table set, the plan-cache replay of a rebuilt
/// tree, and the cross-product repair of the heuristic.
///
/// <para>A correct executor returns the same rows for any valid join order, so a row comparison
/// alone proves nothing about plan quality. Each plan test asserts the chosen plan; the executed
/// tests only prove that the rebuilt trees keep the results.</para>
///
/// <para>The probe tables <c>r0</c> and <c>r1</c> have 1,000,000 rows each and <c>r2</c> has 10,
/// all with columns <c>(id, a, b)</c> and a primary key on <c>id</c> only. The row counts are
/// seeded, so no data is needed for a plan test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestJoinOrderConjunctGraph : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private CamusDBOptions DpOn => Options with { CostBasedJoinOrderEnabled = true, CostBasedAccessPathEnabled = false };

    private CamusDBOptions DpOff => Options with { CostBasedJoinOrderEnabled = false, CostBasedAccessPathEnabled = false };

    // ─── Step cost: the intermediate result counts ───────────────────────────

    [Test]
    public async Task HashOnlyChain_JoinsTheSmallIntermediateResultFirst()
    {
        Db db = await CreateProbeTables(DpOn);

        QueryPlan plan = await Plan(db, "SELECT r0.id FROM r0 JOIN r1 ON r0.a = r1.a JOIN r2 ON r1.b = r2.b", DpOn);

        // r2 ⋈ r1 is about 10^6 rows; r0 ⋈ r1 is about 10^11. Both orders scan the same tables,
        // so only the size of the intermediate result separates them.
        CollectionAssert.AreEqual(new[] { "r2", "r1", "r0" }, JoinOrder(plan.Root), PlanRenderer.Render(plan));
    }

    [Test]
    public async Task HashOnlyChain_DeclaredInTheGoodOrder_KeepsIt()
    {
        Db db = await CreateProbeTables(DpOn);

        QueryPlan plan = await Plan(db, "SELECT r0.id FROM r2 JOIN r1 ON r1.b = r2.b JOIN r0 ON r0.a = r1.a", DpOn);

        CollectionAssert.AreEqual(new[] { "r2", "r1", "r0" }, JoinOrder(plan.Root), PlanRenderer.Render(plan));
    }

    [Test]
    public async Task EqualCostChain_KeepsTheDeclaredOrder()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "t0", "t1", "t2");
        await Seed(db, ("t0", 1000), ("t1", 1000), ("t2", 1000));

        // Every connected order of this chain costs the same; the plan must keep what the user wrote.
        QueryPlan forward = await Plan(db, "SELECT t0.id FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t1.b = t2.b", DpOn);
        CollectionAssert.AreEqual(new[] { "t0", "t1", "t2" }, JoinOrder(forward.Root), PlanRenderer.Render(forward));

        QueryPlan backward = await Plan(db, "SELECT t0.id FROM t2 JOIN t1 ON t1.b = t2.b JOIN t0 ON t0.a = t1.a", DpOn);
        CollectionAssert.AreEqual(new[] { "t2", "t1", "t0" }, JoinOrder(backward.Root), PlanRenderer.Render(backward));
    }

    [Test]
    public async Task HashOnlyChain_FlagOnAndOff_ReturnTheSameRows()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "r0", "r1", "r2");
        await InsertRows(db, "r0", "r1", "r2");
        await Seed(db, ("r0", 1_000_000), ("r1", 1_000_000), ("r2", 10));

        await AssertSameRowsOnAndOff(db,
            "SELECT r0.id AS i0, r1.id AS i1, r2.id AS i2 FROM r0 JOIN r1 ON r0.a = r1.a JOIN r2 ON r1.b = r2.b");
    }

    // ─── Conjunct graph ──────────────────────────────────────────────────────

    [Test]
    public async Task CommaJoin_ConnectedThroughALaterTable_HasNoCrossProduct()
    {
        Db db = await CreateProbeTables(DpOn);

        QueryPlan plan = await Plan(db, "SELECT r0.id FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b", DpOn);
        string rendered = PlanRenderer.Render(plan);

        StringAssert.DoesNotContain("on=true", rendered);
        CollectionAssert.AreEqual(new[] { "r2", "r0", "r1" }, JoinOrder(plan.Root), rendered);
        Assert.IsNull(plan.ExecutionFilter, rendered);
    }

    [Test]
    public async Task ExplicitJoin_ConjunctInsideALaterOn_CanJoinFirst()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c");
        await Ddl(db, "CREATE INDEX c_z_idx ON c (z)");
        await Seed(db, ("a", 10), ("b", 1_000_000), ("c", 1000));

        // c.z = a.z sits in the ON of c together with c.y = b.y. Read as conjuncts, it lets c join a
        // first through the index on c.z, before the scan of the large table b.
        QueryPlan plan = await Plan(db,
            "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.y = b.y AND c.z = a.z", DpOn);
        string rendered = PlanRenderer.Render(plan);

        CollectionAssert.AreEqual(new[] { "a", "c", "b" }, JoinOrder(plan.Root), rendered);

        IndexNestedLoopJoinNode lookup = FindNodes<IndexNestedLoopJoinNode>(plan.Root).Single();
        Assert.AreEqual("c", lookup.RightSource.Alias, rendered);
        Assert.AreEqual("a.z", lookup.LeftLookupColumn, rendered);
    }

    [Test]
    public async Task CompositeKeyJoin_IsCostedThroughTheIndexOnItsLeadingKey()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64, y int64, f int64, g int64, h int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, y int64, f int64, g int64, h int64)");
        await Ddl(db, "CREATE INDEX b_x_idx ON b (x)");
        await Seed(db, ("a", 1_050_000), ("b", 1_000_000));

        // Both sides keep about a thousand rows after their filters. Joining b first is cheaper
        // among hash plans, so only the index lookup on b.x makes a-first the cheapest plan; the DP
        // sees that lookup only when it reads the composite ON as two conjuncts.
        QueryPlan plan = await Plan(db,
            "SELECT a.id FROM a JOIN b ON a.x = b.x AND a.y = b.y " +
            "WHERE a.f = 1 AND a.g = 2 AND a.h = 3 AND b.f = 1 AND b.g = 2 AND b.h = 3", DpOn);
        string rendered = PlanRenderer.Render(plan);

        IndexNestedLoopJoinNode lookup = plan.Root as IndexNestedLoopJoinNode
            ?? throw new AssertionException($"Expected an index-lookup join into b:\n{rendered}");

        Assert.AreEqual("b", lookup.RightSource.Alias, rendered);
        Assert.AreEqual("b_x_idx", lookup.Index.Name, rendered);
        CollectionAssert.AreEqual(new[] { "a", "b" }, JoinOrder(plan.Root), rendered);
    }

    private static readonly string[] ConjunctShapes =
    [
        // chain
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2, t3.id AS i3 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t1.b = t2.b JOIN t3 ON t2.c = t3.c",
        // star
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2, t3.id AS i3 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t0.b = t2.b JOIN t3 ON t0.c = t3.c",
        // cycle
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t1.b = t2.b AND t2.c = t0.c",
        // an edge with three tables
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t2.b = t0.b + t1.b",
        // single-table ON conjuncts
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a AND t1.b > 0 JOIN t2 ON t1.c = t2.c AND t0.b < 1",
        // a constant ON conjunct
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a AND 1 = 1 JOIN t2 ON t1.b = t2.b",
        // a comma join connected through a later table
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0, t1, t2 WHERE t0.a = t2.a AND t1.b = t2.b",
        // a cross-table WHERE conjunct
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t2.b = t1.b WHERE t2.c = t0.c",
        // a non-equi edge
        "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t2.b < t1.b AND t2.c = t0.c",
    ];

    [Test]
    public async Task ConjunctPool_EveryOrderAttachesEachConjunctOnce_AndRowsMatch()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "t0", "t1", "t2", "t3");
        await InsertRows(db, "t0", "t1", "t2", "t3");
        await Seed(db, ("t0", 6), ("t1", 6), ("t2", 6), ("t3", 6));

        foreach (string sql in ConjunctShapes)
        {
            (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db, sql);
            JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

            JoinGraph graph = JoinGraph.TryBuild(bound.Query.Source, bound, pushdown.PostJoinConjuncts)
                ?? throw new AssertionException($"The graph refused `{sql}`");

            int orders = 0;

            foreach (int[] order in Permutations(graph.LeafCount))
            {
                if (!graph.IsConnectedOrder(order))
                    continue;

                QuerySource tree = graph.BuildTree(order)
                    ?? throw new AssertionException($"No tree for order {string.Join(",", order)} of `{sql}`");

                AssertEachConjunctOnceAndInScope(graph, tree, sql);
                orders++;
            }

            Assert.Greater(orders, 0, $"No connected order for `{sql}`");

            await AssertSameRowsOnAndOff(db, sql);
        }
    }

    [Test]
    public async Task QueryThatNeedsACrossProduct_KeepsTheDeclaredOrder()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "t0", "t1");
        await Seed(db, ("t0", 1000), ("t1", 10));

        foreach (CamusDBOptions options in new[] { DpOn, DpOff })
        {
            QueryPlan plan = await Plan(db, "SELECT t0.id FROM t0, t1 WHERE t0.a = 1", options);
            string rendered = PlanRenderer.Render(plan);

            CollectionAssert.AreEqual(new[] { "t0", "t1" }, JoinOrder(plan.Root), rendered);
            StringAssert.Contains("on=true", rendered);
        }
    }

    // ─── Plan cache ──────────────────────────────────────────────────────────

    [Test]
    public async Task PlanCacheHit_RebuildsTheConjunctTreeOfTheMiss()
    {
        CamusDBOptions options = DpOn with { PlanCacheEnabled = true };
        Db db = await CreateDb(options);
        await CreateTables(db, "r0", "r1", "r2");
        await InsertRows(db, "r0", "r1", "r2");
        await Seed(db, ("r0", 1_000_000), ("r1", 1_000_000), ("r2", 10));

        const string sql = "SELECT r0.id AS i0, r1.id AS i1, r2.id AS i2 FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b";

        await AssertCacheHitReplaysMiss(db, sql, options);

        // Executed: the miss and the hit return the same rows, and the rows of the heuristic engine.
        db.Executor.PlanCache.Clear();
        List<string> miss = await QueryRows(db.Executor, db, sql);
        List<string> hit = await QueryRows(db.Executor, db, sql);

        CollectionAssert.AreEqual(miss, hit);
        CollectionAssert.AreEqual(await RowsFromEngine(db, DpOff, sql), hit);
    }

    [Test]
    public async Task PlanCacheHit_KeepsAConjunctMovedWithinTheDeclaredOrder()
    {
        CamusDBOptions options = DpOn with { PlanCacheEnabled = true };
        Db db = await CreateDb(options);
        await CreateTables(db, "t0", "t1", "t2");
        await InsertRows(db, "t0", "t1", "t2");
        await Seed(db, ("t0", 1000), ("t1", 1000), ("t2", 1000));

        // t0.c = t1.c is written in the ON of t2 but names t0 and t1 only, so the tree in declared
        // order evaluates it at the first join. A replay that keeps the declared tree would put it
        // back at the second join.
        const string sql = "SELECT t0.id AS i0, t1.id AS i1, t2.id AS i2 FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t2.b = t1.b AND t0.c = t1.c";

        QueryPlan plan = await AssertCacheHitReplaysMiss(db, sql, options);
        CollectionAssert.AreEqual(new[] { "t0", "t1", "t2" }, JoinOrder(plan.Root), PlanRenderer.Render(plan));

        HashJoinNode first = FindNodes<HashJoinNode>(plan.Root).Single(j => j.Input is TableScanNode);
        CollectionAssert.AreEquivalent(new[] { "t0.a", "t0.c" }, first.ProbeKeyColumns, PlanRenderer.Render(plan));

        db.Executor.PlanCache.Clear();
        List<string> miss = await QueryRows(db.Executor, db, sql);
        List<string> hit = await QueryRows(db.Executor, db, sql);

        CollectionAssert.AreEqual(miss, hit);
        CollectionAssert.AreEqual(await RowsFromEngine(db, DpOff, sql), hit);
    }

    [Test]
    public async Task PlanCache_SchemaChangeBetweenMissAndHit_Invalidates()
    {
        CamusDBOptions options = DpOn with { PlanCacheEnabled = true };
        Db db = await CreateProbeTables(options);
        const string sql = "SELECT r0.id FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b";

        PlanCache cache = db.Executor.PlanCache;
        await Plan(db, sql, options, cache);

        long hits = cache.Hits;
        long misses = cache.Misses;

        await Ddl(db, "ALTER TABLE r1 ADD COLUMN w int64");
        await Plan(db, sql, options, cache);

        Assert.AreEqual(hits, cache.Hits, "A plan after a schema change must not be a hit.");
        Assert.AreEqual(misses + 1, cache.Misses);
    }

    // ─── Cross-table WHERE conjuncts ─────────────────────────────────────────

    private const string WhereEdgeSql =
        "SELECT a.id AS ia, b.id AS ib, c.id AS ic FROM a JOIN b ON a.x = b.x JOIN c ON c.y = b.y WHERE c.z = a.z";

    [Test]
    public async Task CrossTableWhereConjunct_MovesIntoAJoin()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c");
        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1000));

        QueryPlan plan = await Plan(db, WhereEdgeSql, DpOn);
        string rendered = PlanRenderer.Render(plan);

        Assert.IsNull(plan.ExecutionFilter, rendered);
        Assert.IsTrue(FindNodes<HashJoinNode>(plan.Root).Any(j => HasKey(j, "z")), rendered);

        // The heuristic plan of a tree with no cross product is unchanged: the conjunct stays
        // after the last join.
        QueryPlan heuristic = await Plan(db, WhereEdgeSql, DpOff);
        Assert.IsNotNull(heuristic.ExecutionFilter, PlanRenderer.Render(heuristic));
        Assert.IsFalse(FindNodes<HashJoinNode>(heuristic.Root).Any(j => HasKey(j, "z")), PlanRenderer.Render(heuristic));
    }

    [Test]
    public async Task CrossTableWhereConjunct_IsEvaluatedOnce()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c");

        // Two chains match on x and y; the second one fails c.z = a.z and must be removed.
        await Exec(db, "INSERT INTO a (id, x, y, z) VALUES (1, 10, 0, 100), (2, 20, 0, 200)");
        await Exec(db, "INSERT INTO b (id, x, y, z) VALUES (1, 10, 50, 0), (2, 20, 60, 0)");
        await Exec(db, "INSERT INTO c (id, x, y, z) VALUES (1, 0, 50, 100), (2, 0, 60, 999)");
        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1000));

        List<string> rows = await QueryRows(db.Executor, db, WhereEdgeSql);

        CollectionAssert.AreEqual(new[] { "ia=1|ib=1|ic=1" }, rows);
        CollectionAssert.AreEqual(await RowsFromEngine(db, DpOff, WhereEdgeSql), rows);
    }

    [Test]
    public async Task WhereConjunctWithAParameter_StaysAfterTheLastJoin()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c");
        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1000));

        // A placeholder cannot be shipped to a peer node, so moving the conjunct into an ON could
        // stop a broadcast join; it stays after the last join, and the shippable one moves.
        const string sql = "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.y = b.y WHERE c.z = a.z AND c.x + @p > a.x";
        Dictionary<string, ColumnValue> parameters = new() { { "@p", new ColumnValue(ColumnType.Integer64, 1L) } };

        QueryPlan plan = await Plan(db, sql, DpOn, parameters: parameters);
        string rendered = PlanRenderer.Render(plan);

        Assert.IsNotNull(plan.ExecutionFilter, rendered);
        Assert.AreEqual(NodeType.ExprGreaterThan, plan.ExecutionFilter!.nodeType, rendered);
        Assert.IsTrue(FindNodes<HashJoinNode>(plan.Root).Any(j => HasKey(j, "z")), rendered);
    }

    [Test]
    public async Task WhereConjunctWithASubquery_StaysAfterTheLastJoin()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c");

        (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db, "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.y = b.y");

        NodeAst subquery = new(NodeType.ExprScalarSubquery, NodeAst.True, null, null, null, null, null, null, null);
        NodeAst withSubquery = new(NodeType.ExprEquals, Identifier("c.z"),
            new NodeAst(NodeType.ExprAdd, Identifier("a.z"), subquery, null, null, null, null, null, null),
            null, null, null, null, null, null);
        NodeAst plain = new(NodeType.ExprEquals, Identifier("c.z"), Identifier("a.z"), null, null, null, null, null, null);

        JoinGraph graph = JoinGraph.TryBuild(bound.Query.Source, bound, [withSubquery, plain])!;

        Assert.IsNotNull(graph);
        Assert.IsTrue(graph.Conjuncts.Any(c => ReferenceEquals(c.Node, plain)));
        Assert.IsFalse(graph.Conjuncts.Any(c => ReferenceEquals(c.Node, withSubquery)));
        Assert.AreSame(withSubquery, graph.RemainingPostJoinFilter);
    }

    [Test]
    public async Task CrossTableWhereConjunct_PlanCacheHitMatchesMiss()
    {
        CamusDBOptions options = DpOn with { PlanCacheEnabled = true };
        Db db = await CreateDb(options);
        await CreateTables(db, "a", "b", "c");
        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1000));

        QueryPlan hit = await AssertCacheHitReplaysMiss(db, WhereEdgeSql, options);
        Assert.IsNull(hit.ExecutionFilter, PlanRenderer.Render(hit));
    }

    // ─── Row estimate of a table set ─────────────────────────────────────────

    [Test]
    public async Task SubsetEstimate_IsTheSameForEveryDeclaredOrder()
    {
        Db db = await CreateUniqueKeyTables();

        string[] aliases = ["a", "b", "c", "d"];
        Dictionary<string, double>? reference = null;

        foreach (int[] permutation in Permutations(aliases.Length))
        {
            string from = string.Join(", ", permutation.Select(i => aliases[i]));
            string sql = $"SELECT a.id FROM {from} WHERE a.x = b.x AND c.bid = b.id AND d.k = a.k";

            Dictionary<string, double> estimates = await SubsetEstimates(db, sql);

            if (reference is null)
            {
                reference = estimates;
                continue;
            }

            foreach ((string subset, double expected) in reference)
                Assert.AreEqual(expected, estimates[subset], expected * 1e-9, $"{subset} for FROM {from}");
        }
    }

    [Test]
    public async Task SubsetEstimate_UsesTheUniqueKeyOfATableInsideTheLeftSet()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, bid int64)");
        await Seed(db, ("a", 100), ("b", 1000), ("c", 5000));

        Dictionary<string, double> estimates = await SubsetEstimates(db,
            "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.bid = b.id");

        // {a, b}: 100 × 1000 × 0.1. Adding c through the unique b.id: × 5000 / 1000. With the key
        // table known only for a one-table left side, {a, b, c} would get the fallback × 0.1.
        Assert.AreEqual(10_000, estimates["a,b"], 1e-6);
        Assert.AreEqual(50_000, estimates["a,b,c"], 1e-6);
    }

    [Test]
    public async Task SubsetEstimate_CompositeKeyUsesTheCompositeDistinctCount()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, p int64, q int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, p int64, q int64)");

        TableDescriptor b = await db.Database.TableDescriptors["b"];
        await db.Executor.Statistics.SetNdvAsync(db.Database, b, columnNdv: null,
            keyNdv: new Dictionary<string, long> { { StatisticsManager.KeyTupleSignature(["p", "q"]), 500 } });

        await Seed(db, ("a", 1000), ("b", 1000));

        Dictionary<string, double> estimates = await SubsetEstimates(db,
            "SELECT a.id FROM a JOIN b ON a.p = b.p AND a.q = b.q");

        // 1000 × 1000 / 500. A single-conjunct estimate has no key for an AND and uses × 0.1.
        Assert.AreEqual(2000, estimates["a,b"], 1e-6);
    }

    [Test]
    public async Task SubsetEstimate_RedundantEqualityDoesNotLowerTheEstimate()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c");
        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1000));

        Dictionary<string, double> chain = await SubsetEstimates(db,
            "SELECT a.id FROM a, b, c WHERE a.x = b.x AND b.x = c.x");
        Dictionary<string, double> cycle = await SubsetEstimates(db,
            "SELECT a.id FROM a, b, c WHERE a.x = b.x AND b.x = c.x AND a.x = c.x");

        // a.x = c.x follows from the other two; it must not multiply the estimate by 0.1 again.
        Assert.AreEqual(chain["a,b,c"], cycle["a,b,c"], chain["a,b,c"] * 1e-9);
        Assert.AreEqual(10_000_000, cycle["a,b,c"], 1e-3);
    }

    [Test]
    public async Task SubsetEstimate_CliqueOfEqualColumnsCountsEachTableOnce()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "a", "b", "c", "d");

        foreach (string table in new[] { "a", "b", "c", "d" })
        {
            TableDescriptor descriptor = await db.Database.TableDescriptors[table];
            await db.Executor.Statistics.SetNdvAsync(db.Database, descriptor,
                columnNdv: new Dictionary<string, long> { { "x", 100 } }, keyNdv: null);
        }

        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1000), ("d", 1000));

        // Six equalities name one value through four tables: three factors of 1/100, not six.
        Dictionary<string, double> estimates = await SubsetEstimates(db,
            "SELECT a.id FROM a, b, c, d WHERE a.x = b.x AND a.x = c.x AND a.x = d.x AND b.x = c.x AND b.x = d.x AND c.x = d.x");

        Assert.AreEqual(1e12 / 1e6, estimates["a,b,c,d"], 1e-3);
        Assert.AreEqual(1e9 / 1e4, estimates["a,b,c"], 1e-3);
        Assert.AreEqual(1e6 / 1e2, estimates["a,b"], 1e-6);
    }

    // ─── Skewed join keys: the histograms, not the distinct counts alone ─────

    /// <summary>
    /// Rows of the skewed join of <see cref="CreateSkewedTables"/>, counted by hand: 500 × 400 on the
    /// value 1, then the residual values 2..60, where a holds 6 rows of each of 2..6 and 5 of the
    /// rest, and b holds 11 rows of each of 2..11 and 10 of the rest.
    /// </summary>
    private const long SkewedJoinRows = 500 * 400 + 6 * 11 * 5 + 5 * 11 * 5 + 5 * 10 * 49;

    [Test]
    public async Task SubsetEstimate_SkewedKeysOnBothSides_UseTheHistograms()
    {
        Db db = await CreateSkewedTables();

        const string sql = "SELECT a.id FROM a JOIN b ON a.x = b.x";
        Dictionary<string, double> estimates = await SubsetEstimates(db, sql);

        // The distinct counts alone give 1000 × 1000 / 100 = 10,000; the value 1, which half of a
        // and 40% of b hold, makes the join twenty times that. The histograms show it: 50 and 40
        // buckets of one value, so the estimate is 0.5 × 0.4 + 0.5 × 0.6 / 99 of a million.
        Assert.AreEqual(SkewedJoinRows, (await QueryRows(db.Executor, db, sql)).Count);
        Assert.AreEqual(SkewedJoinRows, estimates["a,b"], SkewedJoinRows * 0.01);
    }

    [Test]
    public async Task SubsetEstimate_ClassWithAUniqueKeyAndTwoSkewedColumns_UsesTheHistograms()
    {
        Db db = await CreateSkewedTables();
        await Ddl(db, "CREATE TABLE ci (id int64 primary key)");
        await Exec(db, "INSERT INTO ci (id) VALUES " + string.Join(", ", Enumerable.Range(1, 100).Select(i => $"({i})")));
        await Analyze(db, "ci");

        // One class of three columns: ci.id is unique, a.x and b.x are skewed on the value 1.
        Dictionary<string, double> estimates = await SubsetEstimates(db,
            "SELECT a.id FROM a, b, ci WHERE a.x = ci.id AND b.x = ci.id");

        // Each value of a and b is one row of ci, so the three-way join has the rows of a ⋈ b, and
        // the pair {a, b} inside the class gets the same estimate as the pair group would.
        Assert.AreEqual(SkewedJoinRows, estimates["a,b,ci"], SkewedJoinRows * 0.01);
        Assert.AreEqual(SkewedJoinRows, estimates["a,b"], SkewedJoinRows * 0.01);
        Assert.AreEqual(1000, estimates["a,ci"], 1e-6);
    }

    [Test]
    public async Task Explain_SkewedEquiJoin_ShowsTheHistogramEstimate()
    {
        Db db = await CreateSkewedTables();

        List<QueryResultRow> rows = await Explain(db, "SELECT a.id FROM a JOIN b ON a.x = b.x");

        Assert.AreEqual(SkewedJoinRows, TopJoinEstimate(rows), SkewedJoinRows * 0.01, RenderExplain(rows));
    }

    /// <summary>
    /// a and b with 1,000 rows each and 100 distinct values of x: half of a and 40% of b hold
    /// x = 1; the other rows of a cycle through 2..100 and those of b through 2..60. x is indexed
    /// on both, because ANALYZE builds histograms for indexed columns only (100 buckets of 10
    /// rows, so the value 1 fills 50 and 40 of them).
    /// </summary>
    private async Task<Db> CreateSkewedTables()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64)");
        await Ddl(db, "CREATE INDEX a_x ON a (x)");
        await Ddl(db, "CREATE INDEX b_x ON b (x)");

        await InsertSkewed(db, "a", heavyRows: 500, residualValues: 99);
        await InsertSkewed(db, "b", heavyRows: 400, residualValues: 59);
        await Analyze(db, "a");
        await Analyze(db, "b");

        return db;
    }

    private static async Task InsertSkewed(Db db, string table, int heavyRows, int residualValues)
    {
        List<string> values = new(1000);
        for (int id = 1; id <= 1000; id++)
        {
            long x = id <= heavyRows ? 1 : 2 + (id - heavyRows - 1) % residualValues;
            values.Add($"({id}, {x})");
        }

        for (int start = 0; start < values.Count; start += 250)
            await Exec(db, $"INSERT INTO {table} (id, x) VALUES " + string.Join(", ", values.Skip(start).Take(250)));
    }

    private const string UniqueKeyChainSql =
        "SELECT a.id AS ia, b.id AS ib, c.id AS ic, d.id AS id2 FROM a JOIN b ON a.x = b.x JOIN c ON c.bid = b.id JOIN d ON d.k = a.k";

    [Test]
    public async Task SubsetEstimate_KeepsTheJoinThroughAUniqueKeyBeforeAWideJoin()
    {
        Db db = await CreateUniqueKeyTables();

        // {a, b, c} has 10,000 rows: c joins b through its primary key. The cheapest plan for
        // {a, b, c} builds it from {a, b}, where a tree-dependent estimate does not know the table
        // of b.id and multiplies by 0.1 instead, so it sees a million rows and puts d first.
        QueryPlan plan = await Plan(db, UniqueKeyChainSql, DpOn);

        CollectionAssert.AreEqual(new[] { "a", "b", "c", "d" }, JoinOrder(plan.Root), PlanRenderer.Render(plan));
    }

    [Test]
    public async Task SubsetEstimate_UniqueKeyChain_FlagOnAndOff_ReturnTheSameRows()
    {
        Db db = await CreateUniqueKeyTables();
        await Exec(db, "INSERT INTO a (id, x, k) VALUES (1, 1, 1), (2, 2, 2), (3, 1, 3)");
        await Exec(db, "INSERT INTO b (id, x) VALUES (1, 1), (2, 2), (3, 2)");
        await Exec(db, "INSERT INTO c (id, bid) VALUES (1, 1), (2, 1), (3, 2), (4, 3), (5, 9)");
        await Exec(db, "INSERT INTO d (id, k) VALUES (1, 1), (2, 2), (3, 2), (4, 7)");

        await AssertSameRowsOnAndOff(db, UniqueKeyChainSql);
    }

    [Test]
    public async Task SubsetEstimate_TwelveTableChainOfLargeTablesDoesNotOverflow()
    {
        Db db = await CreateDb(DpOn);
        // Twelve tables: the widest join whose subsets are all estimated up front, 4,096 of them.
        string[] tables = Enumerable.Range(0, 12).Select(i => $"t{i}").ToArray();
        await CreateTables(db, tables);
        await Seed(db, tables.Select(t => (t, 1_000_000L)).ToArray());

        string sql = "SELECT t0.id FROM t0" + string.Concat(
            Enumerable.Range(1, tables.Length - 1).Select(i => $" JOIN t{i} ON t{i - 1}.a = t{i}.a"));

        QueryPlan plan = await Plan(db, sql, DpOn);
        Assert.AreEqual(tables.Length, JoinOrder(plan.Root).Count);
        Assert.Greater(plan.Root.EstimatedCardinality ?? 0, 0, PlanRenderer.Render(plan));

        foreach (double estimate in (await SubsetEstimates(db, sql)).Values)
            Assert.IsTrue(double.IsFinite(estimate) && estimate > 0, $"estimate {estimate}");
    }

    // ─── Joins wider than the dense estimate array ───────────────────────────

    /// <summary>
    /// Fourteen tables in a chain, declared largest first with the ten-row table last. The search
    /// visits the subsets an order can reach, so it plans the chain and starts from the small table;
    /// the heuristic would keep the declared order, because no table has a pushed predicate.
    /// </summary>
    [Test]
    public async Task WideJoin_FourteenTableChain_IsPlannedByTheSearch_FromTheSmallestTable()
    {
        Db db = await CreateDb(DpOn);
        string[] tables = Enumerable.Range(0, 14).Select(i => $"t{i}").ToArray();
        await CreateTables(db, tables);
        await Seed(db, tables.Select(t => (t, t == "t13" ? 10L : 1_000_000L)).ToArray());

        QueryPlan plan = await Plan(db, ChainSql(tables), DpOn);
        string rendered = PlanRenderer.Render(plan);

        Assert.AreEqual("cost-based", plan.JoinOrderSearch?.Path, rendered);
        Assert.Greater(plan.JoinOrderSearch!.CostedJoinSteps, 0);
        CollectionAssert.AreEqual(tables.Reverse(), JoinOrder(plan.Root), rendered);
        StringAssert.DoesNotContain("on=true", rendered);
    }

    /// <summary>
    /// With a step budget of one the search stops at once and the greedy order plans the join:
    /// the table with the fewest rows first, then the one connected table each step. The plan
    /// reports <c>greedy</c>, so EXPLAIN (ANALYZE) and the harness can tell it from a search.
    /// </summary>
    [Test]
    public async Task WideJoin_PastTheStepBudget_TakesTheGreedyOrder()
    {
        Db db = await CreateDb(DpOn);
        string[] tables = Enumerable.Range(0, 14).Select(i => $"t{i}").ToArray();
        await CreateTables(db, tables);
        await Seed(db, tables.Select(t => (t, t == "t13" ? 10L : 1_000_000L)).ToArray());
        db.Executor.Statistics.JoinSearchStepBudgetForTesting = 1;

        QueryPlan plan = await Plan(db, ChainSql(tables), DpOn);
        string rendered = PlanRenderer.Render(plan);

        Assert.AreEqual("greedy", plan.JoinOrderSearch?.Path, rendered);
        CollectionAssert.AreEqual(tables.Reverse(), JoinOrder(plan.Root), rendered);
        StringAssert.DoesNotContain("on=true", rendered);
    }

    /// <summary>
    /// The greedy order starts from the smallest table and, when that table joins two
    /// candidates, takes the one that gives the smaller result: a star with the ten-row center
    /// and satellites of 1,000 and 1,000,000 rows joins the small satellite first.
    /// </summary>
    [Test]
    public async Task GreedyOrder_PrefersTheStepWithTheSmallerResult()
    {
        Db db = await CreateDb(DpOn);
        await CreateTables(db, "hub", "big", "small");
        await Seed(db, ("hub", 10), ("big", 1_000_000), ("small", 1_000));
        db.Executor.Statistics.JoinSearchStepBudgetForTesting = 1;

        QueryPlan plan = await Plan(db, "SELECT hub.id FROM big JOIN hub ON big.a = hub.a JOIN small ON hub.b = small.b", DpOn);
        string rendered = PlanRenderer.Render(plan);

        Assert.AreEqual("greedy", plan.JoinOrderSearch?.Path, rendered);
        CollectionAssert.AreEqual(new[] { "hub", "small", "big" }, JoinOrder(plan.Root), rendered);
    }

    /// <summary><c>SELECT t0.id FROM t0 JOIN t1 ON t0.a = t1.a JOIN t2 ON t1.a = t2.a …</c> over <paramref name="tables"/>.</summary>
    private static string ChainSql(string[] tables) =>
        $"SELECT {tables[0]}.id FROM {tables[0]}" + string.Concat(
            Enumerable.Range(1, tables.Length - 1).Select(i => $" JOIN {tables[i]} ON {tables[i - 1]}.a = {tables[i]}.a"));

    // ─── Heuristic cross-product repair ──────────────────────────────────────

    [Test]
    public async Task FlagOff_CommaJoinConnectedThroughALaterTable_IsRepaired()
    {
        Db db = await CreateProbeTables(DpOff);

        QueryPlan plan = await Plan(db, "SELECT r0.id FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b", DpOff);
        string rendered = PlanRenderer.Render(plan);

        StringAssert.DoesNotContain("on=true", rendered);
        CollectionAssert.AreEqual(new[] { "r0", "r2", "r1" }, JoinOrder(plan.Root), rendered);
    }

    [Test]
    public async Task FlagOn_RowCountNotLoaded_StillRepairsTheCrossProduct()
    {
        Db db = await CreateProbeTables(DpOn);

        TableDescriptor r2 = await db.Database.TableDescriptors["r2"];
        db.Executor.Statistics.EvictForTesting(db.Database, r2);

        QueryPlan plan = await Plan(db, "SELECT r0.id FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b", DpOn);

        StringAssert.DoesNotContain("on=true", PlanRenderer.Render(plan));
    }

    [Test]
    public async Task CommaJoinOfThirteenTables_HasNoCrossProduct_OnAndOff()
    {
        Db db = await CreateDb(DpOn);
        const int n = 13;
        string[] tables = Enumerable.Range(0, n).Select(i => $"t{i}").ToArray();
        await CreateTables(db, tables);
        await Seed(db, tables.Select(t => (t, 1000L)).ToArray());

        // Declared t0, t2, t1, t3, …: t2 connects to nothing before it, so the declared tree has one
        // avoidable cross product. A chain joins t{i} and t{i+1}.
        string[] declared = [tables[0], tables[2], tables[1], .. tables[3..]];
        string sql = $"SELECT t0.id FROM {string.Join(", ", declared)} WHERE " +
            string.Join(" AND ", Enumerable.Range(0, n - 1).Select(i => $"t{i}.a = t{i + 1}.a"));

        foreach (CamusDBOptions options in new[] { DpOn, DpOff })
        {
            QueryPlan plan = await Plan(db, sql, options);
            string rendered = PlanRenderer.Render(plan);

            StringAssert.DoesNotContain("on=true", rendered);
            Assert.AreEqual(n, JoinOrder(plan.Root).Count, rendered);
        }
    }

    [Test]
    public async Task FlagOff_QueryWithNoAvoidableCrossProduct_KeepsTheHeuristicTree()
    {
        Db db = await CreateProbeTables(DpOff);

        (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db,
            "SELECT r0.id FROM r0 JOIN r1 ON r0.a = r1.a JOIN r2 ON r1.b = r2.b WHERE r2.a = r0.b");
        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

        JoinOrderChoice choice = JoinOrderOptimizer.Choose(bound.Query.Source, bound, pushdown);

        // Every score ties, so the heuristic keeps the declared tree; with no cross product to
        // repair, the choice is that same tree and the same post-join filter.
        Assert.AreEqual(JoinBuildMode.WholePredicate, choice.Mode);
        Assert.AreSame(bound.Query.Source, choice.Source);
        Assert.AreSame(pushdown.PostJoinFilter, choice.PostJoinFilter);
    }

    [Test]
    public async Task FlagOff_Repair_RowsMatch_AndPlanCacheHitMatchesMiss()
    {
        CamusDBOptions options = DpOff with { PlanCacheEnabled = true };
        Db db = await CreateDb(options);
        await CreateTables(db, "r0", "r1", "r2");
        await InsertRows(db, "r0", "r1", "r2");

        const string sql = "SELECT r0.id AS i0, r1.id AS i1, r2.id AS i2 FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b";

        QueryPlan plan = await AssertCacheHitReplaysMiss(db, sql, options);
        StringAssert.DoesNotContain("on=true", PlanRenderer.Render(plan));

        // The repaired plan runs from the cache hit; its rows match the hand-checked count and the
        // rows of the cost-based engine.
        List<string> rows = await QueryRows(db.Executor, db, sql);
        Assert.AreEqual(ExpectedProbeRows(), rows.Count);
        CollectionAssert.AreEqual(await RowsFromEngine(db, DpOn, sql), rows);
    }

    // ─── EXPLAIN estimates ───────────────────────────────────────────────────

    [Test]
    public async Task Explain_JoinOverAJoin_UsesTheKeyDistinctCountOfALeftTable()
    {
        Db db = await CreateDb(DpOff);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, bid int64)");
        await Seed(db, ("a", 10), ("b", 1000), ("c", 10_000));

        List<QueryResultRow> rows = await Explain(db, "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.bid = b.id");

        // The top join takes c through b.id, which sits inside the left input. The left input has
        // 10 × 1000 × 0.1 = 1000 rows. b.id is the primary key of b, so its distinct count is the
        // 1000 rows of b: 1000 × 10,000 / 1000 = 10,000. Without the table of b.id the estimate is
        // the fixed 1000 × 10,000 × 0.1.
        Assert.AreEqual(10_000, TopJoinEstimate(rows), RenderExplain(rows));
    }

    [Test]
    public async Task Explain_KeyUniqueInItsTable_IsNotUniqueAfterAFanOutJoin()
    {
        Db db = await CreateDb(DpOff);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, bid int64)");
        await Exec(db, "INSERT INTO a (id, x) VALUES (1, 1), (2, 1), (3, 1)");
        await Exec(db, "INSERT INTO b (id) VALUES (1)");
        await Exec(db, "INSERT INTO c (id, bid) VALUES (1, 1), (2, 1)");
        await Seed(db, ("a", 3), ("b", 1), ("c", 2));

        const string sql = "SELECT a.id AS ia, c.id AS ic FROM a JOIN b ON a.x = b.id JOIN c ON b.id = c.bid";

        // Each of the 3 rows of a carries b.id = 1, and each of them matches both rows of c: 6 rows.
        // b.id is unique in b but repeats 3 times in the left input of the second join, so the
        // unique-key shortcut (one left match per c row → 2) does not apply there.
        List<QueryResultRow> plan = await Explain(db, sql);
        Assert.AreEqual(6, TopJoinEstimate(plan), RenderExplain(plan));

        Assert.AreEqual(6, (await QueryRows(db.Executor, db, sql)).Count);
    }

    // ─── Volatile and non-shippable ON conjuncts ─────────────────────────────

    [Test]
    public async Task VolatileOnConjunct_StaysOnTheJoinItWasWrittenOn()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, bid int64)");
        await Seed(db, ("a", 1), ("b", 1), ("c", 100));

        // random() runs once for each row that reaches its join. Written on the join with c, it
        // decides once for each of the c rows; at the first join it would decide once for all of
        // them.
        const string sql = "SELECT c.id FROM a JOIN b ON a.x = b.x JOIN c ON c.bid = b.id AND random() < 0.5";

        foreach (CamusDBOptions options in new[] { DpOn, DpOff })
        {
            QueryPlan plan = await Plan(db, sql, options);
            string rendered = PlanRenderer.Render(plan);

            foreach (PhysicalPlanNode join in JoinNodes(plan.Root))
            {
                bool hasRandom = ScalarFunctionEvaluator.ContainsVolatileFunction(OnPredicate(join));
                Assert.AreEqual(RightAlias(join) == "c", hasRandom, rendered);
            }
        }
    }

    [Test]
    public async Task VolatileOnConjunct_NoTableJoinsInsideItsScope()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, y int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, y int64)");
        await Seed(db, ("a", 1_000_000), ("b", 1_000_000), ("c", 10));

        // Cost alone would start with the small table c. The volatile conjunct must see each (a, b)
        // pair once, so a and b join first; c, joined between them, would repeat each pair once
        // for each matching c row.
        QueryPlan plan = await Plan(db,
            "SELECT a.id FROM a JOIN b ON a.x = b.x AND random() < 0.5 JOIN c ON c.y = b.y", DpOn);
        string rendered = PlanRenderer.Render(plan);

        List<string> order = JoinOrder(plan.Root);
        Assert.AreEqual("c", order[2], rendered);

        PhysicalPlanNode first = JoinNodes(plan.Root).Single(j => j.Input is TableScanNode);
        Assert.IsTrue(ScalarFunctionEvaluator.ContainsVolatileFunction(OnPredicate(first)), rendered);
    }

    [Test]
    public async Task FlagOff_VolatileOnPredicate_KeepsTheDeclaredOrder()
    {
        Db db = await CreateDb(DpOff);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, y int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, y int64, z int64)");
        await Ddl(db, "CREATE TABLE d (id int64 primary key, z int64)");

        // The primary-key filters score b and c first, which would join c between a and b, inside
        // the scope of random().
        QueryPlan plan = await Plan(db,
            "SELECT a.id FROM a JOIN b ON a.x = b.x AND random() < 0.5 JOIN c ON c.y = b.y JOIN d ON d.z = c.z " +
            "WHERE b.id = 1 AND c.id = 1", DpOff);

        CollectionAssert.AreEqual(new[] { "a", "b", "c", "d" }, JoinOrder(plan.Root), PlanRenderer.Render(plan));
    }

    [Test]
    public async Task NonShippableOnConjunct_DoesNotMoveOntoAnEarlierJoin()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, bid int64)");
        await Seed(db, ("a", 1000), ("b", 1000), ("c", 1_000_000));

        // a.x = @p names a only, but a parameter cannot be shipped to a peer node: on the first
        // join it would stop a broadcast join there. It stays on the join it was written on.
        Dictionary<string, ColumnValue> parameters = new() { { "@p", new ColumnValue(ColumnType.Integer64, 1L) } };
        QueryPlan plan = await Plan(db,
            "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON b.id = c.bid AND a.x = @p", DpOn, parameters: parameters);
        string rendered = PlanRenderer.Render(plan);

        foreach (PhysicalPlanNode join in JoinNodes(plan.Root))
        {
            bool shippable = FragmentFilterShippability.IsShippable(OnPredicate(join));
            Assert.AreEqual(RightAlias(join) != "c", shippable, rendered);
        }
    }

    [Test]
    public async Task NonShippableOnConjunct_StaysWithItsClauseAfterAReorder()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE f (id int64 primary key, k int64)");
        await Ddl(db, "CREATE TABLE d (id int64 primary key, k int64, name string(16))");
        await Ddl(db, "CREATE TABLE t (id int64 primary key, k int64)");
        await Seed(db, ("f", 10_000_000), ("d", 10), ("t", 1_000_000));

        // The cheapest order is d, t, f: the large table f joins last. d.name <> @p was written in
        // the ON clause of t, so it stays on the join of d and t, as the whole clause would. Kept
        // with its full scope instead, it would land on the join that adds f, the join a broadcast
        // probe of f needs.
        Dictionary<string, ColumnValue> parameters = new() { { "@p", new ColumnValue(ColumnType.String, "x") } };
        QueryPlan plan = await Plan(db,
            "SELECT f.id FROM f JOIN d ON f.k = d.k JOIN t ON t.k = d.k AND d.name <> @p", DpOn, parameters: parameters);
        string rendered = PlanRenderer.Render(plan);

        CollectionAssert.AreEqual(new[] { "d", "t", "f" }, JoinOrder(plan.Root), rendered);

        foreach (PhysicalPlanNode join in JoinNodes(plan.Root))
        {
            bool shippable = FragmentFilterShippability.IsShippable(OnPredicate(join));
            Assert.AreEqual(RightAlias(join) == "f", shippable, rendered);
        }
    }

    // ─── The built operator is the costed operator ───────────────────────────

    [Test]
    public async Task FilteredRightInput_GetsTheIndexLookupTheOrderWasCostedWith()
    {
        foreach (bool accessPaths in new[] { false, true })
        {
            CamusDBOptions options = DpOn with { CostBasedAccessPathEnabled = accessPaths };
            Db db = await CreateDb(options);
            await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
            await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, f int64, g int64, h int64)");
            await Ddl(db, "CREATE INDEX b_x_idx ON b (x)");
            await Seed(db, ("a", 100_000), ("b", 1_000_000));

            // About 1000 rows of b pass the filter, but a hash join reads all 1,000,000 rows of b:
            // 100,000 index lookups are cheaper, and that is what the order was costed with.
            const string sql = "SELECT a.id FROM a JOIN b ON a.x = b.x WHERE b.f = 1 AND b.g = 2 AND b.h = 3";

            QueryPlan plan = await Plan(db, sql, options);
            Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root, PlanRenderer.Render(plan));

            await AssertBuiltOperatorsAreTheCostedOperators(db, sql, options);
        }
    }

    [Test]
    public async Task IndexedFilter_IsReadThroughTheIndexOnEitherInput()
    {
        CamusDBOptions options = DpOn with { CostBasedAccessPathEnabled = true };
        Db db = await CreateDb(options);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, f int64)");
        await Ddl(db, "CREATE INDEX b_f_idx ON b (f)");
        await Seed(db, ("a", 1000), ("b", 1_000_000));

        // b is read through its index on f whichever input it is: as the first input it is an index
        // leaf, and as the right input of a hash or nested-loop join the join reads it through the
        // same leaf. The search costs it that way in both positions, so the order is chosen for the
        // reads the plan makes, and the full 1,000,000-row scan of b never happens.
        QueryPlan plan = await Plan(db, "SELECT a.id FROM a JOIN b ON a.x = b.x WHERE b.f = 5", options);
        string rendered = PlanRenderer.Render(plan);

        int indexReads = FindNodes<IndexRangeScanNode>(plan.Root).Count;
        foreach (PhysicalPlanNode join in JoinNodes(plan.Root))
        {
            PhysicalPlanNode? rightInput = join switch
            {
                HashJoinNode hash => hash.RightPhysicalNode,
                NestedLoopJoinNode nestedLoop => nestedLoop.RightPhysicalNode,
                _ => null,
            };

            if (rightInput is IndexRangeScanNode)
                indexReads++;
        }

        Assert.AreEqual(1, indexReads, rendered);
        Assert.That(rendered, Does.Contain("b_f_idx"), rendered);

        await AssertBuiltOperatorsAreTheCostedOperators(db, "SELECT a.id FROM a JOIN b ON a.x = b.x WHERE b.f = 5", options);
    }

    [Test]
    public async Task JoinedLeftInput_GetsTheIndexLookupTheOrderWasCostedWith()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, k int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, k int64)");
        await Ddl(db, "CREATE INDEX c_k_idx ON c (k)");
        await Seed(db, ("a", 10), ("b", 10), ("c", 5000));

        // a ⋈ b has about 10 rows: 10 index lookups into c. Seen as the fixed default of 10,000
        // rows, the same join would look cheaper as a hash join over the 5000 rows of c.
        const string sql = "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.k = b.k";

        QueryPlan plan = await Plan(db, sql, DpOn);
        string rendered = PlanRenderer.Render(plan);

        CollectionAssert.AreEqual(new[] { "a", "b", "c" }, JoinOrder(plan.Root), rendered);
        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root, rendered);

        await AssertBuiltOperatorsAreTheCostedOperators(db, sql, DpOn);
    }

    [Test]
    public async Task CostBasedPlanCacheHit_KeepsTheOperatorsOfTheMiss()
    {
        CamusDBOptions options = DpOn with { PlanCacheEnabled = true };
        Db db = await CreateDb(options);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64, k int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, k int64)");
        await Ddl(db, "CREATE INDEX c_k_idx ON c (k)");
        await Seed(db, ("a", 10), ("b", 10), ("c", 5000));

        QueryPlan hit = await AssertCacheHitReplaysMiss(db, "SELECT a.id FROM a JOIN b ON a.x = b.x JOIN c ON c.k = b.k", options);
        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(hit.Root, PlanRenderer.Render(hit));
    }

    // ─── Assertions ──────────────────────────────────────────────────────────

    /// <summary>
    /// Plans <paramref name="sql"/> two times through one cache: the first is the miss, the second
    /// must be a hit that renders the same plan and the same post-join filter, with no replay
    /// failure. Returns the plan of the hit.
    /// </summary>
    private async Task<QueryPlan> AssertCacheHitReplaysMiss(Db db, string sql, CamusDBOptions options)
    {
        PlanCache cache = db.Executor.PlanCache;
        cache.Clear();

        QueryPlan miss = await Plan(db, sql, options, cache);

        long hits = cache.Hits;
        long failures = cache.ReplayFailures;

        QueryPlan hit = await Plan(db, sql, options, cache);

        Assert.AreEqual(hits + 1, cache.Hits, "The second plan must be a cache hit.");
        Assert.AreEqual(failures, cache.ReplayFailures, "The hit must replay the cached order.");
        Assert.AreEqual(PlanRenderer.Render(miss), PlanRenderer.Render(hit));
        Assert.AreEqual(
            miss.ExecutionFilter is null ? null : PlanRenderer.RenderExpr(miss.ExecutionFilter),
            hit.ExecutionFilter is null ? null : PlanRenderer.RenderExpr(hit.ExecutionFilter));

        // A cost-based replay estimates only the prefixes of the cached order, where the miss
        // estimated every subset. The estimates of each join must still be the same, bit for bit:
        // they choose the operators, and EXPLAIN (ANALYZE) prints them.
        Assert.AreEqual(JoinOrderEstimates(miss.Root), JoinOrderEstimates(hit.Root),
            "The hit must carry the join-order estimates of the miss.");

        return hit;
    }

    /// <summary>The join-order search estimate of each node of the left chain, root first.</summary>
    private static List<double?> JoinOrderEstimates(PhysicalPlanNode root)
    {
        List<double?> estimates = [];
        for (PhysicalPlanNode? node = root; node is not null; node = node.Input)
            estimates.Add(node.JoinOrderEstimatedRows);
        return estimates;
    }

    private static void AssertEachConjunctOnceAndInScope(JoinGraph graph, QuerySource tree, string sql)
    {
        Dictionary<NodeAst, ulong> maskByNode = new(ReferenceEqualityComparer.Instance);
        foreach (JoinGraph.Conjunct conjunct in graph.Conjuncts)
            maskByNode[conjunct.Node] = conjunct.Mask;

        List<NodeAst> attached = [];
        Stack<JoinSource> joins = new();
        QuerySource current = tree;

        while (current is JoinSource join)
        {
            joins.Push(join);
            current = join.Left;
        }

        ulong inTree = LeafBit(graph, current);

        while (joins.Count > 0)
        {
            JoinSource join = joins.Pop();
            inTree |= LeafBit(graph, join.Right);

            List<NodeAst> conjuncts = [];
            PredicateAnalyzer.CollectAndConjuncts(join.OnPredicate, conjuncts);

            foreach (NodeAst conjunct in conjuncts)
            {
                if (ReferenceEquals(conjunct, NodeAst.True))
                    continue;

                Assert.IsTrue(maskByNode.TryGetValue(conjunct, out ulong mask), $"A conjunct outside the pool in `{sql}`");
                Assert.AreEqual(0UL, mask & ~inTree, $"A conjunct attached before its tables in `{sql}`");
                attached.Add(conjunct);
            }
        }

        CollectionAssert.AreEquivalent(graph.Conjuncts.Select(c => c.Node).ToList(), attached,
            $"Each pool conjunct must be attached exactly once in `{sql}`");
    }

    private static ulong LeafBit(JoinGraph graph, QuerySource source)
    {
        string alias = source switch
        {
            TableSource table => table.Alias ?? table.TableName,
            DerivedTableSource derived => derived.Alias,
            _ => throw new AssertionException($"Unexpected leaf {source.GetType().Name}"),
        };

        for (int i = 0; i < graph.LeafCount; i++)
        {
            if (string.Equals(graph.Leaves[i].Alias, alias, StringComparison.OrdinalIgnoreCase))
                return 1UL << i;
        }

        throw new AssertionException($"Alias {alias} is not a leaf");
    }

    private async Task AssertSameRowsOnAndOff(Db db, string sql)
    {
        List<string> on = await QueryRows(db.Executor, db, sql);
        List<string> off = await RowsFromEngine(db, DpOff, sql);

        CollectionAssert.AreEqual(off, on, $"Flag on and off must return the same rows for `{sql}`");
    }

    // ─── Setup ───────────────────────────────────────────────────────────────

    private async Task<Db> CreateDb(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        return new Db(name, database, executor);
    }

    private async Task<Db> CreateProbeTables(CamusDBOptions options)
    {
        Db db = await CreateDb(options);
        await CreateTables(db, "r0", "r1", "r2");
        await Seed(db, ("r0", 1_000_000), ("r1", 1_000_000), ("r2", 10));
        return db;
    }

    /// <summary>a(10), b(1000, primary key id), c(10,000, bid → b.id), d(1000).</summary>
    private async Task<Db> CreateUniqueKeyTables()
    {
        Db db = await CreateDb(DpOn);
        await Ddl(db, "CREATE TABLE a (id int64 primary key, x int64, k int64)");
        await Ddl(db, "CREATE TABLE b (id int64 primary key, x int64)");
        await Ddl(db, "CREATE TABLE c (id int64 primary key, bid int64)");
        await Ddl(db, "CREATE TABLE d (id int64 primary key, k int64)");
        await Seed(db, ("a", 10), ("b", 1000), ("c", 10_000), ("d", 1000));
        return db;
    }

    /// <summary>Tables with columns (id, a, b, c, x, y, z) and a primary key on id only.</summary>
    private static async Task CreateTables(Db db, params string[] names)
    {
        foreach (string name in names)
            await Ddl(db, $"CREATE TABLE {name} (id int64 primary key, a int64, b int64, c int64, x int64, y int64, z int64)");
    }

    /// <summary>Six rows in each table, with repeated small values so the joins match many rows.</summary>
    private static async Task InsertRows(Db db, params string[] names)
    {
        foreach (string name in names)
        {
            string values = string.Join(", ", Enumerable.Range(1, 6).Select(i =>
                $"({i}, {i % 3}, {i % 2}, {(i + 1) % 3}, {i}, {i % 4}, {i % 5})"));
            await Exec(db, $"INSERT INTO {name} (id, a, b, c, x, y, z) VALUES {values}");
        }
    }

    /// <summary>Rows of the probe comma join over the data of <see cref="InsertRows"/>, counted by hand.</summary>
    private static int ExpectedProbeRows()
    {
        int count = 0;
        for (int i0 = 1; i0 <= 6; i0++)
        for (int i1 = 1; i1 <= 6; i1++)
        for (int i2 = 1; i2 <= 6; i2++)
        {
            if (i0 % 3 == i2 % 3 && i1 % 2 == i2 % 2)
                count++;
        }

        return count;
    }

    private static async Task Seed(Db db, params (string Table, long Rows)[] counts)
    {
        foreach ((string table, long rows) in counts)
        {
            TableDescriptor descriptor = await db.Database.TableDescriptors[table];
            db.Executor.Statistics.SeedRowCountForTesting(db.Database, descriptor, rows);
        }
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    /// <summary>Runs <c>ANALYZE</c> on <paramref name="table"/>, so its histograms and distinct counts exist.</summary>
    private static async Task Analyze(Db db, string table)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: "ANALYZE " + table, parameters: null));
        await foreach (QueryResultRow _ in cursor) { }
        await db.Database.Transactions.CommitAsync(txn);
    }

    /// <summary>Runs <paramref name="sql"/> through <paramref name="executor"/>; rows as sorted "column=value" strings.</summary>
    private static async Task<List<string>> QueryRows(CommandExecutor executor, Db db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db.Name);
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<string> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(string.Join("|", row.Row.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"{kv.Key}={(kv.Value.Type == ColumnType.Null ? "null" : kv.Value.LongValue.ToString())}")));

        await database.Transactions.CommitAsync(txn);

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /// <summary>The rows of <paramref name="sql"/> from a second engine with <paramref name="options"/>.</summary>
    private async Task<List<string>> RowsFromEngine(Db db, CamusDBOptions options, string sql)
    {
        CommandExecutor engine = CreateCommandExecutor(options);
        TrackDatabase(db.Name, engine);
        return await QueryRows(engine, db, sql);
    }

    private async Task<(BoundSelectQuery Bound, QueryTicket Ticket)> Bind(
        Db db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        ExecuteSQLTicket executeTicket = new(
            txnState: await db.Database.Transactions.BeginAsync(),
            database: db.Name,
            sql: sql,
            parameters: parameters);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(db.Executor.Catalogs, logger))
            .BindAsync(db.Database, selectQuery);

        return (bound, QueryTicketAdapter.ToQueryTicket(bound, executeTicket));
    }

    private async Task<QueryPlan> Plan(
        Db db,
        string sql,
        CamusDBOptions options,
        PlanCache? cache = null,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db, sql, parameters);
        return new JoinQueryPlanner(db.Executor.Statistics, cache, options).GetPlan(db.Database, bound, ticket);
    }

    /// <summary>The row estimate of each table set of <paramref name="sql"/>, keyed by its sorted aliases.</summary>
    private async Task<Dictionary<string, double>> SubsetEstimates(Db db, string sql)
    {
        (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db, sql);
        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

        JoinGraph graph = JoinGraph.TryBuild(bound.Query.Source, bound, pushdown.PostJoinConjuncts)
            ?? throw new AssertionException($"The graph refused `{sql}`");

        double[] leafRows = new double[graph.LeafCount];
        for (int i = 0; i < graph.LeafCount; i++)
        {
            TableDescriptor table = bound.Sources.Single(s => s.Alias == graph.Leaves[i].Alias).Table;
            leafRows[i] = db.Executor.Statistics.GetRowCountEstimate(db.Database, table)!.Value;
        }

        double[] rows = JoinEnumerator.EstimateSubsetRows(graph, leafRows, bound, db.Database, db.Executor.Statistics);

        Dictionary<string, double> bySubset = new(StringComparer.Ordinal);
        for (int mask = 1; mask < rows.Length; mask++)
        {
            string key = string.Join(",", Enumerable.Range(0, graph.LeafCount)
                .Where(i => (mask & (1 << i)) != 0)
                .Select(i => graph.Leaves[i].Alias)
                .OrderBy(a => a, StringComparer.Ordinal));

            bySubset[key] = rows[mask];
        }

        return bySubset;
    }

    // ─── Plan helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// The operator of each join that the enumerator costed must be the operator the builder
    /// built. Merge join is outside the cost model and must not appear in these shapes.
    /// </summary>
    private async Task AssertBuiltOperatorsAreTheCostedOperators(Db db, string sql, CamusDBOptions options)
    {
        (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db, sql);
        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

        JoinOrderChoice choice = JoinEnumerator.Enumerate(
            bound.Query.Source, bound, pushdown, db.Database, db.Executor.Statistics, options);

        Assert.AreEqual(JoinBuildMode.CostBased, choice.Mode);
        Dictionary<string, JoinStepOperator> costed = new(StringComparer.OrdinalIgnoreCase);
        foreach ((JoinSource join, JoinStepEstimate estimate) in choice.StepEstimates!)
            costed[LeafAlias(join.Right)] = estimate.Operator;

        QueryPlan plan = await Plan(db, sql, options);
        string rendered = PlanRenderer.Render(plan);

        foreach (PhysicalPlanNode join in JoinNodes(plan.Root))
        {
            JoinStepOperator built = join switch
            {
                HashJoinNode => JoinStepOperator.Hash,
                IndexNestedLoopJoinNode => JoinStepOperator.IndexLookup,
                NestedLoopJoinNode => JoinStepOperator.NestedLoop,
                _ => throw new AssertionException($"Unexpected join node {join.GetType().Name}:\n{rendered}"),
            };

            Assert.AreEqual(costed[RightAlias(join)], built, $"join with {RightAlias(join)}:\n{rendered}");
        }
    }

    private static List<PhysicalPlanNode> JoinNodes(PhysicalPlanNode? root) =>
        FindNodes<PhysicalPlanNode>(root)
            .Where(n => n is HashJoinNode or IndexNestedLoopJoinNode or NestedLoopJoinNode or MergeJoinNode)
            .ToList();

    private static string RightAlias(PhysicalPlanNode join) => join switch
    {
        HashJoinNode hj => hj.BuildSource.Alias,
        IndexNestedLoopJoinNode inlj => inlj.RightSource.Alias,
        NestedLoopJoinNode nlj => nlj.RightSource.Alias,
        MergeJoinNode mj => mj.RightSource.Alias,
        _ => throw new AssertionException($"Not a join: {join.GetType().Name}"),
    };

    private static NodeAst OnPredicate(PhysicalPlanNode join) => join switch
    {
        HashJoinNode hj => hj.OnPredicate!,
        IndexNestedLoopJoinNode inlj => inlj.OnPredicate,
        NestedLoopJoinNode nlj => nlj.OnPredicate,
        MergeJoinNode mj => mj.OnPredicate!,
        _ => throw new AssertionException($"Not a join: {join.GetType().Name}"),
    };

    private static string LeafAlias(QuerySource source) => source switch
    {
        TableSource table => table.Alias ?? table.TableName,
        DerivedTableSource derived => derived.Alias,
        _ => throw new AssertionException($"Unexpected leaf {source.GetType().Name}"),
    };

    private static async Task<List<QueryResultRow>> Explain(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: "EXPLAIN " + sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    /// <summary>The estimated rows of the first join row of an EXPLAIN, which is the top join.</summary>
    private static long TopJoinEstimate(List<QueryResultRow> rows) =>
        rows.First(r => r.Row["node"].StrValue is "hash-join" or "nested-loop-join" or "index-nested-loop-join" or "merge-join")
            .Row["estimated_rows"].LongValue;

    private static string RenderExplain(List<QueryResultRow> rows) =>
        string.Join("\n", rows.Select(r => $"{r.Row["node"].StrValue}({r.Row["detail"].StrValue}) rows={r.Row["estimated_rows"]}"));

    private static NodeAst Identifier(string text) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, text);

    private static bool HasKey(HashJoinNode join, string column) =>
        join.BuildKeyColumns.Any(k => k == column) || join.ProbeKeyColumns.Any(k => k.EndsWith("." + column, StringComparison.Ordinal));

    private static IEnumerable<int[]> Permutations(int n)
    {
        int[] items = Enumerable.Range(0, n).ToArray();
        return Permute(items, 0);

        static IEnumerable<int[]> Permute(int[] items, int start)
        {
            if (start == items.Length)
            {
                yield return (int[])items.Clone();
                yield break;
            }

            for (int i = start; i < items.Length; i++)
            {
                (items[start], items[i]) = (items[i], items[start]);
                foreach (int[] permutation in Permute(items, start + 1))
                    yield return permutation;
                (items[start], items[i]) = (items[i], items[start]);
            }
        }
    }

    private static List<T> FindNodes<T>(PhysicalPlanNode? root) where T : PhysicalPlanNode
    {
        List<T> found = [];
        PhysicalPlanNode? current = root;

        while (current is not null)
        {
            if (current is T match)
                found.Add(match);

            current = current.Input;
        }

        return found;
    }

    /// <summary>The aliases of a left-deep join plan, leftmost leaf first.</summary>
    private static List<string> JoinOrder(PhysicalPlanNode node)
    {
        Stack<string> rights = new();
        PhysicalPlanNode? current = node;

        while (current is not null)
        {
            switch (current)
            {
                case HashJoinNode hj:
                    rights.Push(hj.BuildSource.Alias);
                    break;
                case IndexNestedLoopJoinNode inlj:
                    rights.Push(inlj.RightSource.Alias);
                    break;
                case NestedLoopJoinNode nlj:
                    rights.Push(nlj.RightSource.Alias);
                    break;
                case MergeJoinNode mj:
                    rights.Push(mj.RightSource.Alias);
                    break;
                case TableScanNode { BoundSource: { } scan }:
                    return [scan.Alias, .. rights];
                case IndexRangeScanNode { BoundSource: { } range }:
                    return [range.Alias, .. rights];
                case IndexInListScanNode { BoundSource: { } inList }:
                    return [inList.Alias, .. rights];
            }

            current = current.Input;
        }

        throw new AssertionException("No leaf scan on the left spine");
    }
}
