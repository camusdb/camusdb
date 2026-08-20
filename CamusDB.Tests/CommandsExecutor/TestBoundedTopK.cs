
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for the bounded top-k sort. Every case asserts <b>which operator ran</b> through
/// EXPLAIN, because matching rows alone cannot distinguish a bounded retention from a full sort that
/// was trimmed afterwards — both return the same answer.
/// </summary>
internal sealed class TestBoundedTopK : SharedNodeBaseTest
{
    private const int RowCount = 20;

    private static string OID => ObjectIdGenerator.Generate().ToString();

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        return await cursor.ToListAsync();
    }

    /// <summary>The operator names EXPLAIN reports, top of plan first.</summary>
    private static async Task<List<string>> PlanNodes(
        CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await Select(executor, db, "EXPLAIN " + sql, parameters);
        return rows.Select(r => r.Row["node"].StrValue!).ToList();
    }

    private static async Task<string> PlanDetail(
        CommandExecutor executor, DatabaseDescriptor db, string sql, string node)
    {
        List<QueryResultRow> rows = await Select(executor, db, "EXPLAIN " + sql);
        return rows.Single(r => r.Row["node"].StrValue == node).Row["detail"].StrValue ?? "";
    }

    /// <summary>Rows 0..19 inserted in an order that is neither ascending nor descending by n.</summary>
    private Task<(DatabaseDescriptor db, CommandExecutor executor)> Setup() => Setup(Options);

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> Setup(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options);

        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, n int64, label string, PRIMARY KEY (id))");

        foreach (int i in Enumerable.Range(0, RowCount).OrderBy(x => (x * 7) % RowCount))
        {
            await Exec(executor, db, "INSERT INTO t (id, n, label) VALUES (@id, @n, @label)",
                new()
                {
                    { "@id", new(ColumnType.Id, OID) },
                    { "@n", new(ColumnType.Integer64, (long)i) },
                    { "@label", new(ColumnType.String, $"row{i:00}") },
                });
        }

        return (db, executor);
    }

    // ── The operator is chosen, and says so ──────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task OrderByWithLimit_UsesTopKAndReportsIt()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        List<string> nodes = await PlanNodes(executor, db, "SELECT n FROM t ORDER BY label LIMIT 3");

        CollectionAssert.Contains(nodes, "topk");
        CollectionAssert.DoesNotContain(nodes, "sort");
        StringAssert.Contains("k: 3", await PlanDetail(executor, db, "SELECT n FROM t ORDER BY label LIMIT 3", "topk"));
    }

    [Test]
    [NonParallelizable]
    public async Task OrderByWithoutLimit_KeepsTheFullSort()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        List<string> nodes = await PlanNodes(executor, db, "SELECT n FROM t ORDER BY label");

        CollectionAssert.Contains(nodes, "sort");
        CollectionAssert.DoesNotContain(nodes, "topk");
    }

    [Test]
    [NonParallelizable]
    public async Task TopK_ReturnsTheSameRowsAsTheFullSort()
    {
        // Unique keys, so the two operators must agree exactly rather than merely on membership.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        List<QueryResultRow> bounded = await Select(executor, db, "SELECT n FROM t ORDER BY label LIMIT 5");
        List<QueryResultRow> full = await Select(executor, db, "SELECT n FROM t ORDER BY label");

        CollectionAssert.AreEqual(
            full.Take(5).Select(r => r.Row["n"].LongValue).ToArray(),
            bounded.Select(r => r.Row["n"].LongValue).ToArray());
        CollectionAssert.AreEqual(new[] { 0L, 1L, 2L, 3L, 4L }, bounded.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task TopK_HonoursDescendingAndMultipleKeys()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        List<QueryResultRow> descending = await Select(executor, db, "SELECT n FROM t ORDER BY n DESC LIMIT 3");
        CollectionAssert.AreEqual(new[] { 19L, 18L, 17L }, descending.Select(r => r.Row["n"].LongValue).ToArray());

        List<QueryResultRow> mixed = await Select(executor, db,
            "SELECT n FROM t ORDER BY label DESC, n ASC LIMIT 3");
        CollectionAssert.AreEqual(new[] { 19L, 18L, 17L }, mixed.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── OFFSET must be ranked, not skipped ───────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task TopK_WithOffset_RetainsOffsetPlusLimit()
    {
        // The rows OFFSET skips still have to be ranked first, so k is 5, not 2. A bound of 2 would
        // return rows 0 and 1 after the skip — or nothing at all.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        const string sql = "SELECT n FROM t ORDER BY label LIMIT 2 OFFSET 3";

        StringAssert.Contains("k: 5", await PlanDetail(executor, db, sql, "topk"));

        List<QueryResultRow> rows = await Select(executor, db, sql);
        CollectionAssert.AreEqual(new[] { 3L, 4L }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── LIMIT 0 must not touch the data ──────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task LimitZero_ReturnsNothingWithoutEvaluatingTheOrdering()
    {
        // The ordering expression would raise on this data: the stored vectors are 4 elements and
        // the query vector is 2. Reaching a single row would surface that error, so a clean empty
        // result is the proof that no row was examined.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE docs (id OID NOT NULL, v bytes(64), PRIMARY KEY (id))");
        await Exec(executor, db, "INSERT INTO docs (id, v) VALUES (@id, @v)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@v", Pack([1f, 2f, 3f, 4f]) } });

        Dictionary<string, ColumnValue> mismatched = new() { { "@q", Pack([1f, 2f]) } };

        // Same query without the zero limit must genuinely fail, or the test above proves nothing.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await Select(
            executor, db, "SELECT id FROM docs ORDER BY l2_distance(v, @q)", mismatched));
        Assert.AreEqual(CamusDBErrorCodes.VectorDimensionMismatch, ex!.Code);

        List<QueryResultRow> rows = await Select(
            executor, db, "SELECT id FROM docs ORDER BY l2_distance(v, @q) LIMIT 0", mismatched);

        Assert.AreEqual(0, rows.Count);
    }

    // ── Bounds that cannot be represented must fall back, not wrap ───────────

    [Test]
    [NonParallelizable]
    public async Task OverflowingBound_FallsBackToTheFullSort()
    {
        // offset + limit overflows Int64. Wrapping would produce a small bound and silently drop
        // rows the query asked for.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        const string sql = "SELECT n FROM t ORDER BY label LIMIT 9223372036854775807 OFFSET 9223372036854775807";

        List<string> nodes = await PlanNodes(executor, db, sql);
        CollectionAssert.Contains(nodes, "sort");
        CollectionAssert.DoesNotContain(nodes, "topk");
    }

    [Test]
    [NonParallelizable]
    public async Task HugeBoundIsKeptWhenThereIsNoSpillPathToFallBackTo()
    {
        // With spill disabled the full sort buffers every row anyway, and the heap only grows as rows
        // arrive — so it holds min(k, rows), never more. A large k is therefore not a reason to give
        // up the bound here; it is only a reason when the external sort exists to take over.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup(Options with { SpillEnabled = false });

        const string sql = "SELECT n FROM t ORDER BY label LIMIT 100000000";

        CollectionAssert.Contains(await PlanNodes(executor, db, sql), "topk");

        List<QueryResultRow> rows = await Select(executor, db, sql);
        Assert.AreEqual(RowCount, rows.Count);
        CollectionAssert.AreEqual(Enumerable.Range(0, RowCount).Select(i => (long)i).ToArray(),
            rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── A row-reducing operator between sort and limit forbids the bound ─────

    [Test]
    [NonParallelizable]
    public async Task AggregateBetweenSortAndLimit_IsNotBounded()
    {
        // COUNT must see every row. Bounding the sort below it would answer with the bound instead
        // of the count — a wrong answer that looks entirely plausible.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        const string sql = "SELECT count(n) AS c FROM t ORDER BY n LIMIT 1";

        List<string> nodes = await PlanNodes(executor, db, sql);
        CollectionAssert.DoesNotContain(nodes, "topk");

        List<QueryResultRow> rows = await Select(executor, db, sql);
        Assert.AreEqual((long)RowCount, rows[0].Row["c"].LongValue);
    }

    // ── The spill threshold decides, and the fallback is visible ─────────────

    [Test]
    [NonParallelizable]
    public async Task BoundAboveTheSpillThreshold_FallsBackAndStillReturnsTheSameRows()
    {
        // ForceSpillThresholdRows is a test-only override folded into SpillEffectiveThreshold, so a
        // fixture that forces spill low also disables top-k. That interaction is deliberate and is
        // asserted here rather than left to surprise a future parity test.
        (DatabaseDescriptor db, CommandExecutor executor) =
            await Setup(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });

        const string boundedSql = "SELECT n FROM t ORDER BY label LIMIT 2";
        const string unboundedSql = "SELECT n FROM t ORDER BY label LIMIT 5";

        CollectionAssert.Contains(await PlanNodes(executor, db, boundedSql), "topk");
        CollectionAssert.Contains(await PlanNodes(executor, db, unboundedSql), "sort");

        List<QueryResultRow> rows = await Select(executor, db, unboundedSql);
        CollectionAssert.AreEqual(new[] { 0L, 1L, 2L, 3L, 4L }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── NULL placement and ties ──────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task TopK_PlacesNullsWhereTheFullSortDoes()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, n int64, label string, PRIMARY KEY (id))");

        await Exec(executor, db, "INSERT INTO t (id, n, label) VALUES (@id, 1, 'b')", new() { { "@id", new(ColumnType.Id, OID) } });
        await Exec(executor, db, "INSERT INTO t (id, n, label) VALUES (@id, 2, NULL)", new() { { "@id", new(ColumnType.Id, OID) } });
        await Exec(executor, db, "INSERT INTO t (id, n, label) VALUES (@id, 3, 'a')", new() { { "@id", new(ColumnType.Id, OID) } });

        List<QueryResultRow> bounded = await Select(executor, db, "SELECT n FROM t ORDER BY label LIMIT 2");
        List<QueryResultRow> full = await Select(executor, db, "SELECT n FROM t ORDER BY label");

        CollectionAssert.AreEqual(
            full.Take(2).Select(r => r.Row["n"].LongValue).ToArray(),
            bounded.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task TiedKeys_AgreeOnMembershipOnly()
    {
        // SQL leaves the order of equal keys unspecified, and the heap need not resolve a tie the
        // way List.Sort does. Asserting a particular order here would pin behaviour the engine does
        // not promise.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, n int64, label string, PRIMARY KEY (id))");

        for (int i = 0; i < 6; i++)
        {
            await Exec(executor, db, "INSERT INTO t (id, n, label) VALUES (@id, @n, 'same')",
                new() { { "@id", new(ColumnType.Id, OID) }, { "@n", new(ColumnType.Integer64, (long)i) } });
        }

        List<QueryResultRow> rows = await Select(executor, db, "SELECT n FROM t ORDER BY label LIMIT 3");

        Assert.AreEqual(3, rows.Count);
        CollectionAssert.IsSubsetOf(
            rows.Select(r => r.Row["n"].LongValue).ToArray(),
            new[] { 0L, 1L, 2L, 3L, 4L, 5L });
        CollectionAssert.AllItemsAreUnique(rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── The query the feature exists for ─────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task NearestNeighbourQuery_IsBoundedAndReturnsTheNearestRows()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db,
            "CREATE TABLE docs (id OID NOT NULL, tag string, embedding bytes(16), PRIMARY KEY (id))");

        await InsertDoc(executor, db, "d0", [1f, 0f, 0f, 0f]);
        await InsertDoc(executor, db, "d1", [0.9f, 0f, 0f, 0f]);
        await InsertDoc(executor, db, "d2", [0f, 1f, 0f, 0f]);
        await InsertDoc(executor, db, "d3", [0f, 0f, 1f, 0f]);

        Dictionary<string, ColumnValue> query = new() { { "@q", Pack([1f, 0f, 0f, 0f]) } };
        const string sql = "SELECT tag FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 2";

        List<QueryResultRow> plan = await Select(executor, db, "EXPLAIN " + sql, query);
        CollectionAssert.Contains(plan.Select(r => r.Row["node"].StrValue).ToList(), "topk");

        List<QueryResultRow> rows = await Select(executor, db, sql, query);
        CollectionAssert.AreEqual(new[] { "d0", "d1" }, rows.Select(r => r.Row["tag"].StrValue).ToArray());
    }

    private static ColumnValue Pack(float[] elements)
    {
        byte[] bytes = new byte[elements.Length * 4];

        for (int i = 0; i < elements.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), elements[i]);

        return new ColumnValue(bytes);
    }

    private static Task InsertDoc(CommandExecutor executor, DatabaseDescriptor db, string tag, float[] embedding)
        => Exec(executor, db, "INSERT INTO docs (id, tag, embedding) VALUES (@id, @tag, @e)",
            new()
            {
                { "@id", new(ColumnType.Id, OID) },
                { "@tag", new(ColumnType.String, tag) },
                { "@e", Pack(embedding) },
            });

    // ── Costing, EXPLAIN ANALYZE and plan-cache replay ───────────────────────

    /// <summary>All EXPLAIN columns for one node, by node name.</summary>
    private static async Task<QueryResultRow> PlanRow(
        CommandExecutor executor, DatabaseDescriptor db, string sql, string node,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await Select(executor, db, sql, parameters);
        return rows.Single(r => r.Row["node"].StrValue == node);
    }

    [Test]
    [NonParallelizable]
    public async Task BoundedSort_IsCostedByWhatItRetains()
    {
        // Pricing it as a full sort would keep charging for a materialization it no longer performs,
        // and every node above it would estimate from the wrong cardinality.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        QueryResultRow bounded = await PlanRow(executor, db,
            "EXPLAIN SELECT n FROM t ORDER BY label LIMIT 3", "topk");
        QueryResultRow full = await PlanRow(executor, db,
            "EXPLAIN SELECT n FROM t ORDER BY label", "sort");

        Assert.AreEqual(3L, bounded.Row["estimated_rows"].LongValue);
        Assert.Greater(full.Row["estimated_rows"].LongValue, 3L,
            "an unbounded sort must still be estimated at its full input");
    }

    [Test]
    [NonParallelizable]
    public async Task ExplainAnalyze_ShowsRowsExaminedAndRowsRetained()
    {
        // Rows examined is the scan's emitted count; rows retained is the operator's own. Together
        // they are the evidence that the bound did work rather than trimming after the fact.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        List<QueryResultRow> rows = await Select(executor, db,
            "EXPLAIN (ANALYZE) SELECT n FROM t ORDER BY label LIMIT 4");

        QueryResultRow topk = rows.Single(r => r.Row["node"].StrValue == "topk");
        QueryResultRow scan = rows.Single(r => r.Row["node"].StrValue == "table-scan");

        Assert.AreEqual((long)RowCount, scan.Row["actual_rows"].LongValue, "every row must be examined");
        Assert.AreEqual(4L, topk.Row["actual_rows"].LongValue, "only k rows may be retained");
    }

    [Test]
    [NonParallelizable]
    public async Task DifferentLimits_OnTheSameQueryShape_EachGetTheirOwnBound()
    {
        // The plan cache stores the access-path decision without literals and rebuilds the physical
        // nodes, so a second LIMIT must not inherit the first one's bound. A cached bound would
        // silently return the wrong number of rows for every later query of the same shape.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        StringAssert.Contains("k: 3", await PlanDetail(executor, db, "SELECT n FROM t ORDER BY label LIMIT 3", "topk"));
        StringAssert.Contains("k: 7", await PlanDetail(executor, db, "SELECT n FROM t ORDER BY label LIMIT 7", "topk"));
        StringAssert.Contains("k: 3", await PlanDetail(executor, db, "SELECT n FROM t ORDER BY label LIMIT 3", "topk"));

        List<QueryResultRow> three = await Select(executor, db, "SELECT n FROM t ORDER BY label LIMIT 3");
        List<QueryResultRow> seven = await Select(executor, db, "SELECT n FROM t ORDER BY label LIMIT 7");

        Assert.AreEqual(3, three.Count);
        Assert.AreEqual(7, seven.Count);
        CollectionAssert.AreEqual(new[] { 0L, 1L, 2L }, three.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task SameShapeAlternatingBetweenBoundedAndUnbounded_KeepsBothCorrect()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        CollectionAssert.Contains(await PlanNodes(executor, db, "SELECT n FROM t ORDER BY label LIMIT 2"), "topk");
        CollectionAssert.Contains(await PlanNodes(executor, db, "SELECT n FROM t ORDER BY label"), "sort");
        CollectionAssert.Contains(await PlanNodes(executor, db, "SELECT n FROM t ORDER BY label LIMIT 2"), "topk");

        Assert.AreEqual(RowCount, (await Select(executor, db, "SELECT n FROM t ORDER BY label")).Count);
        Assert.AreEqual(2, (await Select(executor, db, "SELECT n FROM t ORDER BY label LIMIT 2")).Count);
    }

    // ── The filter must run below the bound ──────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task SelectiveFilter_RunsBelowTheBound()
    {
        // The rows matching the filter rank last overall. If the bound were applied before the
        // filter, it would retain the globally-first rows, none of which match, and return nothing —
        // a plausible empty result rather than an error.
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        const string sql = "SELECT n FROM t WHERE n > 16 ORDER BY label LIMIT 2";

        List<QueryResultRow> rows = await Select(executor, db, sql);

        CollectionAssert.AreEqual(new[] { 17L, 18L }, rows.Select(r => r.Row["n"].LongValue).ToArray());
        CollectionAssert.Contains(await PlanNodes(executor, db, sql), "topk");
    }

    // ── Index-order elision still wins over the bound ────────────────────────

    [Test]
    [NonParallelizable]
    public async Task OrderSatisfiedByAnIndex_NeedsNeitherSortNorTopK()
    {
        // When the scan already produces the requested order there is nothing to bound: adding a
        // top-k here would reintroduce a buffering operator the planner had just removed.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE k (id OID NOT NULL, n int64, PRIMARY KEY (id))");
        await ExecDDL(executor, db, "CREATE INDEX k_n ON k (n)");

        for (int i = 0; i < 5; i++)
        {
            await Exec(executor, db, "INSERT INTO k (id, n) VALUES (@id, @n)",
                new() { { "@id", new(ColumnType.Id, OID) }, { "@n", new(ColumnType.Integer64, (long)i) } });
        }

        List<string> nodes = await PlanNodes(executor, db, "SELECT n FROM k ORDER BY n LIMIT 2");

        CollectionAssert.DoesNotContain(nodes, "sort");
        CollectionAssert.DoesNotContain(nodes, "topk");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT n FROM k ORDER BY n LIMIT 2");
        CollectionAssert.AreEqual(new[] { 0L, 1L }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task ExplainOfAVectorQuery_MatchesWhatTheDocumentationShows()
    {
        // docs/vector-search.md prints this line. A doc that shows an operator name the engine does
        // not emit sends readers looking for something that is not there.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db,
            "CREATE TABLE docs (id OID NOT NULL, tag string, embedding bytes(16), PRIMARY KEY (id))");
        await InsertDoc(executor, db, "only", [1f, 0f, 0f, 0f]);

        List<QueryResultRow> plan = await Select(executor, db,
            "EXPLAIN SELECT id FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 10",
            new() { { "@q", Pack([1f, 0f, 0f, 0f]) } });

        QueryResultRow topk = plan.Single(r => r.Row["node"].StrValue == "topk");

        Assert.AreEqual("k: 10, l2_distance(…) ASC", topk.Row["detail"].StrValue);
    }
}
