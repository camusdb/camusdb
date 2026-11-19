/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
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
/// The row estimates of derived tables and of <c>LATERAL</c> bodies of expressions. A derived table with a
/// query of its own gets the estimate of the root of that query's plan, so a chain of CTEs carries its
/// estimates up from the bottom. A nested-loop join over a body of expressions gives the rows of the body for
/// each left row, instead of a cross product with the join fallback.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestDerivedAndLateralEstimates : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    // 1,000 items in 10 groups (grp = id % 10, v = id), and the 10 groups.
    private const int Items = 1000;

    private async Task<Db> Seeded(CamusDBOptions? options = null)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE items (id INT64 PRIMARY KEY, grp INT64, v INT64)");
        await Ddl(db, "CREATE TABLE groups (id INT64 PRIMARY KEY, name STRING)");
        await Exec(db, $"INSERT INTO items (id, grp, v) SELECT g, g % 10, g FROM generate_series(0, {Items - 1}) g");
        await Exec(db, "INSERT INTO groups (id, name) SELECT g, 'g' || CAST(g AS STRING) FROM generate_series(0, 9) g");
        await Query(db, "ANALYZE items");
        await Query(db, "ANALYZE groups");
        return db;
    }

    private static long Estimate(QueryResultRow row) => row.Row["estimated_rows"].LongValue;

    private static long Depth(QueryResultRow row) => row.Row["depth"].LongValue;

    private static string Node(QueryResultRow row) => row.Row["node"].StrValue ?? "";

    private static string Detail(QueryResultRow row) => row.Row["detail"].StrValue ?? "";

    private static string Rule(QueryResultRow row)
    {
        using JsonDocument trace = JsonDocument.Parse(row.Row["estimate_trace"].StrValue!);
        return trace.RootElement.GetProperty("rule").GetString()!;
    }

    private static string Dump(List<QueryResultRow> rows) =>
        string.Join("\n", rows.Select(r =>
            $"{new string(' ', 2 * (int)(r.Row.TryGetValue("depth", out ColumnValue? d) && d.Type == ColumnType.Integer64 ? d.LongValue : 0))}" +
            $"{Node(r)}({Detail(r)}) est={(r.Row.TryGetValue("estimated_rows", out ColumnValue? e) && e.Type == ColumnType.Integer64 ? e.LongValue : -1)}" +
            $" rows={(r.Row.TryGetValue("actual_rows", out ColumnValue? a) && a.Type == ColumnType.Integer64 ? a.LongValue : -1)}"));

    /// <summary>The plan rows, without the plan-info row and the span rows.</summary>
    private static List<QueryResultRow> PlanRows(List<QueryResultRow> rows) =>
        rows.Where(r => Node(r) is not ("plan-info" or "gather-span" or "broadcast-span")).ToList();

    /// <summary>The line of the derived table <paramref name="alias"/> and the root line of its query's plan under it.</summary>
    private static (QueryResultRow Derived, QueryResultRow InnerRoot) DerivedLine(List<QueryResultRow> rows, string alias)
    {
        int at = rows.FindIndex(r => Node(r) == "derived-table-scan" && Detail(r).StartsWith($"alias={alias}"));
        Assert.That(at, Is.GreaterThanOrEqualTo(0), $"a line for {alias}:\n{Dump(rows)}");
        Assert.That(at + 1, Is.LessThan(rows.Count), Dump(rows));
        Assert.That(Depth(rows[at + 1]), Is.EqualTo(Depth(rows[at]) + 1), $"the plan of {alias} is under its line:\n{Dump(rows)}");
        return (rows[at], rows[at + 1]);
    }

    [Test]
    public async Task ACteGetsTheEstimateOfItsPlan()
    {
        Db db = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN (ANALYZE) WITH a AS (SELECT id, v FROM items WHERE grp = 3) SELECT a.id FROM a"));

        (QueryResultRow derived, QueryResultRow innerRoot) = DerivedLine(rows, "a");

        Assert.That(Estimate(derived), Is.EqualTo(Estimate(innerRoot)), Dump(rows));
        Assert.That(Estimate(derived), Is.Not.EqualTo(CostEstimator.DefaultTableRowCount), Dump(rows));
        Assert.That(Rule(derived), Is.EqualTo("derived-plan"));

        long actual = derived.Row["actual_rows"].LongValue;
        Assert.That(actual, Is.EqualTo(Items / 10));
        Assert.That(Estimate(derived), Is.InRange(actual / 10, actual * 10), "within 10x of the actual count");
    }

    [Test]
    public async Task AThreeLevelCteChainCarriesEachEstimateUp()
    {
        Db db = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN (ANALYZE) " +
            "WITH a AS (SELECT id, grp, v FROM items WHERE grp < 5), " +
            "b AS (SELECT a.id, a.grp, a.v * 2 AS w FROM a JOIN groups g ON g.id = a.grp), " +
            "c AS (SELECT b.id, b.w + 1 AS x FROM b) " +
            "SELECT c.id, c.x FROM c"));

        foreach (string alias in new[] { "a", "b", "c" })
        {
            (QueryResultRow derived, QueryResultRow innerRoot) = DerivedLine(rows, alias);

            Assert.That(Estimate(derived), Is.EqualTo(Estimate(innerRoot)), $"{alias}:\n{Dump(rows)}");
            Assert.That(Rule(derived), Is.EqualTo("derived-plan"), alias);

            long actual = derived.Row["actual_rows"].LongValue;
            Assert.That(actual, Is.EqualTo(Items / 2), alias);
            Assert.That(Estimate(derived), Is.InRange(actual / 10, actual * 10), $"{alias} within 10x:\n{Dump(rows)}");
        }
    }

    [Test]
    public async Task AnExpressionLateralChainGivesOneRowForEachLeftRow()
    {
        Db db = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN (ANALYZE) SELECT i.id, z.c FROM items i " +
            "CROSS JOIN LATERAL (SELECT i.v * 2 AS a) AS x " +
            "CROSS JOIN LATERAL (SELECT x.a + 1 AS b) AS y " +
            "CROSS JOIN LATERAL (SELECT y.b * 3 AS c) AS z"));

        List<QueryResultRow> joins = rows.Where(r => Node(r) == "nested-loop-join").ToList();
        Assert.That(joins, Has.Count.EqualTo(3), Dump(rows));

        foreach (QueryResultRow join in joins)
        {
            Assert.That(Detail(join), Does.Contain("lateral=expressions("), Dump(rows));
            Assert.That(Estimate(join), Is.EqualTo(Items), $"one row for each left row:\n{Dump(rows)}");
            Assert.That(Rule(join), Is.EqualTo("lateral-rows"));
            Assert.That(join.Row["actual_rows"].LongValue, Is.EqualTo(Items));
        }
    }

    [Test]
    public async Task ALateralValuesBodyGivesItsRowsForEachLeftRowAndAFilterLowersThem()
    {
        Db db = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN (ANALYZE) SELECT i.id, w.x FROM items i " +
            "CROSS JOIN LATERAL (VALUES (i.v), (i.v + 1), (i.v + 2)) AS w (x) WHERE w.x <> 5"));

        QueryResultRow join = rows.Single(r => Node(r) == "nested-loop-join");

        Assert.That(Detail(join), Does.Contain("right-filter="), Dump(rows));
        // Three rows for each item, of which an inequality keeps 0.9 with no statistics.
        Assert.That(Estimate(join), Is.EqualTo((long)(Items * 3 * 0.9)), Dump(rows));
        Assert.That(join.Row["actual_rows"].LongValue, Is.EqualTo(Items * 3 - 3));
    }

    [Test]
    public async Task ARangeFilterOnALateralColumnKeepsAThird()
    {
        Db db = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN SELECT i.id FROM items i CROSS JOIN LATERAL (SELECT i.v - 500 AS d) AS x WHERE x.d > 0"));

        QueryResultRow join = rows.Single(r => Node(r) == "nested-loop-join");

        Assert.That(Detail(join), Does.Contain("right-filter="), Dump(rows));
        Assert.That(Estimate(join), Is.EqualTo(Items / 3), Dump(rows));
    }

    [Test]
    public async Task ANewPlanReadsTheCurrentStatisticsOfTheDerivedQuery()
    {
        Db db = await Seeded();
        const string Sql = "EXPLAIN WITH a AS (SELECT id FROM items WHERE grp = 3) SELECT a.id FROM a";

        long before = Estimate(DerivedLine(PlanRows(await Query(db, Sql)), "a").Derived);

        await Exec(db, $"INSERT INTO items (id, grp, v) SELECT g, g % 10, g FROM generate_series({Items}, {3 * Items - 1}) g");
        await Query(db, "ANALYZE items");

        long after = Estimate(DerivedLine(PlanRows(await Query(db, Sql)), "a").Derived);

        Assert.That(before, Is.EqualTo(Items / 10));
        Assert.That(after, Is.EqualTo(3 * Items / 10), "the same statement planned again reads the new count");
    }

    [Test]
    public async Task APlanCacheHitKeepsTheEstimateOfADerivedTable()
    {
        Db db = await Seeded(Options with { PlanCacheEnabled = true, CostBasedJoinOrderEnabled = true });
        const string Sql = "SELECT g.name FROM (SELECT id, grp FROM items WHERE grp = 3) a JOIN groups g ON g.id = a.grp";

        JoinQueryPlanner planner = new(db.Executor.Statistics, new PlanCache(), db.Executor.Options);

        (BoundSelectQuery firstBound, QueryTicket firstTicket) = await Bind(db, Sql);
        QueryPlan first = planner.GetPlan(db.Database, firstBound, firstTicket);

        (BoundSelectQuery secondBound, QueryTicket secondTicket) = await Bind(db, Sql);
        QueryPlan second = planner.GetPlan(db.Database, secondBound, secondTicket);

        Assert.That(second.JoinOrderSearch?.Path, Is.EqualTo("cached"), "the second plan is a cache hit");
        Assert.That(DerivedTableRowEstimator.RowsOf(secondBound.DerivedSources.Single()), Is.EqualTo(Items / 10),
            "the hit wrote the estimate of the derived table of its own bound query");
        Assert.That(second.Root.EstimatedCardinality, Is.EqualTo(first.Root.EstimatedCardinality));
    }

    // 20 spans of 10 values each over the v column of items: span g covers v from 50g to 50g + 9.
    private const int Spans = 20;

    private static async Task AddSpans(Db db)
    {
        await Ddl(db, "CREATE TABLE spans (id INT64 PRIMARY KEY, lo INT64, hi INT64)");
        await Exec(db, $"INSERT INTO spans (id, lo, hi) SELECT g, g * 50, g * 50 + 9 FROM generate_series(0, {Spans - 1}) g");
        await Query(db, "ANALYZE spans");
    }

    private static readonly string[] RangeJoins =
    [
        "ON i.v >= s.lo AND i.v <= s.hi",
        "ON s.hi >= i.v AND s.lo <= i.v",
        "ON i.v BETWEEN s.lo AND s.hi",
    ];

    [Test]
    public async Task ARangeJoinKeepsTheRangeShareOfTheCrossProduct([ValueSource(nameof(RangeJoins))] string on,
                                                                    [Values(true, false)] bool costBasedOrder)
    {
        Db db = await Seeded(Options with { CostBasedJoinOrderEnabled = costBasedOrder });
        await AddSpans(db);

        List<QueryResultRow> rows = PlanRows(await Query(db, $"EXPLAIN (ANALYZE) SELECT s.id, i.id FROM spans s JOIN items i {on}"));

        QueryResultRow join = rows.Single(r => Node(r).EndsWith("join"));

        // The two bounds of one column make one range, at the range share of the cross product.
        Assert.That(Estimate(join), Is.EqualTo((long)(Spans * Items * CardinalityEstimator.RangeJoinSelectivity)), Dump(rows));
        Assert.That(join.Row["estimate_trace"].StrValue, Does.Contain("\"join-range\""), Dump(rows));

        long actual = join.Row["actual_rows"].LongValue;
        Assert.That(actual, Is.EqualTo(Spans * 10));
        Assert.That(Estimate(join), Is.InRange(actual / 10, actual * 10), "within 10x of the actual count");
    }

    [Test]
    public async Task AnotherConjunctOfARangeJoinAddsTheJoinFallback([Values(true, false)] bool costBasedOrder)
    {
        Db db = await Seeded(Options with { CostBasedJoinOrderEnabled = costBasedOrder });
        await AddSpans(db);

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN SELECT s.id, i.id FROM spans s JOIN items i ON i.v >= s.lo AND i.v <= s.hi AND i.grp <> s.id"));

        QueryResultRow join = rows.Single(r => Node(r).EndsWith("join"));

        Assert.That(Estimate(join),
            Is.EqualTo((long)(Spans * Items * CardinalityEstimator.RangeJoinSelectivity * CardinalityEstimator.FallbackSelectivity)),
            Dump(rows));
    }

    [Test]
    public async Task OneBoundIsNotARange()
    {
        Db db = await Seeded(Options with { CostBasedJoinOrderEnabled = false });
        await AddSpans(db);

        List<QueryResultRow> rows = PlanRows(await Query(db,
            "EXPLAIN (ANALYZE) SELECT s.id, i.id FROM spans s JOIN items i ON i.v >= s.lo"));

        QueryResultRow join = rows.Single(r => Node(r).EndsWith("join"));

        Assert.That(join.Row["estimate_trace"].StrValue, Does.Not.Contain("\"join-range\""), Dump(rows));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<(BoundSelectQuery Bound, QueryTicket Ticket)> Bind(Db db, string sql)
    {
        ExecuteSQLTicket executeTicket = new(txnState: await db.Database.Transactions.BeginAsync(), database: db.Name, sql: sql, parameters: null);
        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(db.Executor.Catalogs, logger)).BindAsync(db.Database, selectQuery);
        return (bound, QueryTicketAdapter.ToQueryTicket(bound, executeTicket));
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

    private static async Task<List<QueryResultRow>> Query(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }
}
