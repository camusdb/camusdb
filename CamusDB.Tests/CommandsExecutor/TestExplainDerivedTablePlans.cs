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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// EXPLAIN and EXPLAIN (ANALYZE) of derived tables: an inlined CTE, a derived table in FROM and a view body
/// show the operators of their own query under their <c>derived-table-scan</c> line, also on the right side
/// of a join, and with ANALYZE each level has its row count and its time.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestExplainDerivedTablePlans : SharedNodeBaseTest
{
    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Twelve items in three groups (<c>grp = id % 3</c>, <c>v = id * 10</c>), and the three groups.</summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY, grp INT64, v INT64)");
        await Ddl(executor, database, db, "CREATE TABLE groups (id INT64 PRIMARY KEY, name STRING)");
        await Dml(executor, database, db, "INSERT INTO items (id, grp, v) SELECT g, g % 3, g * 10 FROM generate_series(1, 12) g");
        await Dml(executor, database, db, "INSERT INTO groups (id, name) VALUES (0, 'zero'), (1, 'one'), (2, 'two')");
        return (db, database, executor);
    }

    private const string ChainSql =
        "WITH a AS (SELECT id, grp, v FROM items WHERE v > 30), " +
        "b AS (SELECT a.id, a.grp, a.v * 2 AS w FROM a JOIN groups g ON g.id = a.grp), " +
        "c AS (SELECT grp, SUM(w) AS total FROM b GROUP BY grp) " +
        "SELECT g.name, c.total FROM groups g JOIN c ON c.grp = g.id";

    /// <summary>One plan line: the node, its detail, and the ANALYZE counters when the statement has them.</summary>
    private sealed record Line(string Node, string Detail, long? ActualRows, long? RowsRead, double? TimeMs);

    private static async Task<List<Line>> Plan(CommandExecutor executor, DatabaseDescriptor database, string db, string sql) =>
        (await Query(executor, database, db, sql)).Select(r => new Line(
            r.Row["node"].StrValue!,
            r.Row["detail"].StrValue!,
            r.Row.TryGetValue("actual_rows", out ColumnValue? rows) && rows.Type == ColumnType.Integer64 ? rows.LongValue : null,
            r.Row.TryGetValue("rows_read", out ColumnValue? read) && read.Type == ColumnType.Integer64 ? read.LongValue : null,
            r.Row.TryGetValue("actual_time_ms", out ColumnValue? ms) && ms.Type == ColumnType.Float64 ? ms.FloatValue : null)).ToList();

    private static int IndexOf(List<Line> plan, string node, string detailStart) =>
        plan.FindIndex(l => l.Node == node && l.Detail.StartsWith(detailStart, System.StringComparison.Ordinal));

    [Test]
    public async Task ACteChainInAJoinShowsTheOperatorsOfEachLevelWithRowsAndTime()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<Line> plan = await Plan(executor, database, db, "EXPLAIN (ANALYZE) " + ChainSql);

        int c = IndexOf(plan, "derived-table-scan", "alias=c");
        int b = IndexOf(plan, "derived-table-scan", "alias=b");
        int a = IndexOf(plan, "derived-table-scan", "alias=a");
        int items = IndexOf(plan, "table-scan", "table=items");

        // Each level comes under the level that reads it.
        Assert.That(c, Is.GreaterThanOrEqualTo(0), string.Join("\n", plan));
        Assert.That(b, Is.GreaterThan(c));
        Assert.That(a, Is.GreaterThan(b));
        Assert.That(items, Is.GreaterThan(a));
        Assert.That(IndexOf(plan, "filter", "v > 30"), Is.GreaterThan(a));

        // v > 30 keeps items 4 to 12; each joins its group; three groups remain.
        Assert.That(plan[a].ActualRows, Is.EqualTo(9));
        Assert.That(plan[b].ActualRows, Is.EqualTo(9));
        Assert.That(plan[c].ActualRows, Is.EqualTo(3));
        Assert.That(plan[items].RowsRead, Is.EqualTo(12));

        foreach (int level in new[] { a, b, c })
            Assert.That(plan[level].TimeMs, Is.Not.Null.And.GreaterThanOrEqualTo(0), plan[level].ToString());

        List<QueryResultRow> rows = await Query(executor, database, db, ChainSql + " ORDER BY g.name");
        CollectionAssert.AreEqual(new long[] { 420, 480, 540 }, rows.Select(r => r.Row["total"].LongValue).ToArray());
    }

    [Test]
    public async Task PlainExplainShowsTheSameLevels()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<Line> plan = await Plan(executor, database, db, "EXPLAIN " + ChainSql);

        int c = IndexOf(plan, "derived-table-scan", "alias=c");
        int b = IndexOf(plan, "derived-table-scan", "alias=b");
        int a = IndexOf(plan, "derived-table-scan", "alias=a");

        Assert.That(c, Is.GreaterThanOrEqualTo(0), string.Join("\n", plan));
        Assert.That(b, Is.GreaterThan(c));
        Assert.That(a, Is.GreaterThan(b));
        Assert.That(IndexOf(plan, "table-scan", "table=items"), Is.GreaterThan(a));
        Assert.That(plan.All(l => l.ActualRows is null), Is.True);
    }

    [Test]
    public async Task ADerivedTableOnTheRightOfAJoinHasALineOfItsOwn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // A left join keeps groups on the left, so the derived table is the right side.
        const string sql =
            "SELECT g.name, d.n FROM groups g LEFT JOIN (SELECT grp, COUNT(*) AS n FROM items WHERE v > 30 GROUP BY grp) d ON d.grp = g.id";

        List<Line> plan = await Plan(executor, database, db, "EXPLAIN (ANALYZE) " + sql);

        int join = plan.FindIndex(l => l.Node.EndsWith("join", System.StringComparison.Ordinal));
        int d = IndexOf(plan, "derived-table-scan", "alias=d");

        Assert.That(join, Is.GreaterThanOrEqualTo(0), string.Join("\n", plan));
        Assert.That(d, Is.EqualTo(join + 1), string.Join("\n", plan));
        Assert.That(plan[d].ActualRows, Is.EqualTo(3));
        Assert.That(plan[d].TimeMs, Is.Not.Null);
        Assert.That(IndexOf(plan, "aggregate", "group=[grp]"), Is.GreaterThan(d));
        Assert.That(plan[IndexOf(plan, "table-scan", "table=items")].RowsRead, Is.EqualTo(12));

        List<QueryResultRow> rows = await Query(executor, database, db, sql);
        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows.All(r => r.Row["n"].LongValue == 3), Is.True);
    }

    [Test]
    public async Task ACteComputedOnceShowsItsPlanOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        const string sql =
            "WITH s AS MATERIALIZED (SELECT grp, v FROM items WHERE v > 50) SELECT x.grp, y.v FROM s x JOIN s y ON y.grp = x.grp";

        List<Line> plan = await Plan(executor, database, db, "EXPLAIN (ANALYZE) " + sql);

        List<Line> readers = plan.Where(l => l.Node == "derived-table-scan" && l.Detail.Contains("computed_once=s")).ToList();
        Assert.That(readers, Has.Count.EqualTo(2), string.Join("\n", plan));
        Assert.That(plan.Count(l => l.Node == "table-scan" && l.Detail.StartsWith("table=items", System.StringComparison.Ordinal)), Is.EqualTo(1));
        // The reader that shows the plan has the time of the run; the other reads the kept rows in its own time.
        Assert.That(readers.All(l => l.TimeMs is not null), Is.True, "each reader has a time");
        Assert.That(readers.All(l => l.ActualRows == 7), Is.True);

        List<QueryResultRow> rows = await Query(executor, database, db, sql);
        Assert.That(rows, Has.Count.EqualTo(17));
    }

    [Test]
    public async Task AViewBodyShowsItsPlan()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE VIEW big AS SELECT id, grp FROM items WHERE v > 60");

        List<Line> plan = await Plan(executor, database, db, "EXPLAIN (ANALYZE) SELECT g.name, b.id FROM groups g JOIN big b ON b.grp = g.id");

        int view = IndexOf(plan, "derived-table-scan", "alias=b");
        Assert.That(view, Is.GreaterThanOrEqualTo(0), string.Join("\n", plan));
        Assert.That(plan[view].ActualRows, Is.EqualTo(6));
        Assert.That(IndexOf(plan, "filter", "v > 60"), Is.GreaterThan(view));
    }

    [TestCase("EXPLAIN ")]
    [TestCase("EXPLAIN (ANALYZE) ")]
    public async Task EachRowHasTheDepthOfItsNodeSoTheRightPlanEndsBeforeTheLeftInput(string explain)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        const string sql =
            "SELECT g.name, d.n FROM groups g LEFT JOIN (SELECT grp, COUNT(*) AS n FROM items WHERE v > 30 GROUP BY grp) d ON d.grp = g.id";

        List<QueryResultRow> rows = await Query(executor, database, db, explain + sql);
        List<(string Node, string Detail, long? Depth)> plan = rows
            .Select(r => (r.Row["node"].StrValue!, r.Row["detail"].StrValue!,
                r.Row.TryGetValue("depth", out ColumnValue? depth) && depth.Type == ColumnType.Integer64 ? depth.LongValue : (long?)null))
            .ToList();
        string all = string.Join("\n", plan);

        int join = plan.FindIndex(l => l.Node.EndsWith("join", System.StringComparison.Ordinal));
        int d = plan.FindIndex(l => l.Node == "derived-table-scan" && l.Detail.StartsWith("alias=d", System.StringComparison.Ordinal));
        int groups = plan.FindIndex(l => l.Node == "table-scan" && l.Detail.StartsWith("table=groups", System.StringComparison.Ordinal));
        int items = plan.FindIndex(l => l.Node == "table-scan" && l.Detail.StartsWith("table=items", System.StringComparison.Ordinal));

        Assert.That(join, Is.GreaterThanOrEqualTo(0), all);
        Assert.That(plan[0].Depth, Is.EqualTo(0), all);

        // The right derived table is one level under the join, and the plan of its query under it.
        Assert.That(plan[d].Depth, Is.EqualTo(plan[join].Depth + 1), all);
        Assert.That(items, Is.GreaterThan(d), all);
        for (int i = d + 1; i <= items; i++)
            Assert.That(plan[i].Depth, Is.GreaterThan(plan[d].Depth), all);

        // The left input comes after that plan, back at the level of the derived table.
        Assert.That(groups, Is.GreaterThan(items), all);
        Assert.That(plan[groups].Depth, Is.EqualTo(plan[d].Depth), all);

        // Parent first: each operator row is at most one level under the row before it. The rows about
        // the whole plan and the span rows describe no node, so they have no depth.
        for (int i = 0; i < plan.Count; i++)
        {
            if (plan[i].Node is "plan-info" or "distribution" or "cache" or "gather-span" or "broadcast-span")
            {
                Assert.That(plan[i].Depth, Is.Null, all);
                continue;
            }

            Assert.That(plan[i].Depth, Is.Not.Null, all);
            if (i > 0 && plan[i - 1].Depth is { } previous)
                Assert.That(plan[i].Depth, Is.LessThanOrEqualTo(previous + 1), all);
        }
    }

    [Test]
    public async Task ALateralBodyKeepsItsOneLine()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<Line> plan = await Plan(executor, database, db,
            "EXPLAIN SELECT g.name, c.n FROM groups g CROSS JOIN LATERAL (SELECT COUNT(*) AS n FROM items x WHERE x.grp = g.id) c");

        Assert.That(plan.Any(l => l.Detail.Contains("lateral", System.StringComparison.Ordinal)), Is.True, string.Join("\n", plan));
        Assert.That(IndexOf(plan, "derived-table-scan", "alias=c"), Is.EqualTo(-1));
    }
}
