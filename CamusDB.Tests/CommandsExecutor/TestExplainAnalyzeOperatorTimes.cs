/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// EXPLAIN (ANALYZE) gives every operator line its inclusive time, also inside the plan of a derived table,
/// and a nested-loop join over a <c>LATERAL</c> body shows the runs of the body and their time. The fixture
/// runs in shared-node mode, where scans can gather spans, and in standalone mode.
/// </summary>
[TestFixture]
[NonParallelizable]
internal class TestExplainAnalyzeOperatorTimes : SharedNodeBaseTest
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

    /// <summary>The plan rows of an EXPLAIN (ANALYZE) result, without the plan-info row and the span rows.</summary>
    private static List<QueryResultRow> PlanRows(List<QueryResultRow> rows) =>
        rows.Where(r => r.Row["node"].StrValue is not ("plan-info" or "gather-span" or "broadcast-span")).ToList();

    private static string Dump(List<QueryResultRow> rows) =>
        string.Join("\n", rows.Select(r =>
            $"{new string(' ', 2 * (int)(r.Row.TryGetValue("depth", out ColumnValue? d) && d.Type == ColumnType.Integer64 ? d.LongValue : 0))}" +
            $"{r.Row["node"].StrValue}({r.Row["detail"].StrValue}) rows={Text(r, "actual_rows")} ms={Text(r, "actual_time_ms")}"));

    private static string Text(QueryResultRow row, string column) =>
        row.Row.TryGetValue(column, out ColumnValue? value) ? value.Type switch
        {
            ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float64 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
            _ => "NULL",
        } : "NULL";

    private static long Counter(string detail, string name)
    {
        Match match = Regex.Match(detail, $@"\b{name}=(\d+)");
        Assert.That(match.Success, Is.True, $"'{name}=' is in '{detail}'");
        return long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    private static double LateralMs(string detail)
    {
        Match match = Regex.Match(detail, @"\blateral-ms=([0-9.]+)");
        Assert.That(match.Success, Is.True, $"'lateral-ms=' is in '{detail}'");
        return double.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    [Test]
    public async Task EveryOperatorOfACteChainHasATimeWithinItsParent()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(executor, database, db,
            "EXPLAIN (ANALYZE) " +
            "WITH a AS (SELECT id, grp, v FROM items WHERE v > 30), " +
            "b AS (SELECT a.id, a.grp, a.v * 2 AS w FROM a JOIN groups g ON g.id = a.grp), " +
            "c AS (SELECT grp, SUM(w) AS total FROM b GROUP BY grp) " +
            "SELECT g.name, c.total FROM groups g JOIN c ON c.grp = g.id"));

        // The operators inside the plans of the derived tables: the join of b, the aggregate of c, the scans.
        Assert.That(rows.Count(r => r.Row["node"].StrValue!.EndsWith("join")), Is.GreaterThanOrEqualTo(2), Dump(rows));
        Assert.That(rows.Count(r => r.Row["node"].StrValue == "table-scan"), Is.GreaterThanOrEqualTo(3), Dump(rows));

        TestExplainAnalyzeExecutor.AssertEachTimeIsWithinItsParent(rows);
    }

    [Test]
    public async Task ALateralBodyOfExpressionsShowsItsRunsAndItsTime()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = PlanRows(await Query(executor, database, db,
            "EXPLAIN (ANALYZE) SELECT i.id, w.x FROM items i CROSS JOIN LATERAL (VALUES (i.v), (i.v * 2)) AS w (x)"));

        QueryResultRow join = rows.Single(r => r.Row["node"].StrValue == "nested-loop-join");
        string detail = join.Row["detail"].StrValue!;

        Assert.That(detail, Does.Contain("lateral=expressions(x)"), Dump(rows));
        Assert.That(Counter(detail, "lateral-runs"), Is.EqualTo(12), "one run for each item");
        Assert.That(detail, Does.Not.Contain("lateral-kept-reads="), "a body of expressions keeps no rows");
        Assert.That(join.Row["actual_rows"].LongValue, Is.EqualTo(24), "two rows for each item");
        Assert.That(LateralMs(detail), Is.LessThanOrEqualTo(join.Row["actual_time_ms"].FloatValue),
            "the time of the body is part of the time of the join");

        TestExplainAnalyzeExecutor.AssertEachTimeIsWithinItsParent(rows);
    }

    [Test]
    public async Task ALateralSubqueryShowsItsRunsAndItsReadsOfKeptRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // Twelve items in three groups: the body runs once for each group, and the nine other items read
        // the rows that the run of their group kept.
        List<QueryResultRow> rows = PlanRows(await Query(executor, database, db,
            "EXPLAIN (ANALYZE) SELECT i.id, c.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM items x WHERE x.grp = i.grp) c"));

        QueryResultRow join = rows.Single(r => r.Row["node"].StrValue == "nested-loop-join");
        string detail = join.Row["detail"].StrValue!;

        Assert.That(Counter(detail, "lateral-runs"), Is.EqualTo(3), Dump(rows));
        Assert.That(Counter(detail, "lateral-kept-reads"), Is.EqualTo(9), Dump(rows));
        Assert.That(join.Row["actual_rows"].LongValue, Is.EqualTo(12));
        Assert.That(LateralMs(detail), Is.LessThanOrEqualTo(join.Row["actual_time_ms"].FloatValue));

        TestExplainAnalyzeExecutor.AssertEachTimeIsWithinItsParent(rows);
    }

    [Test]
    public async Task APlainExplainShowsNoRunsOfALateralBody()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "EXPLAIN SELECT i.id, w.x FROM items i CROSS JOIN LATERAL (VALUES (i.v), (i.v * 2)) AS w (x)");

        string detail = rows.Single(r => r.Row["node"].StrValue == "nested-loop-join").Row["detail"].StrValue!;

        Assert.That(detail, Does.Not.Contain("lateral-runs="));
        Assert.That(detail, Does.Not.Contain("lateral-ms="));
    }

    [Test]
    public async Task ACursorTimeLeavesOutTheWorkOfItsConsumer()
    {
        // Each row takes at least 30 ms to produce, and the consumer then works 100 ms on it. The node's
        // time covers the 3 requests (90 ms or more) and none of the 300 ms of the consumer.
        PlanNodeStats stats = new();

        await foreach (QueryResultRow _ in PlanNodeCursor.CountAndTime(SlowRows(3, 30), stats))
            await Task.Delay(100);

        Assert.That(stats.RowsEmitted, Is.EqualTo(3));
        Assert.That(stats.CursorTimed, Is.True);
        Assert.That(stats.CursorMs, Is.GreaterThanOrEqualTo(85));
        Assert.That(stats.CursorMs, Is.LessThan(250));
    }

    [Test]
    public async Task ACursorThatItsConsumerStopsEarlyAddsNoTimeAfterTheLastRow()
    {
        PlanNodeStats stats = new();

        await foreach (QueryResultRow _ in PlanNodeCursor.CountAndTime(SlowRows(5, 30), stats))
        {
            await Task.Delay(200);
            break;
        }

        Assert.That(stats.RowsEmitted, Is.EqualTo(1));
        Assert.That(stats.CursorMs, Is.GreaterThanOrEqualTo(25));
        Assert.That(stats.CursorMs, Is.LessThan(150), "the 200 ms after the row are the consumer's");
    }

    [Test]
    public void ANodeWithoutATimerShowsNoTime()
    {
        PlanNodeStats stats = new() { CursorTicks = 1000 };

        Assert.That(stats.CursorMs, Is.Null);
        Assert.That(stats.ReportedMs, Is.Null);

        stats.ElapsedMs = 4.5;
        Assert.That(stats.ReportedMs, Is.EqualTo(4.5), "a time set from outside the cursor is printed in its place");
    }

    private static async IAsyncEnumerable<QueryResultRow> SlowRows(int count, int delayMs)
    {
        for (int i = 0; i < count; i++)
        {
            await Task.Delay(delayMs);
            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>());
        }
    }
}

internal sealed class TestExplainAnalyzeOperatorTimesStandalone : TestExplainAnalyzeOperatorTimes
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
