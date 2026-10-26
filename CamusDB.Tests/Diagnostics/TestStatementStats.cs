/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Unit coverage for the parts of statement statistics and index usage that do not need an engine:
/// the shape fingerprint, the table's totals and bounds, and the rule that decides when an index
/// starts being tracked.
/// </summary>
[TestFixture]
internal sealed class TestStatementStats
{
    private static string Shape(string sql) => StatementShape.Of(SQLParserProcessor.Parse(sql));

    // ─── Shape ──────────────────────────────────────────────────────────────

    [Test]
    public void LiteralsAndPlaceholdersDoNotChangeTheShape()
    {
        string shape = Shape("SELECT name FROM robots WHERE year = 2001 AND name = 'a'");

        Assert.AreEqual(shape, Shape("SELECT name FROM robots WHERE year = 1999 AND name = 'zzz'"));
        Assert.AreEqual(shape, Shape("SELECT name FROM robots WHERE year = @y AND name = @n"));
        Assert.AreEqual(shape, Shape("select NAME from ROBOTS where YEAR = 7 and NAME = 'q'"), "identifiers are case-insensitive");
    }

    [Test]
    public void ColumnsOperatorsAndTablesDoChangeTheShape()
    {
        string shape = Shape("SELECT name FROM robots WHERE year = 2001");

        Assert.AreNotEqual(shape, Shape("SELECT name FROM robots WHERE id = 2001"));
        Assert.AreNotEqual(shape, Shape("SELECT name FROM robots WHERE year > 2001"));
        Assert.AreNotEqual(shape, Shape("SELECT name FROM androids WHERE year = 2001"));
        Assert.AreNotEqual(shape, Shape("SELECT year FROM robots WHERE year = 2001"));
    }

    [Test]
    public void ConstantListsCollapseWhateverTheirLength()
    {
        Assert.AreEqual(
            Shape("SELECT * FROM robots WHERE year IN (1, 2)"),
            Shape("SELECT * FROM robots WHERE year IN (1, 2, 3, 4, 5, 6, 7)"));

        Assert.AreEqual(
            Shape("INSERT INTO robots (id, name) VALUES (1, 'a')"),
            Shape("INSERT INTO robots (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"));
    }

    [Test]
    public void AVeryLongInListDoesNotOverflowTheStack()
    {
        string values = string.Join(", ", Enumerable.Range(0, 20_000));

        Assert.AreEqual(
            Shape("SELECT * FROM robots WHERE year IN (1, 2)"),
            Shape($"SELECT * FROM robots WHERE year IN ({values})"));
    }

    [Test]
    public void OnlyTheWorkloadKindsAreRecorded()
    {
        Assert.AreEqual("select", StatementShape.KindOf(NodeType.Select));
        Assert.AreEqual("insert", StatementShape.KindOf(NodeType.Insert));
        Assert.AreEqual("insert", StatementShape.KindOf(NodeType.InsertSelect));
        Assert.AreEqual("update", StatementShape.KindOf(NodeType.Update));
        Assert.AreEqual("delete", StatementShape.KindOf(NodeType.Delete));
        Assert.IsNull(StatementShape.KindOf(NodeType.ShowStatementStats));
        Assert.IsNull(StatementShape.KindOf(NodeType.CreateTable));
    }

    // ─── Table ──────────────────────────────────────────────────────────────

    private static void Record(StatementStatsTable table, string shape, double ms, bool failed = false, bool retryable = false, ulong plan = 1)
        => table.Record("db", shape, "select", "SELECT 1", DateTime.UtcNow, ms, rowsRead: 10, rowsReturned: 1,
            fullScan: false, spilled: false, failed: failed, retryable: retryable, planFingerprint: plan);

    [Test]
    public void TotalsAndDerivedValuesAreRight()
    {
        StatementStatsTable table = new(capacity: 10, maxSqlLength: 100);

        Record(table, "s", 10);
        Record(table, "s", 30);

        StatementStatsSnapshot row = table.Snapshot().Single();

        Assert.AreEqual(2, row.Calls);
        Assert.AreEqual(40, row.TotalMs, 1e-9);
        Assert.AreEqual(1000, row.SumSqMs, 1e-9);
        Assert.AreEqual(10, row.MinMs, 1e-9);
        Assert.AreEqual(30, row.MaxMs, 1e-9);
        Assert.AreEqual(20, row.MeanMs, 1e-9);
        Assert.AreEqual(10, row.StddevMs, 1e-9);
        Assert.AreEqual(0.5, row.Cv, 1e-9);
        Assert.AreEqual(20, row.RowsRead);
    }

    [Test]
    public void ErrorsRetriesAndPlanChangesAreCounted()
    {
        StatementStatsTable table = new(capacity: 10, maxSqlLength: 100);

        Record(table, "s", 1, plan: 7);
        Record(table, "s", 1, plan: 7);
        Record(table, "s", 1, plan: 9);
        Record(table, "s", 1, failed: true, retryable: true, plan: 0);
        Record(table, "s", 1, failed: true, plan: 9);

        StatementStatsSnapshot row = table.Snapshot().Single();

        Assert.AreEqual(2, row.Errors);
        Assert.AreEqual(1, row.Retries);
        Assert.AreEqual(1, row.PlanChanges, "a run with no plan is not a plan change");
        Assert.AreEqual(9UL, row.LastPlan);
    }

    [Test]
    public void TheSampleIsRedactedAndTruncated()
    {
        StatementStatsTable table = new(capacity: 10, maxSqlLength: 30);

        table.Record("db", "s", "select", "CREATE USER bob IDENTIFIED BY 'secret-password' AND MORE TEXT", DateTime.UtcNow,
            1, 0, 0, false, false, false, false, 0);

        string sample = table.Snapshot().Single().SampleSql;
        Assert.LessOrEqual(sample.Length, 30);
        StringAssert.DoesNotContain("secret", sample);
    }

    [Test]
    public async Task AFullTableAdmitsNothingNewUnderConcurrency()
    {
        StatementStatsTable table = new(capacity: 8, maxSqlLength: 100);

        await Task.WhenAll(Enumerable.Range(0, 64).Select(i => Task.Run(() =>
        {
            for (int j = 0; j < 50; j++)
                Record(table, $"shape{(i + j) % 32}", 1);
        })));

        Assert.AreEqual(8, table.Count);
        Assert.AreEqual(8, table.Snapshot().Count);
        Assert.Greater(table.Dropped, 0);
        Assert.AreEqual(64 * 50, table.Snapshot().Sum(r => r.Calls) + table.Dropped);
    }

    // ─── Index usage tracking ───────────────────────────────────────────────

    [Test]
    public async Task AnIndexThatAppearsOnATrackedTableStartsCountingWhenItAppears()
    {
        IndexUsageRegistry registry = new(new CamusDBOptions());

        TableIndexUsage usage = registry.Track("db", "t", ["~pk", "a"]);
        await Task.Delay(30);
        registry.Track("db", "t", ["~pk", "a", "b"]);

        Assert.AreEqual(registry.StartedAtUtc, usage.Find("a")!.TrackedSince);
        Assert.Greater(usage.Find("b")!.TrackedSince, registry.StartedAtUtc.AddMilliseconds(20));
        Assert.AreSame(usage, registry.Find("db", "t"), "a descriptor rebuild keeps the same counters");
    }

    [Test]
    public void CountingStopsWhenDisabled()
    {
        IndexUsageRegistry registry = new(new CamusDBOptions());
        TableIndexUsage usage = registry.Track("db", "t", ["a"]);

        Assert.IsNotNull(usage.BeginRead("a"));
        registry.ApplyOptions(new CamusDBOptions { IndexUsageEnabled = false });
        Assert.IsNull(usage.BeginRead("a"));

        Assert.AreEqual(1, usage.Find("a")!.Reads, "what was counted is kept");
    }
}
