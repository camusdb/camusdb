/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies the partitioned GROUP BY spill path in <see cref="QueryAggregator"/>.
///
/// <para>
/// The spill path is activated by setting <c>SpillEnabled = true</c> and
/// <c>ForceSpillThresholdRows</c> to a small value so that the buffer overflows during a
/// controlled test. Correctness is asserted by comparing sorted GROUP BY results against the
/// flag-off (pure in-memory) path.
/// </para>
///
/// <para>
/// Correctness argument: because <see cref="QueryAggregator.GroupPartitionIndex"/> is
/// deterministic, all rows that share a group key hash to the same partition. Per-partition
/// in-memory aggregation therefore recovers the complete result for every group.
/// </para>
/// </summary>
[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestQueryAggregatorSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_agg_spill_" + System.Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private sealed record AggFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// Creates a <c>sales</c> table with <c>category</c> (string) and <c>amount</c> (int64).
    /// Inserts <paramref name="categories"/> × <paramref name="rowsPerCategory"/> rows so that
    /// each category has an equal number of rows with sequential amounts starting at 1.
    /// </summary>
    /// <summary>Spill disabled — the in-memory grouping path.</summary>
    private CamusDBOptions SpillOff => Options with { SpillEnabled = false, DataDirectory = _dataDir };

    /// <summary>
    /// Spill forced after <paramref name="thresholdRows"/> rows so the partitioned aggregate path runs
    /// on inputs small enough for a unit test. Independent of any other configuration in play.
    /// </summary>
    private CamusDBOptions SpillOn(int thresholdRows, int fanIn = 4) =>
        Options with
        {
            SpillEnabled = true,
            ForceSpillThresholdRows = thresholdRows,
            SpillMergeFanIn = fanIn,
            DataDirectory = _dataDir,
        };

    private async Task<AggFixture> SetupSales(CamusDBOptions options, int categories = 5, int rowsPerCategory = 4)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "sales",
            columns:
            [
                new("id",       ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
                new("amount",   ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> rows = new();
        for (int c = 0; c < categories; c++)
        {
            string cat = "Cat" + c;
            for (int r = 0; r < rowsPerCategory; r++)
            {
                rows.Add(new()
                {
                    { "id",       new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "category", new(ColumnType.String,    cat) },
                    { "amount",   new(ColumnType.Integer64, (long)(r + 1)) },
                });
            }
        }

        await executor.Insert(new InsertTicket(txn, dbname, "sales", values: rows));
        await database.Transactions.CommitAsync(txn);

        return new AggFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(
        AggFixture f, string sql, CamusDB.Core.Diagnostics.StatementProbe? probe = null)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null, probe: probe);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // Sort rows by category name for deterministic comparison (GROUP BY order is unspecified).
    private static List<(string Category, long Total)> SortedSums(List<QueryResultRow> rows) =>
        rows
            .Select(r => (
                Category: r.Row.TryGetValue("category", out var c) ? c.StrValue ?? "" : "",
                Total:    r.Row.TryGetValue("total",    out var t) ? t.LongValue      : 0L))
            .OrderBy(x => x.Category)
            .ToList();

    private static List<(string Category, long Count)> SortedCounts(List<QueryResultRow> rows) =>
        rows
            .Select(r => (
                Category: r.Row.TryGetValue("category", out var c) ? c.StrValue ?? "" : "",
                Count:    r.Row.TryGetValue("cnt",      out var n) ? n.LongValue      : 0L))
            .OrderBy(x => x.Category)
            .ToList();

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task GroupBySpill_Sum_MatchesInMemoryPath()
    {
        const string sql = "SELECT category, SUM(amount) AS total FROM sales GROUP BY category";
        AggFixture fRef = await SetupSales(SpillOff);
        List<QueryResultRow> reference = await Run(fRef, sql);
        // 5 categories × 4 rows; overflow after 5 rows
        AggFixture fSpill = await SetupSales(SpillOn(5, 4));
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        Assert.AreEqual(reference.Count, spillResult.Count,
            "Spill path must produce the same number of groups as the in-memory path.");
        CollectionAssert.AreEqual(SortedSums(reference), SortedSums(spillResult),
            "Each category's SUM must match between the in-memory and spill paths.");
    }

    [Test]
    public async Task GroupBySpill_Count_MatchesInMemoryPath()
    {
        const string sql = "SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category";
        AggFixture fRef = await SetupSales(SpillOff, categories: 6, rowsPerCategory: 3);
        List<QueryResultRow> reference = await Run(fRef, sql);
        // overflows during partition phase
        AggFixture fSpill = await SetupSales(SpillOn(4, 3), categories: 6, rowsPerCategory: 3);
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        Assert.AreEqual(reference.Count, spillResult.Count);
        CollectionAssert.AreEqual(SortedCounts(reference), SortedCounts(spillResult),
            "COUNT(*) per category must match between the in-memory and spill paths.");
    }

    [Test]
    public async Task GroupBySpill_NoSpillFilesRemainAfterCompletion()
    {

        AggFixture f = await SetupSales(SpillOn(3, 4));
        List<QueryResultRow> rows = await Run(f,
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category");

        Assert.That(rows.Count, Is.GreaterThan(0));

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] remaining = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(remaining,
                "All partition spill files must be deleted after the GROUP BY query completes.");
        }
    }

    [Test]
    public async Task GroupBySpill_FlagOff_UsesInMemoryPath()
    {

        AggFixture f = await SetupSales(SpillOff, categories: 3, rowsPerCategory: 2);
        List<QueryResultRow> rows = await Run(f,
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category");

        // 3 categories, each row amounts 1 and 2 → SUM = 3 per category
        Assert.That(rows.Count, Is.EqualTo(3));
        Assert.That(rows.All(r => r.Row.TryGetValue("total", out var v) && v.LongValue == 3L));
    }

    [Test]
    public async Task GroupBySpill_FlagOnVsOff_IdenticalResults()
    {
        const string sql = "SELECT category, SUM(amount) AS total FROM sales GROUP BY category";
        AggFixture fOff = await SetupSales(SpillOff, categories: 8, rowsPerCategory: 5);
        List<QueryResultRow> offRows = await Run(fOff, sql);
        AggFixture fOn = await SetupSales(SpillOn(6, 4), categories: 8, rowsPerCategory: 5);
        List<QueryResultRow> onRows = await Run(fOn, sql);

        CollectionAssert.AreEqual(SortedSums(offRows), SortedSums(onRows),
            "Spill-on and spill-off paths must produce identical sorted GROUP BY results.");
    }

    [Test]
    public async Task GroupBySpill_PartitionIndex_RedistributesDistinctGroupKeys()
    {
        // Verify that GroupPartitionIndex distributes distinct group keys across buckets
        // rather than clustering them all into one bucket (which would defeat the purpose
        // of partitioning). We generate 50 distinct CompositeColumnValue keys and assert
        // that at least 2 distinct buckets are used for K=4.
        const int K = 4;
        const int keyCount = 50;

        HashSet<int> buckets = new();
        for (int i = 0; i < keyCount; i++)
        {
            var key = new CompositeColumnValue(
                new[] { new ColumnValue(ColumnType.String, "key" + i) });
            buckets.Add(QueryAggregator.GroupPartitionIndex(key, K));
        }

        Assert.That(buckets.Count, Is.GreaterThan(1),
            $"GroupPartitionIndex must distribute {keyCount} distinct keys across more than 1 of {K} buckets.");
    }

    // ── Cardinality, not raw row count, decides whether a GROUP BY spills ─────

    /// <summary>
    /// Many rows collapsing into a handful of groups must not spill at all: the trigger is the number
    /// of distinct groups, so an aggregate over a low-cardinality key writes no partition file however
    /// many rows it reads. Before the trigger was aligned, this query spilled every row to disk.
    /// </summary>
    [Test]
    public async Task GroupBy_ManyRowsFewGroups_DoesNotSpill()
    {
        // 3 groups, 40 rows each: far past a 5-row cap, far below a 5-group cap.
        AggFixture f = await SetupSales(SpillOn(5, 4), categories: 3, rowsPerCategory: 40);

        CamusDB.Core.Diagnostics.StatementProbe probe = new();

        List<QueryResultRow> rows = await Run(f,
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category", probe);

        Assert.AreEqual(3, rows.Count);
        Assert.IsFalse(probe.Spilled,
            "an aggregate whose group count stays under the threshold must never start spilling");

        // 40 rows of amounts 1..40 → 820 per category.
        Assert.IsTrue(rows.All(r => r.Row["total"].LongValue == 820L));
    }

    /// <summary>
    /// The rows a group accumulated before spilling started exist nowhere but inside its accumulator,
    /// so the spill must carry that accumulator to the partition that receives the rest of the group.
    /// A group whose rows straddle the spill point is the case that catches a dropped or double-counted
    /// accumulator: its total is only right if the early rows are counted exactly once.
    /// </summary>
    [Test]
    public async Task GroupBy_GroupsStraddlingTheSpillPoint_CountEveryRowExactlyOnce()
    {
        const string sql =
            "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY category";

        // Rows arrive category by category, so the groups admitted before the 4-group cap is reached
        // keep receiving rows after the spill begins.
        AggFixture fOff = await SetupSales(SpillOff, categories: 9, rowsPerCategory: 7);
        AggFixture fOn = await SetupSales(SpillOn(4, 3), categories: 9, rowsPerCategory: 7);

        CamusDB.Core.Diagnostics.StatementProbe probe = new();

        List<QueryResultRow> offRows = await Run(fOff, sql);
        List<QueryResultRow> onRows = await Run(fOn, sql, probe);

        Assert.IsTrue(probe.Spilled, "this fixture must actually cross the spill threshold");
        CollectionAssert.AreEqual(SortedSums(offRows), SortedSums(onRows));
        CollectionAssert.AreEqual(SortedCounts(offRows), SortedCounts(onRows));

        // 7 rows of amounts 1..7 per category, counted once each.
        Assert.IsTrue(onRows.All(r => r.Row["cnt"].LongValue == 7L), "every row must be counted exactly once");
        Assert.IsTrue(onRows.All(r => r.Row["total"].LongValue == 28L), "every row must be summed exactly once");
    }

    /// <summary>
    /// A threshold of one forces a spill at the second group and then forces the partition level to
    /// overflow too, so the recursive path runs with carried accumulators at more than one depth.
    /// </summary>
    [Test]
    public async Task GroupBy_RecursiveOverflow_StillCountsEveryRowExactlyOnce()
    {
        const string sql =
            "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY category";

        AggFixture fOff = await SetupSales(SpillOff, categories: 12, rowsPerCategory: 5);
        AggFixture fOn = await SetupSales(SpillOn(1, 2), categories: 12, rowsPerCategory: 5);

        List<QueryResultRow> offRows = await Run(fOff, sql);
        List<QueryResultRow> onRows = await Run(fOn, sql);

        Assert.AreEqual(12, onRows.Count);
        CollectionAssert.AreEqual(SortedSums(offRows), SortedSums(onRows));
        CollectionAssert.AreEqual(SortedCounts(offRows), SortedCounts(onRows));
        Assert.IsTrue(onRows.All(r => r.Row["cnt"].LongValue == 5L));
    }

    /// <summary>
    /// Skew: one group holds most of the rows while the rest hold one each. The heavy group is admitted
    /// first, so its accumulator is the one carried across the spill boundary.
    /// </summary>
    [Test]
    public async Task GroupBy_SkewedGroups_MatchInMemoryPath()
    {
        const string sql = "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY category";

        async Task<AggFixture> SetupSkewed(CamusDBOptions options)
        {
            AggFixture fixture = await SetupSales(options, categories: 1, rowsPerCategory: 60);

            KvTransaction txn = await fixture.Database.Transactions.BeginAsync();
            List<Dictionary<string, ColumnValue>> tail = [];

            for (int i = 1; i <= 20; i++)
                tail.Add(new()
                {
                    { "id",       new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "category", new(ColumnType.String,    "Tail" + i) },
                    { "amount",   new(ColumnType.Integer64, (long)i) },
                });

            await fixture.Executor.Insert(new InsertTicket(txn, fixture.DbName, "sales", values: tail));
            await fixture.Database.Transactions.CommitAsync(txn);

            return fixture;
        }

        List<QueryResultRow> offRows = await Run(await SetupSkewed(SpillOff), sql);
        List<QueryResultRow> onRows = await Run(await SetupSkewed(SpillOn(3, 4)), sql);

        Assert.AreEqual(21, onRows.Count);
        CollectionAssert.AreEqual(SortedSums(offRows), SortedSums(onRows));
        CollectionAssert.AreEqual(SortedCounts(offRows), SortedCounts(onRows));
    }

    /// <summary>
    /// Wide multi-column group keys, a compound aggregate expression, HAVING, and a floating-point
    /// aggregate all have to survive the spill unchanged.
    /// </summary>
    [Test]
    public async Task GroupBy_WideKeysCompoundAggregatesAndHaving_MatchInMemoryPath()
    {
        async Task<AggFixture> SetupWide(CamusDBOptions options)
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: "wide",
                columns:
                [
                    new("id",     ColumnType.Id),
                    new("region", ColumnType.String, notNull: true),
                    new("label",  ColumnType.String, notNull: true),
                    new("amount", ColumnType.Integer64),
                    new("ratio",  ColumnType.Float64),
                ],
                constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
                ifNotExists: false));

            string padding = new('x', 200);
            List<Dictionary<string, ColumnValue>> rows = [];

            for (int g = 0; g < 12; g++)
            {
                for (int r = 0; r < 6; r++)
                {
                    rows.Add(new()
                    {
                        { "id",     new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "region", new(ColumnType.String,    "region-" + (g % 4) + "-" + padding) },
                        { "label",  new(ColumnType.String,    "label-" + g + "-" + padding) },
                        { "amount", new(ColumnType.Integer64, (long)(r + 1)) },
                        { "ratio",  new(ColumnType.Float64,   (r + 1) * 0.1d) },
                    });
                }
            }

            KvTransaction txn = await database.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(txn, dbname, "wide", values: rows));
            await database.Transactions.CommitAsync(txn);

            return new AggFixture(dbname, database, executor);
        }

        const string sql =
            "SELECT region, label, SUM(amount) + 1 AS total, COUNT(*) AS cnt, SUM(ratio) AS ratio_total " +
            "FROM wide GROUP BY region, label HAVING SUM(amount) > 10";

        List<QueryResultRow> offRows = await Run(await SetupWide(SpillOff), sql);
        List<QueryResultRow> onRows = await Run(await SetupWide(SpillOn(3, 4)), sql);

        static List<string> Render(List<QueryResultRow> rows) => rows
            .Select(r => string.Join("|",
                r.Row["label"].StrValue,
                r.Row["total"].LongValue,
                r.Row["cnt"].LongValue,
                r.Row["ratio_total"].FloatValue.ToString("R", System.Globalization.CultureInfo.InvariantCulture)))
            .OrderBy(v => v, System.StringComparer.Ordinal)
            .ToList();

        Assert.AreEqual(12, onRows.Count, "every group passes the HAVING clause in this fixture");

        // Byte-identical rendering, floating-point total included: rows still fold into one accumulator
        // per group in input order, so the accumulation order is what the in-memory path produces.
        CollectionAssert.AreEqual(Render(offRows), Render(onRows));
    }

    [Test]
    public async Task GroupBy_SpillFilesAreCleanedUpAfterARecursiveOverflow()
    {
        AggFixture f = await SetupSales(SpillOn(1, 2), categories: 10, rowsPerCategory: 4);

        List<QueryResultRow> rows = await Run(f,
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category");

        Assert.AreEqual(10, rows.Count);

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
            Assert.IsEmpty(Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories),
                "every partition and sub-partition file must be removed once the query completes");
    }
}
