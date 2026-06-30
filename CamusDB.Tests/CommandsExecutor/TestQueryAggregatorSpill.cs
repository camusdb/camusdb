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
[NonParallelizable]
public sealed class TestQueryAggregatorSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;
    private int _savedFanIn;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_agg_spill_" + System.Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _savedSpillEnabled   = CamusDBConfig.SpillEnabled;
        _savedThreshold      = CamusDBConfig.SpillThresholdRows;
        _savedForceThreshold = CamusDBConfig.ForceSpillThresholdRows;
        _savedFanIn          = CamusDBConfig.SpillMergeFanIn;

        CamusDBConfig.DataDirectory = _dataDir;
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        CamusDBConfig.SpillEnabled            = _savedSpillEnabled;
        CamusDBConfig.SpillThresholdRows      = _savedThreshold;
        CamusDBConfig.ForceSpillThresholdRows = _savedForceThreshold;
        CamusDBConfig.SpillMergeFanIn         = _savedFanIn;
        CamusDBConfig.DataDirectory           = null!;

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
    private async Task<AggFixture> SetupSales(int categories = 5, int rowsPerCategory = 4)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
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

    private static async Task<List<QueryResultRow>> Run(AggFixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
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

        CamusDBConfig.SpillEnabled = false;
        AggFixture fRef = await SetupSales();
        List<QueryResultRow> reference = await Run(fRef, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 5; // 5 categories × 4 rows; overflow after 5 rows
        CamusDBConfig.SpillMergeFanIn         = 4;
        AggFixture fSpill = await SetupSales();
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

        CamusDBConfig.SpillEnabled = false;
        AggFixture fRef = await SetupSales(categories: 6, rowsPerCategory: 3);
        List<QueryResultRow> reference = await Run(fRef, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 4; // overflows during partition phase
        CamusDBConfig.SpillMergeFanIn         = 3;
        AggFixture fSpill = await SetupSales(categories: 6, rowsPerCategory: 3);
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        Assert.AreEqual(reference.Count, spillResult.Count);
        CollectionAssert.AreEqual(SortedCounts(reference), SortedCounts(spillResult),
            "COUNT(*) per category must match between the in-memory and spill paths.");
    }

    [Test]
    public async Task GroupBySpill_NoSpillFilesRemainAfterCompletion()
    {
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        CamusDBConfig.SpillMergeFanIn         = 4;

        AggFixture f = await SetupSales();
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
        CamusDBConfig.SpillEnabled = false;

        AggFixture f = await SetupSales(categories: 3, rowsPerCategory: 2);
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

        CamusDBConfig.SpillEnabled = false;
        AggFixture fOff = await SetupSales(categories: 8, rowsPerCategory: 5);
        List<QueryResultRow> offRows = await Run(fOff, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 6;
        CamusDBConfig.SpillMergeFanIn         = 4;
        AggFixture fOn = await SetupSales(categories: 8, rowsPerCategory: 5);
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
}
