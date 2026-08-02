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
/// Verifies that spill-aware GROUP BY aggregation produces correct results at every scale and
/// that per-partition dictionary growth is bounded by recursive repartitioning when the number
/// of distinct groups in a single partition exceeds <c>CamusDBOptions.SpillEffectiveThreshold</c>.
///
/// The discriminator for the recursion path is <c>StatisticsManager.GroupByPartitionRecursionCount</c>:
/// the counter is incremented only by the recursive repartitioning branch, so a zero count means
/// unbounded dictionary growth is still occurring.
/// </summary>
[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestGroupBySpill : SharedNodeBaseTest
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_gb_spill_" + System.Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ── helper ───────────────────────────────────────────────────────────────

    /// <summary>Spill disabled — the in-memory grouping path.</summary>
    private CamusDBOptions SpillOff => Options with { SpillEnabled = false, DataDirectory = _dataDir };

    /// <summary>
    /// Spill forced after <paramref name="thresholdRows"/> rows so the partitioned aggregate path runs
    /// on small inputs. Independent of any other configuration in play.
    /// </summary>
    private CamusDBOptions SpillOn(int thresholdRows, int fanIn = 4) =>
        Options with
        {
            SpillEnabled = true,
            ForceSpillThresholdRows = thresholdRows,
            SpillMergeFanIn = fanIn,
            DataDirectory = _dataDir,
        };

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupSalesTable(
        CamusDBOptions options,
        int categoryCount,
        int rowsPerCategory)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "sales",
            columns:
            [
                new("id",       ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
                new("amount",   ColumnType.Integer64, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> rows = new(categoryCount * rowsPerCategory);
        for (int c = 0; c < categoryCount; c++)
        {
            string cat = $"cat{c:D4}";
            for (int r = 0; r < rowsPerCategory; r++)
            {
                rows.Add(new()
                {
                    { "id",       new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "category", new(ColumnType.String, cat) },
                    { "amount",   new(ColumnType.Integer64, (long)(r + 1)) },
                });
            }
        }

        await executor.Insert(new InsertTicket(txn, dbname, "sales", values: rows));
        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunGroupBy(
        string dbname, DatabaseDescriptor database, CommandExecutor executor, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // ── spill-off baseline ────────────────────────────────────────────────

    /// <summary>
    /// With spill disabled, GROUP BY uses the in-memory dictionary and produces the correct
    /// group count and aggregate values. This is the baseline all spill paths must match.
    /// </summary>
    [Test]
    public async Task GroupBySpill_FlagOff_InMemoryPathUsed()
    {        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesTable(
            SpillOff, categoryCount: 4, rowsPerCategory: 3);


        List<QueryResultRow> rows = await RunGroupBy(dbname, database, executor,
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category");

        Assert.That(rows.Count, Is.EqualTo(4), "Four distinct categories must be returned");

        // Each category has rows with amounts 1,2,3 → sum = 6.
        foreach (QueryResultRow row in rows)
            Assert.That(row.Row["total"].LongValue, Is.EqualTo(6L),
                "Each category sum must be 1+2+3 = 6");
    }

    // ── spill-on parity: few groups, no partition overflow ────────────────

    /// <summary>
    /// With spill enabled and few distinct groups (well below the partition threshold),
    /// the result is identical to the in-memory path. No partition recursion should occur.
    /// </summary>
    [Test]
    public async Task GroupBySpill_FewGroups_ResultMatchesInMemory()
    {        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesTable(
            SpillOn(20, 4), categoryCount: 4, rowsPerCategory: 3);

        // threshold=20 → 4 groups per partition easily fit.
        executor.Statistics.GroupByPartitionRecursionCount = 0;

        List<QueryResultRow> rows = await RunGroupBy(dbname, database, executor,
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category");

        Assert.That(rows.Count, Is.EqualTo(4));
        foreach (QueryResultRow row in rows)
            Assert.That(row.Row["total"].LongValue, Is.EqualTo(6L));

        Assert.That(executor.Statistics.GroupByPartitionRecursionCount, Is.EqualTo(0),
            "No partition recursion should be needed when groups comfortably fit the threshold");
    }

    // ── spill-on correctness: many groups, no overflow ───────────────────

    /// <summary>
    /// With spill enabled and a threshold large enough that no partition overflows, the
    /// aggregation still produces the full correct multiset (regression: verifies the basic
    /// spill path was not broken by the recursive repartitioning change).
    /// </summary>
    [Test]
    public async Task GroupBySpill_ManyGroups_BelowThreshold_CorrectResult()
    {
        const int categories = 20;
        const int rowsEach   = 5;        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesTable(
            SpillOn(200, 4), categoryCount: categories, rowsPerCategory: rowsEach);

        // Threshold exceeds groups-per-partition (with K=4, ~5 groups per partition), no overflow.

        List<QueryResultRow> rows = await RunGroupBy(dbname, database, executor,
            "SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM sales GROUP BY category");

        Assert.That(rows.Count, Is.EqualTo(categories), "All categories must appear");
        foreach (QueryResultRow row in rows)
        {
            Assert.That(row.Row["cnt"].LongValue, Is.EqualTo(rowsEach),
                "Each group must have exactly rowsPerCategory rows");
            Assert.That(row.Row["total"].LongValue, Is.EqualTo(rowsEach * (rowsEach + 1) / 2),
                "SUM(1..rowsEach) must equal rowsEach*(rowsEach+1)/2");
        }
    }

    // ── overflow + recursion: GROUP BY partition exceeds threshold ────────

    /// <summary>
    /// When a first-level partition holds more distinct groups than
    /// <c>CamusDBOptions.SpillEffectiveThreshold</c>, the aggregator recursively repartitions it
    /// with a new hash seed. The result must be identical to the in-memory path, and
    /// <c>StatisticsManager.GroupByPartitionRecursionCount</c> must be positive, proving the
    /// recursion branch was actually taken (not load-all).
    ///
    /// With 40 distinct categories and threshold=2, each first-level partition (K=4) will hold
    /// ~10 distinct groups — far above the threshold=2 cap — forcing recursion.
    /// </summary>
    [Test]
    public async Task GroupBySpill_PartitionOverflow_RecursionAndCorrectResult()
    {
        const int categories = 40;
        const int rowsEach   = 3;        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesTable(
            SpillOn(2, 4), categoryCount: categories, rowsPerCategory: rowsEach);

        // Threshold = 2 means any partition with ≥3 distinct groups triggers recursion.
        // With 40 groups and K=4 partitions, every partition gets ~10 groups → always overflows.
        executor.Statistics.GroupByPartitionRecursionCount = 0;

        List<QueryResultRow> rows = await RunGroupBy(dbname, database, executor,
            "SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM sales GROUP BY category");

        // Correctness: full multiset must be recovered after recursive repartitioning.
        Assert.That(rows.Count, Is.EqualTo(categories),
            "Recursive repartitioning must not lose or duplicate any group");

        foreach (QueryResultRow row in rows)
        {
            Assert.That(row.Row["cnt"].LongValue, Is.EqualTo(rowsEach),
                "Each group must accumulate exactly rowsPerCategory rows across all recursion levels");
            Assert.That(row.Row["total"].LongValue, Is.EqualTo((long)rowsEach * (rowsEach + 1) / 2),
                "Aggregate value must equal the in-memory result");
        }

        // Recursion proof: counter must be non-zero.
        Assert.That(executor.Statistics.GroupByPartitionRecursionCount, Is.GreaterThan(0),
            "GroupByPartitionRecursionCount must be > 0 — a zero value means the unbounded " +
            "load-all path was taken instead of recursive repartitioning");
    }

    /// <summary>
    /// Compares spill-path results against the in-memory path for a medium dataset to prove
    /// the two paths are semantically equivalent on sums and counts.
    /// </summary>
    [Test]
    public async Task GroupBySpill_OverflowResult_MatchesInMemoryPath()
    {
        const int categories = 30;
        const int rowsEach   = 4;
        const string sql = "SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category";        (string db1, DatabaseDescriptor d1, CommandExecutor ex1) = await SetupSalesTable(
            SpillOff, categories, rowsEach);
        List<QueryResultRow> reference = await RunGroupBy(db1, d1, ex1, sql);        (string db2, DatabaseDescriptor d2, CommandExecutor ex2) = await SetupSalesTable(
            SpillOn(2, 4), categories, rowsEach);
        List<QueryResultRow> spill = await RunGroupBy(db2, d2, ex2, sql);

        Assert.That(spill.Count, Is.EqualTo(reference.Count),
            "Spill and in-memory paths must return the same number of groups");

        // ORDER BY category → stable order for pairwise comparison.
        for (int i = 0; i < reference.Count; i++)
        {
            Assert.That(spill[i].Row["category"].StrValue, Is.EqualTo(reference[i].Row["category"].StrValue),
                $"Group[{i}] category mismatch");
            Assert.That(spill[i].Row["cnt"].LongValue, Is.EqualTo(reference[i].Row["cnt"].LongValue),
                $"Group[{i}] count mismatch");
            Assert.That(spill[i].Row["total"].LongValue, Is.EqualTo(reference[i].Row["total"].LongValue),
                $"Group[{i}] sum mismatch");
        }
    }

    // ── cleanup ───────────────────────────────────────────────────────────

    /// <summary>
    /// After the GROUP BY query completes — with or without partition overflow — all spill
    /// files must be removed. A leftover spill file is a resource leak.
    /// </summary>
    [Test]
    public async Task GroupBySpill_SpillFilesRemovedAfterCompletion()
    {
        const int categories = 20;        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesTable(
            SpillOn(2, 4), categoryCount: categories, rowsPerCategory: 2);


        await RunGroupBy(dbname, database, executor,
            "SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category");

        string[] remaining = Directory.GetFiles(_dataDir, "*.spill", SearchOption.AllDirectories);
        Assert.That(remaining, Is.Empty,
            "All spill files must be cleaned up after the GROUP BY query completes");
    }

    /// <summary>
    /// Direct discriminator for the recursion's memory-bound mechanism: the seed must actually
    /// redistribute group keys that collided at the parent level into different sub-partitions.
    /// Without this, recursive repartitioning is futile — colliding keys re-collide at every level
    /// and the partition only fits in memory via the depth-cap backstop, defeating the bound.
    ///
    /// The query-level tests cannot see this (the backstop still returns correct results and the
    /// recursion counter still fires with a no-op seed). This test takes a bucket of keys that
    /// share a partition at seed 0 and asserts they land in more than one bucket at seed 1.
    /// Negative proof: making <c>GroupPartitionIndex</c> ignore the seed collapses every seed-1
    /// bucket back to the seed-0 bucket, leaving exactly one distinct bucket and failing here.
    /// </summary>
    [Test]
    public void GroupBySpill_PartitionIndex_RedistributesCollidingGroupKeysAcrossLevels()
    {
        const int K = 16;

        // Build a large set of distinct single-column group keys.
        List<CompositeColumnValue> keys = new();
        for (int i = 0; i < 5000; i++)
            keys.Add(new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, (long)i) }));

        // Find a seed-0 bucket holding several distinct keys (a parent-level collision set).
        List<CompositeColumnValue> collidingAtSeed0 = keys
            .GroupBy(k => QueryAggregator.GroupPartitionIndex(k, K, seed: 0))
            .First(g => g.Count() >= 8)
            .ToList();

        // At seed 1 those colliding keys must spread across more than one sub-partition.
        int distinctBucketsAtSeed1 = collidingAtSeed0
            .Select(k => QueryAggregator.GroupPartitionIndex(k, K, seed: 1))
            .Distinct()
            .Count();

        Assert.That(distinctBucketsAtSeed1, Is.GreaterThan(1),
            "Group keys that collide in one partition at seed 0 must redistribute across multiple " +
            "sub-partitions at seed 1 — otherwise recursive repartitioning cannot bound per-partition memory.");
    }
}
