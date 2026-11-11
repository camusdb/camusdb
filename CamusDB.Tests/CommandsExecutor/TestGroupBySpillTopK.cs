/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Spill pruning of a grouped <c>COUNT(...) DESC</c> top k: when the GROUP BY spills, a spill
/// partition in which no group can reach the k-th count of the bounded sort is never read back.
///
/// <para>Each case runs the same query on two engines, one with spill forced at a low group count and
/// one with spill off, and compares the results: the value at each position must be equal, and each
/// returned group must carry its true count. That check accepts any choice among groups that tie at
/// the k-th count. Skewed data must skip partitions (<see cref="StatementProbe.SpillPartitionsPruned"/>),
/// and every spill file must be gone after the query.</para>
/// </summary>
[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestGroupBySpillTopK : SharedNodeBaseTest
{
    private const int LightGroups = 600;

    private const int HeavyGroups = 10;

    private string dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        dataDir = Path.Combine(Path.GetTempPath(), "camusdb_gb_topk_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        SpillFileManager.AcquireInstanceLock(dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(dataDir, recursive: true); } catch { }
    }

    private CamusDBOptions SpillOff => Options with { SpillEnabled = false, DataDirectory = dataDir };

    /// <summary>Spill forced once 50 distinct groups are held, into 8 partition files.</summary>
    private CamusDBOptions SpillOn => Options with
    {
        SpillEnabled = true,
        ForceSpillThresholdRows = 50,
        SpillMergeFanIn = 8,
        DataDirectory = dataDir,
    };

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// Table <c>t</c>: column <c>g</c> is the group, <c>v</c> is NULL on every third row.
    /// <paramref name="countOf"/> gives the row count of each group. By default the rows are
    /// interleaved, so every group appears early and late. With <paramref name="groupsFirst"/>, all rows
    /// of groups below that number come first: the scan reads rows in insertion order, so those groups
    /// are held in memory with all their rows when the spill starts, and they reach the partitions only
    /// as carried counts.
    /// </summary>
    private async Task<Fixture> Setup(CamusDBOptions options, Func<int, int> countOf, int groups, int groupsFirst = 0)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "t",
            columns:
            [
                new("id", ColumnType.Id),
                new("g", ColumnType.Integer64, notNull: true),
                new("v", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        int[] remaining = Enumerable.Range(0, groups).Select(countOf).ToArray();
        List<Dictionary<string, ColumnValue>> rows = new();
        int written = 0;

        for (int g = 0; g < groupsFirst; g++)
        {
            for (; remaining[g] > 0; remaining[g]--)
            {
                rows.Add(new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "g", new(ColumnType.Integer64, (long)g) },
                    { "v", written++ % 3 == 0 ? ColumnValue.Null : new(ColumnType.Integer64, (long)g) },
                });
            }
        }

        // Round robin over the groups that still have rows, so every group appears early and late.
        for (bool any = true; any;)
        {
            any = false;
            for (int g = 0; g < groups; g++)
            {
                if (remaining[g] == 0)
                    continue;

                remaining[g]--;
                any = true;
                rows.Add(new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "g", new(ColumnType.Integer64, (long)g) },
                    { "v", written++ % 3 == 0 ? ColumnValue.Null : new(ColumnType.Integer64, (long)g) },
                });
            }
        }

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txn, dbname, "t", values: rows));
        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor);
    }

    /// <summary>Heavy groups 0-9 hold 300, 290, ... 210 rows; the 600 light groups hold 1 to 3.</summary>
    private static int Skewed(int g) => g < HeavyGroups ? 300 - 10 * g : 1 + g % 3;

    private static async Task<(List<QueryResultRow> Rows, StatementProbe Probe)> Run(Fixture f, string sql)
    {
        StatementProbe probe = new();
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null, probe: probe);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return (rows, probe);
    }

    /// <summary>
    /// The value at each position must be the spill-off value, and each group of the spill-on result
    /// must carry its true count (read from the full spill-off grouping).
    /// </summary>
    private static void AssertSameTopK(List<QueryResultRow> actual, List<QueryResultRow> expected, Dictionary<long, long> trueCounts, string column)
    {
        Assert.AreEqual(expected.Count, actual.Count, "Row count");

        HashSet<long> seen = new();
        for (int i = 0; i < actual.Count; i++)
        {
            long g = actual[i].Row["g"].LongValue;
            long value = actual[i].Row[column].LongValue;

            Assert.IsTrue(seen.Add(g), $"Group {g} appears twice");
            Assert.AreEqual(trueCounts[g], value, $"Group {g} has the wrong {column}");
            Assert.AreEqual(expected[i].Row[column].LongValue, value, $"Position {i} has the wrong {column}");
        }
    }

    private static async Task<Dictionary<long, long>> TrueCounts(Fixture f, string aggregate)
    {
        (List<QueryResultRow> rows, _) = await Run(f, $"SELECT g, {aggregate} AS c FROM t GROUP BY g");
        return rows.ToDictionary(r => r.Row["g"].LongValue, r => r.Row["c"].LongValue);
    }

    private void AssertNoSpillFilesLeft() =>
        Assert.IsEmpty(Directory.GetFiles(dataDir, "*.spill", SearchOption.AllDirectories),
            "Every spill file, read or skipped, must be deleted after the query");

    [Test]
    public async Task SkewedCountStar_SkipsPartitions_AndMatchesSpillOff()
    {
        Fixture on = await Setup(SpillOn, Skewed, HeavyGroups + LightGroups);
        Fixture off = await Setup(SpillOff, Skewed, HeavyGroups + LightGroups);
        const string sql = "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 5";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(*)"), "c");
        CollectionAssert.AreEqual(new long[] { 0, 1, 2, 3, 4 }, actual.Select(r => r.Row["g"].LongValue).ToArray(),
            "The heavy counts are distinct, so the groups are fixed too");

        Assert.IsTrue(probe.Spilled, "The aggregation must spill for this test to mean anything");
        Assert.Greater(probe.SpillPartitionsPruned, 0L, "Skewed data must skip at least one spill partition");
        AssertNoSpillFilesLeft();
    }

    [Test]
    public async Task HeavyGroupsReadBeforeTheSpill_AreCountedThroughTheirCarriedCounts()
    {
        // The ten heavy groups are read in full before the 51st distinct group starts the spill, so
        // after the spill no row of theirs is written. Only their carried counts put them in the slot
        // totals; without those counts their partitions would look light and be skipped.
        Fixture on = await Setup(SpillOn, Skewed, HeavyGroups + LightGroups, groupsFirst: HeavyGroups);
        Fixture off = await Setup(SpillOff, Skewed, HeavyGroups + LightGroups, groupsFirst: HeavyGroups);
        const string sql = "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 5";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(*)"), "c");
        CollectionAssert.AreEqual(new long[] { 0, 1, 2, 3, 4 }, actual.Select(r => r.Row["g"].LongValue).ToArray());
        Assert.IsTrue(probe.Spilled);
        Assert.Greater(probe.SpillPartitionsPruned, 0L);
    }

    [Test]
    public async Task SkewedCountOfNullableColumn_MatchesSpillOff()
    {
        Fixture on = await Setup(SpillOn, Skewed, HeavyGroups + LightGroups);
        Fixture off = await Setup(SpillOff, Skewed, HeavyGroups + LightGroups);
        const string sql = "SELECT g, COUNT(v) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 7";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(v)"), "c");
        Assert.IsTrue(probe.Spilled);
    }

    [Test]
    public async Task SkewedWithOffset_MatchesSpillOff()
    {
        Fixture on = await Setup(SpillOn, Skewed, HeavyGroups + LightGroups);
        Fixture off = await Setup(SpillOff, Skewed, HeavyGroups + LightGroups);

        // Positions 8-12 cross from the heavy groups into the light ones, where counts tie.
        const string sql = "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 5 OFFSET 8";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(*)"), "c");
        Assert.IsTrue(probe.Spilled);
    }

    [Test]
    public async Task FlatData_TiesEverywhere_MatchesSpillOff()
    {
        Fixture on = await Setup(SpillOn, _ => 3, LightGroups);
        Fixture off = await Setup(SpillOff, _ => 3, LightGroups);
        const string sql = "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 10";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(*)"), "c");
        Assert.IsTrue(probe.Spilled);
        Assert.AreEqual(0L, probe.SpillPartitionsPruned, "Every slot total reaches the tied count, so nothing can be skipped");
    }

    [Test]
    public async Task TiesAtTheKthCount_MatchSpillOff()
    {
        // Groups 0-3 hold 50 rows; groups 4-11 hold 40 rows (an 8-way tie across the 5th place);
        // the rest hold 1 or 2.
        static int Ties(int g) => g < 4 ? 50 : g < 12 ? 40 : 1 + g % 2;

        Fixture on = await Setup(SpillOn, Ties, LightGroups);
        Fixture off = await Setup(SpillOff, Ties, LightGroups);
        const string sql = "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 6";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(*)"), "c");
        Assert.IsTrue(probe.Spilled);
    }

    [Test]
    public async Task KPastTheSpillThreshold_DoesNotBoundTheSort_AndMatchesSpillOff()
    {
        Fixture on = await Setup(SpillOn, Skewed, HeavyGroups + LightGroups);
        Fixture off = await Setup(SpillOff, Skewed, HeavyGroups + LightGroups);

        // k = 1000 is past the forced threshold of 50, so the planner leaves the sort unbounded and
        // no k-th count exists to prune with.
        const string sql = "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 1000";

        (List<QueryResultRow> actual, StatementProbe probe) = await Run(on, sql);
        (List<QueryResultRow> expected, _) = await Run(off, sql);

        AssertSameTopK(actual, expected, await TrueCounts(off, "COUNT(*)"), "c");
        Assert.AreEqual(0L, probe.SpillPartitionsPruned);
    }

    [Test]
    public async Task CursorDisposedAfterTheFirstRow_LeavesNoSpillFile()
    {
        Fixture on = await Setup(SpillOn, Skewed, HeavyGroups + LightGroups);

        KvTransaction txn = await on.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: on.DbName,
            sql: "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 5", parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await on.Executor.ExecuteSQLQuery(ticket);

        await using (IAsyncEnumerator<QueryResultRow> enumerator = cursor.GetAsyncEnumerator())
        {
            Assert.IsTrue(await enumerator.MoveNextAsync());
            Assert.AreEqual(0L, enumerator.Current.Row["g"].LongValue);
        }

        await on.Database.Transactions.RollbackIfNotCompletedAsync(txn);
        AssertNoSpillFilesLeft();
    }
}
