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

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>SHOW INDEX USAGE</c>, driven through <see cref="ExecuteSQLTicket"/> so
/// every statement takes the path a console session takes.
///
/// <para><b>Every assertion is a difference between two readings.</b> The setup itself reads and
/// writes indexes, so an absolute count would test the fixture. Each test reads the counters, runs
/// the one statement under test, reads again, and asserts on the change.</para>
///
/// <para><b>Each access-path test checks its plan first.</b> A test that asserts "an IN list counts on
/// its index" would pass for the wrong reason if the planner chose another path; the EXPLAIN check
/// turns a planner change into a clear failure instead.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIndexUsageStatements : BaseTest
{
    private sealed record Usage(long Reads, long ConstraintReads, long EntriesRead, long Writes, string? LastReadAt, string TrackedSince, bool Unique);

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), database: db, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static async Task NonQueryAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task DdlAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<Dictionary<(string Table, string Index), Usage>> ReadUsageAsync(
        CommandExecutor executor, string db, string sql = "SHOW INDEX USAGE")
    {
        Dictionary<(string, string), Usage> result = new();

        foreach (QueryResultRow row in await QueryAsync(executor, db, sql))
        {
            result[(row.Row["table"].StrValue!, row.Row["index"].StrValue!)] = new Usage(
                row.Row["reads"].LongValue,
                row.Row["constraint_reads"].LongValue,
                row.Row["entries_read"].LongValue,
                row.Row["writes"].LongValue,
                row.Row["last_read_at"].StrValue,
                row.Row["tracked_since"].StrValue!,
                row.Row["unique"].BoolValue);
        }

        return result;
    }

    private static async Task<List<string>> PlanNodesAsync(CommandExecutor executor, string db, string select)
        => (await QueryAsync(executor, db, "EXPLAIN " + select)).Select(row => row.Row["node"].StrValue!).ToList();

    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> SetupAsync(CamusDBOptions? options = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);

        await DdlAsync(executor, database, db, "CREATE TABLE robots (id OID PRIMARY KEY, name STRING NOT NULL, year INT64 NOT NULL)");
        await DdlAsync(executor, database, db, "CREATE INDEX year_idx ON robots (year)");
        await DdlAsync(executor, database, db, "CREATE UNIQUE INDEX name_uq ON robots (name)");

        for (int i = 0; i < 20; i++)
            await NonQueryAsync(executor, database, db, $"INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'robot{i}', {2000 + (i % 10)})");

        await DdlAsync(executor, database, db, "CREATE TABLE owners (id INT64 PRIMARY KEY, robot_year INT64 NOT NULL)");
        await NonQueryAsync(executor, database, db, "INSERT INTO owners (id, robot_year) VALUES (1, 2001), (2, 2003)");

        return (db, database, executor);
    }

    [Test]
    public async Task EveryIndexHasARowIncludingOnesNeverRead()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();

        Dictionary<(string, string), Usage> usage = await ReadUsageAsync(executor, db);

        Assert.IsTrue(usage.ContainsKey(("robots", "~pk")));
        Assert.IsTrue(usage.ContainsKey(("robots", "year_idx")));
        Assert.IsTrue(usage.ContainsKey(("robots", "name_uq")));
        Assert.IsTrue(usage.ContainsKey(("owners", "~pk")));

        Assert.AreEqual(0, usage[("robots", "year_idx")].Reads, "nothing has queried through year_idx yet");
        Assert.IsNull(usage[("robots", "year_idx")].LastReadAt);
        Assert.IsTrue(usage[("robots", "~pk")].Unique);
        Assert.IsTrue(usage[("robots", "name_uq")].Unique);
        Assert.IsFalse(usage[("robots", "year_idx")].Unique);
    }

    [Test]
    public async Task FromNarrowsToOneTableAndAnUnknownTableIsAnError()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();

        Dictionary<(string Table, string Index), Usage> usage = await ReadUsageAsync(executor, db, "SHOW INDEX USAGE FROM owners");
        Assert.IsTrue(usage.Keys.All(key => key.Table == "owners"));
        Assert.AreEqual(1, usage.Count);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await QueryAsync(executor, db, "SHOW INDEX USAGE FROM no_such_table"))!;
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ex.Code);
    }

    /// <summary>An equality on a non-unique index counts one read and the entries it matched.</summary>
    [Test]
    public async Task EqualityOnANonUniqueIndexCountsOneReadAndItsEntries()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();
        const string select = "SELECT * FROM robots WHERE year = 2005";

        CollectionAssert.Contains(await PlanNodesAsync(executor, db, select), "index-range-scan");

        Usage before = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];
        Assert.AreEqual(2, (await QueryAsync(executor, db, select)).Count);
        Usage after = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];

        Assert.AreEqual(1, after.Reads - before.Reads);
        Assert.AreEqual(2, after.EntriesRead - before.EntriesRead);
        Assert.IsNotNull(after.LastReadAt);
    }

    /// <summary>A full scan reads the rows, not an index, so it counts on no index.</summary>
    [Test]
    public async Task AFullScanCountsOnNoIndex()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();
        const string select = "SELECT * FROM robots WHERE name LIKE 'robot1%'";

        CollectionAssert.Contains(await PlanNodesAsync(executor, db, select), "table-scan");

        Dictionary<(string, string), Usage> before = await ReadUsageAsync(executor, db);
        await QueryAsync(executor, db, select);
        Dictionary<(string, string), Usage> after = await ReadUsageAsync(executor, db);

        foreach (string index in new[] { "~pk", "year_idx", "name_uq" })
            Assert.AreEqual(before[("robots", index)].Reads, after[("robots", index)].Reads, index);
    }

    [Test]
    public async Task AnInListCountsOneReadOnItsIndex()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();
        const string select = "SELECT * FROM robots WHERE year IN (2001, 2002, 2003)";

        CollectionAssert.Contains(await PlanNodesAsync(executor, db, select), "index-in-list");

        Usage before = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];
        Assert.AreEqual(6, (await QueryAsync(executor, db, select)).Count);
        Usage after = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];

        Assert.AreEqual(1, after.Reads - before.Reads, "one IN list is one access path");
        Assert.AreEqual(6, after.EntriesRead - before.EntriesRead);
    }

    [Test]
    public async Task ASemiJoinCountsOnTheInnerIndex()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();
        const string select = "SELECT * FROM owners WHERE robot_year IN (SELECT year FROM robots)";

        CollectionAssert.Contains(await PlanNodesAsync(executor, db, select), "semi-join");

        Usage before = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];
        Assert.AreEqual(2, (await QueryAsync(executor, db, select)).Count);
        Usage after = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];

        Assert.Greater(after.Reads, before.Reads);
        Assert.Greater(after.EntriesRead, before.EntriesRead);
    }

    [Test]
    public async Task AnIndexNestedLoopJoinCountsOnTheProbedIndex()
    {
        // On tables this small the planner prefers hashing the whole right side; the test switch
        // keeps the index nested-loop join, whose probes are what this test counts.
        (string db, _, CommandExecutor executor) = await SetupAsync(Options with { CostBasedJoinOrderEnabled = false, AutoAnalyzeEnabled = false });
        executor.Statistics.ForceIndexNestedLoopForTesting = true;
        const string select = "SELECT o.id, r.name FROM owners o JOIN robots r ON o.robot_year = r.year";

        CollectionAssert.Contains(await PlanNodesAsync(executor, db, select), "index-nested-loop-join");

        Usage before = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];
        Assert.AreEqual(4, (await QueryAsync(executor, db, select)).Count);
        Usage after = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];

        Assert.Greater(after.Reads, before.Reads);
        Assert.AreEqual(4, after.EntriesRead - before.EntriesRead);
    }

    /// <summary>
    /// An INSERT writes one entry into every index of the table, and the unique ones check their key
    /// as they write it — a constraint read, never a query read.
    /// </summary>
    [Test]
    public async Task AnInsertWritesEveryIndexAndChecksTheUniqueOnes()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Dictionary<(string, string), Usage> before = await ReadUsageAsync(executor, db);
        await NonQueryAsync(executor, database, db, "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'newcomer', 2042)");
        Dictionary<(string, string), Usage> after = await ReadUsageAsync(executor, db);

        foreach (string index in new[] { "~pk", "year_idx", "name_uq" })
            Assert.AreEqual(1, after[("robots", index)].Writes - before[("robots", index)].Writes, index);

        Assert.AreEqual(1, after[("robots", "name_uq")].ConstraintReads - before[("robots", "name_uq")].ConstraintReads);
        Assert.AreEqual(0, after[("robots", "name_uq")].Reads - before[("robots", "name_uq")].Reads);
        Assert.AreEqual(0, after[("robots", "year_idx")].ConstraintReads - before[("robots", "year_idx")].ConstraintReads);
    }

    /// <summary>
    /// An UPDATE that changes a key removes the old entry and writes the new one; an index whose key
    /// the update leaves alone is not written at all.
    /// </summary>
    [Test]
    public async Task AnUpdateWritesOnlyTheIndexesWhoseKeyChanged()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Dictionary<(string, string), Usage> before = await ReadUsageAsync(executor, db);
        await NonQueryAsync(executor, database, db, "UPDATE robots SET year = 2099 WHERE name = 'robot3'");
        Dictionary<(string, string), Usage> changed = await ReadUsageAsync(executor, db);

        Assert.AreEqual(2, changed[("robots", "year_idx")].Writes - before[("robots", "year_idx")].Writes);
        Assert.AreEqual(0, changed[("robots", "name_uq")].Writes - before[("robots", "name_uq")].Writes);

        await NonQueryAsync(executor, database, db, "UPDATE robots SET year = 2099 WHERE name = 'robot3'");
        Dictionary<(string, string), Usage> unchanged = await ReadUsageAsync(executor, db);

        Assert.AreEqual(0, unchanged[("robots", "year_idx")].Writes - changed[("robots", "year_idx")].Writes,
            "the key did not change, so the index was not touched");
    }

    [Test]
    public async Task ADeleteRemovesAnEntryFromEveryIndex()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Dictionary<(string, string), Usage> before = await ReadUsageAsync(executor, db);
        await NonQueryAsync(executor, database, db, "DELETE FROM robots WHERE name = 'robot4'");
        Dictionary<(string, string), Usage> after = await ReadUsageAsync(executor, db);

        foreach (string index in new[] { "~pk", "year_idx", "name_uq" })
            Assert.AreEqual(1, after[("robots", index)].Writes - before[("robots", index)].Writes, index);
    }

    /// <summary>
    /// An index created after the node started tracking its table starts counting when it appears,
    /// so a reader looking for long-unread indexes does not flag one created a moment ago.
    /// </summary>
    [Test]
    public async Task AnIndexCreatedLaterIsTrackedFromItsCreation()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE year = 2001");
        await Task.Delay(50);

        await DdlAsync(executor, database, db, "CREATE INDEX name_year_idx ON robots (name, year)");
        await QueryAsync(executor, db, "SELECT * FROM robots WHERE year = 2001");

        Dictionary<(string, string), Usage> usage = await ReadUsageAsync(executor, db);

        DateTime original = DateTime.Parse(usage[("robots", "year_idx")].TrackedSince).ToUniversalTime();
        DateTime added = DateTime.Parse(usage[("robots", "name_year_idx")].TrackedSince).ToUniversalTime();

        Assert.Greater(added, original.AddMilliseconds(40));
    }

    /// <summary>Plain EXPLAIN plans and does not execute, so it reads nothing; EXPLAIN (ANALYZE) executes.</summary>
    [Test]
    public async Task PlainExplainDoesNotCountAndExplainAnalyzeDoes()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync();
        const string select = "SELECT * FROM robots WHERE year = 2005";

        Usage before = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];
        await QueryAsync(executor, db, "EXPLAIN " + select);
        Usage afterPlain = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];

        Assert.AreEqual(before.Reads, afterPlain.Reads);

        await QueryAsync(executor, db, "EXPLAIN (ANALYZE) " + select);
        Usage afterAnalyze = (await ReadUsageAsync(executor, db))[("robots", "year_idx")];

        Assert.AreEqual(1, afterAnalyze.Reads - afterPlain.Reads);
    }

    [Test]
    public async Task DisabledAtBootCountsNothingButStillListsTheIndexes()
    {
        (string db, _, CommandExecutor executor) = await SetupAsync(Options with { IndexUsageEnabled = false });

        await QueryAsync(executor, db, "SELECT * FROM robots WHERE year = 2005");
        Dictionary<(string, string), Usage> usage = await ReadUsageAsync(executor, db);

        Assert.AreEqual(0, usage[("robots", "year_idx")].Reads);
        Assert.AreEqual(0, usage[("robots", "year_idx")].Writes);
        Assert.IsTrue(usage.ContainsKey(("robots", "name_uq")));
    }
}
