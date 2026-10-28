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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The spill path of the set operations. Each engine is built with the configuration it runs:
/// <c>SpillEnabled</c> with a forced threshold of a few rows, so a test-sized input moves to disk.
/// The results are compared with exact counts and with an engine that does not spill, and the slow
/// query log (threshold 0) proves that the statement really spilled.
///
/// <para>[NonParallelizable] for the reason <see cref="TestQueryDistincterSpill"/> gives: the spill
/// instance lock is process-wide.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSetOperationsSpill : SharedNodeBaseTest
{
    private string dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        dataDir = Path.Combine(Path.GetTempPath(), "camusdb_setop_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        SpillFileManager.AcquireInstanceLock(dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();
        try { Directory.Delete(dataDir, recursive: true); } catch { }
    }

    private CamusDBOptions SpillOn => Options with
    {
        SpillEnabled = true,
        ForceSpillThresholdRows = 3,
        SpillMergeFanIn = 4,
        DataDirectory = dataDir,
        SlowQueryLogEnabled = true,
        SlowQueryLogThresholdMs = 0,
    };

    private CamusDBOptions SpillOff => Options with
    {
        SpillEnabled = false,
        DataDirectory = dataDir,
        SlowQueryLogEnabled = true,
        SlowQueryLogThresholdMs = 0,
    };

    /// <summary>
    /// l holds v = i % 10 for i in 1..200 (twenty copies of each of 0..9) and r holds v = i % 7 for i
    /// in 1..70 (ten copies of each of 0..6), so every form has duplicates and many distinct rows.
    /// </summary>
    private async Task<(string Db, CommandExecutor Executor)> Setup(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        await Run(executor, database, dbname, "CREATE TABLE l (id int64 PRIMARY KEY, v int64)", ddl: true);
        await Run(executor, database, dbname, "CREATE TABLE r (id int64 PRIMARY KEY, w int64)", ddl: true);
        await Run(executor, database, dbname, "INSERT INTO l SELECT i, i % 10 FROM generate_series(1, 200) AS i");
        await Run(executor, database, dbname, "INSERT INTO r SELECT i, i % 7 FROM generate_series(1, 70) AS i");

        return (dbname, executor);
    }

    private static async Task Run(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, dbname, sql, null);

        if (ddl)
            await executor.ExecuteDDLSQL(ticket);
        else
            await executor.ExecuteNonSQLQuery(ticket);

        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<long>> Values(CommandExecutor executor, string dbname, string sql, string column)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), dbname, sql, null));

        List<long> values = [];
        await foreach (QueryResultRow row in cursor)
            values.Add(row.Row[column].LongValue);

        values.Sort();
        return values;
    }

    private static async Task<bool> Spilled(CommandExecutor executor, string dbname, string sqlFragment)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, dbname, "SHOW SLOW QUERIES", null));

        await foreach (QueryResultRow row in cursor)
        {
            if (row.Row["sql"].StrValue?.Contains(sqlFragment, StringComparison.Ordinal) == true)
                return row.Row["spilled"].BoolValue;
        }

        Assert.Fail("The statement is not in the slow query log: " + sqlFragment);
        return false;
    }

    private static List<long> Repeat(IEnumerable<long> values, int times) =>
        values.SelectMany(v => Enumerable.Repeat(v, times)).OrderBy(v => v).ToList();

    [Test]
    public async Task Union_Spills_AndKeepsOneCopyOfEachRow()
    {
        (string db, CommandExecutor executor) = await Setup(SpillOn);

        const string sql = "SELECT v FROM l UNION SELECT w FROM r";
        CollectionAssert.AreEqual(Enumerable.Range(0, 10).Select(v => (long)v), await Values(executor, db, sql, "v"));
        Assert.IsTrue(await Spilled(executor, db, sql));
    }

    [Test]
    public async Task IntersectAll_Spills_AndKeepsTheMinimumCount()
    {
        (string db, CommandExecutor executor) = await Setup(SpillOn);

        // 0..6 are in both: min(20, 10) = 10 copies each.
        const string sql = "SELECT v FROM l INTERSECT ALL SELECT w FROM r";
        CollectionAssert.AreEqual(Repeat(Enumerable.Range(0, 7).Select(v => (long)v), 10), await Values(executor, db, sql, "v"));
        Assert.IsTrue(await Spilled(executor, db, sql));

        const string distinct = "SELECT v FROM l INTERSECT SELECT w FROM r";
        CollectionAssert.AreEqual(Enumerable.Range(0, 7).Select(v => (long)v), await Values(executor, db, distinct, "v"));
    }

    [Test]
    public async Task ExceptAll_Spills_AndKeepsTheDifferenceOfTheCounts()
    {
        (string db, CommandExecutor executor) = await Setup(SpillOn);

        // 0..6: 20 - 10 = 10 copies; 7..9: all 20 copies.
        const string sql = "SELECT v FROM l EXCEPT ALL SELECT w FROM r";
        List<long> expected = Repeat(Enumerable.Range(0, 7).Select(v => (long)v), 10)
            .Concat(Repeat(Enumerable.Range(7, 3).Select(v => (long)v), 20))
            .OrderBy(v => v)
            .ToList();
        CollectionAssert.AreEqual(expected, await Values(executor, db, sql, "v"));
        Assert.IsTrue(await Spilled(executor, db, sql));

        const string distinct = "SELECT v FROM l EXCEPT SELECT w FROM r";
        CollectionAssert.AreEqual(new long[] { 7, 8, 9 }, await Values(executor, db, distinct, "v"));

        // The other direction: r holds nothing that l does not.
        CollectionAssert.IsEmpty(await Values(executor, db, "SELECT w FROM r EXCEPT ALL SELECT v FROM l", "w"));
    }

    [Test]
    public async Task SpillOff_GivesTheSameRows_AndDoesNotSpill()
    {
        (string onDb, CommandExecutor on) = await Setup(SpillOn);
        (string offDb, CommandExecutor off) = await Setup(SpillOff);

        foreach (string sql in new[]
        {
            "SELECT v FROM l UNION SELECT w FROM r",
            "SELECT v FROM l UNION ALL SELECT w FROM r",
            "SELECT v FROM l INTERSECT SELECT w FROM r",
            "SELECT v FROM l INTERSECT ALL SELECT w FROM r",
            "SELECT v FROM l EXCEPT SELECT w FROM r",
            "SELECT v FROM l EXCEPT ALL SELECT w FROM r",
        })
        {
            CollectionAssert.AreEqual(await Values(off, offDb, sql, "v"), await Values(on, onDb, sql, "v"), sql);
            Assert.IsFalse(await Spilled(off, offDb, sql), sql);
        }
    }
}
