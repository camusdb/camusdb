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
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The spill path of the window operator. Each engine is built with the configuration it runs: one
/// with <c>SpillEnabled</c> and a forced threshold of three rows, so the sort and every partition
/// buffer of a test-sized input move to disk, and one with spill off. The two must return the same
/// rows, the spilled run must say it spilled, and no spill file may stay behind after success, after
/// a failure in the middle of the rows, or after cancellation.
///
/// <para>[NonParallelizable] for the reason <see cref="TestQueryDistincterSpill"/> gives: the spill
/// instance lock is process-wide.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestWindowFunctionsSpill : WindowFunctionsTestBase
{
    private string dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        dataDir = Path.Combine(Path.GetTempPath(), "camusdb_window_spill_" + Guid.NewGuid().ToString("N"));
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
    };

    private CamusDBOptions SpillOff => Options with
    {
        SpillEnabled = false,
        DataDirectory = dataDir,
    };

    /// <summary><c>s</c> holds 200 rows: <c>p = i % 10</c> (ten partitions of twenty) and <c>x = (i * 37) % 101</c>.</summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Setup(CamusDBOptions options)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        await ExecDdl(executor, database, db, "CREATE TABLE s (id int64 PRIMARY KEY, p int64, x int64)");
        await ExecNonQuery(executor, database, db, "INSERT INTO s SELECT i, i % 10, (i * 37) % 101 FROM generate_series(1, 200) AS i");

        return (db, database, executor);
    }

    private string[] SpillFiles() => Directory.GetFiles(dataDir, "*.spill", SearchOption.AllDirectories);

    private const string Query =
        "SELECT id, row_number() OVER (PARTITION BY p ORDER BY x, id) AS rn, "
        + "sum(x) OVER (PARTITION BY p ORDER BY x, id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS slide, "
        + "lead(x, 4) OVER (PARTITION BY p ORDER BY x, id) AS ahead, "
        + "rank() OVER (ORDER BY x DESC) AS r, "
        + "count(*) OVER () AS c FROM s ORDER BY id";

    private static List<string> Flatten(List<QueryResultRow> rows) =>
        rows.Select(r => string.Join("|", new[] { "id", "rn", "slide", "ahead", "r", "c" }.Select(c =>
        {
            ColumnValue value = r.Row[c];
            return value.Type == ColumnType.Null ? "null" : value.LongValue.ToString();
        }))).ToList();

    [Test]
    public async Task SpillOnAndOff_GiveTheSameRows_AndTheSpillIsReal()
    {
        (string onDb, DatabaseDescriptor onDatabase, CommandExecutor on) = await Setup(SpillOn);
        (string offDb, DatabaseDescriptor offDatabase, CommandExecutor off) = await Setup(SpillOff);

        List<QueryResultRow> spilled = await ExecQuery(on, onDatabase, onDb, Query);
        List<QueryResultRow> inMemory = await ExecQuery(off, offDatabase, offDb, Query);

        Assert.AreEqual(200, spilled.Count);
        CollectionAssert.AreEqual(Flatten(inMemory), Flatten(spilled));
        Assert.That(spilled.Select(r => r.Row["c"].LongValue), Is.All.EqualTo(200));

        List<QueryResultRow> analyzed = await ExecQuery(on, onDatabase, onDb, "EXPLAIN (ANALYZE) " + Query);
        string window = analyzed.Single(r => r.Row["node"].StrValue == "window").Row["detail"].StrValue!;
        StringAssert.Contains("partitions=12 rows=600", window);
        StringAssert.DoesNotContain("spilled=0", window, "partitions of twenty rows over a threshold of three spill");

        List<QueryResultRow> analyzedOff = await ExecQuery(off, offDatabase, offDb, "EXPLAIN (ANALYZE) " + Query);
        StringAssert.Contains("spilled=0", analyzedOff.Single(r => r.Row["node"].StrValue == "window").Row["detail"].StrValue!);

        Assert.IsEmpty(SpillFiles(), "every spill file is removed after success");
    }

    [Test]
    public async Task FailureInTheMiddle_RemovesTheSpillFiles()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Setup(SpillOn);

        // ntile reads its argument from each partition's first row: partition 9 asks for 0 buckets,
        // after the partitions before it were computed and emitted.
        await AssertFails(executor, database, db,
            "SELECT id, ntile(CASE WHEN p = 9 THEN 0 ELSE 2 END) OVER (PARTITION BY p ORDER BY id) AS n FROM s",
            CamusDBErrorCodes.InvalidInput, "greater than zero");

        Assert.IsEmpty(SpillFiles(), "every spill file is removed after a failure");
    }

    [Test]
    public async Task Cancellation_RemovesTheSpillFiles()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Setup(SpillOn);

        using CancellationTokenSource cancellation = new();
        KvTransaction tx = await database.Transactions.BeginAsync();

        Assert.CatchAsync<Exception>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                tx, db, "SELECT id, row_number() OVER (PARTITION BY p ORDER BY x) AS rn FROM s", null,
                cancellationToken: cancellation.Token));

            int read = 0;

            await foreach (QueryResultRow _ in cursor)
            {
                if (++read == 5)
                    cancellation.Cancel();
            }
        });

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.IsTrue(cancellation.IsCancellationRequested);
        Assert.IsEmpty(SpillFiles(), "every spill file is removed after cancellation");
    }
}
