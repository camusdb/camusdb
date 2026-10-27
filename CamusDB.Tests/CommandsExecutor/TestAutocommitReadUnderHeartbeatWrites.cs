/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The contract of an autocommit full-scan <c>SELECT</c> over a small table while one short optimistic
/// Serializable writer updates one row and holds its write intent until it commits. The shape is a
/// presence table: a sweep reads every row; a heartbeat begins a transaction, reads one row, updates it,
/// does other work, and commits.
///
/// <para>An autocommit read is a zero-identity snapshot pinned at a local timestamp <c>T</c>
/// (<see cref="KvTransactionsManager.BeginReadOnly"/>). Kahuna makes a snapshot read wait on a live
/// foreign write intent only when the writer could still commit at or before <c>T</c>: for an
/// un-prepared intent the bound is the stamp of the write it staged on the key, so a writer that staged
/// before <c>T</c> holds the page until it commits or rolls back, and a writer that staged after <c>T</c>
/// never does, whatever its transaction id. The wait is a poll with sleeps capped at 50 ms, so the read
/// is served within that of the intent clearing, and a page that still answers transient after the scan
/// page retry budget fails the statement with <see cref="CamusDBErrorCodes.TransactionMustRetry"/>.
/// These tests pin the three outcomes: no wait, a wait bounded by the writer's hold that ends with every
/// row read, and the retryable failure. The timings they print are the measured cost of the wait.</para>
/// </summary>
[TestFixture]
// Serial: the timings assert on wall-clock waits against the shared embedded node.
[NonParallelizable]
public sealed class TestAutocommitReadUnderHeartbeatWrites : BaseTest
{
    private const int Rows = 82;

    /// <summary>The sweep: every row survives the scan, none survives the predicate.</summary>
    private const string SweepSql =
        "SELECT usersId, id, createdAt, isOnline, location, updatedAt, version " +
        "FROM user_presence WHERE isOnline AND updatedAt < 500 ORDER BY updatedAt LIMIT 256";

    /// <summary>Kahuna's default scan page retry budget; a hold past it fails the page.</summary>
    private static readonly TimeSpan ScanPageRetryBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// What a read may take beyond the writer's hold: Kahuna's poll cap of 50 ms, the page round trips and
    /// timer jitter. Far below the 1,023 ms a one-second back-off cost a 700 ms hold, so a regression to
    /// that back-off fails here.
    /// </summary>
    private static readonly TimeSpan WaitSlack = TimeSpan.FromMilliseconds(300);

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { SlowQueryLogEnabled = true, SlowQueryLogThresholdMs = 0 };

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname,
            "CREATE TABLE user_presence (usersId STRING(36), id STRING(36), location STRING(64), isOnline BOOL, " +
            "createdAt INT64, updatedAt INT64, version INT64, PRIMARY KEY (usersId, id))");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            for (int i = 0; i < Rows; i++)
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                    $"INSERT INTO user_presence (usersId, id, location, isOnline, createdAt, updatedAt, version) " +
                    $"VALUES ('u-{i}', 'r-{i}', 'Lobby', true, 1000, 1000, 0)", null));
            }

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        return (dbname, database, executor);
    }

    /// <summary>
    /// Begins the heartbeat writer, optimistic Serializable like a by-id business write, and does its
    /// read of the row it will update. No intent exists yet: a read stages nothing.
    /// </summary>
    private static async Task<KvTransaction> BeginHeartbeat(CommandExecutor executor, string dbname, DatabaseDescriptor database, int slot)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Optimistic);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, $"SELECT * FROM user_presence WHERE usersId = 'u-{slot}'", null));
        Assert.AreEqual(1, (await cursor.ToListAsync()).Count);

        return tx;
    }

    /// <summary>Stages the heartbeat's UPDATE. From here the row holds the writer's intent until commit or rollback.</summary>
    private static Task StageHeartbeat(CommandExecutor executor, string dbname, KvTransaction tx, int slot)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"UPDATE user_presence SET updatedAt = 2000, version = version + 1 " +
            $"WHERE usersId = 'u-{slot}' AND id = 'r-{slot}' AND version = 0", null));

    /// <summary>Runs the sweep on <paramref name="reader"/> and returns how long it took and how many rows it returned.</summary>
    private static async Task<(TimeSpan elapsed, int rows)> Sweep(CommandExecutor executor, string dbname, KvTransaction reader)
    {
        long started = Stopwatch.GetTimestamp();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(reader, dbname, SweepSql, null));
        int n = 0;
        await foreach (QueryResultRow _ in cursor)
            n++;
        return (Stopwatch.GetElapsedTime(started), n);
    }

    /// <summary>The <c>updatedAt</c> of one row as <paramref name="reader"/> sees it.</summary>
    private static async Task<long> UpdatedAtOf(CommandExecutor executor, string dbname, KvTransaction reader, int slot)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(reader, dbname,
            $"SELECT updatedAt FROM user_presence WHERE usersId = 'u-{slot}'", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(1, rows.Count);
        return rows[0].Row["updatedAt"].LongValue;
    }

    /// <summary>The newest slow-log entry of the sweep.</summary>
    private static async Task<QueryResultRow> LastSweepEntry(CommandExecutor executor, string dbname)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, dbname, "SHOW SLOW QUERIES", null));
        List<QueryResultRow> entries = (await cursor.ToListAsync())
            .Where(r => r.Row["sql"].StrValue?.Contains("ORDER BY updatedAt", StringComparison.Ordinal) == true)
            .ToList();
        Assert.IsNotEmpty(entries, "the sweep is recorded in the slow query log");
        return entries[0];
    }

    /// <summary>
    /// A heartbeat that begins after the read's snapshot can never commit inside it, so the read
    /// neither waits for it nor sees its write: every row is read at once, and the updated row shows
    /// the value from before the heartbeat.
    /// </summary>
    [Test]
    public async Task WriterThatBeginsAfterTheSnapshot_NeverDelaysTheRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction reader = database.Transactions.BeginReadOnly();

        KvTransaction writer = await BeginHeartbeat(executor, dbname, database, 9);
        await StageHeartbeat(executor, dbname, writer, 9);
        try
        {
            (TimeSpan elapsed, int rows) = await Sweep(executor, dbname, reader);
            TestContext.Out.WriteLine($"writer began after the snapshot: sweep took {elapsed.TotalMilliseconds:F0} ms");

            Assert.AreEqual(0, rows, "no row is stale");
            Assert.That(elapsed, Is.LessThan(TimeSpan.FromMilliseconds(500)), "the read must not wait on a younger writer");
            Assert.AreEqual(1000, await UpdatedAtOf(executor, dbname, reader, 9), "the snapshot predates the heartbeat");

            QueryResultRow entry = await LastSweepEntry(executor, dbname);
            Assert.AreEqual(Rows, entry.Row["rows_read"].LongValue);
            Assert.AreEqual("completed", entry.Row["outcome"].StrValue);
        }
        finally
        {
            await database.Transactions.CommitAsync(writer);
        }
    }

    /// <summary>
    /// A heartbeat that staged its write before the read's snapshot holds the page until it commits:
    /// the read waits the writer's hold and little more, and then reads every row. The commit lands
    /// above the snapshot, so the read still shows the value from before the heartbeat. A writer that
    /// began before the snapshot but staged after it does not delay the read at all: Kahuna bounds an
    /// un-prepared intent by the stage time, not by the writer's transaction id.
    /// </summary>
    [Test]
    public async Task WriterThatStagedBeforeTheSnapshot_DelaysTheReadByItsHold_ThenReadsEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        TimeSpan hold = TimeSpan.FromMilliseconds(700);

        // Staged before the snapshot.
        {
            KvTransaction writer = await BeginHeartbeat(executor, dbname, database, 7);
            await StageHeartbeat(executor, dbname, writer, 7);
            KvTransaction reader = database.Transactions.BeginReadOnly();
            Task commit = Task.Run(async () => { await Task.Delay(hold); await database.Transactions.CommitAsync(writer); });

            (TimeSpan elapsed, int rows) = await Sweep(executor, dbname, reader);
            await commit;
            TestContext.Out.WriteLine($"writer staged before the snapshot and held {hold.TotalMilliseconds:F0} ms: sweep took {elapsed.TotalMilliseconds:F0} ms");

            Assert.AreEqual(0, rows);
            Assert.That(elapsed, Is.GreaterThanOrEqualTo(hold), "the read waits for the writer's intent");
            Assert.That(elapsed, Is.LessThan(hold + WaitSlack), "the read is served soon after the intent clears");
            Assert.AreEqual(1000, await UpdatedAtOf(executor, dbname, reader, 7), "the commit landed above the snapshot");

            QueryResultRow entry = await LastSweepEntry(executor, dbname);
            Assert.AreEqual(Rows, entry.Row["rows_read"].LongValue, "a page served after a wait counts every row");
            Assert.AreEqual("completed", entry.Row["outcome"].StrValue);
        }

        // Began before the snapshot, staged after it.
        {
            KvTransaction writer = await BeginHeartbeat(executor, dbname, database, 8);
            KvTransaction reader = database.Transactions.BeginReadOnly();
            await StageHeartbeat(executor, dbname, writer, 8);
            Task commit = Task.Run(async () => { await Task.Delay(hold); await database.Transactions.CommitAsync(writer); });

            (TimeSpan elapsed, int rows) = await Sweep(executor, dbname, reader);
            await commit;
            TestContext.Out.WriteLine($"writer began before the snapshot, staged after it, held {hold.TotalMilliseconds:F0} ms: sweep took {elapsed.TotalMilliseconds:F0} ms");

            Assert.AreEqual(0, rows);
            Assert.That(elapsed, Is.LessThan(WaitSlack), "a write staged above the snapshot never delays the read");
            Assert.AreEqual(1000, await UpdatedAtOf(executor, dbname, reader, 8));
            Assert.AreEqual(Rows, (await LastSweepEntry(executor, dbname)).Row["rows_read"].LongValue);
        }
    }

    /// <summary>
    /// A heartbeat that holds its intent past the scan page retry budget fails the read with the
    /// retryable code instead of handing back a truncated result; the slow log records the failure
    /// with no row read. Once the writer commits, a fresh read completes.
    /// </summary>
    [Test]
    public async Task WriterThatHoldsPastThePageBudget_FailsTheReadWithTheRetryableCode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction writer = await BeginHeartbeat(executor, dbname, database, 10);
        await StageHeartbeat(executor, dbname, writer, 10);
        KvTransaction reader = database.Transactions.BeginReadOnly();
        Task commit = Task.Run(async () => { await Task.Delay(ScanPageRetryBudget + TimeSpan.FromMilliseconds(1500)); await database.Transactions.CommitAsync(writer); });

        long started = Stopwatch.GetTimestamp();
        CamusDBException failure = Assert.ThrowsAsync<CamusDBException>(async () => await Sweep(executor, dbname, reader))!;
        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
        await commit;
        TestContext.Out.WriteLine($"writer held past the budget: the read failed after {elapsed.TotalMilliseconds:F0} ms: {failure.Message}");

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, failure.Code);
        Assert.That(failure.Message, Does.Contain("row scan of table user_presence"));
        Assert.That(elapsed, Is.GreaterThanOrEqualTo(ScanPageRetryBudget));

        QueryResultRow entry = await LastSweepEntry(executor, dbname);
        Assert.AreEqual("failed", entry.Row["outcome"].StrValue);
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, entry.Row["error_code"].StrValue);
        Assert.AreEqual(0, entry.Row["rows_read"].LongValue, "the page was never served");

        (TimeSpan retried, int rows) = await Sweep(executor, dbname, database.Transactions.BeginReadOnly());
        Assert.AreEqual(0, rows);
        Assert.That(retried, Is.LessThan(TimeSpan.FromMilliseconds(500)), "with the writer gone a fresh read completes at once");
        Assert.AreEqual(2000, await UpdatedAtOf(executor, dbname, database.Transactions.BeginReadOnly(), 10));
    }
}
