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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// The wait-die rule on the exclusive row-key lock that a pessimistic <c>UPDATE</c>, <c>DELETE</c> and
/// <c>SELECT … FOR UPDATE</c> take, at Read Committed, end to end through SQL.
///
/// <para>Kahuna refuses a held key at once and names the holder. A requester that started before the
/// holder, or that holds nothing yet, polls until the lock-wait deadline; a younger requester that
/// already holds a lock fails at once with <c>TransactionConflict</c>. <c>NOWAIT</c> and
/// <c>SKIP LOCKED</c> never wait. The deadline is raised to two seconds so that "failed at once" and
/// "waited out the deadline" are far apart.</para>
///
/// <para>A transaction gets its identity, and so its age, at its first data statement. Every test makes
/// the transaction meant to be older run a statement first, and asserts the order.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestKeyLockWaitDie : SharedNodeBaseTest
{
    private const int DeadlineMs = 2_000;

    /// <summary>Upper bound for a requester that must fail without waiting, well below the deadline.</summary>
    private const int AtOnceMs = DeadlineMs / 2;

    /// <summary>How long a holder keeps its row before it ends, well inside the deadline.</summary>
    private const int HoldMs = 250;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { LockWaitDeadlineMs = DeadlineMs };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task<long[]> IdsAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return (await cursor.ToListAsync()).Select(r => r.Row["id"].LongValue).ToArray();
    }

    private static async Task<long> ValueOfAsync(CommandExecutor executor, DatabaseDescriptor database, string db, long id)
    {
        KvTransaction tx = await BeginAsync(database);
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, $"SELECT v FROM acc WHERE id = {id}", null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual(1, rows.Count, $"row {id} must exist");
            await database.Transactions.CommitAsync(tx);
            return rows[0].Row["v"].LongValue;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static Task<KvTransaction> BeginAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);

    private static async Task<(CamusDBException Error, long ElapsedMs)> ExpectFailureAsync(Func<Task> action)
    {
        Stopwatch sw = Stopwatch.StartNew();
        CamusDBException? failure = null;
        try
        {
            await action();
        }
        catch (CamusDBException e)
        {
            failure = e;
        }

        sw.Stop();
        Assert.IsNotNull(failure, "the statement was expected to fail");
        return (failure!, sw.ElapsedMilliseconds);
    }

    /// <summary>Ends <paramref name="tx"/> after <see cref="HoldMs"/>, by a commit or a rollback.</summary>
    private static Task EndLaterAsync(DatabaseDescriptor database, KvTransaction tx, bool commit)
        => Task.Run(async () =>
        {
            await Task.Delay(HoldMs);
            if (commit)
                await database.Transactions.CommitAsync(tx);
            else
                await database.Transactions.RollbackAsync(tx);
        });

    private static void AssertOlder(KvTransaction older, KvTransaction younger)
        => Assert.Less(older.TransactionId.CompareTo(younger.TransactionId), 0,
            "the test needs the first transaction to have started before the second");

    /// <summary>Creates <c>acc</c> with ids 1..5, each with <c>v = 0</c>.</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db, "CREATE TABLE acc (id INT64 PRIMARY KEY, v INT64)", null));

        KvTransaction seed = await BeginAsync(database);
        await ExecAsync(executor, db, seed, "INSERT INTO acc (id, v) VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)");
        await database.Transactions.CommitAsync(seed);

        return (db, database, executor);
    }

    [Test]
    public async Task OlderRequester_WaitsForAYoungerHolder_AndAddsToTheValueItCommitted()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The older transaction holds a row of its own, so only its age lets it wait.
        KvTransaction older = await BeginAsync(database);
        await ExecAsync(executor, db, older, "UPDATE acc SET v = v + 1 WHERE id = 2");

        KvTransaction younger = await BeginAsync(database);
        await ExecAsync(executor, db, younger, "UPDATE acc SET v = v + 10 WHERE id = 1");
        AssertOlder(older, younger);

        Task release = EndLaterAsync(database, younger, commit: true);

        // One batch of two row locks; one of them is held.
        Stopwatch sw = Stopwatch.StartNew();
        await ExecAsync(executor, db, older, "UPDATE acc SET v = v + 1 WHERE id IN (1, 3)");
        sw.Stop();
        await database.Transactions.CommitAsync(older);
        await release;

        Assert.GreaterOrEqual(sw.ElapsedMilliseconds, HoldMs / 2, "the older transaction must have waited for the holder");
        Assert.AreEqual(11, await ValueOfAsync(executor, database, db, 1), "the waiter adds to the value the holder committed");
        Assert.AreEqual(1, await ValueOfAsync(executor, database, db, 2));
        Assert.AreEqual(1, await ValueOfAsync(executor, database, db, 3));
    }

    [Test]
    public async Task YoungerRequesterThatHoldsARowLock_DiesAtOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction older = await BeginAsync(database);
        await ExecAsync(executor, db, older, "UPDATE acc SET v = 1 WHERE id = 1");

        KvTransaction younger = await BeginAsync(database);
        await ExecAsync(executor, db, younger, "UPDATE acc SET v = 2 WHERE id = 2");
        AssertOlder(older, younger);

        (CamusDBException e, long elapsed) = await ExpectFailureAsync(
            () => ExecAsync(executor, db, younger, "UPDATE acc SET v = 2 WHERE id = 1"));

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, e.Code, e.Message);
        StringAssert.Contains("wait-die", e.Message);
        Assert.Less(elapsed, AtOnceMs, "a younger requester that holds a lock must not wait");

        await database.Transactions.RollbackIfNotCompletedAsync(younger);
        await database.Transactions.CommitAsync(older);
        Assert.AreEqual(1, await ValueOfAsync(executor, database, db, 1));
    }

    /// <summary>
    /// A row lock from <c>SELECT … FOR UPDATE</c> is held with no write behind it. It must still count as
    /// a holding: a transaction that holds one could otherwise wait as if it held nothing, and two such
    /// transactions could wait for each other until the deadline.
    /// </summary>
    [Test]
    public async Task ARowLockFromForUpdate_CountsAsAHolding()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction older = await BeginAsync(database);
        Assert.AreEqual(new long[] { 1 }, await IdsAsync(executor, db, older, "SELECT id FROM acc WHERE id = 1 FOR UPDATE"));

        KvTransaction younger = await BeginAsync(database);
        Assert.AreEqual(new long[] { 2 }, await IdsAsync(executor, db, younger, "SELECT id FROM acc WHERE id = 2 FOR UPDATE"));
        AssertOlder(older, younger);

        (CamusDBException e, long elapsed) = await ExpectFailureAsync(
            () => IdsAsync(executor, db, younger, "SELECT id FROM acc WHERE id = 1 FOR UPDATE"));

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, e.Code, e.Message);
        Assert.Less(elapsed, AtOnceMs, "the FOR UPDATE lock on row 2 makes the younger transaction a holder");

        await database.Transactions.RollbackIfNotCompletedAsync(younger);
        await database.Transactions.CommitAsync(older);
    }

    [Test]
    public async Task YoungerRequesterThatHoldsNothing_WaitsForTheHolder_AndProceeds()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        await ExecAsync(executor, db, holder, "UPDATE acc SET v = v + 1 WHERE id = 1");

        Task release = EndLaterAsync(database, holder, commit: true);

        // The first statement of this transaction: it holds nothing, so it may wait although it is younger.
        KvTransaction fresh = await BeginAsync(database);
        Stopwatch sw = Stopwatch.StartNew();
        await ExecAsync(executor, db, fresh, "UPDATE acc SET v = v + 1 WHERE id = 1");
        sw.Stop();
        AssertOlder(holder, fresh);
        await database.Transactions.CommitAsync(fresh);
        await release;

        Assert.GreaterOrEqual(sw.ElapsedMilliseconds, HoldMs / 2, "the requester must have waited for the holder");
        Assert.AreEqual(2, await ValueOfAsync(executor, database, db, 1), "both increments are kept");
    }

    [Test]
    public async Task AHolderThatOutlivesTheDeadline_FailsTheWaiterWithTransactionConflict()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        await ExecAsync(executor, db, holder, "UPDATE acc SET v = 1 WHERE id = 1");

        KvTransaction waiter = await BeginAsync(database);
        (CamusDBException e, long elapsed) = await ExpectFailureAsync(
            () => ExecAsync(executor, db, waiter, "UPDATE acc SET v = 2 WHERE id = 1"));

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, e.Code, e.Message);
        StringAssert.Contains("lock_wait_deadline_ms", e.Message);
        Assert.GreaterOrEqual(elapsed, DeadlineMs * 9 / 10, "the requester holds nothing, so it waits out the deadline");

        await database.Transactions.RollbackIfNotCompletedAsync(waiter);
        await database.Transactions.CommitAsync(holder);
        Assert.AreEqual(1, await ValueOfAsync(executor, database, db, 1));
    }

    /// <summary>
    /// Two transactions lock two rows in opposite order. The older one waits for the younger; the younger
    /// one, which holds a row, fails at once instead of waiting for the older. No wait cycle forms, and the
    /// older transaction commits as soon as the younger one rolls back, well inside the deadline.
    /// </summary>
    [Test]
    public async Task ReverseOrderLocking_TheYoungerDiesAtOnce_AndTheOlderCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction older = await BeginAsync(database);
        await ExecAsync(executor, db, older, "UPDATE acc SET v = v + 1 WHERE id = 1");

        KvTransaction younger = await BeginAsync(database);
        await ExecAsync(executor, db, younger, "UPDATE acc SET v = v + 10 WHERE id = 2");
        AssertOlder(older, younger);

        Stopwatch sw = Stopwatch.StartNew();
        Task olderWaits = ExecAsync(executor, db, older, "UPDATE acc SET v = v + 1 WHERE id = 2");

        (CamusDBException e, long elapsed) = await ExpectFailureAsync(
            () => ExecAsync(executor, db, younger, "UPDATE acc SET v = v + 10 WHERE id = 1"));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, e.Code, e.Message);
        Assert.Less(elapsed, AtOnceMs, "the younger transaction must not wait for the older one");
        await database.Transactions.RollbackIfNotCompletedAsync(younger);

        await olderWaits;
        sw.Stop();
        await database.Transactions.CommitAsync(older);

        Assert.Less(sw.ElapsedMilliseconds, DeadlineMs, "the older transaction goes on once the younger rolls back");
        Assert.AreEqual(1, await ValueOfAsync(executor, database, db, 1));
        Assert.AreEqual(1, await ValueOfAsync(executor, database, db, 2), "the younger transaction's write was rolled back");
    }

    [Test]
    public async Task DeleteWaitsForTheHolder_AndRechecksTheRowItCommitted()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        await ExecAsync(executor, db, holder, "UPDATE acc SET v = 100 WHERE id = 3");

        Task release = EndLaterAsync(database, holder, commit: true);

        // Located while v is still 0, locked after the holder committed v = 100: the re-check skips it.
        KvTransaction deleter = await BeginAsync(database);
        Stopwatch sw = Stopwatch.StartNew();
        await ExecAsync(executor, db, deleter, "DELETE FROM acc WHERE id = 3 AND v = 0");
        sw.Stop();
        await database.Transactions.CommitAsync(deleter);
        await release;

        Assert.GreaterOrEqual(sw.ElapsedMilliseconds, HoldMs / 2, "the DELETE must have waited for the holder");
        Assert.AreEqual(100, await ValueOfAsync(executor, database, db, 3), "the row no longer matched, so it was kept");
    }

    [Test]
    public async Task ForUpdateWaitsForAHolderThatRollsBack()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        await ExecAsync(executor, db, holder, "UPDATE acc SET v = 7 WHERE id = 4");

        Task release = EndLaterAsync(database, holder, commit: false);

        KvTransaction locker = await BeginAsync(database);
        Assert.AreEqual(new long[] { 4 }, await IdsAsync(executor, db, locker, "SELECT id FROM acc WHERE id = 4 AND v = 0 FOR UPDATE"));
        await release;

        // The row is now held by the locker.
        KvTransaction other = await BeginAsync(database);
        (CamusDBException e, _) = await ExpectFailureAsync(
            () => IdsAsync(executor, db, other, "SELECT id FROM acc WHERE id = 4 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, e.Code, e.Message);
        await database.Transactions.RollbackIfNotCompletedAsync(other);

        await database.Transactions.CommitAsync(locker);
    }

    /// <summary>
    /// NOWAIT and SKIP LOCKED keep their no-wait behavior for a requester that the wait-die rule would
    /// let wait: one older than the holder.
    /// </summary>
    [Test]
    public async Task NoWaitAndSkipLocked_DoNotWait_EvenForAnOlderRequester()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction olderNoWait = await BeginAsync(database);
        await IdsAsync(executor, db, olderNoWait, "SELECT id FROM acc WHERE id = 4 FOR UPDATE");

        KvTransaction olderSkip = await BeginAsync(database);
        await IdsAsync(executor, db, olderSkip, "SELECT id FROM acc WHERE id = 5 FOR UPDATE");

        KvTransaction holder = await BeginAsync(database);
        await IdsAsync(executor, db, holder, "SELECT id FROM acc WHERE id = 1 FOR UPDATE");
        AssertOlder(olderNoWait, holder);
        AssertOlder(olderSkip, holder);

        (CamusDBException e, long elapsed) = await ExpectFailureAsync(
            () => IdsAsync(executor, db, olderNoWait, "SELECT id FROM acc WHERE id = 1 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, e.Code, e.Message);
        Assert.Less(elapsed, AtOnceMs);
        await database.Transactions.RollbackIfNotCompletedAsync(olderNoWait);

        Stopwatch sw = Stopwatch.StartNew();
        long[] free = await IdsAsync(executor, db, olderSkip, "SELECT id FROM acc WHERE id <= 2 ORDER BY id FOR UPDATE SKIP LOCKED");
        sw.Stop();
        Assert.AreEqual(new long[] { 2 }, free);
        Assert.Less(sw.ElapsedMilliseconds, AtOnceMs);

        await database.Transactions.CommitAsync(olderSkip);
        await database.Transactions.CommitAsync(holder);
    }
}
