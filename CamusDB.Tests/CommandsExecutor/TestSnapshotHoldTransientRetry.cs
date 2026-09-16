/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers what a transient Kahuna answer means on the snapshot-hold paths.
///
/// <para>A snapshot-floor hold is committed on Kahuna's meta partition, so acquiring or renewing one
/// answers <c>MustRetry</c> while that partition has no confirmed leader — a routine election, and an
/// answer that carries no decision at all. Decoding it as a refusal failed <c>CREATE DATABASE …
/// BRANCH FROM</c> on roughly half of consecutive runs against a healthy node, refused every pinned
/// read, and discarded a live hold from under a copy that was still running. These tests hold that
/// line from both sides: a transient answer must be ridden out, and the one definitive answer
/// (<c>DoesNotExist</c>) must still fail closed at once.</para>
/// </summary>
// Serial: boots an embedded Kahuna node per test, and the acquire seam is process-wide.
[NonParallelizable]
public sealed class TestSnapshotHoldTransientRetry : BaseTest
{
    /// <summary>
    /// Fault fake: answers every snapshot-hold renewal with one scripted status and counts the
    /// attempts. Every other operation passes through to the real node.
    /// </summary>
    private sealed class ScriptedRenewKahuna : DelegatingKahuna
    {
        private readonly KeyValueResponseType status;
        private int renewCalls;

        public ScriptedRenewKahuna(IKahuna inner, KeyValueResponseType status) : base(inner) => this.status = status;

        public int RenewCalls => Volatile.Read(ref renewCalls);

        public override Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> LocateAndRenewSnapshotHold(
            string holdId, int leaseMs, CancellationToken ct)
        {
            Interlocked.Increment(ref renewCalls);
            return Task.FromResult((status, HLCTimestamp.Zero));
        }
    }

    private static async Task RunNonQuery(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> RunSelect(string dbName, CommandExecutor executor, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        return await cursor.ToListAsync();
    }

    /// <summary>
    /// The reproduced failure: an election on the snapshot-hold partition answers the fork-point
    /// acquire with <c>MustRetry</c>, and the branch create must ride it out rather than abort. The
    /// branch must also end up genuinely protected — a create that "succeeded" without a live hold
    /// would be the worse bug.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchCreate_RidesOutTransientAcquire_AndStillTakesTheHold()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateDatabase();

        string branchName = "b_" + Guid.NewGuid().ToString("n");

        SnapshotHoldRetry.InjectTransientAcquiresForTesting(4);
        try
        {
            await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
            TrackDatabase(branchName, executor);
        }
        finally
        {
            SnapshotHoldRetry.InjectTransientAcquiresForTesting(0);
        }

        DatabaseRegistryEntry? branchEntry = sharedRegistry!.Get(branchName);
        Assert.That(branchEntry, Is.Not.Null, "the branch must be registered after riding out the transient acquires");
        Assert.That(branchEntry!.ImmediateParentHoldId, Is.Not.Empty,
            "the branch must persist the id of the hold it finally acquired");

        (_, HLCTimestamp floor, int live) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);

        Assert.That(live, Is.GreaterThanOrEqualTo(1),
            "the hold must be live — a create that rides out a transient must still end up protected");
        Assert.That(floor, Is.EqualTo(branchEntry.Ancestors[0].ForkTimestamp),
            "the effective floor must sit at the branch's fork timestamp");
    }

    /// <summary>
    /// A budget that is genuinely spent is still a failure — but a retryable one. It reports that the
    /// hold could not be confirmed, not that the statement was wrong, so a caller may simply re-issue
    /// it. The branch must not be registered, because nothing protects its frozen view.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchCreate_SpentBudget_SurfacesRetryable_AndCreatesNothing()
    {
        // A budget of zero makes exactly one attempt, so one injected transient spends it.
        (string rootName, _, CommandExecutor executor) =
            await CreateDatabase(Options with { SnapshotHoldRetryBudgetMs = 0 });

        string branchName = "b_" + Guid.NewGuid().ToString("n");

        SnapshotHoldRetry.InjectTransientAcquiresForTesting(1);
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));

            Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
                "an unconfirmed hold is retryable unavailability, never a rejected statement");
        }
        finally
        {
            SnapshotHoldRetry.InjectTransientAcquiresForTesting(0);
        }

        Assert.That(sharedRegistry!.Get(branchName), Is.Null,
            "no branch may be registered when its frozen view was never pinned");
    }

    /// <summary>
    /// The same rule on the pinned-read path: an <c>AS OF SYSTEM TIME</c> read whose hold acquire is
    /// answered transiently must still run, and must still read the historical value.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task PinnedRead_RidesOutTransientAcquire()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE leaderboard (id OBJECT_ID PRIMARY KEY, name STRING, score INT64)",
            parameters: null));

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO leaderboard (id, name, score) VALUES (gen_id(), \"player1\", 10)");

        await Task.Delay(60);
        long snapshotMs = TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(TestNode!.Raft.GetLocalNodeId()).L;
        await Task.Delay(60);

        await RunNonQuery(dbName, db, executor, "UPDATE leaderboard SET score = 20 WHERE name = \"player1\"");

        List<QueryResultRow> historical;

        SnapshotHoldRetry.InjectTransientAcquiresForTesting(4);
        try
        {
            historical = await RunSelect(dbName, executor,
                $"SELECT score FROM leaderboard AS OF SYSTEM TIME {snapshotMs}");
        }
        finally
        {
            SnapshotHoldRetry.InjectTransientAcquiresForTesting(0);
        }

        Assert.That(historical.Count, Is.EqualTo(1));
        Assert.That(historical[0].Row["score"].LongValue, Is.EqualTo(10L),
            "the pinned read must still see the pre-update value after riding out a transient acquire");
    }

    /// <summary>
    /// A renewal answered <c>MustRetry</c> must not discard the hold. This is the case that aborted a
    /// long branch metadata copy over a routine election: the hold was still alive, and only the
    /// answer was unavailable. The loop must keep probing instead of latching lost.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RenewLoop_TransientStatus_KeepsProbing_ThenFailsClosedAtTheLease()
    {
        const int leaseMs = 4000;

        ScriptedRenewKahuna fault = new(TestNode!.Kahuna, KeyValueResponseType.MustRetry);

        await using SnapshotHoldLease lease = SnapshotHoldLease.AdoptForKeepAlive(
            fault, logger, "hold-that-is-still-alive", HLCTimestamp.Zero, leaseMs);

        // Well inside the lease: the answer is unavailable, so nothing has been proven and the hold
        // must still be usable.
        await Task.Delay(2000);

        Assert.That(lease.IsLost, Is.False,
            "a transient renewal answer says nothing about the hold and must never discard it");
        Assert.That(fault.RenewCalls, Is.GreaterThan(1),
            "the loop must keep probing after a transient answer rather than latching on the first one");

        Assert.DoesNotThrow(() => lease.ThrowIfLost("the metadata copy"),
            "a reader must not be aborted while its hold is only unconfirmed");

        // Past the point where the lease can still be assumed live, nothing proves the hold intact —
        // so the reader is cut off. Failing closed is preserved; only its trigger is corrected.
        await WaitUntilLostAsync(lease, TimeSpan.FromMilliseconds(6000));

        Assert.That(lease.IsLost, Is.True,
            "once the lease can no longer be assumed live the hold must be presumed gone");
    }

    /// <summary>
    /// The one definitive refusal must still latch immediately. <c>DoesNotExist</c> means the hold was
    /// released or purged — replicated, permanent, and the same answer on every node — so waiting out
    /// the lease would only delay a reader that is already reading unprotected history.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RenewLoop_DoesNotExist_MarksLostOnTheFirstAnswer()
    {
        const int leaseMs = 4000;

        ScriptedRenewKahuna fault = new(TestNode!.Kahuna, KeyValueResponseType.DoesNotExist);

        await using SnapshotHoldLease lease = SnapshotHoldLease.AdoptForKeepAlive(
            fault, logger, "hold-that-is-gone", HLCTimestamp.Zero, leaseMs);

        await WaitUntilLostAsync(lease, TimeSpan.FromMilliseconds(4000));

        Assert.That(lease.IsLost, Is.True, "a removed hold must fail closed at once");
        Assert.That(fault.RenewCalls, Is.EqualTo(1),
            "a definitive refusal must latch on its first answer, not be retried like a transient one");

        Assert.Throws<CamusDBException>(() => lease.ThrowIfLost("the pinned read"),
            "a lost hold must refuse the read that depended on it");
    }

    private static async Task WaitUntilLostAsync(SnapshotHoldLease lease, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;

        while (!lease.IsLost && DateTime.UtcNow < deadline)
            await Task.Delay(50);
    }
}
