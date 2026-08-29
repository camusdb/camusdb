/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers what a rollback does when the coordinator answers that it does not know the transaction:
/// no session, no retained outcome, no durable record. The coordinator releases nothing in that case —
/// the working set it finalizes from died with the session — so the staged writes and point locks the
/// transaction planted stay at the participants, where an undecided foreign intent blocks every scan
/// of its key space for as long as it lives.
///
/// <para>These tests plant that exact shape: an exclusive lock taken with no expiry (which is what the
/// engine's write path takes, so the intent is session-owned and never lapses) plus a staged value,
/// owned by a transaction id that has no coordinator session at all. A rollback of such a handle is
/// the wedge; the release-by-mirror pass is what clears it.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestCoordinatorUnknownRelease : SharedNodeBaseTest
{
    /// <summary>Release age used by the aged cases: any transaction older than a few ms qualifies.</summary>
    private const int CompressedReleaseAgeMs = 20;

    /// <summary>Slept before an aged rollback so the mirror is unambiguously past the release age.</summary>
    private const int PastReleaseAgeDelayMs = 60;

    private static CamusDBOptions Aged(CamusDBOptions defaults) =>
        defaults with { AbandonedTransactionReleaseAfterMs = CompressedReleaseAgeMs };

    /// <summary>
    /// A transaction id minted from the node's clock but never started as a session — the identity of a
    /// transaction whose coordinator session is gone, from the participants' point of view.
    /// </summary>
    private HLCTimestamp MintOrphanTransactionId() =>
        SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId());

    private static string NewKey() => "unknowntxn/" + Guid.NewGuid().ToString("n");

    /// <summary>
    /// Plants the wedge on <paramref name="key"/>: the session-owned exclusive lock the engine's write
    /// path takes (no expiry, so nothing ages it out) plus a staged value, both owned by
    /// <paramref name="txId"/>.
    /// </summary>
    private async Task PlantSessionOwnedHoldingAsync(HLCTimestamp txId, string key)
    {
        (KeyValueResponseType locked, _, _, _) = await SharedKahuna.LocateAndTryAcquireExclusiveLock(
            txId, key, expiresMs: 0, KeyValueDurability.Persistent, CancellationToken.None);

        Assert.That(locked, Is.EqualTo(KeyValueResponseType.Locked));

        (KeyValueResponseType set, _, _) = await SharedKahuna.LocateAndTrySetKeyValue(
            txId, key, value: [7, 7, 7], compareValue: null, compareRevision: -1,
            KeyValueFlags.None, expiresMs: 0, KeyValueDurability.Persistent, CancellationToken.None);

        Assert.That(set, Is.EqualTo(KeyValueResponseType.Set));
    }

    /// <summary>
    /// Whether the key is still held: a foreign transaction tries to take it and reports what it found.
    /// A successful probe releases immediately so it leaves nothing of its own behind. This is the
    /// symptom under test — a key nobody can take is a key no writer or scan can get past.
    /// </summary>
    private async Task<KeyValueResponseType> ProbeHoldingAsync(string key)
    {
        HLCTimestamp probeId = MintOrphanTransactionId();

        (KeyValueResponseType type, _, _, _) = await SharedKahuna.LocateAndTryAcquireExclusiveLock(
            probeId, key, expiresMs: 5_000, KeyValueDurability.Persistent, CancellationToken.None);

        if (type == KeyValueResponseType.Locked)
            await SharedKahuna.LocateAndTryReleaseExclusiveLock(
                probeId, key, KeyValueDurability.Persistent, CancellationToken.None);

        return type;
    }

    /// <summary>
    /// The client-side record of an abandoned transaction: its identity plus the keys it wrote. This is
    /// what survives on the client when the coordinator session does not, and it is the only key list
    /// the release pass has.
    /// </summary>
    private static KvTransaction MirrorOf(HLCTimestamp txId, params string[] keys)
    {
        KvTransaction mirror = new(txId, Guid.NewGuid().ToString("n"));

        foreach (string key in keys)
            mirror.TrackModified(key, KeyValueDurability.Persistent);

        return mirror;
    }

    [Test]
    public async Task AgedUnknownRollbackReleasesMirroredHoldings()
    {
        (_, DatabaseDescriptor db, _) = await CreateDatabase(Aged(Options));

        HLCTimestamp orphanId = MintOrphanTransactionId();
        string key = NewKey();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.AlreadyLocked),
            "the planted holding must block another transaction before the reap");

        KvTransaction mirror = MirrorOf(orphanId, key);
        await Task.Delay(PastReleaseAgeDelayMs);

        await db.Transactions.RollbackAsync(mirror, CancellationToken.None);

        Assert.That(mirror.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.Locked),
            "the mirrored key must be released, so another transaction can take it");
    }

    [Test]
    public async Task RecentUnknownRollbackLeavesHoldingsUntouched()
    {
        // Shipped defaults: the release age is derived from the session ceiling (an hour at defaults),
        // so a transaction seconds old is nowhere near it and the reap must change nothing.
        (_, DatabaseDescriptor db, _) = await CreateDatabase(Options);

        HLCTimestamp orphanId = MintOrphanTransactionId();
        string key = NewKey();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        KvTransaction mirror = MirrorOf(orphanId, key);

        await db.Transactions.RollbackAsync(mirror, CancellationToken.None);

        Assert.That(mirror.Status, Is.EqualTo(KvTransactionStatus.RolledBack),
            "the rollback is still terminal — only the release is withheld");
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.AlreadyLocked),
            "below the release age the holding must be left for the session that may still own it");
        Assert.That(await db.Transactions.ReleaseDueMirroredHoldingsAsync(CancellationToken.None), Is.EqualTo(0),
            "a sweep must not bring the release forward — the mirror is parked until its age is reached");

        // Leave the shared node clean: the holding this test deliberately preserved is immortal.
        await SharedKahuna.LocateAndTryReleaseExclusiveLock(
            orphanId, key, KeyValueDurability.Persistent, CancellationToken.None);
    }

    /// <summary>
    /// The transaction that is too young to release is the common case, not the exception: the reaper
    /// reclaims an abandoned transaction after a few idle minutes, while the age at which no session
    /// can still own its holdings is the session ceiling — an hour at the shipped defaults. The reap
    /// finishes the transaction and drops it from the in-flight map, so if the key mirror went with it
    /// nothing would ever release those keys. It is parked instead, and the sweep releases it once the
    /// age is reached.
    /// </summary>
    [Test]
    public async Task DeferredHoldingsAreReleasedByTheSweepOnceTheAgeIsReached()
    {
        const int releaseAgeMs = 250;

        (_, DatabaseDescriptor db, CommandExecutor executor) =
            await CreateDatabase(Options with { AbandonedTransactionReleaseAfterMs = releaseAgeMs });

        HLCTimestamp orphanId = MintOrphanTransactionId();
        string key = NewKey();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        // Reaped immediately, well inside the release age — exactly the reaper's timing.
        KvTransaction mirror = MirrorOf(orphanId, key);
        await db.Transactions.RollbackAsync(mirror, CancellationToken.None);

        Assert.That(mirror.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.AlreadyLocked),
            "nothing may be released while a session could still own it");

        // A sweep before the age is reached must still leave it alone.
        Assert.That(await executor.ReleaseDueMirroredHoldingsAsync(CancellationToken.None), Is.EqualTo(0));
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.AlreadyLocked));

        await Task.Delay(releaseAgeMs + PastReleaseAgeDelayMs);

        Assert.That(await executor.ReleaseDueMirroredHoldingsAsync(CancellationToken.None), Is.EqualTo(1),
            "the parked mirror must be released once no session can still own it");
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.Locked));

        // Drained for good: a later sweep finds nothing and releases nothing.
        Assert.That(await executor.ReleaseDueMirroredHoldingsAsync(CancellationToken.None), Is.EqualTo(0));
    }

    [Test]
    public async Task DisabledReleaseParksNothingAndReleasesNothing()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) =
            await CreateDatabase(Options with { AbandonedTransactionReleaseAfterMs = -1 });

        HLCTimestamp orphanId = MintOrphanTransactionId();
        string key = NewKey();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        KvTransaction mirror = MirrorOf(orphanId, key);
        await Task.Delay(PastReleaseAgeDelayMs);
        await db.Transactions.RollbackAsync(mirror, CancellationToken.None);

        Assert.That(await executor.ReleaseDueMirroredHoldingsAsync(CancellationToken.None), Is.EqualTo(0));
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.AlreadyLocked),
            "an operator who switched the release off must get exactly the previous behaviour");

        await SharedKahuna.LocateAndTryReleaseExclusiveLock(
            orphanId, key, KeyValueDurability.Persistent, CancellationToken.None);
    }

    [Test]
    public async Task ReleasePassDoesNotTouchAnotherTransactionsHoldings()
    {
        (_, DatabaseDescriptor db, _) = await CreateDatabase(Aged(Options));

        HLCTimestamp orphanId = MintOrphanTransactionId();
        HLCTimestamp otherId = MintOrphanTransactionId();

        string orphanKey = NewKey();
        string otherKey = NewKey();

        await PlantSessionOwnedHoldingAsync(orphanId, orphanKey);
        await PlantSessionOwnedHoldingAsync(otherId, otherKey);

        // The mirror names a key the other transaction holds. Releases are keyed by (transaction id,
        // key), so naming a key is not enough to take it — only the owner's own state is removed.
        KvTransaction mirror = MirrorOf(orphanId, orphanKey, otherKey);
        await Task.Delay(PastReleaseAgeDelayMs);

        await db.Transactions.RollbackAsync(mirror, CancellationToken.None);

        Assert.That(await ProbeHoldingAsync(orphanKey), Is.EqualTo(KeyValueResponseType.Locked),
            "the abandoned transaction's own key must be released");
        Assert.That(await ProbeHoldingAsync(otherKey), Is.EqualTo(KeyValueResponseType.AlreadyLocked),
            "another transaction's holding must survive a release that merely names its key");

        await SharedKahuna.LocateAndTryReleaseExclusiveLock(
            otherId, otherKey, KeyValueDurability.Persistent, CancellationToken.None);
    }

    [Test]
    public async Task ReleasePassIsIdempotent()
    {
        (_, DatabaseDescriptor db, _) = await CreateDatabase(Aged(Options));

        HLCTimestamp orphanId = MintOrphanTransactionId();
        string key = NewKey();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        KvTransaction firstMirror = MirrorOf(orphanId, key);
        await Task.Delay(PastReleaseAgeDelayMs);
        await db.Transactions.RollbackAsync(firstMirror, CancellationToken.None);

        // A second reap of the same handle — the state the first pass already cleared. Nothing is left
        // to release and nothing may fail; the key stays available.
        KvTransaction secondMirror = MirrorOf(orphanId, key);
        await Task.Delay(PastReleaseAgeDelayMs);

        Assert.DoesNotThrowAsync(async () =>
            await db.Transactions.RollbackAsync(secondMirror, CancellationToken.None));

        Assert.That(secondMirror.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.Locked));
    }

    [Test]
    public async Task ReapedTransactionIsTerminalAndNeverReleasesTwice()
    {
        (_, DatabaseDescriptor db, _) = await CreateDatabase(Aged(Options));

        HLCTimestamp orphanId = MintOrphanTransactionId();
        string key = NewKey();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        KvTransaction mirror = MirrorOf(orphanId, key);
        await Task.Delay(PastReleaseAgeDelayMs);
        await db.Transactions.RollbackAsync(mirror, CancellationToken.None);

        // Re-plant the same holding. If anything re-attempted the reap of this handle it would release
        // the key a second time, and the probe below would find it free.
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        // The cleanup path must see a finished transaction and do nothing at all.
        await db.Transactions.RollbackIfNotCompletedAsync(mirror, CancellationToken.None);

        // An explicit rollback of a finished handle is refused rather than silently re-run.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await db.Transactions.RollbackAsync(mirror, CancellationToken.None))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted));

        Assert.That(await ProbeHoldingAsync(key), Is.EqualTo(KeyValueResponseType.AlreadyLocked),
            "no second release pass may run for a handle that was already reaped");

        await SharedKahuna.LocateAndTryReleaseExclusiveLock(
            orphanId, key, KeyValueDurability.Persistent, CancellationToken.None);
    }

    [Test]
    public async Task CommittedTransactionKeepsItsDataWhenItsHandleIsReapedLater()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(Aged(Options));

        KvTransaction ddl = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            ddl, dbname, "CREATE TABLE reaped_commit (id INT64 PRIMARY KEY, name STRING)", null));
        await db.Transactions.CommitAsync(ddl);

        KvTransaction writer = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            writer, dbname, "INSERT INTO reaped_commit (id, name) VALUES (1, 'kept')", null));

        // The keys the committed transaction wrote — the same list a reap of its handle would replay.
        List<(string key, KeyValueDurability durability)> writtenKeys = writer.GetModifiedKeyPairs();
        Assert.That(writtenKeys, Is.Not.Empty);

        await db.Transactions.CommitAsync(writer);

        // A stale reap of the committed handle: same transaction id, same keys, aged past the release
        // age. Every release is keyed by that id, and the commit already settled everything it owned,
        // so the pass has nothing to remove and the row must survive it.
        KvTransaction staleMirror = new(writer.TransactionId, writer.UniqueId);
        foreach ((string key, KeyValueDurability durability) in writtenKeys)
            staleMirror.TrackModified(key, durability);

        await Task.Delay(PastReleaseAgeDelayMs);

        Assert.DoesNotThrowAsync(async () =>
            await db.Transactions.RollbackAsync(staleMirror, CancellationToken.None));

        KvTransaction reader = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(reader, dbname, "SELECT id, name FROM reaped_commit", null));

        int rows = 0;
        await foreach (QueryResultRow row in cursor)
        {
            rows++;
            Assert.That(row.Row["name"].StrValue, Is.EqualTo("kept"));
        }

        await db.Transactions.CommitAsync(reader);

        Assert.That(rows, Is.EqualTo(1), "a committed row must survive a later reap of its transaction handle");
    }
}
