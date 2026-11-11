
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Verifies shared-point-lock escalation for Serializable+RW transactions.
///
/// Uses <see cref="KvTableStore"/> directly (bypassing the query executor) so that
/// <see cref="KvTableStore.GetRow"/> is called per-row and triggers per-row point locks —
/// the full query executor always takes a whole-bucket scan lock instead.
///
/// Tests build their manager with <see cref="CamusDBOptions.LockEscalationThreshold"/> at 3 so escalation triggers
/// after a few reads and not after the default count.
///
/// Serial: each test starts its own embedded Kahuna node. The escalation threshold belongs to the
/// manager and store the test constructs, so the node is the only remaining reason.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableLockEscalation
{
    /// <summary>
    /// A deliberately low escalation threshold, so a handful of row locks is enough to trip escalation
    /// without inserting thousands of rows. No engine is involved here — the manager and store are
    /// constructed directly with these options.
    /// </summary>
    private static CamusDBOptions EscalatingOptions =>
        CamusDBOptions.Default with { LockEscalationThreshold = 3 };

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag, CamusDBOptions? options = null)
    {
        CamusDBOptions effective = options ?? EscalatingOptions;

        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        KvTransactionsManager mgr = new(node.Kahuna, effective);
        KvTableStore store = new(node.Kahuna, effective, "testdb", tag);
        return (node, mgr, store);
    }

    private static async Task<ObjectIdValue[]> InsertRowsAsync(
        KvTransactionsManager mgr, KvTableStore store, int count)
    {
        var ids = new ObjectIdValue[count];
        for (int i = 0; i < count; i++)
        {
            ids[i] = ObjectIdGenerator.Generate();
            KvTransaction tx = await mgr.BeginAsync();
            await store.InsertRow(tx, ids[i], [(byte)i]);
            await mgr.CommitAsync(tx);
        }
        return ids;
    }

    // Point lock: [key, key] (StartKey == EndKey != null)
    private static int CountPointLocks(KvTransaction tx)
        => tx.GetAcquiredRangeLocks().Count(b => b.StartKey is not null && b.StartKey == b.EndKey);

    // Whole-bucket lock: StartKey == null && EndKey == null
    private static bool HasAnyWholeBucketLock(KvTransaction tx)
        => tx.GetAcquiredRangeLocks().Any(b => b.StartKey is null && b.EndKey is null);

    // -----------------------------------------------------------------------
    // 1. Below threshold: per-row point locks accumulate normally.
    // -----------------------------------------------------------------------

    [Test]
    public async Task BelowThreshold_PerPointLocksTracked()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-01");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 2);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await store.GetRow(tx, ids[0]);
        await store.GetRow(tx, ids[1]);

        // 2 reads, threshold = 3 → no escalation
        Assert.IsFalse(HasAnyWholeBucketLock(tx),
            "No whole-bucket lock before threshold is reached");
        Assert.AreEqual(2, CountPointLocks(tx),
            "Exactly one point lock per read before escalation");

        await mgr.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 2. The threshold+1th read escalates to a whole-bucket lock.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AtThreshold_EscalatesToWholeBucketLock()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-02");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 4);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await store.GetRow(tx, ids[0]);
        await store.GetRow(tx, ids[1]);
        await store.GetRow(tx, ids[2]);
        // Exactly at threshold — no escalation yet
        Assert.IsFalse(HasAnyWholeBucketLock(tx),
            "Not escalated after exactly threshold reads");

        await store.GetRow(tx, ids[3]); // triggers escalation
        Assert.IsTrue(HasAnyWholeBucketLock(tx),
            "Whole-bucket lock must appear on the escalation read");

        await mgr.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 3. After escalation: additional reads do NOT add new point locks.
    //    The whole-bucket lock covers future reads at O(1) RPCs.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AfterEscalation_NoNewPointLocksAdded()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-03");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 6);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await store.GetRow(tx, ids[0]);
        await store.GetRow(tx, ids[1]);
        await store.GetRow(tx, ids[2]);
        await store.GetRow(tx, ids[3]); // escalates
        int pointLocksAfterEscalation = CountPointLocks(tx);

        await store.GetRow(tx, ids[4]);
        await store.GetRow(tx, ids[5]);
        int pointLocksAfterMoreReads = CountPointLocks(tx);

        Assert.AreEqual(pointLocksAfterEscalation, pointLocksAfterMoreReads,
            "No new point locks added after escalation");

        await mgr.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 4. After escalation: a foreign write is blocked by the whole-bucket lock.
    //    The whole-bucket Shared lock prevents mutations from other transactions.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AfterEscalation_ForeignWriteIsBlocked()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-04");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 4);

        KvTransaction alice = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await store.GetRow(alice, ids[0]);
        await store.GetRow(alice, ids[1]);
        await store.GetRow(alice, ids[2]);
        await store.GetRow(alice, ids[3]); // escalates

        Assert.IsTrue(HasAnyWholeBucketLock(alice),
            "Alice must hold a whole-bucket lock after escalation");

        // Bob tries to insert a new row into the same bucket — must be blocked.
        KvTransaction bob = await mgr.BeginAsync();
        ObjectIdValue newId = ObjectIdGenerator.Generate();
        CamusDBException? conflict = Assert.ThrowsAsync<CamusDBException>(
            () => store.InsertRow(bob, newId, [99]));

        await mgr.RollbackAsync(bob);
        await mgr.RollbackAsync(alice);

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, conflict?.Code,
            "Bob's insert must be blocked by Alice's escalated whole-bucket lock");
    }

    // -----------------------------------------------------------------------
    // 5. After escalation: the transaction writes and commits cleanly.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AfterEscalation_CommitSucceeds()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-05");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 4);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Read all 4 rows — escalates on the 4th
        for (int i = 0; i < 4; i++)
            await store.GetRow(tx, ids[i]);

        // Write one of them
        await store.UpdateRow(tx, ids[0], [77]);

        Assert.DoesNotThrowAsync(async () => await mgr.CommitAsync(tx),
            "Commit must succeed after escalation");

        // Verify: a fresh RC read sees the updated value
        KvTransaction snap = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        ReadOnlyMemory<byte>? data = await store.GetRow(snap, ids[0]);
        Assert.AreEqual((byte)77, data!.Value.Span[0], "Committed write must be visible after escalation");
        await mgr.RollbackAsync(snap);
    }

    // -----------------------------------------------------------------------
    // 6. RC transactions: no shared point locks → escalation never fires.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_NoEscalation()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-06");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 4);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        for (int i = 0; i < 4; i++)
            await store.GetRow(tx, ids[i]);

        Assert.IsFalse(HasAnyWholeBucketLock(tx), "RC must not escalate");
        Assert.AreEqual(0, CountPointLocks(tx),    "RC must not acquire point locks");

        await mgr.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 7. The default threshold is above the row count of an ordinary page read.
    // -----------------------------------------------------------------------

    [Test]
    public void DefaultThresholdIsAboveAPageRead()
    {
        Assert.AreEqual(1000, CamusDBOptions.Default.LockEscalationThreshold,
            "A bounded read of some hundred rows must not escalate to a whole-table lock");
    }

    // -----------------------------------------------------------------------
    // 8. A batch read decides escalation once for the whole batch.
    // -----------------------------------------------------------------------

    [Test]
    public async Task BatchAtThreshold_TakesOnePointLockPerRow()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-08");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 3);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // A repeated id must not count twice toward the threshold.
        await store.GetRowsBatchForRead(tx, [ids[0], ids[1], ids[2], ids[0]]);

        Assert.IsFalse(HasAnyWholeBucketLock(tx), "A batch of exactly threshold rows must not escalate");
        Assert.AreEqual(3, CountPointLocks(tx), "One point lock per distinct row");

        // The same rows again are covered: no escalation, no new lock.
        await store.GetRowsBatchForRead(tx, [ids[2], ids[1]]);
        Assert.IsFalse(HasAnyWholeBucketLock(tx), "Rows already locked must not count toward escalation");
        Assert.AreEqual(3, CountPointLocks(tx));

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BatchPastThreshold_TakesTheWholeBucketLockAndNoPointLock()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-09");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 5);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await store.GetRowsBatchForRead(tx, [ids[0], ids[1]]);
        Assert.AreEqual(2, CountPointLocks(tx));

        // 2 held + 2 new > 3: the batch escalates before it takes a point lock of its own.
        ReadOnlyMemory<byte>?[] rows = await store.GetRowsBatchForRead(tx, [ids[2], ids[3]]);
        Assert.IsTrue(rows.All(r => r is not null), "The escalated batch still reads its rows");
        Assert.IsTrue(HasAnyWholeBucketLock(tx), "A batch that crosses the threshold escalates");
        Assert.AreEqual(2, CountPointLocks(tx), "The escalated batch adds no point lock");

        // The whole-bucket lock fences a row the transaction did not read.
        KvTransaction bob = await mgr.BeginAsync();
        Assert.ThrowsAsync<CamusDBException>(() => store.UpdateRow(bob, ids[4], [42]));
        await mgr.RollbackAsync(bob);

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BatchBelowThreshold_ForeignWriteToAReadRowIsBlocked_OtherRowIsNot()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("ESC-10", CamusDBOptions.Default with { LockEscalationThreshold = 64 });
        await using EmbeddedKahuna _ = node;

        ObjectIdValue[] ids = await InsertRowsAsync(mgr, store, 41);

        KvTransaction alice = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // 40 rows: more than the concurrency of the batch acquire, so every lock must still land.
        await store.GetRowsBatchForRead(alice, ids[..40]);
        Assert.AreEqual(40, CountPointLocks(alice));
        Assert.IsFalse(HasAnyWholeBucketLock(alice));

        KvTransaction bob = await mgr.BeginAsync();
        Assert.ThrowsAsync<CamusDBException>(() => store.UpdateRow(bob, ids[39], [42]),
            "A row of the batch is locked");
        await mgr.RollbackAsync(bob);

        KvTransaction carol = await mgr.BeginAsync();
        await store.UpdateRow(carol, ids[40], [42]);
        Assert.DoesNotThrowAsync(async () => await mgr.CommitAsync(carol),
            "A row outside the batch is not locked");

        await mgr.RollbackAsync(alice);
    }
}
