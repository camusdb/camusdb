/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The storage calls that foreign-key enforcement is built on: the rendezvous lock
/// (<c>AcquireForeignKeyLocksAsync</c>, through <c>LockAndLookupUniqueManyAsync</c>), the batched unique
/// lookup, and the child-index prefix probe. Each test drives a real embedded Kahuna node with two or
/// more transactions, because the property under test is how they interleave.
///
/// <para>The parent side of every scenario deletes a unique index entry directly. That is the exact
/// write a parent DELETE, or an UPDATE of a referenced key, issues on the key the child locks.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyStoragePrimitives
{
    private const string Db = "fkdb";
    private const string ParentIndex = "parent_uk";
    private const string ChildIndex = "child_fk";

    private static readonly ColumnType[] OneInt = [ColumnType.Integer64];

    private static CompositeColumnValue Key(long value) => new(new ColumnValue(ColumnType.Integer64, value));

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr)> StartAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        // The minter is what lets an explicit transaction defer its session start, as the server's
        // explicit-transaction path does; without it every transaction starts eagerly.
        return (node, new KvTransactionsManager(node.Kahuna, CamusDBOptions.Default,
            _ => node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId())));
    }

    private static KvTableStore Store(EmbeddedKahuna node, string tableId, string dbId = Db) =>
        new(node.Kahuna, CamusDBOptions.Default, dbId, tableId);

    /// <summary>Commits one parent row whose unique index entry is <paramref name="key"/>.</summary>
    private static async Task<ObjectIdValue> CommitParentAsync(KvTransactionsManager mgr, KvTableStore parent, long key)
    {
        ObjectIdValue rowId = ObjectIdGenerator.Generate();
        KvTransaction tx = await mgr.BeginAsync();
        await parent.InsertRow(tx, rowId, [1]);
        await parent.PutIndexEntry(tx, ParentIndex, Key(key), rowId, unique: true);
        await mgr.CommitAsync(tx);
        return rowId;
    }

    // ── The rendezvous: a child's lock fences a parent's delete ────────────

    [TestCase(KeyValueTransactionLocking.Pessimistic, KeyValueTransactionLocking.Pessimistic)]
    [TestCase(KeyValueTransactionLocking.Optimistic, KeyValueTransactionLocking.Pessimistic)]
    [TestCase(KeyValueTransactionLocking.Pessimistic, KeyValueTransactionLocking.Optimistic)]
    public async Task ChildLockFencesParentDeleteUntilTheChildCommits(KeyValueTransactionLocking childLocking, KeyValueTransactionLocking parentLocking)
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-01");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        ObjectIdValue parentRow = await CommitParentAsync(mgr, parent, 7);

        KvTransaction child = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, locking: childLocking);
        bool[] found = await parent.LockAndLookupUniqueManyAsync(child, ParentIndex, [Key(7)]);
        Assert.AreEqual(new[] { true }, found);

        KvTransaction deleter = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, locking: parentLocking);
        Task delete = parent.DeleteIndexEntry(deleter, ParentIndex, Key(7), parentRow, unique: true);

        await Task.Delay(150);
        Assert.IsFalse(delete.IsCompleted, "The parent's write of the locked key must wait for the child");

        // A pessimistic child commits and releases the lock. An optimistic child can instead be aborted
        // at commit: the waiting parent holds the key's exclusive lock, and the child's read of that key
        // no longer validates. Either way the parent's write went through only after the child finished,
        // so no child row could outlive its parent.
        try
        {
            await mgr.CommitAsync(child);
        }
        catch (CamusDBException ex) when (childLocking == KeyValueTransactionLocking.Optimistic)
        {
            Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex.Code, ex.Message);
        }

        await delete;
        await mgr.CommitAsync(deleter);
    }

    [Test]
    public async Task ParentDeleteFailsWhenTheChildOutlivesTheLockWaitDeadline()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-02");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        ObjectIdValue parentRow = await CommitParentAsync(mgr, parent, 7);

        KvTransaction child = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        await parent.LockAndLookupUniqueManyAsync(child, ParentIndex, [Key(7)]);

        KvTransaction deleter = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            () => parent.DeleteIndexEntry(deleter, ParentIndex, Key(7), parentRow, unique: true))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, exception.Code);

        await mgr.RollbackAsync(deleter);
        await mgr.CommitAsync(child);
    }

    /// <summary>The other order: a parent write is pending, and the child arrives. It is never granted the lock.</summary>
    [Test]
    public async Task ChildLockIsNeverGrantedOverAPendingParentDelete()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-03");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        ObjectIdValue parentRow = await CommitParentAsync(mgr, parent, 7);

        KvTransaction deleter = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        await parent.DeleteIndexEntry(deleter, ParentIndex, Key(7), parentRow, unique: true);

        KvTransaction child = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            () => parent.LockAndLookupUniqueManyAsync(child, ParentIndex, [Key(7)]))!;

        Assert.That(exception.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry).Or.EqualTo(CamusDBErrorCodes.TransactionConflict));

        await mgr.RollbackAsync(child);
        await mgr.RollbackAsync(deleter);
    }

    [Test]
    public async Task ChildWaitingOnAParentDeleteSeesTheParentGoneOnceItCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-04");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        ObjectIdValue parentRow = await CommitParentAsync(mgr, parent, 7);

        KvTransaction deleter = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        await parent.DeleteIndexEntry(deleter, ParentIndex, Key(7), parentRow, unique: true);

        // A child that holds nothing yet may wait out the lock-wait deadline for the holder.
        KvTransaction child = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        Task<bool[]> lookup = parent.LockAndLookupUniqueManyAsync(child, ParentIndex, [Key(7)]);

        await Task.Delay(100);
        await mgr.CommitAsync(deleter);

        Assert.AreEqual(new[] { false }, await lookup, "After the parent's delete commits, the child must see the key gone");
        await mgr.RollbackAsync(child);
    }

    /// <summary>
    /// A transaction that wrote the parent itself — an INSERT of the parent and then of the child in one
    /// transaction — already holds the key; the lock must not conflict with its own write.
    /// </summary>
    [TestCase(KeyValueTransactionLocking.Pessimistic)]
    [TestCase(KeyValueTransactionLocking.Optimistic)]
    public async Task LockOnAKeyTheTransactionWroteItselfSucceeds(KeyValueTransactionLocking locking)
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-05");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, locking: locking);
        ObjectIdValue rowId = ObjectIdGenerator.Generate();
        await parent.InsertRow(tx, rowId, [1]);
        await parent.PutIndexEntry(tx, ParentIndex, Key(9), rowId, unique: true);

        Assert.AreEqual(new[] { true }, await parent.LockAndLookupUniqueManyAsync(tx, ParentIndex, [Key(9)]));
        await mgr.CommitAsync(tx);
    }

    // ── Lock bookkeeping ────────────────────────────────────────────────────

    [Test]
    public async Task RendezvousLocksNeverEscalateToTheWholeBucket()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-06");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable);
        List<CompositeColumnValue> keys = [.. Enumerable.Range(0, 60).Select(i => Key(i))];

        await parent.LockAndLookupUniqueManyAsync(tx, ParentIndex, keys);

        IReadOnlyList<RangeLockBounds> held = tx.GetAcquiredRangeLocks();
        Assert.AreEqual(60, held.Count(b => b.StartKey is not null && b.StartKey == b.EndKey), "one point lock per key");
        Assert.IsFalse(held.Any(b => b.StartKey is null && b.EndKey is null), "no whole-bucket lock");

        await mgr.CommitAsync(tx);
    }

    [Test]
    public async Task RepeatedAndAlreadyHeldKeysCostNoLockRoundTrip()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-07");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        await CommitParentAsync(mgr, parent, 1);

        using ForeignKeyOperationCounter counters = ForeignKeyOperationCounter.Start();

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        await parent.LockAndLookupUniqueManyAsync(tx, ParentIndex, [Key(1), Key(1), Key(2), Key(1)]);
        await parent.LockAndLookupUniqueManyAsync(tx, ParentIndex, [Key(2), Key(1)]);
        await mgr.CommitAsync(tx);

        Assert.AreEqual(2, counters.Count("lock_acquired"), "two distinct keys, each locked once");
        Assert.AreEqual(2, counters.Count("lock_covered"), "the second call finds both keys held");
        Assert.AreEqual(2, counters.Count("child_probe_batch"), "one batched read per call");
        Assert.AreEqual(6, counters.Count("child_probe_key"));
    }

    /// <summary>
    /// The session is anchored on the first table a transaction touches, and a one-phase commit needs
    /// every written key on the anchor's partition. A rendezvous lock taken after the child's write must
    /// therefore leave the anchor on the child and add nothing to the written keys; taken first, it
    /// moves the anchor to the parent, which is why enforcement runs after the statement's writes.
    /// </summary>
    [Test]
    public async Task LockAfterTheChildWriteLeavesTheSessionAnchoredOnTheChild()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-08");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        KvTableStore child = Store(node, "c");
        await CommitParentAsync(mgr, parent, 1);

        KvTransaction afterWrite = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, deferStart: true);
        await child.InsertRow(afterWrite, ObjectIdGenerator.Generate(), [1]);
        await parent.LockAndLookupUniqueManyAsync(afterWrite, ParentIndex, [Key(1)]);

        Assert.That(afterWrite.CoordinatorKey, Does.StartWith($"{Db}:c|"));
        Assert.IsTrue(afterWrite.GetModifiedKeyPairs().All(k => k.key.StartsWith($"{Db}:c|", StringComparison.Ordinal)),
            "a rendezvous lock is not a write");
        await mgr.CommitAsync(afterWrite);

        KvTransaction lockFirst = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, deferStart: true);
        await parent.LockAndLookupUniqueManyAsync(lockFirst, ParentIndex, [Key(1)]);
        Assert.That(lockFirst.CoordinatorKey, Does.StartWith($"{Db}:p|"), "a lock taken first anchors the session on the parent");
        await mgr.RollbackAsync(lockFirst);
    }

    // ── The batched lookup ──────────────────────────────────────────────────

    [Test]
    public async Task LookupSeesTheTransactionsOwnUncommittedParent()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-09");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        await CommitParentAsync(mgr, parent, 1);

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        ObjectIdValue rowId = ObjectIdGenerator.Generate();
        await parent.InsertRow(tx, rowId, [1]);
        await parent.PutIndexEntry(tx, ParentIndex, Key(2), rowId, unique: true);

        Assert.AreEqual(new[] { true, true, false }, await parent.LookupUniqueManyAsync(tx, ParentIndex, [Key(1), Key(2), Key(3)]));
        await mgr.RollbackAsync(tx);
    }

    /// <summary>
    /// The classic orphan: a Serializable transaction that read nothing about the parent must still see
    /// a parent committed after it began, because a read-write transaction reads the newest state.
    /// </summary>
    [Test]
    public async Task SerializableLookupSeesAParentCommittedAfterItBegan()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-10");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        KvTableStore other = Store(node, "o");

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable);
        await other.InsertRow(tx, ObjectIdGenerator.Generate(), [1]);

        await CommitParentAsync(mgr, parent, 5);

        Assert.AreEqual(new[] { true }, await parent.LockAndLookupUniqueManyAsync(tx, ParentIndex, [Key(5)]));
        await mgr.CommitAsync(tx);
    }

    /// <summary>
    /// A key read without the lock, then deleted by another transaction, must never be reported as
    /// present by the locked read that follows. Read Committed does not pin the first read, so the locked
    /// read answers with the newest state; a pinned read would fail the transaction instead. Both are
    /// safe. A stale "present" would let a child reference a parent that is gone.
    /// </summary>
    [Test]
    public async Task KeyReadBeforeTheLockAndDeletedSinceIsNeverReportedPresent()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-11");
        await using EmbeddedKahuna _ = node;
        KvTableStore parent = Store(node, "p");
        ObjectIdValue parentRow = await CommitParentAsync(mgr, parent, 7);

        KvTransaction child = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        Assert.AreEqual(new[] { true }, await parent.LookupUniqueManyAsync(child, ParentIndex, [Key(7)]));

        KvTransaction deleter = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        await parent.DeleteIndexEntry(deleter, ParentIndex, Key(7), parentRow, unique: true);
        await mgr.CommitAsync(deleter);

        try
        {
            Assert.AreEqual(new[] { false }, await parent.LockAndLookupUniqueManyAsync(child, ParentIndex, [Key(7)]),
                "the locked read must see the parent gone");
        }
        catch (CamusDBException ex)
        {
            Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code, ex.Message);
        }

        await mgr.RollbackAsync(child);
    }

    // ── The prefix probe ────────────────────────────────────────────────────

    [Test]
    public async Task ProbeFindsCommittedChildrenAndOnlyThoseWithTheKey()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-12");
        await using EmbeddedKahuna _ = node;
        KvTableStore child = Store(node, "c");

        KvTransaction writer = await mgr.BeginAsync();
        await child.PutIndexEntry(writer, ChildIndex, Key(7), ObjectIdGenerator.Generate(), unique: false);
        await mgr.CommitAsync(writer);

        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        Assert.IsTrue(await child.IndexPrefixExistsAsync(reader, ChildIndex, OneInt, Key(7), unique: false));
        Assert.IsFalse(await child.IndexPrefixExistsAsync(reader, ChildIndex, OneInt, Key(8), unique: false));
        Assert.IsFalse(await child.IndexPrefixExistsAsync(reader, ChildIndex, OneInt, Key(6), unique: false));
        await mgr.RollbackAsync(reader);
    }

    /// <summary>
    /// A composite backing index probed by its leading column, with string keys where one value is a
    /// string prefix of another. The raw range can over-read "abc" for the prefix "ab"; the decoded
    /// filter must reject it.
    /// </summary>
    [Test]
    public async Task ProbeMatchesTheDecodedPrefixNotARawStringPrefix()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-13");
        await using EmbeddedKahuna _ = node;
        KvTableStore child = Store(node, "c");
        ColumnType[] types = [ColumnType.String, ColumnType.Integer64];

        KvTransaction writer = await mgr.BeginAsync();
        await child.PutIndexEntry(writer, ChildIndex,
            new CompositeColumnValue([new ColumnValue(ColumnType.String, "abc"), new ColumnValue(ColumnType.Integer64, 1L)]),
            ObjectIdGenerator.Generate(), unique: false);
        await mgr.CommitAsync(writer);

        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        Assert.IsTrue(await child.IndexPrefixExistsAsync(reader, ChildIndex, types, new(new ColumnValue(ColumnType.String, "abc")), unique: false));
        Assert.IsFalse(await child.IndexPrefixExistsAsync(reader, ChildIndex, types, new(new ColumnValue(ColumnType.String, "ab")), unique: false));
        await mgr.RollbackAsync(reader);
    }

    /// <summary>
    /// More children than one probe page, deleted by the probing transaction. The probe must page past
    /// its own deletes, and see "none left" only when every child is gone.
    /// </summary>
    [Test]
    public async Task ProbePagesPastTheTransactionsOwnDeletes()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-14");
        await using EmbeddedKahuna _ = node;
        KvTableStore child = Store(node, "c");
        List<ObjectIdValue> rows = [.. Enumerable.Range(0, 40).Select(_ => ObjectIdGenerator.Generate())];

        KvTransaction writer = await mgr.BeginAsync();
        foreach (ObjectIdValue row in rows)
            await child.PutIndexEntry(writer, ChildIndex, Key(5), row, unique: false);
        await mgr.CommitAsync(writer);

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        foreach (ObjectIdValue row in rows.Take(39))
            await child.DeleteIndexEntry(tx, ChildIndex, Key(5), row, unique: false);

        Assert.IsTrue(await child.IndexPrefixExistsAsync(tx, ChildIndex, OneInt, Key(5), unique: false), "one child is left");

        await child.DeleteIndexEntry(tx, ChildIndex, Key(5), rows[39], unique: false);
        Assert.IsFalse(await child.IndexPrefixExistsAsync(tx, ChildIndex, OneInt, Key(5), unique: false), "the transaction deleted every child");

        await mgr.RollbackAsync(tx);
    }

    // ── Branches ────────────────────────────────────────────────────────────

    [Test]
    public async Task OnABranchProbeAndLookupMergeTheAncestryAndHonourTombstones()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await StartAsync("FK-15");
        await using EmbeddedKahuna _ = node;

        KvTableStore ancestorParent = Store(node, "p", dbId: "anc");
        KvTableStore ancestorChild = Store(node, "c", dbId: "anc");

        ObjectIdValue parentRow = await CommitParentAsync(mgr, ancestorParent, 1);
        ObjectIdValue childRow = ObjectIdGenerator.Generate();
        KvTransaction seed = await mgr.BeginAsync();
        await ancestorChild.PutIndexEntry(seed, ChildIndex, Key(1), childRow, unique: false);
        await mgr.CommitAsync(seed);

        await Task.Delay(60);
        HLCTimestamp forkTimestamp = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        await Task.Delay(60);

        // Written to the ancestor after the fork: the branch must never see it.
        await CommitParentAsync(mgr, ancestorParent, 2);

        KvTableStore branchParent = new(node.Kahuna, CamusDBOptions.Default, "br", "p", ancestorStores: [(ancestorParent, forkTimestamp)]);
        KvTableStore branchChild = new(node.Kahuna, CamusDBOptions.Default, "br", "c", ancestorStores: [(ancestorChild, forkTimestamp)]);

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        Assert.AreEqual(new[] { true, false, false }, await branchParent.LockAndLookupUniqueManyAsync(tx, ParentIndex, [Key(1), Key(2), Key(3)]),
            "inherited parent found; post-fork and missing parents not");
        Assert.IsTrue(await branchChild.IndexPrefixExistsAsync(tx, ChildIndex, OneInt, Key(1), unique: false), "inherited child found");

        await branchChild.DeleteIndexEntry(tx, ChildIndex, Key(1), childRow, unique: false);
        await branchParent.DeleteIndexEntry(tx, ParentIndex, Key(1), parentRow, unique: true);

        Assert.IsFalse(await branchChild.IndexPrefixExistsAsync(tx, ChildIndex, OneInt, Key(1), unique: false), "a branch tombstone hides the inherited child");
        Assert.AreEqual(new[] { false }, await branchParent.LookupUniqueManyAsync(tx, ParentIndex, [Key(1)]), "a branch tombstone hides the inherited parent");
        await mgr.CommitAsync(tx);

        KvTransaction after = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted);
        Assert.IsFalse(await branchChild.IndexPrefixExistsAsync(after, ChildIndex, OneInt, Key(1), unique: false));
        Assert.IsTrue(await ancestorChild.IndexPrefixExistsAsync(after, ChildIndex, OneInt, Key(1), unique: false),
            "the branch's delete never touches the ancestor");
        await mgr.RollbackAsync(after);
    }
}
