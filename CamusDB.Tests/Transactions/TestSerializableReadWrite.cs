
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

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Verifies strict two-phase locking (S2PL) for Serializable read-write transactions.
///
/// A Serializable+ReadWrite transaction acquires a shared point lock on every key it reads
/// (GetRow, LookupUnique) and holds that lock until commit/rollback. Kahuna enforces the
/// constraint at the writer's prepare/commit time: if a key covered by a foreign shared
/// range lock is about to be committed, prepare returns MustRetry, and CommitAsync eventually
/// throws TransactionMustRetry.
///
/// Guarantees verified here:
///   - S2PL read blocks concurrent writer commit (GetRow path).
///   - S2PL read blocks concurrent writer commit (LookupUnique path).
///   - Two Serializable+RW readers on the same key coexist (S∩S compatible).
///   - ReadCommitted GetRow takes no shared lock — writer commits freely.
///   - Serializable+ReadOnly GetRow takes no lock (Zero-identity guard) — writer commits freely.
///   - Shared lock is released on commit — a subsequent writer succeeds.
/// </summary>
[TestFixture]
public sealed class TestSerializableReadWrite
{
    private static readonly string IndexName = "srw_idx";
    private static readonly ColumnType[] KeyTypes = [ColumnType.Integer64];

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        KvTransactionsManager mgr = new(node.Kahuna);
        KvTableStore store = new(node.Kahuna, "testdb", tag);
        return (node, mgr, store);
    }

    private static async Task InsertInitial(
        KvTransactionsManager mgr,
        KvTableStore store,
        ObjectIdValue rowId,
        byte[] data)
    {
        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);
        await mgr.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 1. GetRow shared lock blocks a concurrent writer at commit time
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_GetRow_BlocksWriterCommit()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-01");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];

        await InsertInitial(mgr, store, rowId, v1);

        // Reader: acquires shared point lock on rowId.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        ReadOnlyMemory<byte>? seen = await store.GetRow(reader, rowId);
        Assert.IsNotNull(seen);

        // Writer: the set operation is blocked immediately by Kahuna's range lock fence in
        // TrySetHandler — KeyCoveredByForeignRangeLock returns MustRetry at write time, not
        // only at prepare/commit. After exhausting retries, WriteRow throws TransactionMustRetry.
        KvTransaction writer = await mgr.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.UpdateRow(writer, rowId, v2));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "UpdateRow on a key covered by a foreign shared range lock must fail with TransactionMustRetry");

        await mgr.RollbackAsync(writer);
        await mgr.CommitAsync(reader);
    }

    // -----------------------------------------------------------------------
    // 2. LookupUnique shared lock blocks a concurrent writer at commit time
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_LookupUnique_BlocksWriterCommit()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-02");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];

        // Insert a unique index entry pointing to rowId.
        KvTransaction setup = await mgr.BeginAsync();
        await store.InsertRow(setup, rowId, v1);
        CompositeColumnValue indexKey = new(new ColumnValue(ColumnType.Integer64, 42L));
        await store.PutIndexEntry(setup, IndexName, indexKey, rowId, unique: true);
        await mgr.CommitAsync(setup);

        // Reader: Serializable+RW LookupUnique acquires shared point lock on index key.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        ObjectIdValue? found = await store.LookupUnique(reader, IndexName, indexKey);
        Assert.IsNotNull(found);

        // Writer: the delete of the index entry is blocked immediately by Kahuna's range lock
        // fence in TryDeleteHandler — same MustRetry mechanism as TrySetHandler.
        ObjectIdValue rowId2 = new(2, 0, 0);
        KvTransaction writer = await mgr.BeginAsync();
        await store.InsertRow(writer, rowId2, [2]);

        CamusDBException? ex2 = Assert.ThrowsAsync<CamusDBException>(
            () => store.DeleteIndexEntry(writer, IndexName, indexKey, rowId, unique: true));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex2?.Code,
            "DeleteIndexEntry on a key covered by a foreign shared range lock must fail with TransactionMustRetry");

        await mgr.RollbackAsync(writer);
        await mgr.CommitAsync(reader);
    }

    // -----------------------------------------------------------------------
    // 3. Two Serializable+RW readers on the same key coexist (S∩S)
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_TwoReadersOnSameKey_Coexist()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-03");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        await InsertInitial(mgr, store, rowId, [99]);

        KvTransaction r1 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction r2 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Both readers acquire shared point locks on the same key — neither should throw.
        ReadOnlyMemory<byte>? seen1 = await store.GetRow(r1, rowId);
        ReadOnlyMemory<byte>? seen2 = await store.GetRow(r2, rowId);

        Assert.IsNotNull(seen1, "First Serializable+RW reader must see the row");
        Assert.IsNotNull(seen2, "Second Serializable+RW reader must see the row (S∩S compatible)");

        await mgr.CommitAsync(r1);
        await mgr.CommitAsync(r2);
    }

    // -----------------------------------------------------------------------
    // 4. ReadCommitted GetRow takes NO shared lock — writer commits freely
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_GetRow_TakesNoSharedLock()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-04");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];
        await InsertInitial(mgr, store, rowId, v1);

        // Reader at ReadCommitted (default): no shared lock acquired.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        ReadOnlyMemory<byte>? seen = await store.GetRow(reader, rowId);
        Assert.IsNotNull(seen);

        // Writer commits to the same key while reader is live — must succeed.
        KvTransaction writer = await mgr.BeginAsync();
        await store.UpdateRow(writer, rowId, v2);
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(writer),
            "A writer must not be blocked by a ReadCommitted reader (no shared lock)");

        await mgr.CommitAsync(reader);
    }

    // -----------------------------------------------------------------------
    // 5. Serializable+ReadOnly GetRow takes NO lock (Zero-identity guard)
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_GetRow_TakesNoLock()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-05");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];
        await InsertInitial(mgr, store, rowId, v1);

        // Snapshot reader: TransactionId=Zero → AcquireRangeLockAsync early-returns, no lock.
        KvTransaction snapshot = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        ReadOnlyMemory<byte>? seen = await store.GetRow(snapshot, rowId);
        Assert.IsNotNull(seen);

        // Writer commits to the same key while snapshot reader is live — must succeed.
        KvTransaction writer = await mgr.BeginAsync();
        await store.UpdateRow(writer, rowId, v2);
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(writer),
            "A writer must not be blocked by a Serializable+ReadOnly snapshot reader (no lock taken)");
    }

    // -----------------------------------------------------------------------
    // 6. Read-then-write the same key in one Serializable RW tx does not self-deadlock
    // -----------------------------------------------------------------------
    //
    // Kahuna's range lock fence skips the owning transaction's own locks
    // (rangeLock.TransactionId == txId → continue in TrySetHandler). Without this
    // guard, a regression in that skip would cause every serializable read-modify-write
    // to self-deadlock. The shared→exclusive upgrade calls AcquireRangeLock on the same
    // txn's existing lock; Kahuna's idempotency path handles re-entry correctly.

    [Test]
    public async Task SerializableRW_ReadThenWriteSameKey_CommitsWithoutSelfDeadlock()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-07");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];
        await InsertInitial(mgr, store, rowId, v1);

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Read acquires shared point lock on rowId (owned by tx).
        ReadOnlyMemory<byte>? seen = await store.GetRow(tx, rowId);
        Assert.IsNotNull(seen);

        // Write to the same key within the same tx — Kahuna skips its own range lock,
        // so this must not block or throw.
        Assert.DoesNotThrowAsync(() => store.UpdateRow(tx, rowId, v2),
            "A Serializable+RW tx must be able to write a key it has read-locked without self-deadlock");

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(tx),
            "Commit of a read-then-write Serializable+RW tx must succeed");
    }

    // -----------------------------------------------------------------------
    // 7. Shared lock is released on reader commit — subsequent writer succeeds
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_AfterReaderCommits_WriterSucceeds()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-08");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];
        await InsertInitial(mgr, store, rowId, v1);

        // Reader holds shared lock.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.GetRow(reader, rowId);

        // Release the lock.
        await mgr.CommitAsync(reader);

        // Writer now commits freely.
        KvTransaction writer = await mgr.BeginAsync();
        await store.UpdateRow(writer, rowId, v2);
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(writer),
            "Writer must succeed after the reader that held the shared lock has committed");
    }

    // -----------------------------------------------------------------------
    // 8. No window: third writer cannot write K between A's read and A's write
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_ThirdWriterCannotWriteBetweenReadAndWrite()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-09");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];
        byte[] v3 = [3];
        await InsertInitial(mgr, store, rowId, v1);

        // Tx A: read K → shared point lock.
        KvTransaction txA = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        ReadOnlyMemory<byte>? seen = await store.GetRow(txA, rowId);
        Assert.IsNotNull(seen);

        // Tx C: concurrent third writer — must be blocked by A's shared range lock.
        KvTransaction txC = await mgr.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.UpdateRow(txC, rowId, v3));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Third writer must not be able to write K while A holds a shared range lock on K");
        await mgr.RollbackAsync(txC);

        // Tx A: write K — triggers S→X upgrade, then commits.
        Assert.DoesNotThrowAsync(() => store.UpdateRow(txA, rowId, v2),
            "A must be able to write K it has read-locked (S→X upgrade must not self-block)");
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(txA),
            "A must commit successfully after the read-then-write");
    }

    // -----------------------------------------------------------------------
    // 9. Exclusive upgrade blocks new shared readers
    //    After A writes K (S→X promotion), a concurrent Serializable+RW reader
    //    cannot acquire a new shared lock on K.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_AfterUpgrade_NewSharedReaderIsBlocked()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRW-10");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];
        await InsertInitial(mgr, store, rowId, v1);

        // Tx A: read K (shared) then write K (upgrades to exclusive range lock).
        KvTransaction txA = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.GetRow(txA, rowId);         // acquires Shared [K,K]
        await store.UpdateRow(txA, rowId, v2);  // upgrades to Exclusive [K,K]

        // Tx B: Serializable+RW GetRow on the same key must fail — A holds Exclusive [K,K]
        // and Kahuna's TryAcquireRangeLock returns AlreadyLocked (X∩S incompatible).
        KvTransaction txB = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.GetRow(txB, rowId));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "A new Serializable+RW reader must not be able to acquire Shared on K while A holds Exclusive after upgrade");

        await mgr.RollbackAsync(txB);
        await mgr.CommitAsync(txA);
    }
}
