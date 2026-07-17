
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
/// Verifies that a Serializable read-only transaction pins every read — point reads,
/// unique-index lookups, row scans, and index scans — to the single HLC timestamp
/// minted at <see cref="KvTransactionsManager.BeginAsync"/> time, so the transaction
/// observes a consistent point-in-time snapshot:
///
///   - Non-repeatable read: a second read of the same key after a committed write
///     must return the original value, not the new one.
///   - Phantom: a row inserted and committed after the snapshot timestamp must not
///     appear in a re-scan.
///   - Read Committed path: unchanged — reads always return the latest committed value.
/// </summary>
[TestFixture]
public sealed class TestSnapshotRead
{
    private static readonly string IndexName = "pk_idx";
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

    private static async Task<KvTransaction> WriteRow(
        KvTransactionsManager mgr,
        KvTableStore store,
        ObjectIdValue rowId,
        byte[] data)
    {
        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);
        await mgr.CommitAsync(tx);
        return tx;
    }

    private static async Task OverwriteRow(
        KvTransactionsManager mgr,
        KvTableStore store,
        ObjectIdValue rowId,
        byte[] newData)
    {
        KvTransaction tx = await mgr.BeginAsync();
        await store.UpdateRow(tx, rowId, newData);
        await mgr.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 1. Point-read (GetRow): snapshot is not disrupted by a subsequent write
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_GetRow_IgnoresWriteAfterSnapshot()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SR-01");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] original = [1, 2, 3];
        byte[] updated  = [9, 8, 7];

        await WriteRow(mgr, store, rowId, original);

        // Open snapshot AFTER initial write so original is visible.
        KvTransaction roTx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Another writer commits a new version AFTER the snapshot was taken.
        await OverwriteRow(mgr, store, rowId, updated);

        // Snapshot should still see the original bytes.
        ReadOnlyMemory<byte>? seen = await store.GetRow(roTx, rowId);
        Assert.IsNotNull(seen);
        Assert.AreEqual(original, seen!.Value.ToArray(),
            "A Serializable+ReadOnly snapshot must not observe a write committed after the snapshot timestamp");
    }

    // -----------------------------------------------------------------------
    // 2. Row scan (ScanRows): no phantom from a write after the snapshot
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_ScanRows_DoesNotSeePhantomInsert()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SR-02");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue existing = new(1, 0, 0);
        ObjectIdValue phantom  = new(2, 0, 0);
        byte[] data = [42];

        await WriteRow(mgr, store, existing, data);

        KvTransaction roTx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Phantom insert committed after snapshot.
        await WriteRow(mgr, store, phantom, data);

        List<ObjectIdValue> scanned = [];
        await foreach ((ObjectIdValue rowId, _) in store.ScanRows(roTx))
            scanned.Add(rowId);

        Assert.IsTrue(scanned.Contains(existing),
            "The pre-snapshot row must be visible");
        Assert.IsFalse(scanned.Contains(phantom),
            "A row inserted after the snapshot timestamp must not appear in a scan");
    }

    // -----------------------------------------------------------------------
    // 3. Non-repeatable read: same ScanRows call twice, with a write in between
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_ScanRows_StableAcrossRepeatedScans()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SR-03");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(10, 0, 0);
        byte[] v1 = [1];
        byte[] v2 = [2];

        await WriteRow(mgr, store, rowId, v1);

        KvTransaction roTx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // First scan under snapshot.
        List<byte[]> first = [];
        await foreach ((_, ReadOnlyMemory<byte> d) in store.ScanRows(roTx))
            first.Add(d.ToArray());

        // Concurrent update committed after snapshot.
        await OverwriteRow(mgr, store, rowId, v2);

        // Second scan under same snapshot.
        List<byte[]> second = [];
        await foreach ((_, ReadOnlyMemory<byte> d) in store.ScanRows(roTx))
            second.Add(d.ToArray());

        Assert.AreEqual(first.Count, second.Count,
            "Row count must be identical across repeated scans within the same snapshot");
        Assert.AreEqual(first[0], second[0],
            "Row bytes must be identical across repeated scans within the same snapshot — no non-repeatable read");
    }

    // -----------------------------------------------------------------------
    // 4. Read Committed default: always sees latest committed value
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_GetRow_SeesLatestCommit()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SR-04");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        byte[] v1 = [10];
        byte[] v2 = [20];

        await WriteRow(mgr, store, rowId, v1);
        await OverwriteRow(mgr, store, rowId, v2);

        // Default BeginAsync = ReadCommitted, reads at Zero (latest committed).
        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);

        Assert.IsNotNull(got);
        Assert.AreEqual(v2, got!.Value.ToArray(), "Read Committed must always return the latest committed value");
    }

    // -----------------------------------------------------------------------
    // 5. Unique-index lookup: snapshot pinning works through LookupUnique
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_LookupUnique_PinnedToSnapshot()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SR-05");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId1 = new(1, 0, 0);
        ObjectIdValue rowId2 = new(2, 0, 0);

        CompositeColumnValue key1 = new(new ColumnValue(ColumnType.Integer64, 100L));
        CompositeColumnValue key2 = new(new ColumnValue(ColumnType.Integer64, 200L));

        // Write first entry before snapshot.
        KvTransaction writeTx = await mgr.BeginAsync();
        await store.PutIndexEntry(writeTx, IndexName, key1, rowId1, unique: true);
        await mgr.CommitAsync(writeTx);

        // Take snapshot.
        KvTransaction roTx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Write second entry AFTER snapshot.
        KvTransaction writeTx2 = await mgr.BeginAsync();
        await store.PutIndexEntry(writeTx2, IndexName, key2, rowId2, unique: true);
        await mgr.CommitAsync(writeTx2);

        // key1 must be visible (committed before snapshot).
        ObjectIdValue? found1 = await store.LookupUnique(roTx, IndexName, key1);
        Assert.AreEqual(rowId1, found1, "key1 committed before snapshot must be visible");

        // key2 must be invisible (committed after snapshot).
        ObjectIdValue? found2 = await store.LookupUnique(roTx, IndexName, key2);
        Assert.IsNull(found2, "key2 committed after snapshot timestamp must not be visible");
    }

    // -----------------------------------------------------------------------
    // 6. Index scan: no phantom entries from a write after the snapshot
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_ScanIndex_DoesNotSeePhantomEntry()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SR-06");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId1 = new(1, 0, 0);
        ObjectIdValue rowId2 = new(2, 0, 0);

        CompositeColumnValue key1 = new(new ColumnValue(ColumnType.Integer64, 100L));
        CompositeColumnValue key2 = new(new ColumnValue(ColumnType.Integer64, 200L));

        // Pre-snapshot write.
        KvTransaction tx1 = await mgr.BeginAsync();
        await store.PutIndexEntry(tx1, IndexName, key1, rowId1, unique: true);
        await mgr.CommitAsync(tx1);

        // Snapshot.
        KvTransaction roTx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Post-snapshot write.
        KvTransaction tx2 = await mgr.BeginAsync();
        await store.PutIndexEntry(tx2, IndexName, key2, rowId2, unique: true);
        await mgr.CommitAsync(tx2);

        List<ObjectIdValue> scanned = [];
        await foreach ((_, ObjectIdValue rowId) in store.ScanIndex(
            roTx, IndexName, KeyTypes, null, null, unique: true))
        {
            scanned.Add(rowId);
        }

        Assert.IsTrue(scanned.Contains(rowId1),
            "Index entry committed before snapshot must be visible");
        Assert.IsFalse(scanned.Contains(rowId2),
            "Index entry committed after snapshot timestamp must not be visible");
    }
}
