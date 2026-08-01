
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// KvTableStore primary row operations.
///
/// Each test boots a fresh embedded Kahuna node (in-memory) and exercises the
/// public surface of KvTableStore against it.
/// </summary>
[TestFixture]
public sealed class TestKvTableStore
{
    // ---- schema helpers ---------------------------------------------------

    private static TableColumnSchema Col(string name, ColumnType type) =>
        new(name, name, type, false, null);

    private static TableSchema MakeSchema(params TableColumnSchema[] columns)
    {
        List<TableColumnSchema> cols = new(columns);
        List<TableSchemaHistory> history = [new TableSchemaHistory { Version = 0, Columns = cols }];
        return new TableSchema
        {
            Id      = "test-table",
            Name    = "test",
            Version = 0,
            Columns = cols,
            SchemaHistory = history
        };
    }

    // ---- transaction helpers ----------------------------------------------

    private static async Task<KvTransaction> BeginTransaction(IKahuna kahuna, string uniqueId)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Set, type, "StartTransaction must return Set");

        return new KvTransaction(handle.TransactionId, uniqueId);
    }

    private static async Task CommitTransaction(IKahuna kahuna, KvTransaction tx)
    {
        (KeyValueResponseType result, _) = await kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);

        Assert.AreEqual(KeyValueResponseType.Committed, result, "Commit must succeed");
    }

    // ---- node factory -----------------------------------------------------

    private static async Task<(EmbeddedKahuna node, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        return (node, new KvTableStore(node.Kahuna, CamusDBConfig.Ambient, "testdb", tableId));
    }

    // ---- tests ------------------------------------------------------------

    [Test]
    public async Task GetRow_ReturnsNull_WhenKeyDoesNotExist()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t1");
        await using EmbeddedKahuna __ = node;

        ReadOnlyMemory<byte>? result = await store.GetRow(KvTransaction.CreateReadOnly(), new ObjectIdValue(1, 2, 3));

        Assert.IsNull(result);
    }

    [Test]
    public async Task InsertRow_ThenGetRow_ReturnsBytes()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t2");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("name", ColumnType.String));
        ObjectIdValue rowId = new(10, 20, 30);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["name"] = new(ColumnType.String, "alice") }, rowId);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t2-insert");
        await store.InsertRow(tx, rowId, data);
        await CommitTransaction(node.Kahuna, tx);

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);

        Assert.IsNotNull(got);
        Assert.AreEqual(data, got!.Value.ToArray());
    }

    [Test]
    public async Task UpdateRow_OverwritesExistingBytes()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t3");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("n", ColumnType.Integer64));
        ObjectIdValue rowId = new(1, 1, 1);

        byte[] v1 = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["n"] = new(ColumnType.Integer64, 1L) }, rowId);
        byte[] v2 = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["n"] = new(ColumnType.Integer64, 2L) }, rowId);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "t3-insert");
        await store.InsertRow(tx1, rowId, v1);
        await CommitTransaction(node.Kahuna, tx1);

        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "t3-update");
        await store.UpdateRow(tx2, rowId, v2);
        await CommitTransaction(node.Kahuna, tx2);

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);

        Assert.IsNotNull(got);
        Assert.AreEqual(v2, got!.Value.ToArray());
    }

    [Test]
    public async Task DeleteRow_RemovesRow()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t4");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("x", ColumnType.Bool));
        ObjectIdValue rowId = new(5, 5, 5);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["x"] = new(ColumnType.Bool, true) }, rowId);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "t4-insert");
        await store.InsertRow(tx1, rowId, data);
        await CommitTransaction(node.Kahuna, tx1);

        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "t4-delete");
        await store.DeleteRow(tx2, rowId);
        await CommitTransaction(node.Kahuna, tx2);

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNull(got);
    }

    [Test]
    public async Task ScanRows_ReturnsAllInsertedRows()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t5");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("v", ColumnType.Integer64));
        ObjectIdValue[] ids = [new(1, 0, 0), new(2, 0, 0), new(3, 0, 0)];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t5-insert");
        foreach (ObjectIdValue id in ids)
        {
            byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, (long)id.a) }, id);
            await store.InsertRow(tx, id, data);
        }
        await CommitTransaction(node.Kahuna, tx);

        List<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> scanned = [];
        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in store.ScanRows(KvTransaction.CreateReadOnly()))
            scanned.Add((rowId, data));

        Assert.AreEqual(3, scanned.Count, "Scan must return all inserted rows");

        HashSet<string> insertedIds = new(ids.Select(id => id.ToString()));
        foreach ((ObjectIdValue rowId, _) in scanned)
            Assert.IsTrue(insertedIds.Contains(rowId.ToString()), $"Unexpected rowId {rowId}");
    }

    [Test]
    public async Task ScanRows_IsEmpty_WhenNoRowsInserted()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t6");
        await using EmbeddedKahuna __ = node;

        List<(ObjectIdValue, ReadOnlyMemory<byte>)> scanned = [];
        await foreach ((ObjectIdValue id, ReadOnlyMemory<byte> data) in store.ScanRows(KvTransaction.CreateReadOnly()))
            scanned.Add((id, data));

        Assert.AreEqual(0, scanned.Count);
    }

    [Test]
    public async Task ScanRows_ReturnsRowsInAscendingRowIdOrder()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t7");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("n", ColumnType.Integer64));

        // Insert in descending order to prove scan is KV-ordered, not insertion-ordered.
        ObjectIdValue[] ids = [new(300, 0, 0), new(100, 0, 0), new(200, 0, 0)];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t7-insert");
        foreach (ObjectIdValue id in ids)
        {
            byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["n"] = new(ColumnType.Integer64, (long)id.a) }, id);
            await store.InsertRow(tx, id, data);
        }
        await CommitTransaction(node.Kahuna, tx);

        List<ObjectIdValue> scannedIds = [];
        await foreach ((ObjectIdValue rowId, _) in store.ScanRows(KvTransaction.CreateReadOnly()))
            scannedIds.Add(rowId);

        Assert.AreEqual(3, scannedIds.Count);

        for (int i = 0; i + 1 < scannedIds.Count; i++)
        {
            Assert.That(
                string.CompareOrdinal(scannedIds[i].ToString(), scannedIds[i + 1].ToString()),
                Is.LessThanOrEqualTo(0),
                $"Scan not sorted at position {i}: {scannedIds[i]} vs {scannedIds[i + 1]}"
            );

            Assert.That(
                scannedIds[i].CompareTo(scannedIds[i + 1]),
                Is.LessThanOrEqualTo(0),
                $"ObjectId comparison diverged from scan order at position {i}: {scannedIds[i]} vs {scannedIds[i + 1]}"
            );
        }
    }

    [Test]
    public async Task ScanRows_AfterRowIdUsesSameOrderAsFixedWidthObjectIdKeys()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t7-resume");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("n", ColumnType.Integer64));
        ObjectIdValue[] ids =
        [
            new(0x00000001, 0, 0),
            new(unchecked((int)0x80000000), 0, 0),
            new(unchecked((int)0xffffffff), 0, 0),
            new(unchecked((int)0xffffffff), 0x00000001, 0),
            new(unchecked((int)0xffffffff), unchecked((int)0xffffffff), 0x00000001)
        ];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t7-resume-insert");
        for (int i = ids.Length - 1; i >= 0; i--)
        {
            ObjectIdValue id = ids[i];
            byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["n"] = new(ColumnType.Integer64, 1L) }, id);
            await store.InsertRow(tx, id, data);
        }
        await CommitTransaction(node.Kahuna, tx);

        ObjectIdValue[] expected = ids.OrderBy(x => x.ToString(), StringComparer.Ordinal).ToArray();
        ObjectIdValue afterRowId = expected[1];

        List<ObjectIdValue> scannedIds = [];
        await foreach ((ObjectIdValue rowId, _) in store.ScanRows(KvTransaction.CreateReadOnly(), afterRowId: afterRowId))
            scannedIds.Add(rowId);

        Assert.AreEqual(expected.Skip(2).ToArray(), scannedIds);

        for (int i = 0; i + 1 < expected.Length; i++)
        {
            Assert.AreEqual(
                Math.Sign(string.CompareOrdinal(expected[i].ToString(), expected[i + 1].ToString())),
                Math.Sign(expected[i].CompareTo(expected[i + 1])),
                $"ObjectId string order and CompareTo order diverged for {expected[i]} vs {expected[i + 1]}"
            );
        }
    }

    [Test]
    public async Task ScanRows_RespectsMaxRows()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t7-max");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("n", ColumnType.Integer64));
        ObjectIdValue[] ids = [new(1, 0, 0), new(2, 0, 0), new(3, 0, 0)];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t7-max-insert");
        foreach (ObjectIdValue id in ids)
        {
            byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["n"] = new(ColumnType.Integer64, (long)id.a) }, id);
            await store.InsertRow(tx, id, data);
        }
        await CommitTransaction(node.Kahuna, tx);

        List<ObjectIdValue> scannedIds = [];
        await foreach ((ObjectIdValue rowId, _) in store.ScanRows(KvTransaction.CreateReadOnly(), maxRows: 2))
            scannedIds.Add(rowId);

        Assert.AreEqual(2, scannedIds.Count);
    }

    // Only the modified-key mirror is kept client-side (for cache invalidation when the frozen server
    // working set is unavailable). Point locks are owned by the coordinator and no longer mirrored here.
    [Test]
    public async Task InsertRow_TracksModifiedKey()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t8");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("k", ColumnType.Integer64));
        ObjectIdValue rowId = new(7, 8, 9);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["k"] = new(ColumnType.Integer64, 42L) }, rowId);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t8-track");
        await store.InsertRow(tx, rowId, data);

        Assert.AreEqual(1, tx.GetModifiedKeyPairs().Count, "One modified key must be tracked for cache invalidation");

        await CommitTransaction(node.Kahuna, tx);
    }

    // Shared row range lock (key-range-sharding mode): scan range locks are shared, so two
    // transactions scanning the same table row range coexist (S∩S) — phantom protection comes
    // from the write-path fence, not reader-vs-reader exclusion. The lock is released when the
    // owning transaction commits. Range locks are only active when KeyRangeShardingEnabled=true;
    // in single-partition mode the method is a no-op.
    [Test]
    [NonParallelizable]
    public async Task AcquireRowRangeLock_SharedScansCoexist_AndReleasedOnCommit()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("trangelock");
            await using EmbeddedKahuna __ = node;

            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            KvTransaction tx1 = await transactions.BeginAsync();
            await store.AcquireRowRangeLockAsync(tx1);
            Assert.AreEqual(1, tx1.GetAcquiredRangeLocks().Count, "tx1 must track its range lock");

            // A second concurrent scan over the same row range must COEXIST with tx1's shared lock.
            KvTransaction tx2 = await transactions.BeginAsync();
            Assert.DoesNotThrowAsync(async () => await store.AcquireRowRangeLockAsync(tx2),
                "two shared scan locks over the same row range must coexist");
            Assert.AreEqual(1, tx2.GetAcquiredRangeLocks().Count, "tx2 must track its own shared range lock");

            // Committing tx1 releases its range lock; tx2's is unaffected.
            await transactions.CommitAsync(tx1);
            Assert.AreEqual(1, tx2.GetAcquiredRangeLocks().Count);

            await transactions.CommitAsync(tx2);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    /// <summary>
    /// Counts the client-issued lock-release RPCs on the outer <see cref="IKahuna"/> surface. The
    /// coordinator releases a transaction's folded range/prefix locks from inside the node (via its own
    /// manager) as part of finalize, so a correct client must issue <b>zero</b> release calls on this
    /// surface — the pre-coordinator client-side release would double-release every predicate lock.
    /// </summary>
    private sealed class ReleaseCountingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public int RangeReleaseCalls;
        public int PrefixReleaseCalls;

        public override Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
            HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            Interlocked.Increment(ref RangeReleaseCalls);
            return inner.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken, coordinatorKey, operationId);
        }

        public override Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
            HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            Interlocked.Increment(ref PrefixReleaseCalls);
            return inner.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken, coordinatorKey, operationId);
        }
    }

    // A Serializable+RW transaction that acquired a range lock must NOT release it from the client on
    // commit or rollback — the coordinator owns the folded lock and releases it exactly once at finalize.
    [Test]
    [NonParallelizable]
    public async Task RangeLock_FinalizedByCoordinator_NoClientSideRelease()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            EmbeddedKahuna node = new();
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("norelease/warmup", CancellationToken.None);
            await using EmbeddedKahuna __ = node;

            ReleaseCountingKahuna counting = new(node.Kahuna);
            KvTableStore store = new(counting, CamusDBConfig.Ambient, "testdb", "norelease");
            KvTransactionsManager transactions = new(counting, CamusDBConfig.Ambient);

            // Commit path.
            KvTransaction committed = await transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            await store.AcquireRowRangeLockAsync(committed, exclusive: true);
            Assert.AreEqual(1, committed.GetAcquiredRangeLocks().Count, "the range lock must be tracked for coverage");
            await transactions.CommitAsync(committed);

            // Rollback path.
            KvTransaction rolledBack = await transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            await store.AcquireRowRangeLockAsync(rolledBack, exclusive: true);
            await transactions.RollbackAsync(rolledBack);

            Assert.AreEqual(0, counting.RangeReleaseCalls,
                "the client must not issue any range-lock release; the coordinator releases the folded lock at finalize");
            Assert.AreEqual(0, counting.PrefixReleaseCalls,
                "the client must not issue any prefix-lock release");
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // "Optimistic" only skips the exclusive WRITE lock. Under Serializable, reads still take shared
    // predicate locks (gated on isolation, not on locking), so Serializable+Optimistic is a HYBRID — it
    // is not lock-free. Read Committed+Optimistic takes no predicate lock and is fully lock-free. This
    // pins that contract at the mechanism level (predicate-lock tracking), the honest counterpart to the
    // lock-free optimistic tests that run under Read Committed.
    [Test]
    [NonParallelizable]
    public async Task SerializableOptimistic_StillTakesReadPredicateLock_Hybrid()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("hybridopt");
            await using EmbeddedKahuna __ = node;

            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            ObjectIdValue rowId = new(9, 9, 9);
            KvTransaction seed = await transactions.BeginAsync();
            await store.InsertRow(seed, rowId, [1]);
            await transactions.CommitAsync(seed);

            // Serializable + Optimistic: the write lock is skipped, but the READ still takes a shared
            // predicate lock — the hybrid.
            KvTransaction hybrid = await transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite,
                locking: KeyValueTransactionLocking.Optimistic);
            Assert.That(hybrid.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic), "the tx must be optimistic");
            await store.GetRow(hybrid, rowId);
            Assert.Greater(hybrid.GetAcquiredRangeLocks().Count, 0,
                "Serializable+Optimistic must still take a shared read predicate lock (hybrid, not lock-free)");
            await transactions.RollbackAsync(hybrid);

            // Read Committed + Optimistic: fully lock-free — no predicate lock on the read.
            KvTransaction lockFree = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                locking: KeyValueTransactionLocking.Optimistic);
            await store.GetRow(lockFree, rowId);
            Assert.AreEqual(0, lockFree.GetAcquiredRangeLocks().Count,
                "ReadCommitted+Optimistic must take no read predicate lock (fully lock-free)");
            await transactions.RollbackAsync(lockFree);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // ---- GetRowsBatch tests -----------------------------------------------

    [Test]
    public async Task GetRowsBatch_EmptyList_ReturnsEmptyArray()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("gbatch-empty");
        await using EmbeddedKahuna __ = node;

        ReadOnlyMemory<byte>?[] result = await store.GetRowsBatch(KvTransaction.CreateReadOnly(), []);

        Assert.AreEqual(0, result.Length);
    }

    [Test]
    public async Task GetRowsBatch_ReturnsNullForMissingIds()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("gbatch-missing");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue[] ids = [new(1, 0, 0), new(2, 0, 0), new(3, 0, 0)];
        ReadOnlyMemory<byte>?[] result = await store.GetRowsBatch(KvTransaction.CreateReadOnly(), ids);

        Assert.AreEqual(3, result.Length);
        Assert.IsNull(result[0]);
        Assert.IsNull(result[1]);
        Assert.IsNull(result[2]);
    }

    [Test]
    public async Task GetRowsBatch_ReturnsSameBytesAsGetRow()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("gbatch-parity");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("name", ColumnType.String));
        ObjectIdValue[] ids = [new(10, 0, 0), new(20, 0, 0), new(30, 0, 0)];
        byte[][] inserted = new byte[ids.Length][];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "gbatch-parity-insert");
        for (int i = 0; i < ids.Length; i++)
        {
            inserted[i] = RowEncoder.Encode(schema,
                new Dictionary<string, ColumnValue> { ["name"] = new(ColumnType.String, $"row{i}") },
                ids[i]);
            await store.InsertRow(tx, ids[i], inserted[i]);
        }
        await CommitTransaction(node.Kahuna, tx);

        ReadOnlyMemory<byte>?[] batchResult = await store.GetRowsBatch(KvTransaction.CreateReadOnly(), ids);

        Assert.AreEqual(ids.Length, batchResult.Length, "batch result count must match id count");
        for (int i = 0; i < ids.Length; i++)
        {
            ReadOnlyMemory<byte>? single = await store.GetRow(KvTransaction.CreateReadOnly(), ids[i]);
            Assert.IsNotNull(batchResult[i], $"batch result[{i}] must not be null");
            Assert.AreEqual(single!.Value.ToArray(), batchResult[i]!.Value.ToArray(), $"batch result[{i}] must equal single GetRow result");
        }
    }

    // GetRowsBatch must return results indexed to the *caller's* input order regardless of the
    // order Kahuna returns them (which is leader-group order in a multi-node cluster).
    // EmbeddedKahuna is single-node so all keys go to one leader, making positional and
    // key-matched paths indistinguishable via the response list — we can't simulate ≥2 leaders
    // here.  Instead the test encodes a unique integer payload per row and verifies that each
    // output slot contains that row's specific payload, so any key/slot transposition produces
    // a payload mismatch and a clear assertion failure.
    [Test]
    public async Task GetRowsBatch_EachSlotContainsItsOwnRowPayload()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("gbatch-order");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("n", ColumnType.Integer64));

        // Use intentionally non-ascending ids so KV scan order ≠ input order.
        ObjectIdValue[] ids = [new(300, 0, 0), new(100, 0, 0), new(200, 0, 0)];
        long[] sentinels = [31L, 12L, 23L]; // unique per slot; any swap produces a wrong value

        KvTransaction tx = await BeginTransaction(node.Kahuna, "gbatch-order-insert");
        for (int i = 0; i < ids.Length; i++)
        {
            byte[] data = RowEncoder.Encode(schema,
                new Dictionary<string, ColumnValue> { ["n"] = new(ColumnType.Integer64, sentinels[i]) },
                ids[i]);
            await store.InsertRow(tx, ids[i], data);
        }
        await CommitTransaction(node.Kahuna, tx);

        ReadOnlyMemory<byte>?[] result = await store.GetRowsBatch(KvTransaction.CreateReadOnly(), ids);

        Assert.AreEqual(ids.Length, result.Length);
        for (int i = 0; i < ids.Length; i++)
        {
            Assert.IsNotNull(result[i], $"result[{i}] must not be null");
            // Verify this slot holds exactly the sentinel for ids[i], not another row's bytes.
            ReadOnlyMemory<byte>? expected = await store.GetRow(KvTransaction.CreateReadOnly(), ids[i]);
            Assert.AreEqual(expected!.Value.ToArray(), result[i]!.Value.ToArray(),
                $"result[{i}] bytes must match GetRow(ids[{i}]={ids[i]}) — sentinel {sentinels[i]}");
        }
    }

    [Test]
    public async Task GetRowsBatch_MixOfPresentAndMissingIds()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("gbatch-mix");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("v", ColumnType.Integer64));
        ObjectIdValue present1 = new(1, 0, 0);
        ObjectIdValue missing  = new(2, 0, 0);
        ObjectIdValue present2 = new(3, 0, 0);

        byte[] data1 = RowEncoder.Encode(schema,
            new Dictionary<string, ColumnValue> { ["v"] = new(ColumnType.Integer64, 11L) }, present1);
        byte[] data2 = RowEncoder.Encode(schema,
            new Dictionary<string, ColumnValue> { ["v"] = new(ColumnType.Integer64, 33L) }, present2);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "gbatch-mix-insert");
        await store.InsertRow(tx, present1, data1);
        await store.InsertRow(tx, present2, data2);
        await CommitTransaction(node.Kahuna, tx);

        ReadOnlyMemory<byte>?[] result = await store.GetRowsBatch(KvTransaction.CreateReadOnly(),
            [present1, missing, present2]);

        Assert.AreEqual(3, result.Length);
        Assert.IsNotNull(result[0], "present1 must be found");
        Assert.IsNull(result[1],    "missing must return null");
        Assert.IsNotNull(result[2], "present2 must be found");
        Assert.AreEqual(data1, result[0]!.Value.ToArray());
        Assert.AreEqual(data2, result[2]!.Value.ToArray());
    }
}
