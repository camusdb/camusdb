/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
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
using static CamusDB.Core.Util.ObjectIds.ObjectIdGenerator;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Counts the lock round-trips an optimistic transaction actually issues, so the cost of the
/// Serializable+Optimistic hybrid is measured rather than assumed. Optimistic skips exclusive write
/// locks entirely; under Serializable it still takes one shared predicate lock the first time it
/// touches a key (that lock is what guards the read→write window, which the coordinator's read-set
/// validation deliberately skips for keys the transaction also writes). What it must not do is pay
/// a round trip re-asserting coverage it already holds, which is what these counts pin down.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestOptimisticLockAudit
{
    /// <summary>Wraps a real node and tallies every lock-acquire round trip by kind.</summary>
    private sealed class CountingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public readonly List<(string prefix, string? start, RangeLockMode mode)> RangeLocks = [];
        public int ExclusivePointLocks;
        public int BatchExclusiveLockCalls;

        public override Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
            HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            RangeLocks.Add((prefix, startKey, mode));
            return inner.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
                expiresMs, durability, mode, cancellationToken, coordinatorKey, operationId);
        }

        public override Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> LocateAndTryAcquireExclusiveLock(
            HLCTimestamp txId, string key, int expiresMs, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            ExclusivePointLocks++;
            return inner.LocateAndTryAcquireExclusiveLock(txId, key, expiresMs, durability, ct, coordinatorKey, operationId);
        }

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
            HLCTimestamp txId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            BatchExclusiveLockCalls++;
            return inner.LocateAndTryAcquireManyExclusiveLocks(txId, keys, ct, coordinatorKey, operationId);
        }
    }

    private static async Task<(EmbeddedKahuna node, CountingKahuna stub, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        CountingKahuna stub = new(node.Kahuna);
        return (node, stub, new KvTableStore(stub, CamusDBConfig.Ambient, "lockaudit", tableId));
    }

    private static async Task<KvTransaction> BeginAsync(IKahuna kahuna, KeyValueTransactionLocking locking, CamusIsolationLevel level)
    {
        string uniqueId = System.Guid.NewGuid().ToString("N");
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = uniqueId,
                Locking = locking,
                ReadValidation = ReadValidation.None
            },
            CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(handle.TransactionId, uniqueId, isReadOnly: false, level,
            CamusTransactionMode.ReadWrite, locking: locking);
    }

    [Test]
    public async Task RepeatedPointReads_UnderSerializableOptimistic_LockOnce()
    {
        (EmbeddedKahuna node, CountingKahuna stub, KvTableStore store) = await CreateStoreAsync("t1");

        try
        {
            ObjectIdValue rowId = Generate();

            // Seed one row outside the measured transaction.
            KvTransaction seed = await BeginAsync(stub, KeyValueTransactionLocking.Pessimistic, CamusIsolationLevel.ReadCommitted);
            await store.InsertRow(seed, rowId, [1, 2, 3]);
            (KeyValueResponseType seedCommit, _) = await stub.LocateAndCommitTransaction(seed.Handle, CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Committed, seedCommit);

            stub.RangeLocks.Clear();
            stub.ExclusivePointLocks = 0;

            KvTransaction tx = await BeginAsync(stub, KeyValueTransactionLocking.Optimistic, CamusIsolationLevel.Serializable);

            // Read the SAME key three times. The first read's [key,key] Shared lock is held to
            // commit under strict two-phase locking, so reads 2 and 3 are already covered.
            for (int i = 0; i < 3; i++)
                await store.GetRow(tx, rowId);

            TestContext.Out.WriteLine($"range-lock RPCs after 3 reads of one key: {stub.RangeLocks.Count}");
            foreach ((string prefix, string? start, RangeLockMode mode) in stub.RangeLocks)
                TestContext.Out.WriteLine($"  {mode} {prefix} start={start ?? "-inf"}");

            Assert.AreEqual(0, stub.ExclusivePointLocks, "optimistic must not take exclusive write locks");
            Assert.AreEqual(1, stub.RangeLocks.Count,
                "the lock the first read acquired covers every later read of the same key");
            Assert.AreEqual(RangeLockMode.Shared, stub.RangeLocks[0].mode);

            await stub.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task PointReads_UnderReadCommittedOptimistic_TakeNoLocks()
    {
        (EmbeddedKahuna node, CountingKahuna stub, KvTableStore store) = await CreateStoreAsync("t2");

        try
        {
            ObjectIdValue rowId = Generate();

            KvTransaction seed = await BeginAsync(stub, KeyValueTransactionLocking.Pessimistic, CamusIsolationLevel.ReadCommitted);
            await store.InsertRow(seed, rowId, [1, 2, 3]);
            (KeyValueResponseType seedCommit, _) = await stub.LocateAndCommitTransaction(seed.Handle, CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Committed, seedCommit);

            stub.RangeLocks.Clear();
            stub.ExclusivePointLocks = 0;

            KvTransaction tx = await BeginAsync(stub, KeyValueTransactionLocking.Optimistic, CamusIsolationLevel.ReadCommitted);

            for (int i = 0; i < 3; i++)
                await store.GetRow(tx, rowId);

            await store.InsertRow(tx, rowId, [4, 5, 6]);

            TestContext.Out.WriteLine($"range-lock RPCs (read committed + optimistic): {stub.RangeLocks.Count}");

            Assert.AreEqual(0, stub.RangeLocks.Count, "read committed takes no predicate locks");
            Assert.AreEqual(0, stub.ExclusivePointLocks, "optimistic takes no exclusive write locks");

            await stub.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    /// <summary>
    /// The read→write window on a key the transaction also writes is NOT covered by the
    /// coordinator's read-set validation (it skips keys in the modified set), so the shared lock and
    /// its S→X upgrade are the only guard and must still fire under optimistic. What must not fire
    /// is a further acquire once the key is already Exclusive.
    /// </summary>
    [Test]
    public async Task ReadThenWrite_UnderSerializableOptimistic_AddsAnUpgradeRoundTrip()
    {
        (EmbeddedKahuna node, CountingKahuna stub, KvTableStore store) = await CreateStoreAsync("t3");

        try
        {
            ObjectIdValue rowId = Generate();

            KvTransaction seed = await BeginAsync(stub, KeyValueTransactionLocking.Pessimistic, CamusIsolationLevel.ReadCommitted);
            await store.InsertRow(seed, rowId, [1, 2, 3]);
            (KeyValueResponseType seedCommit, _) = await stub.LocateAndCommitTransaction(seed.Handle, CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Committed, seedCommit);

            stub.RangeLocks.Clear();
            stub.ExclusivePointLocks = 0;

            KvTransaction tx = await BeginAsync(stub, KeyValueTransactionLocking.Optimistic, CamusIsolationLevel.Serializable);

            await store.GetRow(tx, rowId);
            await store.InsertRow(tx, rowId, [4, 5, 6]);

            TestContext.Out.WriteLine($"range-lock RPCs for one read-modify-write: {stub.RangeLocks.Count}");
            foreach ((string prefix, string? start, RangeLockMode mode) in stub.RangeLocks)
                TestContext.Out.WriteLine($"  {mode} {prefix} start={start ?? "-inf"}");

            Assert.AreEqual(0, stub.ExclusivePointLocks, "optimistic must not take exclusive write locks");
            Assert.AreEqual(2, stub.RangeLocks.Count, "one shared acquire for the read, one S->X upgrade for the write");
            Assert.AreEqual(RangeLockMode.Shared, stub.RangeLocks[0].mode);
            Assert.AreEqual(RangeLockMode.Exclusive, stub.RangeLocks[1].mode);

            // The key is Exclusive now: neither a re-read nor a second write needs another acquire.
            await store.GetRow(tx, rowId);
            await store.InsertRow(tx, rowId, [7, 8, 9]);

            Assert.AreEqual(2, stub.RangeLocks.Count,
                "an already-Exclusive key is covered for both further reads and further writes");

            await stub.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }
}
