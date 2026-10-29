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
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using static CamusDB.Core.Util.ObjectIds.ObjectIdGenerator;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The batched row-lock loop reads Kahuna's many-key answer as Kahuna writes it: each leader-node
/// group stops at its first key that is not granted and says nothing about the keys after it. A key
/// with no answer is still pending and must be sent again; a loop that took the silence for a grant
/// would return as if it held locks it never took. The SKIP LOCKED and NOWAIT forms of the loop are
/// checked here too, at the storage layer, where the exact answers can be controlled.
/// </summary>
[TestFixture]
// Serial: starts and stops its own embedded Kahuna node, which is too heavy to run alongside
// other node-booting fixtures.
[NonParallelizable]
public sealed class TestRowLockBatchAnswers
{
    /// <summary>
    /// Counts the many-key lock calls. When armed, the next call answers only its first key, with the
    /// transient <c>MustRetry</c>, and nothing for the others, without asking the real node: the shape
    /// of a group that stopped at a transient key.
    /// </summary>
    private sealed class StoppingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public int ManyLockCalls;
        public bool StopNextCall;

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
            HLCTimestamp txId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            Interlocked.Increment(ref ManyLockCalls);

            if (StopNextCall && keys.Count > 1)
            {
                StopNextCall = false;
                return Task.FromResult(new List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>
                {
                    (KeyValueResponseType.MustRetry, keys[0].key, keys[0].durability, HLCTimestamp.Zero),
                });
            }

            return inner.LocateAndTryAcquireManyExclusiveLocks(txId, keys, ct, coordinatorKey, operationId);
        }
    }

    private static async Task<(EmbeddedKahuna node, StoppingKahuna stub, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        StoppingKahuna stub = new(node.Kahuna);
        return (node, stub, new KvTableStore(stub, CamusDBOptions.Default, "rowlockanswers", tableId));
    }

    private static async Task<KvTransaction> BeginAsync(IKahuna kahuna, CamusIsolationLevel level = CamusIsolationLevel.ReadCommitted)
    {
        string uniqueId = System.Guid.NewGuid().ToString("N");
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = uniqueId,
                Locking = KeyValueTransactionLocking.Pessimistic,
                ReadValidation = ReadValidation.None
            },
            CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(handle.TransactionId, uniqueId, isReadOnly: false, level,
            CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);
    }

    /// <summary>Inserts and commits <paramref name="count"/> rows, and returns their ids in key order.</summary>
    private static async Task<List<ObjectIdValue>> SeedAsync(IKahuna kahuna, KvTableStore store, int count)
    {
        List<ObjectIdValue> rowIds = [];
        KvTransaction seed = await BeginAsync(kahuna);

        for (int i = 0; i < count; i++)
        {
            ObjectIdValue rowId = Generate();
            await store.InsertRow(seed, rowId, [1, 2, (byte)i]);
            rowIds.Add(rowId);
        }

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(seed.Handle, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Committed, commit);

        rowIds.Sort((a, b) => string.CompareOrdinal(a.ToString(), b.ToString()));
        return rowIds;
    }

    private static async Task RollbackAsync(IKahuna kahuna, KvTransaction tx)
        => await kahuna.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);

    [Test]
    public async Task KeysAGroupDidNotAnswer_AreSentAgain_AndEndUpLocked()
    {
        (EmbeddedKahuna node, StoppingKahuna stub, KvTableStore store) = await CreateStoreAsync("t1");

        try
        {
            List<ObjectIdValue> rowIds = await SeedAsync(stub, store, 3);

            KvTransaction holder = await BeginAsync(stub);
            stub.ManyLockCalls = 0;
            stub.StopNextCall = true;

            ReadOnlyMemory<byte>?[] rows = await store.LockAndReadRowsForMutationAsync(holder, rowIds);
            Assert.AreEqual(3, rows.Length);
            Assert.That(rows, Has.All.Not.Null);

            // One call answered only the transient key; the next one had to carry all three keys again.
            Assert.GreaterOrEqual(stub.ManyLockCalls, 2);

            // The last key never had an answer in the first round. It is locked all the same.
            KvTransaction other = await BeginAsync(stub);
            CamusDBException? conflict = Assert.ThrowsAsync<CamusDBException>(
                async () => await store.LockAndReadRowsForMutationAsync(other, [rowIds[2]]));
            Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, conflict!.Code);

            await RollbackAsync(stub, other);
            await RollbackAsync(stub, holder);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task SkipLocked_SkipsTheHeldRow_AndLocksTheRowsAfterIt()
    {
        (EmbeddedKahuna node, StoppingKahuna stub, KvTableStore store) = await CreateStoreAsync("t2");

        try
        {
            List<ObjectIdValue> rowIds = await SeedAsync(stub, store, 3);

            KvTransaction first = await BeginAsync(stub);
            await store.LockAndReadRowsForMutationAsync(first, [rowIds[1]]);

            // One node, one group: Kahuna stops at the held middle row and answers nothing for the last
            // one, which the loop then sends again on its own.
            KvTransaction second = await BeginAsync(stub);
            stub.ManyLockCalls = 0;

            (ReadOnlyMemory<byte>?[] rows, bool[]? skipped) =
                await store.LockAndReadRowsForLockingReadAsync(second, rowIds, RowLockWaitPolicy.SkipLocked);

            Assert.AreEqual(new[] { false, true, false }, skipped);
            Assert.IsNotNull(rows[0]);
            Assert.IsNull(rows[1]);
            Assert.IsNotNull(rows[2]);
            Assert.AreEqual(2, stub.ManyLockCalls);

            KvTransaction third = await BeginAsync(stub);
            CamusDBException? conflict = Assert.ThrowsAsync<CamusDBException>(
                async () => await store.LockAndReadRowsForLockingReadAsync(third, [rowIds[2]], RowLockWaitPolicy.NoWait));
            Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, conflict!.Code);

            await RollbackAsync(stub, third);
            await RollbackAsync(stub, second);
            await RollbackAsync(stub, first);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    /// <summary>
    /// Records the cost of the SKIP LOCKED resend on one leader-node group. Each held row stops the group,
    /// so a page costs one round trip, plus one round trip for each held row in it. A page with no held
    /// row costs exactly one round trip.
    /// </summary>
    [Test]
    public async Task SkipLocked_CostsOneRoundTripPerPage_PlusOnePerHeldRow()
    {
        (EmbeddedKahuna node, StoppingKahuna stub, KvTableStore store) = await CreateStoreAsync("t4");

        try
        {
            List<ObjectIdValue> rowIds = await SeedAsync(stub, store, 10);

            KvTransaction holder = await BeginAsync(stub);
            await store.LockAndReadRowsForMutationAsync(holder, [rowIds[2], rowIds[5], rowIds[8]]);

            KvTransaction skipper = await BeginAsync(stub);
            stub.ManyLockCalls = 0;
            (_, bool[]? skipped) =
                await store.LockAndReadRowsForLockingReadAsync(skipper, rowIds, RowLockWaitPolicy.SkipLocked);

            Assert.AreEqual(new[] { false, false, true, false, false, true, false, false, true, false }, skipped);
            Assert.AreEqual(4, stub.ManyLockCalls, "one round trip for the page, plus one for each of the three held rows");

            KvTransaction free = await BeginAsync(stub);
            stub.ManyLockCalls = 0;
            await store.LockAndReadRowsForLockingReadAsync(free, [rowIds[0]], RowLockWaitPolicy.SkipLocked);
            Assert.AreEqual(1, stub.ManyLockCalls, "the row is held by the skipper, so it is skipped in one round trip");

            await RollbackAsync(stub, free);
            await RollbackAsync(stub, skipper);
            await RollbackAsync(stub, holder);

            KvTransaction clear = await BeginAsync(stub);
            stub.ManyLockCalls = 0;
            await store.LockAndReadRowsForLockingReadAsync(clear, rowIds, RowLockWaitPolicy.SkipLocked);
            Assert.AreEqual(1, stub.ManyLockCalls, "a page with no held row costs one round trip");
            await RollbackAsync(stub, clear);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task AHeldRow_FailsWithTheCodeOfThePolicy()
    {
        (EmbeddedKahuna node, StoppingKahuna stub, KvTableStore store) = await CreateStoreAsync("t3");

        try
        {
            List<ObjectIdValue> rowIds = await SeedAsync(stub, store, 2);

            KvTransaction holder = await BeginAsync(stub);
            await store.LockAndReadRowsForLockingReadAsync(holder, [rowIds[0]], RowLockWaitPolicy.Wait);

            KvTransaction other = await BeginAsync(stub);

            CamusDBException? byDefault = Assert.ThrowsAsync<CamusDBException>(
                async () => await store.LockAndReadRowsForLockingReadAsync(other, rowIds, RowLockWaitPolicy.Wait));
            Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, byDefault!.Code);

            CamusDBException? noWait = Assert.ThrowsAsync<CamusDBException>(
                async () => await store.LockAndReadRowsForLockingReadAsync(other, rowIds, RowLockWaitPolicy.NoWait));
            Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, noWait!.Code);

            // The holder's own second request is granted again: the lock is re-entrant.
            (ReadOnlyMemory<byte>?[] again, _) =
                await store.LockAndReadRowsForLockingReadAsync(holder, [rowIds[0]], RowLockWaitPolicy.NoWait);
            Assert.IsNotNull(again[0]);

            await RollbackAsync(stub, other);
            await RollbackAsync(stub, holder);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }
}
