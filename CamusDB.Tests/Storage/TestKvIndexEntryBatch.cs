
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
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
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
/// The batched entry write of an index build (<see cref="KvTableStore.PutIndexEntriesBatch"/>): the round
/// trips it costs, the duplicates it refuses in each of the three places one can hide, and the entries a
/// backfill accepts because another writer or an earlier run of the batch wrote them.
/// </summary>
[TestFixture]
public sealed class TestKvIndexEntryBatch
{
    private const string Index = "ix";

    private EmbeddedKahuna node = null!;

    private CountingKahuna kahuna = null!;

    [OneTimeSetUp]
    public async Task StartNode()
    {
        node = new EmbeddedKahuna();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("entrybatch/warmup", CancellationToken.None);
        kahuna = new CountingKahuna(node.Kahuna);
    }

    [OneTimeTearDown]
    public async Task StopNode() => await node.DisposeAsync();

    [SetUp]
    public void ResetCounts() => kahuna.Reset();

    /// <summary>Counts the batched lock and set calls, and the single-key sets that the batch replaces.</summary>
    private sealed class CountingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public List<int> AcquireManySizes { get; } = [];

        public List<int> SetManySizes { get; } = [];

        public int SingleSets;

        /// <summary>Set-many calls to answer with one transient item and the rest held back, as Kahuna does.</summary>
        public int HeldBackRounds;

        public void Reset()
        {
            AcquireManySizes.Clear();
            SetManySizes.Clear();
            SingleSets = 0;
            HeldBackRounds = 0;
        }

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
            HLCTimestamp txId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            AcquireManySizes.Add(keys.Count);
            return inner.LocateAndTryAcquireManyExclusiveLocks(txId, keys, ct, coordinatorKey, operationId);
        }

        public override Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
            List<KahunaSetKeyValueRequestItem> items, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            SetManySizes.Add(items.Count);

            if (HeldBackRounds-- > 0)
                return Task.FromResult(items.Select((item, i) => new KahunaSetKeyValueResponseItem
                {
                    Key = item.Key,
                    Type = i == 0 ? KeyValueResponseType.MustRetry : KeyValueResponseType.NotApplied,
                }).ToList());

            return inner.LocateAndTrySetManyKeyValue(items, ct, coordinatorKey, operationId);
        }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs,
            KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            SingleSets++;
            return inner.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration, coordinatorKey, operationId);
        }
    }

    private KvTableStore Store(string tableId, params (KvTableStore store, HLCTimestamp forkTimestamp)[] ancestors) =>
        new(kahuna, CamusDBOptions.Default, ancestors.Length == 0 ? "entrydb" : "entrybranch", tableId, ancestorStores: ancestors.Length == 0 ? null : ancestors);

    private async Task<KvTransaction> Begin()
    {
        string uniqueId = Guid.NewGuid().ToString("N");
        (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None);
        Assert.That(type, Is.EqualTo(KeyValueResponseType.Set));
        return new KvTransaction(handle.TransactionId, uniqueId);
    }

    private async Task Commit(KvTransaction tx)
    {
        (KeyValueResponseType result, _) = await node.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        Assert.That(result, Is.EqualTo(KeyValueResponseType.Committed));
    }

    private async Task Rollback(KvTransaction tx) =>
        await node.Kahuna.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);

    private static CompositeColumnValue Key(long value) => new([new ColumnValue(ColumnType.Integer64, value)]);

    private static ObjectIdValue Row(int i) => new(i, 7, 7);

    /// <summary>Entries for rows <c>1..count</c>, with key <c>row * 10</c>.</summary>
    private static List<KvTableStore.IndexEntryWrite> Entries(int count, byte[]? includeTuple = null) =>
        [.. Enumerable.Range(1, count).Select(i => new KvTableStore.IndexEntryWrite(Key(i * 10L), Row(i), includeTuple))];

    private static async Task<Dictionary<long, ObjectIdValue>> Read(KvTableStore store, bool unique)
    {
        Dictionary<long, ObjectIdValue> found = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), Index, [ColumnType.Integer64], null, null, unique))
            found.Add(key.Values[0].LongValue, rowId);
        return found;
    }

    private async Task WriteCommitted(KvTableStore store, List<KvTableStore.IndexEntryWrite> entries, bool unique, bool backfillMode = false)
    {
        KvTransaction tx = await Begin();
        await store.PutIndexEntriesBatch(tx, Index, entries, unique, backfillMode);
        await Commit(tx);
    }

    /// <summary>
    /// A backfill batch of 500 rows costs one lock call and one set call, and no single-key call. A call per
    /// entry is what made a batch hold its row locks for a thousand round trips.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task ABatchCostsOneLockCallAndOneSetCall(bool unique)
    {
        KvTableStore store = Store($"calls_{unique}");

        await WriteCommitted(store, Entries(500), unique, backfillMode: true);

        Assert.That(kahuna.AcquireManySizes, Is.EqualTo(new[] { 500 }));
        Assert.That(kahuna.SetManySizes, Is.EqualTo(new[] { 500 }));
        Assert.That(kahuna.SingleSets, Is.Zero);

        Dictionary<long, ObjectIdValue> found = await Read(store, unique);
        Assert.That(found, Has.Count.EqualTo(500));
        Assert.That(found[10], Is.EqualTo(Row(1)));
        Assert.That(found[5_000], Is.EqualTo(Row(500)));
    }

    /// <summary>More entries than one call may carry, as a GIN index makes, go out in several calls.</summary>
    [Test]
    public async Task AnOversizedBatchIsSplitByCount()
    {
        KvTableStore store = Store("bycount");
        const int count = KvStoreConstants.IndexEntryBatchMaxEntries * 2 + 3;

        await WriteCommitted(store, Entries(count), unique: false, backfillMode: true);

        Assert.That(kahuna.SetManySizes, Is.EqualTo(new[] { KvStoreConstants.IndexEntryBatchMaxEntries, KvStoreConstants.IndexEntryBatchMaxEntries, 3 }));
        Assert.That(await Read(store, unique: false), Has.Count.EqualTo(count));
    }

    /// <summary>Entries with a large INCLUDE payload are split by bytes, so one call stays a bounded message.</summary>
    [Test]
    public async Task AnOversizedBatchIsSplitByBytes()
    {
        KvTableStore store = Store("bybytes");
        byte[] payload = new byte[KvStoreConstants.IndexEntryBatchMaxBytes / 4];

        await WriteCommitted(store, Entries(10, payload), unique: false, backfillMode: true);

        Assert.That(kahuna.SetManySizes.Sum(), Is.EqualTo(10));
        Assert.That(kahuna.SetManySizes, Has.All.LessThanOrEqualTo(3), "four payloads and their keys exceed the limit");
        Assert.That(await Read(store, unique: false), Has.Count.EqualTo(10));
    }

    /// <summary>An entry larger than the byte limit still goes out, alone.</summary>
    [Test]
    public async Task AnEntryAboveTheByteLimitGoesAlone()
    {
        KvTableStore store = Store("alone");
        byte[] payload = new byte[KvStoreConstants.IndexEntryBatchMaxBytes + 1];

        await WriteCommitted(store, Entries(2, payload), unique: false, backfillMode: true);

        Assert.That(kahuna.SetManySizes, Is.EqualTo(new[] { 1, 1 }));
        Assert.That(await Read(store, unique: false), Has.Count.EqualTo(2));
    }

    [Test]
    public async Task AnEmptyBatchMakesNoCall()
    {
        KvTableStore store = Store("empty");

        KvTransaction tx = await Begin();
        await store.PutIndexEntriesBatch(tx, Index, [], unique: true, backfillMode: true);
        await Rollback(tx);

        Assert.That(kahuna.SetManySizes, Is.Empty);
        Assert.That(kahuna.AcquireManySizes, Is.Empty);
    }

    /// <summary>Two rows of one batch with the same unique key: a duplicate, found before anything is sent.</summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task ADuplicateWithinABatchIsRefused(bool backfillMode)
    {
        KvTableStore store = Store($"dupin_{backfillMode}");
        List<KvTableStore.IndexEntryWrite> entries = [.. Entries(5), new(Key(30), Row(99))];

        KvTransaction tx = await Begin();
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntriesBatch(tx, Index, entries, unique: true, backfillMode))!;
        await Rollback(tx);

        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
        Assert.That(await Read(store, unique: true), Is.Empty);
    }

    /// <summary>
    /// Two rows with the same unique key in two chunks of one call: the second is refused against the
    /// transaction's own staged copy of the key.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task ADuplicateAcrossChunksIsRefused(bool backfillMode)
    {
        KvTableStore store = Store($"dupchunk_{backfillMode}");
        List<KvTableStore.IndexEntryWrite> entries = [.. Entries(KvStoreConstants.IndexEntryBatchMaxEntries), new(Key(30), Row(9_999))];

        KvTransaction tx = await Begin();
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntriesBatch(tx, Index, entries, unique: true, backfillMode))!;
        await Rollback(tx);

        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
        Assert.That(await Read(store, unique: true), Is.Empty);
    }

    /// <summary>A committed entry of another row: a duplicate in both modes, and the batch leaves nothing.</summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task AnEntryOfAnotherRowIsADuplicate(bool backfillMode)
    {
        KvTableStore store = Store($"dupother_{backfillMode}");
        await WriteCommitted(store, [new(Key(30), Row(99))], unique: true);

        KvTransaction tx = await Begin();
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntriesBatch(tx, Index, Entries(5), unique: true, backfillMode))!;
        await Rollback(tx);

        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
        Dictionary<long, ObjectIdValue> found = await Read(store, unique: true);
        Assert.That(found, Has.Count.EqualTo(1));
        Assert.That(found[30], Is.EqualTo(Row(99)));
    }

    /// <summary>
    /// A backfill that meets entries of its own rows, written by an UPDATE or by an earlier run, accepts
    /// them and writes the rest. The set refuses the whole batch for one entry in place, so the rest must be
    /// sent again without it. With an INCLUDE payload the stored value is longer than the row id.
    /// </summary>
    [TestCase(0)]
    [TestCase(16)]
    public async Task ABackfillAcceptsTheEntriesOfItsOwnRows(int payloadBytes)
    {
        KvTableStore store = Store($"own_{payloadBytes}");
        byte[]? payload = payloadBytes == 0 ? null : new byte[payloadBytes];
        List<KvTableStore.IndexEntryWrite> all = Entries(20, payload);

        await WriteCommitted(store, [all[2], all[7], all[19]], unique: true);
        kahuna.Reset();

        await WriteCommitted(store, all, unique: true, backfillMode: true);

        Assert.That(kahuna.SetManySizes, Is.EqualTo(new[] { 20, 17 }), "one refused set, and one with the entries it held back");
        Dictionary<long, ObjectIdValue> found = await Read(store, unique: true);
        Assert.That(found, Has.Count.EqualTo(20));
        Assert.That(found[30], Is.EqualTo(Row(3)));

        // And a second run of the whole batch changes nothing.
        await WriteCommitted(store, all, unique: true, backfillMode: true);
        Assert.That(await Read(store, unique: true), Is.EqualTo(found));
    }

    /// <summary>The same batch outside a backfill is a duplicate: no row writes its own entry twice.</summary>
    [Test]
    public async Task APlainBatchRefusesTheEntriesOfItsOwnRows()
    {
        KvTableStore store = Store("ownplain");
        await WriteCommitted(store, Entries(3), unique: true);

        KvTransaction tx = await Begin();
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntriesBatch(tx, Index, Entries(3), unique: true, backfillMode: false))!;
        await Rollback(tx);

        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }

    /// <summary>
    /// A transient answer holds a batch back, and the batch is sent again whole; an entry in place is then
    /// still recognized as the row's own.
    /// </summary>
    [Test]
    public async Task ATransientAnswerResendsTheBatch()
    {
        KvTableStore store = Store("transient");
        await WriteCommitted(store, [new(Key(30), Row(3))], unique: true);
        kahuna.Reset();
        kahuna.HeldBackRounds = 1;

        await WriteCommitted(store, Entries(6), unique: true, backfillMode: true);

        Assert.That(kahuna.SetManySizes, Is.EqualTo(new[] { 6, 6, 5 }));
        Assert.That(await Read(store, unique: true), Has.Count.EqualTo(6));
    }

    /// <summary>
    /// A key that another transaction holds refuses the batch with the retryable code, which is how a
    /// backfill batch loses: the runner retries it.
    /// </summary>
    [Test]
    public async Task AKeyHeldByAnotherTransactionIsARetryableRefusal()
    {
        KvTableStore store = Store("held");

        KvTransaction holder = await Begin();
        await store.PutIndexEntriesBatch(holder, Index, [new(Key(30), Row(99))], unique: true, backfillMode: false);

        KvTransaction tx = await Begin();
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntriesBatch(tx, Index, Entries(5), unique: true, backfillMode: true))!;
        await Rollback(tx);
        await Rollback(holder);

        Assert.That(SerializableRetryHelper.IsRetryable(refused), Is.True, refused.Code);
        Assert.That(await Read(store, unique: true), Is.Empty);
    }

    /// <summary>Each entry counts as one mutation, as a write per entry did.</summary>
    [Test]
    public async Task EachEntryCountsAsOneMutation()
    {
        KvTableStore store = Store("mutations");
        string uniqueId = Guid.NewGuid().ToString("N");
        (_, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic }, CancellationToken.None);
        KvTransaction tx = new(handle.TransactionId, uniqueId, mutationLimit: 4);

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntriesBatch(tx, Index, Entries(5), unique: false, backfillMode: false))!;
        await Rollback(tx);

        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMutationLimitExceeded));
    }

    // ── Branch databases ─────────────────────────────────────────────────────────────────────

    private async Task<(KvTableStore root, KvTableStore branch)> Branch(string tableId, List<KvTableStore.IndexEntryWrite> inherited)
    {
        KvTableStore root = Store(tableId);
        if (inherited.Count > 0)
            await WriteCommitted(root, inherited, unique: true);

        HLCTimestamp fork = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        KvTableStore branch = Store(tableId, (root, fork));
        kahuna.Reset();
        return (root, branch);
    }

    /// <summary>On a branch, the unique entries go out in the same one set call as the others.</summary>
    [Test]
    public async Task ABranchBatchCostsOneSetCall()
    {
        (KvTableStore root, KvTableStore branch) = await Branch("br_calls", []);

        await WriteCommitted(branch, Entries(50), unique: true, backfillMode: true);

        Assert.That(kahuna.SetManySizes, Is.EqualTo(new[] { 50 }));
        Assert.That(kahuna.SingleSets, Is.Zero);
        Assert.That(await Read(branch, unique: true), Has.Count.EqualTo(50));
        Assert.That(await Read(root, unique: true), Is.Empty, "the ancestor is untouched");
    }

    /// <summary>An inherited entry of another row refuses the batch; one of the same row does not.</summary>
    [Test]
    public async Task AnInheritedEntryOfAnotherRowIsADuplicate()
    {
        (_, KvTableStore branch) = await Branch("br_dup", [new(Key(30), Row(99))]);

        KvTransaction tx = await Begin();
        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => branch.PutIndexEntriesBatch(tx, Index, Entries(5), unique: true, backfillMode: true))!;
        await Rollback(tx);

        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }

    /// <summary>
    /// A backfill on a branch that meets entries of its own rows accepts them. The entries of a covering
    /// index hold the INCLUDE payload after the row id, and only the row id identifies the row: a compare of
    /// the whole value reported a duplicate.
    /// </summary>
    [TestCase(0)]
    [TestCase(16)]
    public async Task ABranchBackfillAcceptsTheEntriesOfItsOwnRows(int payloadBytes)
    {
        (_, KvTableStore branch) = await Branch($"br_own_{payloadBytes}", []);
        byte[]? payload = payloadBytes == 0 ? null : new byte[payloadBytes];
        List<KvTableStore.IndexEntryWrite> all = Entries(10, payload);

        await WriteCommitted(branch, [all[2], all[7]], unique: true, backfillMode: true);
        await WriteCommitted(branch, all, unique: true, backfillMode: true);

        Dictionary<long, ObjectIdValue> found = await Read(branch, unique: true);
        Assert.That(found, Has.Count.EqualTo(10));
        Assert.That(found[30], Is.EqualTo(Row(3)));
    }

    /// <summary>A slot that the branch cleared is free again for another row.</summary>
    [Test]
    public async Task ATombstonedSlotOfABranchIsReplaced()
    {
        (_, KvTableStore branch) = await Branch("br_tomb", [new(Key(30), Row(99))]);

        KvTransaction clear = await Begin();
        await branch.WriteUniqueIndexTombstoneForTesting(clear, Index, Key(30));
        await Commit(clear);

        await WriteCommitted(branch, Entries(5), unique: true, backfillMode: true);

        Dictionary<long, ObjectIdValue> found = await Read(branch, unique: true);
        Assert.That(found, Has.Count.EqualTo(5));
        Assert.That(found[30], Is.EqualTo(Row(3)));
    }
}
