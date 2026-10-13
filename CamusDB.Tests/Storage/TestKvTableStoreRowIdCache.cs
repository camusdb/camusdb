/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The remembered key → row-id mapping behind a primary-key point lookup: populated by a unique lookup,
/// consulted before the index read, forgotten by the delete that removes the entry, absent when the
/// feature is off or the store is a branch.
/// </summary>
[TestFixture]
public sealed class TestKvTableStoreRowIdCache
{
    private const string PrimaryKey = CamusDBConstants.PrimaryKeyInternalName;

    private static readonly CamusDBOptions CacheOn = CamusDBOptions.Default with { PointReadRowIdCacheEnabled = true };

    private static async Task<KvTransaction> BeginTransaction(IKahuna kahuna, string uniqueId)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(handle.TransactionId, uniqueId);
    }

    private static async Task CommitTransaction(IKahuna kahuna, KvTransaction tx)
    {
        (KeyValueResponseType result, _) = await kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Committed, result);
    }

    private static async Task<(EmbeddedKahuna node, KvTableStore store)> CreateStoreAsync(string tableId, CamusDBOptions options)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        return (node, new KvTableStore(node.Kahuna, options, "testdb", tableId));
    }

    private static CompositeColumnValue Key(long value) => new([new ColumnValue(ColumnType.Integer64, value)]);

    [Test]
    public async Task OnByDefault_TheStoreRemembersRowIds()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("rc-default", CamusDBOptions.Default);
        await using EmbeddedKahuna __ = node;

        Assert.That(CamusDBOptions.Default.PointReadRowIdCacheEnabled, Is.True, "the cache ships on");
        Assert.That(store.RemembersUniqueRowIds, Is.True);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "rc-default-put");
        await store.PutIndexEntry(tx, PrimaryKey, Key(1), new ObjectIdValue(1, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        Assert.That(await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(1)), Is.Not.Null);
        Assert.That(store.RememberedUniqueRowIds, Is.EqualTo(1));
    }

    [Test]
    public async Task ExplicitFalse_TheStoreRemembersNothing()
    {
        CamusDBOptions cacheOff = CamusDBOptions.Default with { PointReadRowIdCacheEnabled = false };
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("rc-off", cacheOff);
        await using EmbeddedKahuna __ = node;

        Assert.That(store.RemembersUniqueRowIds, Is.False);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "rc-off-put");
        await store.PutIndexEntry(tx, PrimaryKey, Key(1), new ObjectIdValue(1, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        Assert.That(await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(1)), Is.Not.Null);
        Assert.That(store.RememberedUniqueRowIds, Is.EqualTo(0));
        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(1), out _, out string? kvKey), Is.False);
        Assert.That(kvKey, Is.Null, "a store without a cache does not even build the key");
    }

    [Test]
    public async Task ALookupRemembersTheRowIdItResolved()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("rc-hit", CacheOn);
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(7, 7, 7);
        KvTransaction tx = await BeginTransaction(node.Kahuna, "rc-hit-put");
        await store.PutIndexEntry(tx, PrimaryKey, Key(42), rowId, unique: true);
        await CommitTransaction(node.Kahuna, tx);

        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(42), out _, out _), Is.False, "nothing is remembered before a lookup");

        ObjectIdValue? found = await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(42));
        Assert.That(found, Is.EqualTo(rowId));

        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(42), out ObjectIdValue remembered, out string? kvKey), Is.True);
        Assert.That(remembered, Is.EqualTo(rowId));
        Assert.That(kvKey, Is.Not.Null);
        Assert.That(store.RememberedUniqueRowIds, Is.EqualTo(1));

        // A miss is not remembered: a key with no entry stays unknown.
        Assert.That(await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(43)), Is.Null);
        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(43), out _, out _), Is.False);
    }

    [Test]
    public async Task EvictionForgetsOneKeyAndLeavesTheRest()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("rc-evict", CacheOn);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await BeginTransaction(node.Kahuna, "rc-evict-put");
        await store.PutIndexEntry(tx, PrimaryKey, Key(1), new ObjectIdValue(1, 0, 0), unique: true);
        await store.PutIndexEntry(tx, PrimaryKey, Key(2), new ObjectIdValue(2, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(1));
        await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(2));
        Assert.That(store.RememberedUniqueRowIds, Is.EqualTo(2));

        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(1), out _, out string? kvKey), Is.True);
        store.EvictCachedUniqueRowId(kvKey!);

        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(1), out _, out _), Is.False);
        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(2), out _, out _), Is.True);
        Assert.That(store.RememberedUniqueRowIds, Is.EqualTo(1));
    }

    [Test]
    public async Task DeletingTheRowForgetsItsKeyBeforeTheDeleteCommits()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("rc-delete", CacheOn);
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(3, 3, 3);
        KvTransaction put = await BeginTransaction(node.Kahuna, "rc-delete-put");
        await store.PutIndexEntry(put, PrimaryKey, Key(5), rowId, unique: true);
        await CommitTransaction(node.Kahuna, put);

        await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(5));
        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(5), out _, out _), Is.True);

        KvTransaction del = await BeginTransaction(node.Kahuna, "rc-delete-del");
        await store.DeleteRowsBatch(del, [new KvTableStore.RowDelete { RowId = rowId, IndexEntries = [new KvTableStore.IndexDelete(PrimaryKey, Key(5), rowId, Unique: true)] }]);

        // Forgotten at write time: a rolled-back delete would only cost the next lookup an index read.
        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(5), out _, out _), Is.False);
        await CommitTransaction(node.Kahuna, del);

        Assert.That(await store.LookupUnique(KvTransaction.CreateReadOnly(), PrimaryKey, Key(5)), Is.Null);
        Assert.That(store.TryGetCachedUniqueRowId(PrimaryKey, Key(5), out _, out _), Is.False);
    }

    [Test]
    public async Task ABranchStoreRemembersNothing()
    {
        (EmbeddedKahuna node, KvTableStore root) = await CreateStoreAsync("rc-root", CacheOn);
        await using EmbeddedKahuna __ = node;

        KvTableStore branch = new(node.Kahuna, CacheOn, "branchdb", "rc-root",
            ancestorStores: [(root, Kommander.Time.HLCTimestamp.Zero)]);

        Assert.That(root.RemembersUniqueRowIds, Is.True);
        Assert.That(branch.RemembersUniqueRowIds, Is.False);
    }

    [Test]
    public void TheBoundClearsTheCacheInsteadOfGrowingPastIt()
    {
        UniqueRowIdCache cache = new(maxEntries: 3);
        cache.Set("a", new ObjectIdValue(1, 0, 0));
        cache.Set("b", new ObjectIdValue(2, 0, 0));
        cache.Set("c", new ObjectIdValue(3, 0, 0));
        Assert.That(cache.Count, Is.EqualTo(3));

        cache.Set("d", new ObjectIdValue(4, 0, 0));
        Assert.That(cache.Count, Is.EqualTo(0), "the fourth entry trips the bound and the cache starts over");
        Assert.That(cache.TryGet("a", out _), Is.False);

        // Re-setting a known key replaces the value without counting it twice.
        cache.Set("a", new ObjectIdValue(1, 0, 0));
        cache.Set("a", new ObjectIdValue(9, 0, 0));
        Assert.That(cache.Count, Is.EqualTo(1));
        Assert.That(cache.TryGet("a", out ObjectIdValue v) && v == new ObjectIdValue(9, 0, 0), Is.True);
    }
}
