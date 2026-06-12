
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// T2.3 — KvTableStore secondary index operations:
///   LookupUnique, ScanIndex, PutIndexEntry, DeleteIndexEntry.
/// </summary>
[TestFixture]
public sealed class TestKvTableStoreIndex
{
    // ---- transaction helpers ----------------------------------------------

    private static async Task<KvTransaction> BeginTransaction(IKahuna kahuna, string uniqueId)
    {
        (KeyValueResponseType type, HLCTimestamp txId) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { UniqueId = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(txId, uniqueId);
    }

    private static async Task CommitTransaction(IKahuna kahuna, KvTransaction tx)
    {
        KeyValueResponseType result = await kahuna.LocateAndCommitTransaction(
            tx.UniqueId,
            tx.TransactionId,
            tx.GetAcquiredLocks(),
            tx.GetModifiedKeys(),
            [],
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Committed, result);
    }

    // ---- node / store factory ---------------------------------------------

    private static async Task<(EmbeddedKahuna node, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        return (node, new KvTableStore(node.Kahuna, tableId));
    }

    // ---- helper: single ColumnValue composite ----------------------------

    private static CompositeColumnValue CV(ColumnValue value) => new(new[] { value });

    // ---- tests: LookupUnique ---------------------------------------------

    [Test]
    public async Task LookupUnique_ReturnsNull_WhenNoEntryExists()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("u1");
        await using EmbeddedKahuna __ = node;

        CompositeColumnValue key = CV(new ColumnValue(ColumnType.Integer64, 42L));
        ObjectIdValue? rowId = await store.LookupUnique(HLCTimestamp.Zero, "idx_age", key);

        Assert.IsNull(rowId);
    }

    [Test]
    public async Task PutIndexEntry_Unique_ThenLookup_ReturnsRowId()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("u2");
        await using EmbeddedKahuna __ = node;

        CompositeColumnValue key = CV(new ColumnValue(ColumnType.Integer64, 100L));
        ObjectIdValue rowId = new(1, 2, 3);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "u2-put");
        await store.PutIndexEntry(tx, "idx_age", key, rowId, unique: true);
        await CommitTransaction(node.Kahuna, tx);

        ObjectIdValue? found = await store.LookupUnique(HLCTimestamp.Zero, "idx_age", key);

        Assert.IsNotNull(found);
        Assert.AreEqual(rowId.ToString(), found!.Value.ToString());
    }

    [Test]
    public async Task PutIndexEntry_Unique_DuplicateThrows()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("u3");
        await using EmbeddedKahuna __ = node;

        CompositeColumnValue key = CV(new ColumnValue(ColumnType.String, "alice"));
        ObjectIdValue rowId1 = new(1, 0, 0);
        ObjectIdValue rowId2 = new(2, 0, 0);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "u3-put1");
        await store.PutIndexEntry(tx1, "idx_name", key, rowId1, unique: true);
        await CommitTransaction(node.Kahuna, tx1);

        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "u3-put2");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await store.PutIndexEntry(tx2, "idx_name", key, rowId2, unique: true)
        );
    }

    [Test]
    public async Task DeleteIndexEntry_Unique_RemovesEntry()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("u4");
        await using EmbeddedKahuna __ = node;

        CompositeColumnValue key = CV(new ColumnValue(ColumnType.Integer64, 7L));
        ObjectIdValue rowId = new(9, 9, 9);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "u4-put");
        await store.PutIndexEntry(tx1, "idx_x", key, rowId, unique: true);
        await CommitTransaction(node.Kahuna, tx1);

        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "u4-del");
        await store.DeleteIndexEntry(tx2, "idx_x", key, rowId, unique: true);
        await CommitTransaction(node.Kahuna, tx2);

        ObjectIdValue? found = await store.LookupUnique(HLCTimestamp.Zero, "idx_x", key);
        Assert.IsNull(found);
    }

    // ---- tests: ScanIndex ------------------------------------------------

    [Test]
    public async Task ScanIndex_Unique_ReturnsAllEntries()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s1");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        // Insert three entries (out of order to prove KV ordering).
        (long age, ObjectIdValue rowId)[] entries =
        [
            (30L, new ObjectIdValue(3, 0, 0)),
            (10L, new ObjectIdValue(1, 0, 0)),
            (20L, new ObjectIdValue(2, 0, 0)),
        ];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "s1-put");
        foreach ((long age, ObjectIdValue rowId) in entries)
            await store.PutIndexEntry(tx, "idx_age", CV(new ColumnValue(ColumnType.Integer64, age)), rowId, unique: true);
        await CommitTransaction(node.Kahuna, tx);

        List<(CompositeColumnValue key, ObjectIdValue rowId)> scanned = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, null, null, unique: true))
        {
            scanned.Add((key, rowId));
        }

        Assert.AreEqual(3, scanned.Count);

        // Verify ascending order by the integer key.
        long[] expectedOrder = [10L, 20L, 30L];
        for (int i = 0; i < expectedOrder.Length; i++)
            Assert.AreEqual(expectedOrder[i], scanned[i].key.Values[0].LongValue, $"Position {i}");
    }

    [Test]
    public async Task ScanIndex_NonUnique_ReturnsAllEntries()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s2");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.String];

        // Two rows with the same category "tools", one row with "books".
        (string category, ObjectIdValue rowId)[] entries =
        [
            ("tools", new ObjectIdValue(10, 0, 0)),
            ("books", new ObjectIdValue(20, 0, 0)),
            ("tools", new ObjectIdValue(30, 0, 0)),
        ];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "s2-put");
        foreach ((string category, ObjectIdValue rowId) in entries)
            await store.PutIndexEntry(tx, "idx_cat",
                CV(new ColumnValue(ColumnType.String, category)), rowId, unique: false);
        await CommitTransaction(node.Kahuna, tx);

        List<(CompositeColumnValue key, ObjectIdValue rowId)> scanned = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_cat", keyTypes, null, null, unique: false))
        {
            scanned.Add((key, rowId));
        }

        Assert.AreEqual(3, scanned.Count, "All three non-unique entries must be returned");

        HashSet<string> insertedIds = new(entries.Select(e => e.rowId.ToString()));
        foreach ((_, ObjectIdValue rowId) in scanned)
            Assert.IsTrue(insertedIds.Contains(rowId.ToString()), $"Unexpected rowId {rowId}");
    }

    [Test]
    public async Task ScanIndex_WithFromBound_FiltersCorrectly()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s3");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        long[] ages = [10L, 20L, 30L, 40L, 50L];
        KvTransaction tx = await BeginTransaction(node.Kahuna, "s3-put");
        for (int i = 0; i < ages.Length; i++)
            await store.PutIndexEntry(tx, "idx_age",
                CV(new ColumnValue(ColumnType.Integer64, ages[i])),
                new ObjectIdValue(i + 1, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        CompositeColumnValue from = CV(new ColumnValue(ColumnType.Integer64, 30L));

        List<long> scannedAges = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue _) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, from, null, unique: true))
        {
            scannedAges.Add(key.Values[0].LongValue);
        }

        CollectionAssert.AreEqual(new long[] { 30L, 40L, 50L }, scannedAges);
    }

    [Test]
    public async Task ScanIndex_WithToBound_FiltersCorrectly()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s4");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        long[] ages = [10L, 20L, 30L, 40L, 50L];
        KvTransaction tx = await BeginTransaction(node.Kahuna, "s4-put");
        for (int i = 0; i < ages.Length; i++)
            await store.PutIndexEntry(tx, "idx_age",
                CV(new ColumnValue(ColumnType.Integer64, ages[i])),
                new ObjectIdValue(i + 1, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        CompositeColumnValue to = CV(new ColumnValue(ColumnType.Integer64, 30L));

        List<long> scannedAges = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue _) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, null, to, unique: true))
        {
            scannedAges.Add(key.Values[0].LongValue);
        }

        CollectionAssert.AreEqual(new long[] { 10L, 20L, 30L }, scannedAges);
    }

    [Test]
    public async Task ScanIndex_WithFromAndToBounds_FiltersCorrectly()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s5");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        long[] ages = [10L, 20L, 30L, 40L, 50L];
        KvTransaction tx = await BeginTransaction(node.Kahuna, "s5-put");
        for (int i = 0; i < ages.Length; i++)
            await store.PutIndexEntry(tx, "idx_age",
                CV(new ColumnValue(ColumnType.Integer64, ages[i])),
                new ObjectIdValue(i + 1, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        CompositeColumnValue from = CV(new ColumnValue(ColumnType.Integer64, 20L));
        CompositeColumnValue to   = CV(new ColumnValue(ColumnType.Integer64, 40L));

        List<long> scannedAges = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue _) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, from, to, unique: true))
        {
            scannedAges.Add(key.Values[0].LongValue);
        }

        CollectionAssert.AreEqual(new long[] { 20L, 30L, 40L }, scannedAges);
    }

    [Test]
    public async Task ScanIndex_IsEmpty_WhenNoEntries()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s6");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        List<(CompositeColumnValue, ObjectIdValue)> scanned = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, null, null, unique: true))
        {
            scanned.Add((key, rowId));
        }

        Assert.AreEqual(0, scanned.Count);
    }

    [Test]
    public async Task ScanIndex_RespectsMaxRows()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s6-max");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        long[] ages = [10L, 20L, 30L];
        KvTransaction tx = await BeginTransaction(node.Kahuna, "s6-max-put");
        for (int i = 0; i < ages.Length; i++)
            await store.PutIndexEntry(tx, "idx_age",
                CV(new ColumnValue(ColumnType.Integer64, ages[i])),
                new ObjectIdValue(i + 1, 0, 0), unique: true);
        await CommitTransaction(node.Kahuna, tx);

        List<long> scannedAges = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue _) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, null, null, unique: true, maxRows: 2))
        {
            scannedAges.Add(key.Values[0].LongValue);
        }

        CollectionAssert.AreEqual(new long[] { 10L, 20L }, scannedAges);
    }

    [Test]
    public async Task ScanIndex_NonUnique_SameKeyMultipleRowIds()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s7");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];

        // Three rows all with age = 25 (non-unique index).
        ObjectIdValue[] rowIds = [new(1, 0, 0), new(2, 0, 0), new(3, 0, 0)];
        CompositeColumnValue key25 = CV(new ColumnValue(ColumnType.Integer64, 25L));

        KvTransaction tx = await BeginTransaction(node.Kahuna, "s7-put");
        foreach (ObjectIdValue rowId in rowIds)
            await store.PutIndexEntry(tx, "idx_age", key25, rowId, unique: false);
        await CommitTransaction(node.Kahuna, tx);

        List<ObjectIdValue> scannedIds = [];
        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, null, null, unique: false))
        {
            scannedIds.Add(rowId);
        }

        Assert.AreEqual(3, scannedIds.Count, "All three entries for the same key must be returned");

        HashSet<string> expected = new(rowIds.Select(r => r.ToString()));
        foreach (ObjectIdValue rid in scannedIds)
            Assert.IsTrue(expected.Contains(rid.ToString()), $"Unexpected rowId {rid}");
    }

    [Test]
    public async Task DeleteIndexEntry_NonUnique_RemovesOnlyTargetEntry()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s8");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];
        CompositeColumnValue key = CV(new ColumnValue(ColumnType.Integer64, 10L));

        ObjectIdValue rowId1 = new(1, 0, 0);
        ObjectIdValue rowId2 = new(2, 0, 0);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "s8-put");
        await store.PutIndexEntry(tx1, "idx_age", key, rowId1, unique: false);
        await store.PutIndexEntry(tx1, "idx_age", key, rowId2, unique: false);
        await CommitTransaction(node.Kahuna, tx1);

        // Delete only rowId1's entry.
        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "s8-del");
        await store.DeleteIndexEntry(tx2, "idx_age", key, rowId1, unique: false);
        await CommitTransaction(node.Kahuna, tx2);

        List<ObjectIdValue> remaining = [];
        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_age", keyTypes, null, null, unique: false))
        {
            remaining.Add(rowId);
        }

        Assert.AreEqual(1, remaining.Count, "Only one entry should remain");
        Assert.AreEqual(rowId2.ToString(), remaining[0].ToString());
    }

    [Test]
    public async Task ScanIndex_CompositeKey_RoundTrips()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("s9");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.String, ColumnType.Integer64];

        // (name, age) composite unique index.
        (string name, long age, ObjectIdValue rowId)[] entries =
        [
            ("alice", 30L, new ObjectIdValue(1, 0, 0)),
            ("bob",   25L, new ObjectIdValue(2, 0, 0)),
            ("alice", 25L, new ObjectIdValue(3, 0, 0)),
        ];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "s9-put");
        foreach ((string name, long age, ObjectIdValue rowId) in entries)
        {
            ColumnValue[] cols = [new ColumnValue(ColumnType.String, name), new ColumnValue(ColumnType.Integer64, age)];
            await store.PutIndexEntry(tx, "idx_name_age", new CompositeColumnValue(cols), rowId, unique: true);
        }
        await CommitTransaction(node.Kahuna, tx);

        List<(CompositeColumnValue key, ObjectIdValue rowId)> scanned = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in
            store.ScanIndex(HLCTimestamp.Zero, "idx_name_age", keyTypes, null, null, unique: true))
        {
            scanned.Add((key, rowId));
        }

        Assert.AreEqual(3, scanned.Count);

        // Verify decoded keys round-trip correctly.
        foreach ((CompositeColumnValue key, ObjectIdValue rowId) in scanned)
        {
            Assert.AreEqual(ColumnType.String,    key.Values[0].Type);
            Assert.AreEqual(ColumnType.Integer64, key.Values[1].Type);

            (string name, long age, ObjectIdValue expectedRowId) = entries.First(e =>
                e.name == key.Values[0].StrValue && e.age == key.Values[1].LongValue);
            Assert.AreEqual(expectedRowId.ToString(), rowId.ToString());
        }
    }

    /// <summary>
    /// Verifies that index entries written in one transaction are visible to a
    /// ScanIndex call in a subsequent (different) transaction — i.e. cross-transaction
    /// reads via LocateAndGetByBucket work with a real HLCTimestamp.
    /// </summary>
    [Test]
    public async Task ScanIndex_NonUnique_CrossTransaction_ReturnsAllEntries()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("xt1");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.String];

        (string category, ObjectIdValue rowId)[] entries =
        [
            ("alpha", new ObjectIdValue(1, 0, 0)),
            ("beta",  new ObjectIdValue(2, 0, 0)),
            ("gamma", new ObjectIdValue(3, 0, 0)),
        ];

        // Transaction 1 — write index entries.
        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "xt1-put");
        foreach ((string category, ObjectIdValue rowId) in entries)
            await store.PutIndexEntry(tx1, "idx_cat",
                CV(new ColumnValue(ColumnType.String, category)), rowId, unique: false);
        await CommitTransaction(node.Kahuna, tx1);

        // Transaction 2 — a *different* transaction scans using its own HLC timestamp.
        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "xt1-scan");

        List<(CompositeColumnValue key, ObjectIdValue rowId)> scanned = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId) in
            store.ScanIndex(tx2.TransactionId, "idx_cat", keyTypes, null, null, unique: false))
        {
            scanned.Add((key, rowId));
        }

        Assert.AreEqual(3, scanned.Count,
            "ScanIndex via a different transaction's txId must return all committed entries");

        HashSet<string> ids = new(entries.Select(e => e.rowId.ToString()));
        foreach ((_, ObjectIdValue rowId) in scanned)
            Assert.IsTrue(ids.Contains(rowId.ToString()), $"Unexpected rowId {rowId}");
    }

    // ---- range lock tests (C3) -------------------------------------------

    // Mirrors TestKvTableStore.AcquireRowRangeLock_IsExclusive_AndReleasedOnCommit for index spaces.
    // An index marked as ranged (MarkIndexAsRanged) uses a Kahuna range lock; an unmarked index uses
    // a prefix lock. Both are exclusive within the session and are released on commit.
    [Test]
    public async Task AcquireIndexRangeLock_IsExclusive_AndReleasedOnCommit()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange1");
        await using EmbeddedKahuna __ = node;

        // Mark the index as ranged (simulates what TableOpener does after RegisterKeyRangeAsync).
        // When KeyRangeShardingEnabled is false this call is a no-op in terms of routing, but
        // AcquireIndexRangeLockAsync checks both the flag and the set, so the lock type adapts.
        store.MarkIndexAsRanged("idx_age");

        KvTransactionsManager transactions = new(node.Kahuna);

        static int HeldLocks(KvTransaction tx) =>
            CamusDBConfig.KeyRangeShardingEnabled
                ? tx.GetAcquiredRangeLocks().Count
                : tx.GetAcquiredPrefixLocks().Count;

        KvTransaction tx1 = await transactions.BeginAsync();
        await store.AcquireIndexRangeLockAsync(tx1, "idx_age");
        Assert.AreEqual(1, HeldLocks(tx1), "tx1 must track its index range lock");

        KvTransaction tx2 = await transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await store.AcquireIndexRangeLockAsync(tx2, "idx_age"));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex!.Code,
            "a concurrent index range lock must be rejected as a conflict");

        await transactions.CommitAsync(tx1);

        Assert.DoesNotThrowAsync(async () => await store.AcquireIndexRangeLockAsync(tx2, "idx_age"),
            "the index range must be lockable again once the holder commits");
        Assert.AreEqual(1, HeldLocks(tx2));

        await transactions.CommitAsync(tx2);
    }

    // ---- IsIndexRangeable gate tests (C3) ------------------------------------

    // The per-type gate is the load-bearing correctness decision of the second slice: it must
    // classify numeric/id/bool indexes as rangeable and keep String-keyed ones hash-routed.
    // These tests pin the classification so a regression (e.g. accidentally allowing String)
    // is caught without a full TableOpener integration test.

    [Test]
    public void IsIndexRangeable_Integer64_IsRangeable()
    {
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Integer64 };
        TableIndexSchema entry = new("id1", "idx_age", new[] { "c1" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_Float64_IsRangeable()
    {
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Float64 };
        TableIndexSchema entry = new("id1", "idx_score", new[] { "c1" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_Bool_IsRangeable()
    {
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Bool };
        TableIndexSchema entry = new("id1", "idx_active", new[] { "c1" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_Id_IsRangeable()
    {
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Id };
        TableIndexSchema entry = new("id1", "idx_fk", new[] { "c1" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_String_IsNotRangeable()
    {
        // String stays hash-routed until C3b lands (UTF-8 / UTF-16 persistence divergence).
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.String };
        TableIndexSchema entry = new("id1", "idx_name", new[] { "c1" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsFalse(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_CompositeWithString_IsNotRangeable()
    {
        // A single String column anywhere in the composite disqualifies the whole index.
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Integer64, ["c2"] = ColumnType.String };
        TableIndexSchema entry = new("id1", "idx_composite", new[] { "c1", "c2" }, IndexType.Multi, SchemaElementState.Public);
        Assert.IsFalse(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_CompositeAllNumeric_IsRangeable()
    {
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Integer64, ["c2"] = ColumnType.Bool };
        TableIndexSchema entry = new("id1", "idx_composite", new[] { "c1", "c2" }, IndexType.Multi, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_NullColumnIds_IsNotRangeable()
    {
        // Legacy SystemSchema path may yield an entry without column IDs — conservative fallback.
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Integer64 };
        TableIndexSchema entry = new("idx_legacy", new[] { "col" }, IndexType.Unique);
        Assert.IsFalse(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_UnknownColumnId_IsNotRangeable()
    {
        // If a column ID can't be resolved in the lookup, keep hash-routed (conservative).
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Integer64 };
        TableIndexSchema entry = new("id1", "idx_x", new[] { "c_unknown" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsFalse(TableOpener.IsIndexRangeable(entry, types));
    }

    // An index NOT marked as ranged always uses a prefix lock, even when KeyRangeShardingEnabled
    // is true. This covers the String-keyed-index case (C3b) and the legacy SystemSchema path.
    [Test]
    public async Task AcquireIndexRangeLock_UnmarkedIndex_UsesPrefixLock()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange2");
        await using EmbeddedKahuna __ = node;

        // idx_name is NOT marked as ranged — it should use a prefix lock regardless of the flag.
        KvTransactionsManager transactions = new(node.Kahuna);

        KvTransaction tx1 = await transactions.BeginAsync();
        await store.AcquireIndexRangeLockAsync(tx1, "idx_name");
        Assert.AreEqual(1, tx1.GetAcquiredPrefixLocks().Count, "unmarked index must use a prefix lock");
        Assert.AreEqual(0, tx1.GetAcquiredRangeLocks().Count, "no range lock for unmarked index");

        await transactions.CommitAsync(tx1);
    }
}
