
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
/// KvTableStore secondary index operations:
///   LookupUnique, ScanIndex, PutIndexEntry, DeleteIndexEntry.
/// </summary>
[TestFixture]
public sealed class TestKvTableStoreIndex
{
    // ---- transaction helpers ----------------------------------------------

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

    // ---- node / store factory ---------------------------------------------

    private static async Task<(EmbeddedKahuna node, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        return (node, new KvTableStore(node.Kahuna, CamusDBConfig.Ambient, "testdb", tableId));
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
        ObjectIdValue? rowId = await store.LookupUnique(KvTransaction.CreateReadOnly(), "idx_age", key);

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

        ObjectIdValue? found = await store.LookupUnique(KvTransaction.CreateReadOnly(), "idx_age", key);

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

        ObjectIdValue? found = await store.LookupUnique(KvTransaction.CreateReadOnly(), "idx_x", key);
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
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, null, unique: true))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_cat", keyTypes, null, null, unique: false))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue _, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, from, null, unique: true))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue _, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, to, unique: true))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue _, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, from, to, unique: true))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, null, unique: true))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue _, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, null, unique: true, maxRows: 2))
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
        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, null, unique: false))
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
        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, null, unique: false))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_name_age", keyTypes, null, null, unique: true))
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
        await foreach ((CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(tx2, "idx_cat", keyTypes, null, null, unique: false))
        {
            scanned.Add((key, rowId));
        }

        Assert.AreEqual(3, scanned.Count,
            "ScanIndex via a different transaction's txId must return all committed entries");

        HashSet<string> ids = new(entries.Select(e => e.rowId.ToString()));
        foreach ((_, ObjectIdValue rowId) in scanned)
            Assert.IsTrue(ids.Contains(rowId.ToString()), $"Unexpected rowId {rowId}");
    }

    // ---- range lock tests -------------------------------------------------

    // Scan range locks are SHARED: two concurrent read scans over the same index range coexist
    // (S∩S) rather than conflicting — the serializable, phantom-free guarantee comes from the
    // write-path fence (a foreign write into the range conflicts), not from reader-vs-reader
    // exclusion. Range locks are only active when KeyRangeShardingEnabled=true AND the index is
    // marked as ranged; in single-partition mode the call is a no-op.
    [Test]
    [NonParallelizable]
    public async Task AcquireIndexRangeLock_SharedScansCoexist_AndReleasedOnCommit()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange1");
            await using EmbeddedKahuna __ = node;

            // Mark the index as ranged (simulates what TableOpener does after RegisterKeyRangeAsync).
            store.MarkIndexAsRanged("idx_age");

            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            KvTransaction tx1 = await transactions.BeginAsync();
            await store.AcquireIndexRangeLockAsync(tx1, "idx_age");
            Assert.AreEqual(1, tx1.GetAcquiredRangeLocks().Count, "tx1 must track its index range lock");

            // A second concurrent scan over the same range must COEXIST with tx1's shared lock.
            KvTransaction tx2 = await transactions.BeginAsync();
            Assert.DoesNotThrowAsync(async () => await store.AcquireIndexRangeLockAsync(tx2, "idx_age"),
                "two shared scan locks over the same index range must coexist");
            Assert.AreEqual(1, tx2.GetAcquiredRangeLocks().Count, "tx2 must track its own shared range lock");

            await transactions.CommitAsync(tx1);

            // tx2's lock is unaffected by tx1 committing; it can still be released cleanly.
            Assert.AreEqual(1, tx2.GetAcquiredRangeLocks().Count);
            await transactions.CommitAsync(tx2);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // ---- IsIndexRangeable gate tests -----------------------------------------

    // The per-type gate classifies which indexes may be key-range routed. With the order-safe
    // ASCII-hex String encoding in KeyEncoder, every column type is rangeable; the only remaining
    // disqualifier is missing/unresolvable column IDs (conservative fallback). These tests pin the
    // classification so a regression is caught without a full TableOpener integration test.

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
    public void IsIndexRangeable_String_IsRangeable()
    {
        // String encodes to order-safe ASCII hex (KeyEncoder), so its UTF-8 byte order
        // matches the in-memory UTF-16-ordinal order end-to-end — String indexes are rangeable.
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.String };
        TableIndexSchema entry = new("id1", "idx_name", new[] { "c1" }, IndexType.Unique, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
    }

    [Test]
    public void IsIndexRangeable_CompositeWithString_IsRangeable()
    {
        // A composite mixing numeric and String columns is rangeable: every field encodes to ASCII.
        Dictionary<string, ColumnType> types = new() { ["c1"] = ColumnType.Integer64, ["c2"] = ColumnType.String };
        TableIndexSchema entry = new("id1", "idx_composite", new[] { "c1", "c2" }, IndexType.Multi, SchemaElementState.Public);
        Assert.IsTrue(TableOpener.IsIndexRangeable(entry, types));
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

    // An index NOT marked as ranged (or when key-range sharding is off) acquires NO lock.
    // In single-partition mode MVCC provides snapshot isolation; phantom protection via
    // range locks only activates with KeyRangeShardingEnabled + a registered ranged index.
    [Test]
    public async Task AcquireIndexRangeLock_UnmarkedIndex_AcquiresNoLock()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange2");
        await using EmbeddedKahuna __ = node;

        // idx_name is NOT marked as ranged — no lock should be acquired.
        KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

        KvTransaction tx1 = await transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await store.AcquireIndexRangeLockAsync(tx1, "idx_name");
        Assert.AreEqual(0, tx1.GetAcquiredRangeLocks().Count, "unmarked index must not acquire a range lock");

        await transactions.CommitAsync(tx1);
    }

    // A bounded range lock [1,50] must BLOCK a concurrent write (PutIndexEntry) whose
    // index value falls inside the range, and must ALLOW a write whose value falls outside it.
    // This validates that the encoded bounds in AcquireBoundedIndexRangeLockAsync match actual stored-key
    // positions — the thing that the lock-vs-lock test (below) cannot catch because two self-consistent
    // encodings are always disjoint/overlapping by value order regardless of encoding bugs.
    [Test]
    [NonParallelizable]
    public async Task BoundedIndexRangeLock_BlocksWriteInsideRange_AllowsWriteOutside()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange-f1");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_age");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            CompositeColumnValue cv1  = new(new ColumnValue(ColumnType.Integer64, 1L));
            CompositeColumnValue cv25 = new(new ColumnValue(ColumnType.Integer64, 25L));
            CompositeColumnValue cv50 = new(new ColumnValue(ColumnType.Integer64, 50L));
            CompositeColumnValue cv75 = new(new ColumnValue(ColumnType.Integer64, 75L));

            // txA holds a bounded range lock [1, 50] on idx_age.
            KvTransaction txA = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txA, "idx_age", cv1, true, cv50, true, unique: false);
            Assert.AreEqual(1, txA.GetAcquiredRangeLocks().Count, "txA must hold the [1,50] range lock");

            // txB tries to write value=25 (inside [1,50]) — must be blocked.
            // Kahuna returns MustRetry (key falls in the locked range), so RetryOnMustRetry loops
            // until the CancellationToken fires.  We give it 500 ms; the first retry delay is 1 ms
            // so cancellation happens well before the 32-retry budget would expire.
            KvTransaction txB = await transactions.BeginAsync();
            ObjectIdValue rowId25 = new(25, 0, 0);
            using CancellationTokenSource ctsInside = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txB, "idx_age", cv25, rowId25, unique: false, cancellationToken: ctsInside.Token),
                "write at value=25 must be blocked by the active [1,50] range lock");

            // txC writes value=75 (outside [1,50]) — must succeed without blocking.
            KvTransaction txC = await transactions.BeginAsync();
            ObjectIdValue rowId75 = new(75, 0, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txC, "idx_age", cv75, rowId75, unique: false),
                "write at value=75 must not be blocked — it is outside the locked range");
            await transactions.CommitAsync(txC);

            // After txA releases the range lock, the previously-blocked range becomes writable.
            await transactions.CommitAsync(txA);

            KvTransaction txD = await transactions.BeginAsync();
            ObjectIdValue rowId25b = new(25, 0, 1);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txD, "idx_age", cv25, rowId25b, unique: false),
                "write at value=25 must succeed once the range lock is released");
            await transactions.CommitAsync(txD);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // A promoted read-only scan (key-range mode) is a REAL transaction: it has a non-zero identity,
    // takes an enforced shared range lock during its scan, and releases that lock when it commits —
    // exactly like an implicit single-statement transaction. This proves the autocommit-SELECT
    // promotion end-to-end: the lock is real (a foreign write into the range is blocked while held)
    // and the commit path releases it (the same write succeeds afterwards).
    [Test]
    [NonParallelizable]
    public async Task PromotedReadOnlyScan_HoldsEnforcedSharedRangeLock_ReleasedOnCommit()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange-rop");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_age");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            CompositeColumnValue cv1  = new(new ColumnValue(ColumnType.Integer64, 1L));
            CompositeColumnValue cv25 = new(new ColumnValue(ColumnType.Integer64, 25L));
            CompositeColumnValue cv50 = new(new ColumnValue(ColumnType.Integer64, 50L));

            // A scan SELECT promotes to a real read-only transaction.
            KvTransaction roTx = await transactions.BeginReadOnlyAsync(promote: true);
            Assert.AreNotEqual(HLCTimestamp.Zero, roTx.TransactionId, "a promoted read-only scan must have a real identity");
            Assert.IsTrue(roTx.IsReadOnly, "the promoted scan transaction is still read-only");

            // The read-only scan takes a shared range lock over [1,50].
            await store.AcquireBoundedIndexRangeLockAsync(roTx, "idx_age", cv1, true, cv50, true, unique: false);
            Assert.AreEqual(1, roTx.GetAcquiredRangeLocks().Count, "the read-only scan must hold its shared range lock");

            // While held, a foreign write into the range is blocked (the lock is genuinely enforced).
            KvTransaction writer = await transactions.BeginAsync();
            ObjectIdValue rowId25 = new(25, 0, 0);
            using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(writer, "idx_age", cv25, rowId25, unique: false, cancellationToken: cts.Token),
                "a write inside the range must be blocked while the read-only scan holds the shared lock");

            // Committing the read-only scan releases the lock (no leak, despite no writes).
            await transactions.CommitAsync(roTx);

            KvTransaction writer2 = await transactions.BeginAsync();
            ObjectIdValue rowId25b = new(25, 0, 1);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(writer2, "idx_age", cv25, rowId25b, unique: false),
                "the write must succeed once the read-only scan commits and releases its shared lock");
            await transactions.CommitAsync(writer2);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // The promotion is scoped: a read-only begin stays on the lightweight HLCTimestamp.Zero snapshot
    // when promotion is not requested, or whenever key-range sharding is disabled (single-partition
    // mode, where range locks are no-ops). A Zero transaction has no Kahuna identity and needs no
    // commit/rollback round-trips.
    [Test]
    [NonParallelizable]
    public async Task BeginReadOnly_StaysZeroSnapshot_WhenNotPromotedOrShardingDisabled()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        try
        {
            (EmbeddedKahuna node, _) = await CreateStoreAsync("irange-zero");
            await using EmbeddedKahuna __ = node;
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            // Sharding ON but promotion NOT requested (e.g. a point read) → Zero snapshot.
            CamusDBConfig.KeyRangeShardingEnabled = true;
            KvTransaction noPromote = await transactions.BeginReadOnlyAsync(promote: false);
            Assert.AreEqual(HLCTimestamp.Zero, noPromote.TransactionId, "an un-promoted read-only begin must stay on the Zero snapshot");

            // Promotion requested but sharding OFF → still Zero (range locks are no-ops in hash mode).
            CamusDBConfig.KeyRangeShardingEnabled = false;
            KvTransaction shardingOff = await transactions.BeginReadOnlyAsync(promote: true);
            Assert.AreEqual(HLCTimestamp.Zero, shardingOff.TransactionId, "promotion must be a no-op when key-range sharding is disabled");
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // Bounded scan range locks are SHARED. Two transactions scanning DISJOINT index ranges
    // coexist (non-overlapping locks never conflict regardless of mode); two transactions scanning
    // OVERLAPPING ranges ALSO coexist now that read scans take a shared lock (S∩S). Phantom
    // protection for these ranges is enforced on the write path instead — see the
    // write-fence tests below, which prove a foreign write into a held range conflicts.
    [Test]
    [NonParallelizable]
    public async Task BoundedIndexRangeLock_DisjointAndOverlappingSharedScansCoexist()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange3");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_age");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            CompositeColumnValue low  = new(new ColumnValue(ColumnType.Integer64, 1L));
            CompositeColumnValue mid  = new(new ColumnValue(ColumnType.Integer64, 50L));
            CompositeColumnValue high = new(new ColumnValue(ColumnType.Integer64, 100L));

            // --- Case 1: disjoint ranges [1,50] and [51,100] coexist ---
            CompositeColumnValue boundary = new(new ColumnValue(ColumnType.Integer64, 51L));

            KvTransaction txA = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txA, "idx_age", low, true, mid, true, unique: false);
            Assert.AreEqual(1, txA.GetAcquiredRangeLocks().Count, "txA must hold [1,50] range lock");

            KvTransaction txB = await transactions.BeginAsync();
            Assert.DoesNotThrowAsync(
                async () => await store.AcquireBoundedIndexRangeLockAsync(txB, "idx_age", boundary, true, high, true, unique: false),
                "disjoint [51,100] must not conflict with [1,50]");
            Assert.AreEqual(1, txB.GetAcquiredRangeLocks().Count, "txB must hold [51,100] range lock");

            await transactions.CommitAsync(txA);
            await transactions.CommitAsync(txB);

            // --- Case 2: overlapping ranges [1,75] and [50,100] also coexist (both shared) ---
            CompositeColumnValue r1end  = new(new ColumnValue(ColumnType.Integer64, 75L));
            CompositeColumnValue r2start = mid; // 50

            KvTransaction txC = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txC, "idx_age", low, true, r1end, true, unique: false);

            KvTransaction txD = await transactions.BeginAsync();
            Assert.DoesNotThrowAsync(
                async () => await store.AcquireBoundedIndexRangeLockAsync(txD, "idx_age", r2start, true, high, true, unique: false),
                "two overlapping shared scan ranges must coexist");
            Assert.AreEqual(1, txD.GetAcquiredRangeLocks().Count, "txD must hold its overlapping shared range lock");

            await transactions.CommitAsync(txC);
            await transactions.CommitAsync(txD);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // The non-unique sentinel (￿) must cover every rowId suffix for the upper-bound
    // value.  A write at value=50 (the exact upper bound) with any rowId must be blocked; a write at
    // value=51 must pass through.  This is the case the lock-vs-lock test cannot catch: a missing or
    // wrong sentinel would silently let phantom writes at the boundary value through.
    [Test]
    [NonParallelizable]
    public async Task NonUniqueSentinel_CoversAllRowIdSuffixesAtUpperBound()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange-f2a");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_age");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            CompositeColumnValue cv1  = new(new ColumnValue(ColumnType.Integer64, 1L));
            CompositeColumnValue cv50 = new(new ColumnValue(ColumnType.Integer64, 50L));
            CompositeColumnValue cv51 = new(new ColumnValue(ColumnType.Integer64, 51L));

            KvTransaction txA = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txA, "idx_age", cv1, true, cv50, true, unique: false);

            // Write at value=50 with rowId (0,50,0) — non-unique key is enc(50)+rowId.
            // The sentinel (￿) appended to the encoded upper bound must cover it.
            KvTransaction txB = await transactions.BeginAsync();
            ObjectIdValue rowId50a = new(0, 50, 0);
            using CancellationTokenSource cts50a = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txB, "idx_age", cv50, rowId50a, unique: false, cancellationToken: cts50a.Token),
                "write at value=50 rowId=(0,50,0) must be blocked — sentinel covers all rowId suffixes");

            // Repeat with a different rowId to confirm it is not rowId-specific.
            KvTransaction txC = await transactions.BeginAsync();
            ObjectIdValue rowId50b = new(0, 99, 0);
            using CancellationTokenSource cts50b = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txC, "idx_age", cv50, rowId50b, unique: false, cancellationToken: cts50b.Token),
                "write at value=50 rowId=(0,99,0) must also be blocked");

            // Write at value=51 — enc(51) > enc(50)+sentinel → outside range → must succeed.
            KvTransaction txD = await transactions.BeginAsync();
            ObjectIdValue rowId51 = new(0, 51, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txD, "idx_age", cv51, rowId51, unique: false),
                "write at value=51 must not be blocked — enc(51) is past the sentinel");
            await transactions.CommitAsync(txD);

            await transactions.CommitAsync(txA);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // String bounds exercise the ordered ASCII encoding path. A String-indexed column's lock bounds
    // use a variable-width prefix-free body plus the \x00\x01 terminator. A bug in that path
    // (e.g., a non-prefix-free code or missing terminator) would silently misplace the lock
    // boundary while two lock-vs-lock tests with self-consistent encodings would still agree.
    [Test]
    [NonParallelizable]
    public async Task StringBounds_BlocksWriteInsideRange_AllowsWriteOutside()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange-f2b");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_name");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            CompositeColumnValue cvApple  = new(new ColumnValue(ColumnType.String, "apple"));
            CompositeColumnValue cvBanana = new(new ColumnValue(ColumnType.String, "banana"));
            CompositeColumnValue cvMango  = new(new ColumnValue(ColumnType.String, "mango"));
            CompositeColumnValue cvZebra  = new(new ColumnValue(ColumnType.String, "zebra"));

            // Lock ["apple", "mango"] inclusive on a non-unique String index.
            KvTransaction txA = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txA, "idx_name", cvApple, true, cvMango, true, unique: false);

            // "banana" is inside ["apple","mango"] → must be blocked.
            KvTransaction txB = await transactions.BeginAsync();
            ObjectIdValue rowBanana = new(0, 1, 0);
            using CancellationTokenSource ctsBanana = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txB, "idx_name", cvBanana, rowBanana, unique: false, cancellationToken: ctsBanana.Token),
                "write \"banana\" must be blocked — it is inside [\"apple\",\"mango\"]");

            // "mango" with any rowId — sentinel must cover it.
            KvTransaction txC = await transactions.BeginAsync();
            ObjectIdValue rowMango = new(0, 2, 0);
            using CancellationTokenSource ctsMango = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txC, "idx_name", cvMango, rowMango, unique: false, cancellationToken: ctsMango.Token),
                "write \"mango\" must be blocked — sentinel covers all rowId suffixes at the upper bound");

            // "zebra" is beyond "mango" → must succeed.
            KvTransaction txD = await transactions.BeginAsync();
            ObjectIdValue rowZebra = new(0, 3, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txD, "idx_name", cvZebra, rowZebra, unique: false),
                "write \"zebra\" must not be blocked — it is past the locked range");
            await transactions.CommitAsync(txD);

            await transactions.CommitAsync(txA);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // Exclusive bounds on a UNIQUE index (no rowId suffix, so startInclusive/endInclusive
    // have exact-key semantics).  A write at the exact boundary value must be ALLOWED; a write
    // strictly inside must be BLOCKED.  Non-unique tests cannot cover this cleanly because any rowId
    // suffix makes the write key strictly greater than the startKey regardless of startInclusive.
    [Test]
    [NonParallelizable]
    public async Task ExclusiveBounds_UniqueIndex_BoundaryAllowed_InteriorBlocked()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange-f2c");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_age");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            CompositeColumnValue cv1  = new(new ColumnValue(ColumnType.Integer64, 1L));
            CompositeColumnValue cv25 = new(new ColumnValue(ColumnType.Integer64, 25L));
            CompositeColumnValue cv50 = new(new ColumnValue(ColumnType.Integer64, 50L));

            // Lock (1, 50) exclusive-exclusive on a UNIQUE index.
            KvTransaction txA = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txA, "idx_age", cv1, fromInclusive: false, cv50, toInclusive: false, unique: true);

            // Write at value=1 (exact lower bound, exclusive) → key == startKey → NOT in range → ALLOWED.
            KvTransaction txB = await transactions.BeginAsync();
            ObjectIdValue rowId1 = new(0, 1, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txB, "idx_age", cv1, rowId1, unique: true),
                "write at lower-exclusive boundary (value=1) must be allowed");
            await transactions.CommitAsync(txB);

            // Write at value=25 (strictly inside) → BLOCKED.
            KvTransaction txC = await transactions.BeginAsync();
            ObjectIdValue rowId25 = new(0, 25, 0);
            using CancellationTokenSource cts25 = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txC, "idx_age", cv25, rowId25, unique: true, cancellationToken: cts25.Token),
                "write at interior value=25 must be blocked by the exclusive (1,50) range lock");

            // Write at value=50 (exact upper bound, exclusive) → key == endKey → NOT in range → ALLOWED.
            KvTransaction txD = await transactions.BeginAsync();
            ObjectIdValue rowId50 = new(0, 50, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txD, "idx_age", cv50, rowId50, unique: true),
                "write at upper-exclusive boundary (value=50) must be allowed");
            await transactions.CommitAsync(txD);

            await transactions.CommitAsync(txA);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }

    // Composite (String, Integer64) bounds exercise the multi-field encoding path.
    // ("tools", 25) inside [("tools",10), ("tools",50)] → blocked.
    // ("tools", 75) past the upper bound → allowed.
    // ("books", 25) below the lower bound (different String prefix) → allowed.
    [Test]
    [NonParallelizable]
    public async Task CompositeBounds_BlocksWriteInsideRange_AllowsWriteOutside()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("irange-f2d");
            await using EmbeddedKahuna __ = node;

            store.MarkIndexAsRanged("idx_cat_age");
            KvTransactionsManager transactions = new(node.Kahuna, CamusDBConfig.Ambient);

            static CompositeColumnValue CV2(string cat, long age) => new(new ColumnValue[]
            {
                new(ColumnType.String, cat),
                new(ColumnType.Integer64, age),
            });

            CompositeColumnValue tools10 = CV2("tools", 10L);
            CompositeColumnValue tools25 = CV2("tools", 25L);
            CompositeColumnValue tools50 = CV2("tools", 50L);
            CompositeColumnValue tools75 = CV2("tools", 75L);
            CompositeColumnValue books25 = CV2("books", 25L);

            // Lock [("tools",10), ("tools",50)] inclusive.
            KvTransaction txA = await transactions.BeginAsync();
            await store.AcquireBoundedIndexRangeLockAsync(txA, "idx_cat_age", tools10, true, tools50, true, unique: false);

            // ("tools", 25) is inside the range → blocked.
            KvTransaction txB = await transactions.BeginAsync();
            ObjectIdValue rowTools25 = new(1, 25, 0);
            using CancellationTokenSource ctsTools25 = new(TimeSpan.FromMilliseconds(500));
            Assert.CatchAsync<OperationCanceledException>(
                async () => await store.PutIndexEntry(txB, "idx_cat_age", tools25, rowTools25, unique: false, cancellationToken: ctsTools25.Token),
                "write at (\"tools\",25) must be blocked — it is inside [(\"tools\",10),(\"tools\",50)]");

            // ("tools", 75) is past the upper bound → allowed.
            KvTransaction txC = await transactions.BeginAsync();
            ObjectIdValue rowTools75 = new(1, 75, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txC, "idx_cat_age", tools75, rowTools75, unique: false),
                "write at (\"tools\",75) must not be blocked — it is past the locked range");
            await transactions.CommitAsync(txC);

            // ("books", 25) has a smaller String prefix → below the lower bound → allowed.
            KvTransaction txE = await transactions.BeginAsync();
            ObjectIdValue rowBooks25 = new(2, 25, 0);
            Assert.DoesNotThrowAsync(
                async () => await store.PutIndexEntry(txE, "idx_cat_age", books25, rowBooks25, unique: false),
                "write at (\"books\",25) must not be blocked — \"books\" < \"tools\" places it below the range");
            await transactions.CommitAsync(txE);

            await transactions.CommitAsync(txA);
        }
        finally { CamusDBConfig.KeyRangeShardingEnabled = prev; }
    }
}
