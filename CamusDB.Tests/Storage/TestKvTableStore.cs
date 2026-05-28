
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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// T2.2 — KvTableStore primary row operations.
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
        (KeyValueResponseType type, HLCTimestamp txId) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { UniqueId = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Set, type, "StartTransaction must return Set");

        return new KvTransaction(txId, uniqueId);
    }

    private static async Task CommitTransaction(IKahuna kahuna, KvTransaction tx)
    {
        KeyValueResponseType result = await kahuna.LocateAndCommitTransaction(
            tx.UniqueId,
            tx.TransactionId,
            tx.GetAcquiredLocks(),
            tx.GetModifiedKeys(),
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Committed, result, "Commit must succeed");
    }

    // ---- node factory -----------------------------------------------------

    private static async Task<(EmbeddedKahuna node, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        return (node, new KvTableStore(node.Kahuna, tableId));
    }

    // ---- tests ------------------------------------------------------------

    [Test]
    public async Task GetRow_ReturnsNull_WhenKeyDoesNotExist()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t1");
        await using EmbeddedKahuna __ = node;

        byte[]? result = await store.GetRow(HLCTimestamp.Zero, new ObjectIdValue(1, 2, 3));

        Assert.IsNull(result);
    }

    [Test]
    public async Task InsertRow_ThenGetRow_ReturnsBytes()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t2");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("name", ColumnType.String));
        ObjectIdValue rowId = new(10, 20, 30);
        byte[] data = RowEncoder.Encode(schema, new() { ["name"] = new(ColumnType.String, "alice") }, rowId);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t2-insert");
        await store.InsertRow(tx, rowId, data);
        await CommitTransaction(node.Kahuna, tx);

        byte[]? got = await store.GetRow(HLCTimestamp.Zero, rowId);

        Assert.IsNotNull(got);
        Assert.AreEqual(data, got);
    }

    [Test]
    public async Task UpdateRow_OverwritesExistingBytes()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t3");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("n", ColumnType.Integer64));
        ObjectIdValue rowId = new(1, 1, 1);

        byte[] v1 = RowEncoder.Encode(schema, new() { ["n"] = new(ColumnType.Integer64, 1L) }, rowId);
        byte[] v2 = RowEncoder.Encode(schema, new() { ["n"] = new(ColumnType.Integer64, 2L) }, rowId);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "t3-insert");
        await store.InsertRow(tx1, rowId, v1);
        await CommitTransaction(node.Kahuna, tx1);

        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "t3-update");
        await store.UpdateRow(tx2, rowId, v2);
        await CommitTransaction(node.Kahuna, tx2);

        byte[]? got = await store.GetRow(HLCTimestamp.Zero, rowId);

        Assert.IsNotNull(got);
        Assert.AreEqual(v2, got);
    }

    [Test]
    public async Task DeleteRow_RemovesRow()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t4");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("x", ColumnType.Bool));
        ObjectIdValue rowId = new(5, 5, 5);
        byte[] data = RowEncoder.Encode(schema, new() { ["x"] = new(ColumnType.Bool, true) }, rowId);

        KvTransaction tx1 = await BeginTransaction(node.Kahuna, "t4-insert");
        await store.InsertRow(tx1, rowId, data);
        await CommitTransaction(node.Kahuna, tx1);

        KvTransaction tx2 = await BeginTransaction(node.Kahuna, "t4-delete");
        await store.DeleteRow(tx2, rowId);
        await CommitTransaction(node.Kahuna, tx2);

        byte[]? got = await store.GetRow(HLCTimestamp.Zero, rowId);
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
            byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, (long)id.a) }, id);
            await store.InsertRow(tx, id, data);
        }
        await CommitTransaction(node.Kahuna, tx);

        List<(ObjectIdValue rowId, byte[] data)> scanned = [];
        await foreach ((ObjectIdValue rowId, byte[] data) in store.ScanRows(HLCTimestamp.Zero))
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

        List<(ObjectIdValue, byte[])> scanned = [];
        await foreach ((ObjectIdValue id, byte[] data) in store.ScanRows(HLCTimestamp.Zero))
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
            byte[] data = RowEncoder.Encode(schema, new() { ["n"] = new(ColumnType.Integer64, (long)id.a) }, id);
            await store.InsertRow(tx, id, data);
        }
        await CommitTransaction(node.Kahuna, tx);

        List<ObjectIdValue> scannedIds = [];
        await foreach ((ObjectIdValue rowId, _) in store.ScanRows(HLCTimestamp.Zero))
            scannedIds.Add(rowId);

        Assert.AreEqual(3, scannedIds.Count);

        for (int i = 0; i + 1 < scannedIds.Count; i++)
        {
            Assert.That(
                string.CompareOrdinal(scannedIds[i].ToString(), scannedIds[i + 1].ToString()),
                Is.LessThanOrEqualTo(0),
                $"Scan not sorted at position {i}: {scannedIds[i]} vs {scannedIds[i + 1]}"
            );
        }
    }

    [Test]
    public async Task InsertRow_TracksModifiedAndLockKeys()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("t8");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("k", ColumnType.Integer64));
        ObjectIdValue rowId = new(7, 8, 9);
        byte[] data = RowEncoder.Encode(schema, new() { ["k"] = new(ColumnType.Integer64, 42L) }, rowId);

        KvTransaction tx = await BeginTransaction(node.Kahuna, "t8-track");
        await store.InsertRow(tx, rowId, data);

        Assert.AreEqual(1, tx.GetModifiedKeys().Count, "One modified key must be tracked");
        Assert.AreEqual(1, tx.GetAcquiredLocks().Count, "One acquired lock must be tracked");

        await CommitTransaction(node.Kahuna, tx);
    }
}
