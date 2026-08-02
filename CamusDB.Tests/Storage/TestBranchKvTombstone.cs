
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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Verifies that a <see cref="BranchKvKind.Tombstone"/> record written at a row or index key is
/// treated as a miss by every KvTableStore read path — not as a value, and not confused with a
/// genuine Kahuna DoesNotExist. The tombstone is the level-0 record that branch deletes and stale
/// inherited-index suppression will write; this test exercises the read-side skip before any DML
/// path produces one, using the test-only tombstone writers on the store.
/// </summary>
[TestFixture]
public sealed class TestBranchKvTombstone
{
    private static TableSchema MakeSchema(params TableColumnSchema[] columns)
    {
        List<TableColumnSchema> cols = new(columns);
        List<TableSchemaHistory> history = [new TableSchemaHistory { Version = 0, Columns = cols }];
        return new TableSchema { Id = "test-table", Name = "test", Version = 0, Columns = cols, SchemaHistory = history };
    }

    private static TableColumnSchema Col(string name, ColumnType type) => new(name, name, type, false, null);

    private static CompositeColumnValue CV(ColumnValue value) => new(new[] { value });

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

    private static async Task<(EmbeddedKahuna node, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        return (node, new KvTableStore(node.Kahuna, CamusDBOptions.Default, "testdb", tableId));
    }

    [Test]
    public async Task RowTombstone_MakesGetRowReturnNull()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("tomb1");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("name", ColumnType.String));
        ObjectIdValue rowId = new(11, 22, 33);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["name"] = new(ColumnType.String, "alice") }, rowId);

        KvTransaction insertTx = await BeginTransaction(node.Kahuna, "tomb1-insert");
        await store.InsertRow(insertTx, rowId, data);
        await CommitTransaction(node.Kahuna, insertTx);

        // Sanity: the live value is readable before the tombstone.
        Assert.IsNotNull(await store.GetRow(KvTransaction.CreateReadOnly(), rowId));

        KvTransaction tombTx = await BeginTransaction(node.Kahuna, "tomb1-tombstone");
        await store.WriteRowTombstoneForTesting(tombTx, rowId);
        await CommitTransaction(node.Kahuna, tombTx);

        // The tombstone must read back as a miss, not as the old value.
        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNull(got, "A tombstoned row must read as a miss, not return the stale value");
    }

    [Test]
    public async Task RowTombstone_IsSkippedByScanRows()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("tomb2");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = MakeSchema(Col("v", ColumnType.Integer64));
        ObjectIdValue live = new(1, 0, 0);
        ObjectIdValue doomed = new(2, 0, 0);

        KvTransaction insertTx = await BeginTransaction(node.Kahuna, "tomb2-insert");
        await store.InsertRow(insertTx, live, RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 1L) }, live));
        await store.InsertRow(insertTx, doomed, RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 2L) }, doomed));
        await CommitTransaction(node.Kahuna, insertTx);

        KvTransaction tombTx = await BeginTransaction(node.Kahuna, "tomb2-tombstone");
        await store.WriteRowTombstoneForTesting(tombTx, doomed);
        await CommitTransaction(node.Kahuna, tombTx);

        List<string> scanned = [];
        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in store.ScanRows(KvTransaction.CreateReadOnly()))
            scanned.Add(rowId.ToString());

        Assert.AreEqual(1, scanned.Count, "ScanRows must skip the tombstoned row");
        Assert.Contains(live.ToString(), scanned);
        Assert.IsFalse(scanned.Contains(doomed.ToString()), "Tombstoned row must not appear in a scan");
    }

    [Test]
    public async Task UniqueIndexTombstone_MakesLookupReturnNull()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("tomb3");
        await using EmbeddedKahuna __ = node;

        CompositeColumnValue key = CV(new ColumnValue(ColumnType.Integer64, 42L));
        ObjectIdValue rowId = new(7, 7, 7);

        KvTransaction putTx = await BeginTransaction(node.Kahuna, "tomb3-put");
        await store.PutIndexEntry(putTx, "idx_age", key, rowId, unique: true);
        await CommitTransaction(node.Kahuna, putTx);

        Assert.IsNotNull(await store.LookupUnique(KvTransaction.CreateReadOnly(), "idx_age", key));

        KvTransaction tombTx = await BeginTransaction(node.Kahuna, "tomb3-tombstone");
        await store.WriteUniqueIndexTombstoneForTesting(tombTx, "idx_age", key);
        await CommitTransaction(node.Kahuna, tombTx);

        ObjectIdValue? found = await store.LookupUnique(KvTransaction.CreateReadOnly(), "idx_age", key);
        Assert.IsNull(found, "A tombstoned unique-index entry must read as a miss");
    }

    [Test]
    public async Task UniqueIndexTombstone_IsSkippedByScanIndex()
    {
        (EmbeddedKahuna node, KvTableStore store) = await CreateStoreAsync("tomb4");
        await using EmbeddedKahuna __ = node;

        ColumnType[] keyTypes = [ColumnType.Integer64];
        CompositeColumnValue liveKey = CV(new ColumnValue(ColumnType.Integer64, 10L));
        CompositeColumnValue doomedKey = CV(new ColumnValue(ColumnType.Integer64, 20L));
        ObjectIdValue liveRow = new(1, 1, 1);
        ObjectIdValue doomedRow = new(2, 2, 2);

        KvTransaction putTx = await BeginTransaction(node.Kahuna, "tomb4-put");
        await store.PutIndexEntry(putTx, "idx_age", liveKey, liveRow, unique: true);
        await store.PutIndexEntry(putTx, "idx_age", doomedKey, doomedRow, unique: true);
        await CommitTransaction(node.Kahuna, putTx);

        KvTransaction tombTx = await BeginTransaction(node.Kahuna, "tomb4-tombstone");
        await store.WriteUniqueIndexTombstoneForTesting(tombTx, "idx_age", doomedKey);
        await CommitTransaction(node.Kahuna, tombTx);

        List<string> rows = [];
        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_age", keyTypes, null, null, unique: true))
        {
            rows.Add(rowId.ToString());
        }

        Assert.AreEqual(1, rows.Count, "ScanIndex must skip the tombstoned index entry");
        Assert.Contains(liveRow.ToString(), rows);
        Assert.IsFalse(rows.Contains(doomedRow.ToString()), "Tombstoned index entry must not appear in a scan");
    }
}
