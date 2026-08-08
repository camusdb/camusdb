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
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// KV-level repro: a row deleted after a snapshot timestamp must remain visible when the
/// same store is scanned/read at that snapshot.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestKvAsOfDeletedRepro
{
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

    [Test]
    public async Task DeletedRowVisibleAtPreDeleteSnapshot()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("asofdel/warmup", CancellationToken.None);

        try
        {
            KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, "testdb", "t1");

            ObjectIdValue keepId = ObjectIdGenerator.Generate();
            ObjectIdValue delId = ObjectIdGenerator.Generate();

            KvTransaction tx1 = await BeginTransaction(node.Kahuna, "tx-insert");
            await store.InsertRow(tx1, keepId, [1, 2, 3]);
            await store.InsertRow(tx1, delId, [4, 5, 6]);
            await CommitTransaction(node.Kahuna, tx1);

            await Task.Delay(60);
            HLCTimestamp snapshot = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
            await Task.Delay(60);

            KvTransaction tx2 = await BeginTransaction(node.Kahuna, "tx-delete");
            await store.DeleteRow(tx2, delId);
            await CommitTransaction(node.Kahuna, tx2);

            // Point read at the snapshot
            KvTransaction snapTx = KvTransaction.CreateSnapshotReadOnly(snapshot);
            System.ReadOnlyMemory<byte>? row = await store.GetRow(snapTx, delId);
            TestContext.Progress.WriteLine($"point-read-at-snapshot: found={row is not null}");

            // Scan at the snapshot
            List<string> seen = [];
            await foreach ((ObjectIdValue rowId, System.ReadOnlyMemory<byte> _) in store.ScanRows(snapTx))
                seen.Add(rowId.ToString());
            TestContext.Progress.WriteLine($"scan-at-snapshot: rows={seen.Count} [{string.Join(",", seen)}]");

            // Raw Kahuna probes over the row bucket, latest vs snapshot.
            string bucket = "testdb:t1:r";
            List<string> rawLatest = [];
            await foreach ((string k, _) in node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, bucket, null, true, null, true, 100,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
                rawLatest.Add(k);
            TestContext.Progress.WriteLine($"raw-scan-latest: [{string.Join(",", rawLatest)}]");

            List<string> rawSnap = [];
            await foreach ((string k, _) in node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, bucket, null, true, null, true, 100,
                snapshot, KeyValueDurability.Persistent, CancellationToken.None))
                rawSnap.Add(k);
            TestContext.Progress.WriteLine($"raw-scan-at-snapshot: [{string.Join(",", rawSnap)}]");

            Kahuna.Server.KeyValues.KeyValueGetByBucketResult bucketSnap = await node.Kahuna.LocateAndGetByBucket(
                HLCTimestamp.Zero, bucket, snapshot, KeyValueDurability.Persistent, CancellationToken.None);
            TestContext.Progress.WriteLine(
                $"raw-bucket-at-snapshot: type={bucketSnap.Type} count={bucketSnap.Items?.Count ?? -1}");

            await node.FlushAsync();

            List<string> rawSnapAfterFlush = [];
            await foreach ((string k, _) in node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, bucket, null, true, null, true, 100,
                snapshot, KeyValueDurability.Persistent, CancellationToken.None))
                rawSnapAfterFlush.Add(k);
            TestContext.Progress.WriteLine($"raw-scan-at-snapshot-after-flush: [{string.Join(",", rawSnapAfterFlush)}]");

            Assert.IsNotNull(row, "point read at pre-delete snapshot must find the deleted row");
            Assert.AreEqual(2, seen.Count, "scan at pre-delete snapshot must see both rows");
        }
        finally
        {
            await node.DisposeAsync();
        }
    }
}
