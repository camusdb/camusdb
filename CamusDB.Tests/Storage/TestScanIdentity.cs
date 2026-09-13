/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
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
/// The identity a range scan carries to Kahuna (<see cref="KvTransaction.RangeScanIdentity"/>), and
/// the failure a scan Kahuna gives up on surfaces as.
///
/// <para>A scan that carries the transaction id pins every visited key's revision server-side, and a
/// page retried after a transient aborts the read once one of those keys has been overwritten — the
/// mechanism that turned a read-only <c>COUNT(*)</c> under replication lag into a <c>CADB0504</c>
/// (feature e31cf9bc). The id is therefore carried only when the scan has something to observe
/// through it: folded reads (optimistic / TrackAndValidate) or the transaction's own pending writes.
/// </para>
/// </summary>
[TestFixture]
public sealed class TestScanIdentity
{
    /// <summary>Records the identity and coordinator key of every range scan, and can fail the scan
    /// the way Kahuna's streaming scan does — with a <see cref="KahunaServerException"/> carrying the
    /// page's response type.</summary>
    private sealed class RecordingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public List<HLCTimestamp> ScanIdentities { get; } = [];
        public List<string> ScanCoordinatorKeys { get; } = [];
        public KeyValueResponseType? FailScansWith;

        public override IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
            HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            ScanIdentities.Add(txId);
            ScanCoordinatorKeys.Add(coordinatorKey);

            if (FailScansWith is KeyValueResponseType type)
                return FailingScan(prefix, type, ct);

            return inner.LocateAndScanRange(txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct, coordinatorKey, operationId);
        }

        private static async IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> FailingScan(
            string prefix, KeyValueResponseType type, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            throw new KahunaServerException(
                $"Range scan page 0 over '{prefix}' [-inf,+inf) failed with {type}; the scan was aborted instead of returning a truncated result.",
                type);
#pragma warning disable CS0162 // unreachable: the iterator must contain a yield to be an iterator
            yield break;
#pragma warning restore CS0162
        }
    }

    private static async Task<(EmbeddedKahuna node, RecordingKahuna stub, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        RecordingKahuna stub = new(node.Kahuna);
        return (node, stub, new KvTableStore(stub, CamusDBOptions.Default, "testdb", tableId));
    }

    private static async Task<KvTransaction> BeginAsync(IKahuna kahuna, string uniqueId, KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = locking },
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(handle.TransactionId, uniqueId, locking: locking);
    }

    private static async Task CommitAsync(IKahuna kahuna, KvTransaction tx)
    {
        (KeyValueResponseType result, _) = await kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Committed, result);
    }

    private static async Task<int> CountRowsAsync(KvTableStore store, KvTransaction tx)
    {
        int count = 0;
        await foreach ((ObjectIdValue _, System.ReadOnlyMemory<byte> _) in store.ScanRows(tx))
            count++;
        return count;
    }

    [Test]
    public async Task ReadCommittedScanWithoutWrites_CarriesNoIdentity()
    {
        (EmbeddedKahuna node, RecordingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_scan_id_ro");
        await using EmbeddedKahuna __ = node;

        KvTransaction writer = await BeginAsync(stub, "scan_id_seed");
        await store.InsertRow(writer, Generate(), [1, 2, 3]);
        await store.InsertRow(writer, Generate(), [4, 5, 6]);
        await CommitAsync(stub, writer);
        stub.ScanIdentities.Clear();

        KvTransaction reader = await BeginAsync(stub, "scan_id_reader");
        Assert.That(reader.TransactionId, Is.Not.EqualTo(HLCTimestamp.Zero), "the reader owns a real Kahuna session");
        Assert.That(reader.FoldReads, Is.False);
        Assert.That(reader.HasPendingWrites, Is.False);

        Assert.That(await CountRowsAsync(store, reader), Is.EqualTo(2));
        await CommitAsync(stub, reader);

        Assert.That(stub.ScanIdentities, Has.Count.EqualTo(1));
        Assert.That(stub.ScanIdentities[0], Is.EqualTo(HLCTimestamp.Zero),
            "a read-committed scan that folds nothing and has no own writes to see must not carry the transaction id: " +
            "the id pins every scanned key server-side and a retried page then aborts the read");
        Assert.That(stub.ScanCoordinatorKeys[0], Is.Empty);
    }

    [Test]
    public async Task ScanAfterOwnWrite_CarriesTheIdentityAndSeesTheWrite()
    {
        (EmbeddedKahuna node, RecordingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_scan_id_rw");
        await using EmbeddedKahuna __ = node;

        KvTransaction seed = await BeginAsync(stub, "scan_id_rw_seed");
        await store.InsertRow(seed, Generate(), [1]);
        await CommitAsync(stub, seed);
        stub.ScanIdentities.Clear();

        KvTransaction tx = await BeginAsync(stub, "scan_id_rw");
        Assert.That(await CountRowsAsync(store, tx), Is.EqualTo(1));
        Assert.That(stub.ScanIdentities[0], Is.EqualTo(HLCTimestamp.Zero), "no writes yet: untracked scan");

        await store.InsertRow(tx, Generate(), [2]);
        Assert.That(tx.HasPendingWrites, Is.True);
        Assert.That(tx.RangeScanIdentity, Is.EqualTo(tx.TransactionId));

        Assert.That(await CountRowsAsync(store, tx), Is.EqualTo(2), "read-your-own-writes needs the transaction id");
        Assert.That(stub.ScanIdentities[1], Is.EqualTo(tx.TransactionId));
        await CommitAsync(stub, tx);
    }

    [Test]
    public async Task OptimisticScan_CarriesTheIdentityAndFoldsReads()
    {
        (EmbeddedKahuna node, RecordingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_scan_id_occ");
        await using EmbeddedKahuna __ = node;

        KvTransaction seed = await BeginAsync(stub, "scan_id_occ_seed");
        await store.InsertRow(seed, Generate(), [1]);
        await CommitAsync(stub, seed);
        stub.ScanIdentities.Clear();

        KvTransaction tx = await BeginAsync(stub, "scan_id_occ", KeyValueTransactionLocking.Optimistic);
        Assert.That(tx.FoldReads, Is.True);
        Assert.That(tx.RangeScanIdentity, Is.EqualTo(tx.TransactionId));

        Assert.That(await CountRowsAsync(store, tx), Is.EqualTo(1));
        await CommitAsync(stub, tx);

        Assert.That(stub.ScanIdentities[0], Is.EqualTo(tx.TransactionId), "a folded scan is tracked by design");
        Assert.That(stub.ScanCoordinatorKeys[0], Is.EqualTo(tx.CoordinatorKey));
    }

    [Test]
    public void SnapshotReadOnlyTransaction_HasNoScanIdentity()
    {
        KvTransaction snapshot = new(
            HLCTimestamp.Zero, "snap", isReadOnly: true,
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly,
            readTimestamp: new HLCTimestamp(0, 1_000, 0));

        Assert.That(snapshot.RangeScanIdentity, Is.EqualTo(HLCTimestamp.Zero));
        Assert.That(snapshot.FoldReads, Is.False);
    }

    [Test]
    public async Task ScanKahunaGivesUpOn_SurfacesAsRetryableWordedForARead()
    {
        (EmbeddedKahuna node, RecordingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_scan_id_fail");
        await using EmbeddedKahuna __ = node;

        stub.FailScansWith = KeyValueResponseType.Aborted;

        KvTransaction reader = await BeginAsync(stub, "scan_id_fail");
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await CountRowsAsync(store, reader))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
        Assert.That(ex.Message, Does.Contain("row scan"));
        Assert.That(ex.Message, Does.Contain("Aborted"));
        Assert.That(ex.Message, Does.Not.Contain("Batched set"), "a read must never be reported as a failed write");
    }

    [Test]
    public async Task IndexScanKahunaGivesUpOn_SurfacesAsRetryableWordedForARead()
    {
        (EmbeddedKahuna node, RecordingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_scan_idx_fail");
        await using EmbeddedKahuna __ = node;

        stub.FailScansWith = KeyValueResponseType.WaitingForReplication;

        KvTransaction reader = await BeginAsync(stub, "scan_idx_fail");
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await foreach ((CompositeColumnValue _, ObjectIdValue _, System.ReadOnlyMemory<byte> _) in
                store.ScanIndex(reader, "ix_missing", [ColumnType.Integer64], from: null, to: null, unique: false))
            {
            }
        })!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
        Assert.That(ex.Message, Does.Contain("index scan"));
        Assert.That(ex.Message, Does.Contain("WaitingForReplication"));
        Assert.That(stub.ScanIdentities[0], Is.EqualTo(HLCTimestamp.Zero), "an index scan follows the same identity rule");
    }
}
