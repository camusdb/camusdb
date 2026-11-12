/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;
using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.App.Grpc;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Kahuna answers <c>SnapshotTooOld</c> to a snapshot read whose timestamp is older than the revision
/// history the store still keeps. CamusDB must raise that as its own error, not as an absent key, and
/// not as a retry: most of its read code decodes any answer other than <c>Get</c> as "the key is
/// absent", so the answer would otherwise read as a missing row, and a retry at the same timestamp
/// gets the same answer.
///
/// <para>The store tests drive the real read paths of <see cref="KvTableStore"/> over a node that
/// refuses every read, which is what a read below the pruned history meets. A real refusal needs
/// revisions older than the retention age; Kahuna's own tests build that store directly.</para>
/// </summary>
[NonParallelizable]
[TestFixture]
public sealed class TestSnapshotTooOldAnswer
{
    private const string PrimaryKey = CamusDBConstants.PrimaryKeyInternalName;

    [Test]
    public void GetRetry_RaisesTheRefusal_AfterOneAttempt()
    {
        int calls = 0;

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                calls++;
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.SnapshotTooOld, null));
            },
            CancellationToken.None))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld));
        Assert.That(calls, Is.EqualTo(1), "a read at the same timestamp gets the same answer, so a retry only delays the error");
    }

    [Test]
    public void TypeOnlyRetry_RaisesTheRefusal_AfterOneAttempt()
    {
        int calls = 0;

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                calls++;
                return Task.FromResult(KeyValueResponseType.SnapshotTooOld);
            },
            CancellationToken.None))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void TheCheck_RaisesEachRefusalWithItsOwnCode()
    {
        CamusDBException tooOld = Assert.Throws<CamusDBException>(() => KahunaRetryPolicy.ThrowIfReadRefused(KeyValueResponseType.SnapshotTooOld))!;
        Assert.That(tooOld.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld));

        CamusDBException damaged = Assert.Throws<CamusDBException>(() => KahunaRetryPolicy.ThrowIfReadRefused(KeyValueResponseType.StorageCorrupt))!;
        Assert.That(damaged.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
    }

    [Test]
    public void AScanBelowThePrunedHistory_IsNotRetryable()
    {
        CamusDBException ex = KvScanFailure.Describe(
            new KahunaServerException("page refused", KeyValueResponseType.SnapshotTooOld), "row scan of table t");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld));
        Assert.That(ex.Message, Does.Contain("row scan of table t"));
        Assert.That(ex.Message, Does.Contain("page refused"), "Kahuna's own message is kept");
    }

    [Test]
    public void TheErrorCode_MapsToItsTransportStatus()
    {
        Assert.That(CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.SnapshotTooOld), Is.EqualTo(409));
        Assert.That(GrpcErrorMapper.GetGrpcStatus(CamusDBErrorCodes.SnapshotTooOld), Is.EqualTo(StatusCode.FailedPrecondition),
            "not Aborted: a client that retries an aborted call at the same timestamp would loop");
    }

    [Test]
    public async Task APointRead_RaisesTheRefusal_NotAnAbsentRow()
    {
        await using Harness harness = await Harness.StartAsync("sto-point");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await harness.Store.GetRow(harness.Snapshot(), new ObjectIdValue(1, 2, 3)))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld), ex.Message);
    }

    [Test]
    public async Task ABatchRead_RaisesTheRefusal_NotAbsentRows()
    {
        await using Harness harness = await Harness.StartAsync("sto-batch");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await harness.Store.GetRowsBatch(harness.Snapshot(), [new ObjectIdValue(1, 2, 3), new ObjectIdValue(4, 5, 6)]))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld), ex.Message);
    }

    [Test]
    public async Task ARowScan_RaisesTheRefusal_NotAShortResult()
    {
        await using Harness harness = await Harness.StartAsync("sto-scan");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await foreach ((ObjectIdValue _, ReadOnlyMemory<byte> _) in harness.Store.ScanRows(harness.Snapshot()))
            {
            }
        })!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld), ex.Message);
    }

    [Test]
    public async Task AUniqueLookup_RaisesTheRefusal_NotAMiss()
    {
        await using Harness harness = await Harness.StartAsync("sto-unique");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await harness.Store.LookupUnique(harness.Snapshot(), PrimaryKey, new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, 1)])))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SnapshotTooOld), ex.Message);
    }

    /// <summary>One embedded node, wrapped so that every read answers <c>SnapshotTooOld</c>, and a table store over the wrapper.</summary>
    private sealed class Harness : IAsyncDisposable
    {
        private readonly EmbeddedKahuna node;

        private Harness(EmbeddedKahuna node, KvTableStore store)
        {
            this.node = node;
            Store = store;
        }

        public KvTableStore Store { get; }

        public KvTransaction Snapshot() => KvTransaction.CreateSnapshotReadOnly(new HLCTimestamp(0, 1_000, 0));

        public static async Task<Harness> StartAsync(string tableId)
        {
            EmbeddedKahuna node = new();
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);

            CamusDBOptions options = CamusDBOptions.Default with { PointReadRowIdCacheEnabled = false };
            return new Harness(node, new KvTableStore(new TooOldKahuna(node.Kahuna), options, "testdb", tableId));
        }

        public async ValueTask DisposeAsync() => await node.DisposeAsync();
    }

    /// <summary>
    /// Test-only: a node whose read history is gone. Each point read, batch probe and scan page answers
    /// <c>SnapshotTooOld</c>, the way Kahuna answers a read below its pruned-history floor. A scan
    /// throws, because Kahuna's scan enumerator reports a refused page with an exception.
    /// </summary>
    private sealed class TooOldKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public override ValueTask<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability,
            CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default,
            KeyValueReadMode readMode = KeyValueReadMode.Default, KeyValueIntentWait intentWait = default)
            => ValueTask.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.SnapshotTooOld, null));

        public override Task<KeyValueProbeResult[]> LocateAndTryProbeMany(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp, IReadOnlyList<string> keys, KeyValueProbeLock lockMode,
            int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "",
            TransactionOperationId operationId = default, KeyValueReadMode readMode = KeyValueReadMode.Default, KeyValueIntentWait intentWait = default)
        {
            KeyValueProbeResult[] results = new KeyValueProbeResult[keys.Count];
            for (int i = 0; i < results.Length; i++)
                results[i] = new KeyValueProbeResult(KeyValueResponseType.SnapshotTooOld, null, HLCTimestamp.Zero, false);

            return Task.FromResult(results);
        }

        public override IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
            HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default,
            KeyValueScanObservationSink? observations = null, KeyValueReadMode readMode = KeyValueReadMode.Default)
            => RefusedScan(ct);

        private static async IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> RefusedScan([EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            throw new KahunaServerException("Range scan page 0 failed with SnapshotTooOld", KeyValueResponseType.SnapshotTooOld);
#pragma warning disable CS0162 // The iterator needs a yield to compile as one.
            yield break;
#pragma warning restore CS0162
        }
    }
}
