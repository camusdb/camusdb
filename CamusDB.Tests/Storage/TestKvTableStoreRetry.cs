
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using static CamusDB.Core.Util.ObjectIds.ObjectIdGenerator;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Generation-fence retry audit.
///
/// Every key-range-touching call site in <see cref="KvTableStore"/> must absorb a transient
/// <see cref="KeyValueResponseType.MustRetry"/> (the generation-fence response during a
/// key-range split) and re-resolve rather than propagate the error to the caller.
///
/// <list type="bullet">
///   <item>Single-key point ops go through <c>RetryOnMustRetry</c> (private static helper).</item>
///   <item>Batch lock/set/delete ops have their own per-key inline retry loops
///         (<c>AcquireManyWithRetry</c>, <c>SetManyWithRetry</c>, <c>DeleteManyWithRetry</c>).</item>
///   <item>Scan ops (<c>ScanRows</c>, <c>ScanIndex</c>, <c>DropIndexEntries</c>) call
///         <c>LocateAndScanRange</c> which handles <c>MustRetry</c> internally inside Kahuna's
///         <c>KeyValuesManager</c> page-level backoff + cursor-resume loop — no CamusDB-side
///         retry is needed or tested here.</item>
/// </list>
/// </summary>
[TestFixture]
public sealed class TestKvTableStoreRetry
{
    // -----------------------------------------------------------------------
    // Fault-injecting IKahuna wrapper
    // -----------------------------------------------------------------------

    /// <summary>
    /// Wraps a real <see cref="IKahuna"/> and returns
    /// <see cref="KeyValueResponseType.MustRetry"/> N times for specific operations before
    /// delegating to the underlying node. All other interface methods are delegated unchanged.
    /// </summary>
    private sealed class FaultInjectingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        // Decrement post-check counters. Set to N before the call under test;
        // the wrapper returns MustRetry N times then delegates to the real node.
        public int InjectAcquireLockFaults;
        public int InjectGetValueFaults;
        public int InjectSetKeyValueFaults;
        public int InjectDeleteKeyValueFaults;
        public int InjectAcquireManyFaults;
        public int InjectSetManyFaults;
        public int InjectDeleteManyFaults;
        public int InjectRangeLockFaults;

        // Operation ids seen per batch/range call, in order — lets a test assert the id is reused across
        // an unchanged transient resend and freshly minted only when the pending set shrinks.
        public List<TransactionOperationId> SetManyOpIds { get; } = [];
        public List<TransactionOperationId> AcquireManyOpIds { get; } = [];
        public List<TransactionOperationId> RangeLockOpIds { get; } = [];

        // ---- intercepted: single-key exclusive lock ----
        public override Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> LocateAndTryAcquireExclusiveLock(
            HLCTimestamp txId, string key, int expiresMs, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (InjectAcquireLockFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, string.Empty, durability, HLCTimestamp.Zero));
            return inner.LocateAndTryAcquireExclusiveLock(txId, key, expiresMs, durability, ct, coordinatorKey, operationId);
        }

        // ---- intercepted: single-key get ----
        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp txId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (InjectGetValueFaults-- > 0)
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.MustRetry, null));
            if (InjectGetValueTerminalFaults-- > 0)
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((GetValueTerminalType, null));
            return inner.LocateAndTryGetValue(txId, key, revision, readTimestamp, durability, ct, coordinatorKey, operationId);
        }

        // ---- intercepted: single-key get, non-confirmed terminal answer ----
        // Unlike InjectGetValueFaults (a transient the retry loop must absorb), this makes the get
        // return a NON-confirmed terminal type (e.g. Errored) that the retry loop does not retry.
        // ProbeRaw must surface it as TransactionMustRetry, never as a row miss.
        public int InjectGetValueTerminalFaults;
        public KeyValueResponseType GetValueTerminalType = KeyValueResponseType.Errored;

        // ---- intercepted: batch get, per-key non-confirmed answer ----
        public int InjectGetManyErroredFaults;

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
            HLCTimestamp txId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys,
            CancellationToken ct, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (InjectGetManyErroredFaults-- > 0)
                return Task.FromResult(keys
                    .Select(k => (KeyValueResponseType.Errored, k.key, k.durability, (ReadOnlyKeyValueEntry?)null))
                    .ToList());
            return inner.LocateAndTryGetManyValues(txId, readTimestamp, keys, ct, coordinatorKey, operationId);
        }

        // ---- intercepted: single-key set ----
        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp txId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken ct, long routedGeneration = 0,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (InjectSetKeyValueFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, -1L, HLCTimestamp.Zero));
            return inner.LocateAndTrySetKeyValue(txId, key, value, compareValue, compareRevision, flags, expiresMs, durability, ct, routedGeneration, coordinatorKey, operationId);
        }

        // ---- intercepted: single-key delete ----
        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
            HLCTimestamp txId, string key, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (InjectDeleteKeyValueFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, -1L, HLCTimestamp.Zero));
            return inner.LocateAndTryDeleteKeyValue(txId, key, durability, ct, coordinatorKey, operationId);
        }

        // ---- intercepted: batch acquire locks ----
        public override async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
            HLCTimestamp txId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            AcquireManyOpIds.Add(operationId);

            if (InjectAcquireManyFaults-- > 0)
                return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();

            return await inner.LocateAndTryAcquireManyExclusiveLocks(txId, keys, ct, coordinatorKey, operationId).ConfigureAwait(false);
        }

        // ---- intercepted: batch set ----
        // For the whole-batch case (InjectSetManyFaults), ALL keys return MustRetry and nothing
        // is written to the real node on the faulted call(s).
        //
        // For the partial-mix case (SetManyPartialFaultPredicate), keys matching the predicate
        // return MustRetry WITHOUT being sent to the real node; all other keys are delegated
        // immediately. The predicate is cleared after the first activation so the retry (which
        // sends only the faulted keys) goes straight to the real node. This exercises the guard
        // in SetManyWithRetry that rebuilds pending from MustRetry-only responses rather than
        // resending the full original batch — if that guard regressed, an already-Set unique key
        // would come back NotSet → false DuplicateUniqueKeyValue.
        public Func<string, bool>? SetManyPartialFaultPredicate;

        public override async Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
            List<KahunaSetKeyValueRequestItem> items, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            SetManyOpIds.Add(operationId);

            if (InjectSetManyFaults-- > 0)
                return items.Select(i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry }).ToList();

            if (SetManyPartialFaultPredicate is { } pred)
            {
                SetManyPartialFaultPredicate = null; // one-shot: next call goes straight to inner
                List<KahunaSetKeyValueRequestItem> toSet   = items.Where(i => !pred(i.Key ?? "")).ToList();
                List<KahunaSetKeyValueRequestItem> toFault = items.Where(i =>  pred(i.Key ?? "")).ToList();

                List<KahunaSetKeyValueResponseItem> innerResults = toSet.Count > 0
                    ? await inner.LocateAndTrySetManyKeyValue(toSet, ct, coordinatorKey, operationId).ConfigureAwait(false)
                    : [];

                return [..innerResults, ..toFault.Select(i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry })];
            }

            return await inner.LocateAndTrySetManyKeyValue(items, ct, coordinatorKey, operationId).ConfigureAwait(false);
        }

        // ---- intercepted: batch delete ----
        public override Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
            List<KahunaDeleteKeyValueRequestItem> items, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (InjectDeleteManyFaults-- > 0)
                return Task.FromResult(items.Select(i => new KahunaDeleteKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry }).ToList());
            return inner.LocateAndTryDeleteManyKeyValue(items, ct, coordinatorKey, operationId);
        }

        // ---- intercepted: range lock ----
        public override Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
            HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            RangeLockOpIds.Add(operationId);
            if (InjectRangeLockFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, HLCTimestamp.Zero));
            return inner.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
                expiresMs, durability, mode, cancellationToken, coordinatorKey, operationId);
        }
    }

    // -----------------------------------------------------------------------
    // Transaction helpers
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Node / store factory
    // -----------------------------------------------------------------------

    private static async Task<(EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        FaultInjectingKahuna stub = new(node.Kahuna);
        return (node, stub, new KvTableStore(stub, CamusDBOptions.Default, "testdb", tableId));
    }

    // -----------------------------------------------------------------------
    // Single-key retry tests (RetryOnMustRetry)
    // -----------------------------------------------------------------------

    [Test]
    public async Task GetRow_SurvivesMustRetryOnGet()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_get");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();
        byte[] data = [1, 2, 3, 4];

        KvTransaction writeTx = await BeginTransaction(stub, "rt_get_w");
        await store.InsertRow(writeTx, rowId, data);
        await CommitTransaction(stub, writeTx);

        stub.InjectGetValueFaults = 2;

        KvTransaction readTx = await BeginTransaction(stub, "rt_get_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result);
        Assert.AreEqual(data, result!.Value.ToArray());
        Assert.AreEqual(-1, stub.InjectGetValueFaults, "All 2 injected faults were consumed before success");
    }

    [Test]
    public async Task InsertRow_AcquireLock_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_lock");
        await using EmbeddedKahuna __ = node;

        stub.InjectAcquireLockFaults = 2;

        ObjectIdValue rowId = Generate();
        byte[] data = [10, 20, 30];

        KvTransaction tx = await BeginTransaction(stub, "rt_lock_w");
        await store.InsertRow(tx, rowId, data);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectAcquireLockFaults, "All 2 injected lock faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_lock_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result);
        Assert.AreEqual(data, result!.Value.ToArray());
    }

    [Test]
    public async Task InsertRow_SetKeyValue_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_set");
        await using EmbeddedKahuna __ = node;

        stub.InjectSetKeyValueFaults = 2;

        ObjectIdValue rowId = Generate();
        byte[] data = [5, 6, 7, 8];

        KvTransaction tx = await BeginTransaction(stub, "rt_set_w");
        await store.InsertRow(tx, rowId, data);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectSetKeyValueFaults, "All 2 injected set faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_set_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result);
        Assert.AreEqual(data, result!.Value.ToArray());
    }

    [Test]
    public async Task DeleteRow_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_del");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();

        KvTransaction writeTx = await BeginTransaction(stub, "rt_del_w");
        await store.InsertRow(writeTx, rowId, [1, 2, 3]);
        await CommitTransaction(stub, writeTx);

        stub.InjectDeleteKeyValueFaults = 2;

        KvTransaction deleteTx = await BeginTransaction(stub, "rt_del_d");
        await store.DeleteRow(deleteTx, rowId);
        await CommitTransaction(stub, deleteTx);

        Assert.AreEqual(-1, stub.InjectDeleteKeyValueFaults, "All 2 injected delete faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_del_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNull(result, "Row must be absent after DeleteRow");
    }

    // -----------------------------------------------------------------------
    // Unknown-is-not-absent tests
    //
    // Regression for the bank-soak atomicity violation (soak run K, SUM(balance) −3): an exhausted
    // transient or a non-confirmed terminal read answer was converted into a definitive row miss.
    // An UPDATE's locate phase then matched 0 rows and reported success, and because the transient
    // read folds no coordinator observation, commit validation could not catch the dropped write.
    // A read that cannot confirm the key's state must FAIL the statement, never report absence.
    // -----------------------------------------------------------------------

    [Test]
    public async Task GetRow_ExhaustedMustRetry_ThrowsInsteadOfReportingMiss()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_unknown_get");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();

        KvTransaction writeTx = await BeginTransaction(stub, "unk_get_w");
        await store.InsertRow(writeTx, rowId, [1, 2, 3]);
        await CommitTransaction(stub, writeTx);

        // Never stops faulting: the whole retry budget is consumed and the read stays MustRetry.
        stub.InjectGetValueFaults = int.MaxValue;

        KvTransaction readTx = await BeginTransaction(stub, "unk_get_r");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await store.GetRow(readTx, rowId))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code,
            "an exhausted transient must surface as a retryable failure, not as a missing row");

        stub.InjectGetValueFaults = 0;
        await stub.LocateAndRollbackTransaction(readTx.Handle, CancellationToken.None);
    }

    [Test]
    public async Task GetRow_ErroredAnswer_ThrowsInsteadOfReportingMiss()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_unknown_err");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();

        KvTransaction writeTx = await BeginTransaction(stub, "unk_err_w");
        await store.InsertRow(writeTx, rowId, [4, 5, 6]);
        await CommitTransaction(stub, writeTx);

        // Errored is terminal (the retry loop does not absorb it) but NOT a confirmed absence.
        stub.InjectGetValueTerminalFaults = 1;

        KvTransaction readTx = await BeginTransaction(stub, "unk_err_r");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await store.GetRow(readTx, rowId))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code,
            "an Errored read answer must surface as a retryable failure, not as a missing row");

        await stub.LocateAndRollbackTransaction(readTx.Handle, CancellationToken.None);
    }

    [Test]
    public async Task GetRow_ConfirmedAbsence_StillReadsAsMiss()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_confirmed_miss");
        await using EmbeddedKahuna __ = node;

        // A key that was never written: the node answers DoesNotExist — a confirmed absence — and
        // the unknown-is-not-absent rule must NOT turn that into an error.
        KvTransaction readTx = await BeginTransaction(stub, "miss_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, Generate());
        await CommitTransaction(stub, readTx);

        Assert.IsNull(result, "a confirmed absence still reads as a missing row");
    }

    [Test]
    public async Task GetRowsBatch_ErroredKey_ThrowsInsteadOfReportingMiss()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_unknown_batch");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();

        KvTransaction writeTx = await BeginTransaction(stub, "unk_batch_w");
        await store.InsertRow(writeTx, rowId, [7, 8, 9]);
        await CommitTransaction(stub, writeTx);

        stub.InjectGetManyErroredFaults = 1;

        KvTransaction readTx = await BeginTransaction(stub, "unk_batch_r");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await store.GetRowsBatch(readTx, [rowId]))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code,
            "a per-key Errored batch answer must surface as a retryable failure, not as a missing row");

        await stub.LocateAndRollbackTransaction(readTx.Handle, CancellationToken.None);
    }

    // -----------------------------------------------------------------------
    // Batch retry tests — write path
    // -----------------------------------------------------------------------

    [Test]
    public async Task WriteRowsBatch_AcquireManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_batchlock");
        await using EmbeddedKahuna __ = node;

        const int Count = 5;
        List<(ObjectIdValue rowId, byte[] data)> expected = Enumerable.Range(0, Count)
            .Select(i => (Generate(), new byte[] { (byte)i }))
            .ToList();

        List<KvTableStore.RowWrite> batch = expected.Select(e => new KvTableStore.RowWrite
        {
            RowId = e.rowId,
            RowData = BranchKvCodec.EncodeValue(e.data),
        }).ToList();

        stub.InjectAcquireManyFaults = 2;

        KvTransaction tx = await BeginTransaction(stub, "rt_batchlock_w");
        await store.WriteRowsBatch(tx, batch);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectAcquireManyFaults, "All 2 batch-acquire faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_batchlock_r");
        foreach ((ObjectIdValue rowId, byte[] data) in expected)
        {
            ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
            Assert.IsNotNull(result, $"Row {rowId} must be readable after WriteRowsBatch");
            Assert.AreEqual(data, result!.Value.ToArray());
        }
        await CommitTransaction(stub, readTx);
    }

    [Test]
    public async Task WriteRowsBatch_SetManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_batchset");
        await using EmbeddedKahuna __ = node;

        const int Count = 5;
        List<(ObjectIdValue rowId, byte[] data)> expected = Enumerable.Range(0, Count)
            .Select(i => (Generate(), new byte[] { (byte)(i + 10) }))
            .ToList();

        List<KvTableStore.RowWrite> batch = expected.Select(e => new KvTableStore.RowWrite
        {
            RowId = e.rowId,
            RowData = BranchKvCodec.EncodeValue(e.data),
        }).ToList();

        stub.InjectSetManyFaults = 2;

        KvTransaction tx = await BeginTransaction(stub, "rt_batchset_w");
        await store.WriteRowsBatch(tx, batch);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectSetManyFaults, "All 2 batch-set faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_batchset_r");
        foreach ((ObjectIdValue rowId, byte[] data) in expected)
        {
            ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
            Assert.IsNotNull(result, $"Row {rowId} must be readable after WriteRowsBatch");
            Assert.AreEqual(data, result!.Value.ToArray());
        }
        await CommitTransaction(stub, readTx);
    }

    // -----------------------------------------------------------------------
    // Batch retry tests — delete path
    // -----------------------------------------------------------------------

    [Test]
    public async Task DeleteRowsBatch_AcquireManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_delock");
        await using EmbeddedKahuna __ = node;

        const int Count = 4;
        List<ObjectIdValue> rowIds = Enumerable.Range(0, Count).Select(_ => Generate()).ToList();

        // Write rows without fault injection
        List<KvTableStore.RowWrite> batch = rowIds.Select(id => new KvTableStore.RowWrite
        {
            RowId = id,
            RowData = BranchKvCodec.EncodeValue([99]),
        }).ToList();

        KvTransaction writeTx = await BeginTransaction(stub, "rt_delock_w");
        await store.WriteRowsBatch(tx: writeTx, rows: batch);
        await CommitTransaction(stub, writeTx);

        // Now delete with injected AcquireMany faults
        List<KvTableStore.RowDelete> deletes = rowIds.Select(id => new KvTableStore.RowDelete { RowId = id }).ToList();

        stub.InjectAcquireManyFaults = 2;

        KvTransaction deleteTx = await BeginTransaction(stub, "rt_delock_d");
        await store.DeleteRowsBatch(deleteTx, deletes);
        await CommitTransaction(stub, deleteTx);

        Assert.AreEqual(-1, stub.InjectAcquireManyFaults, "All 2 batch-acquire-for-delete faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_delock_r");
        foreach (ObjectIdValue rowId in rowIds)
        {
            ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
            Assert.IsNull(result, $"Row {rowId} must be absent after DeleteRowsBatch");
        }
        await CommitTransaction(stub, readTx);
    }

    [Test]
    public async Task DeleteRowsBatch_DeleteManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_delmany");
        await using EmbeddedKahuna __ = node;

        const int Count = 4;
        List<ObjectIdValue> rowIds = Enumerable.Range(0, Count).Select(_ => Generate()).ToList();

        List<KvTableStore.RowWrite> batch = rowIds.Select(id => new KvTableStore.RowWrite
        {
            RowId = id,
            RowData = BranchKvCodec.EncodeValue([42]),
        }).ToList();

        KvTransaction writeTx = await BeginTransaction(stub, "rt_delmany_w");
        await store.WriteRowsBatch(tx: writeTx, rows: batch);
        await CommitTransaction(stub, writeTx);

        List<KvTableStore.RowDelete> deletes = rowIds.Select(id => new KvTableStore.RowDelete { RowId = id }).ToList();

        stub.InjectDeleteManyFaults = 2;

        KvTransaction deleteTx = await BeginTransaction(stub, "rt_delmany_d");
        await store.DeleteRowsBatch(deleteTx, deletes);
        await CommitTransaction(stub, deleteTx);

        Assert.AreEqual(-1, stub.InjectDeleteManyFaults, "All 2 batch-delete faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_delmany_r");
        foreach (ObjectIdValue rowId in rowIds)
        {
            ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
            Assert.IsNull(result, $"Row {rowId} must be absent after DeleteRowsBatch");
        }
        await CommitTransaction(stub, readTx);
    }

    // -----------------------------------------------------------------------
    // Partial-mix SetMany test — pins the duplicate-avoidance guard
    // -----------------------------------------------------------------------

    /// <summary>
    /// Covers the realistic split scenario: the first <c>LocateAndTrySetManyKeyValue</c> call
    /// returns a partial response — the unique index entry is <c>Set</c>, while the (non-unique) row
    /// key returns <c>MustRetry</c> without being written. The retry must resend <em>only</em> the
    /// <c>MustRetry</c> (row) key; if it mistakenly resent the already-<c>Set</c> unique index key
    /// too, that key would come back <c>NotSet</c> on re-attempt, which <c>SetManyWithRetry</c>
    /// promotes to <c>DuplicateUniqueKeyValue</c> — a false positive. The unique key must be the
    /// already-Set one, since re-Setting the non-unique row (<c>Flags.Set</c>) is a harmless overwrite.
    ///
    /// The <see cref="FaultInjectingKahuna.SetManyPartialFaultPredicate"/> is used to split the
    /// batch on the first call: it delegates all non-faulted keys immediately (they are committed
    /// to the real node) and returns <c>MustRetry</c> for the matching key without writing it.
    /// The predicate is cleared so the retry goes straight to the real node.
    /// </summary>
    [Test]
    public async Task WriteRowsBatch_SetManyPartialMustRetry_DoesNotResendAlreadySetUniqueKey()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_partial");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();
        byte[] rowData = [7, 8, 9];

        // Unique index entry — this is the key that gets SET on the first (partial) call and must
        // therefore NOT be resent. Fixed composite value so its KV key reliably contains ":i:".
        CompositeColumnValue indexKey = new([new ColumnValue(ColumnType.Integer64, 42L)]);
        const string IndexId = "idx_unique_test";

        KvTableStore.RowWrite row = new()
        {
            RowId = rowId,
            RowData = BranchKvCodec.EncodeValue(rowData),
            IndexEntries = [new KvTableStore.IndexWrite(IndexId, indexKey, Unique: true)],
        };

        // Fault predicate: the first call faults the NON-index keys (the row key "{tableId}:r/…")
        // and SETs the unique index key. The retry must resend only the row key; if it also resent
        // the already-Set unique index key, that key would come back NotSet → DuplicateUniqueKeyValue.
        // (The unique key MUST be the already-Set one — re-Setting a non-unique row is a harmless
        // overwrite, so faulting the index key instead would not exercise this guard.)
        stub.SetManyPartialFaultPredicate = key => !key.Contains(":i:", StringComparison.Ordinal);

        KvTransaction tx = await BeginTransaction(stub, "rt_partial_w");
        // Should complete without throwing DuplicateUniqueKeyValue.
        await store.WriteRowsBatch(tx, [row]);
        await CommitTransaction(stub, tx);

        Assert.IsNull(stub.SetManyPartialFaultPredicate, "Predicate must have been consumed on the first call");

        // Verify the row is readable.
        KvTransaction readTx = await BeginTransaction(stub, "rt_partial_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result, "Row must be readable after WriteRowsBatch with partial MustRetry");
        Assert.AreEqual(rowData, result!.Value.ToArray());

        // Verify the unique index entry is present by doing a point lookup.
        KvTransaction lookupTx = await BeginTransaction(stub, "rt_partial_lu");
        ObjectIdValue? found = await store.LookupUnique(lookupTx, IndexId, indexKey);
        await CommitTransaction(stub, lookupTx);

        Assert.AreEqual(rowId, found, "Unique index entry must map to the inserted row");
    }

    // -----------------------------------------------------------------------
    // Stable operation identity across the transient-retry resend
    //
    // These pin the CamusDB half of the contract: the operation id bound to a batch/range declaration
    // is RETAINED while the pending set is resent unchanged, and only re-minted when the pending set
    // shrinks (a genuinely new declaration). The retained id is what lets Kahuna's
    // ParticipantOperationCache fold a lost-ack retry exactly once; that end-to-end idempotent replay
    // is Kahuna's own contract (and is covered by Kahuna's tests) — it cannot be driven from here,
    // because the completion-loss window lives inside the node and is invisible to this outer wrapper.
    // -----------------------------------------------------------------------

    /// <summary>
    /// A whole-batch set-many transient (every key <c>MustRetry</c>, nothing applied) is resent
    /// unchanged: the pending set did not shrink, so the retry must reuse the SAME operation id. That
    /// stable identity is the precondition for Kahuna to replay a lost-ack completion exactly once
    /// instead of re-running <c>SetIfNotExists</c> and fabricating a duplicate.
    /// </summary>
    [Test]
    public async Task WriteRowsBatch_SetManyFullRetry_ReusesSameId()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_setid_full");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();
        byte[] rowData = [1, 2, 3];
        CompositeColumnValue indexKey = new([new ColumnValue(ColumnType.Integer64, 77L)]);
        const string IndexId = "idx_setid_unique";

        KvTableStore.RowWrite row = new() { RowId = rowId, RowData = BranchKvCodec.EncodeValue(rowData), IndexEntries = [new KvTableStore.IndexWrite(IndexId, indexKey, Unique: true)] };

        stub.InjectSetManyFaults = 1; // whole batch MustRetry once, nothing written; retry resends unchanged

        KvTransaction tx = await BeginTransaction(stub, "setid_full_w");
        await store.WriteRowsBatch(tx, [row]);
        await CommitTransaction(stub, tx);

        Assert.That(stub.SetManyOpIds.Count, Is.GreaterThanOrEqualTo(2), "the transient forced a set-many resend");
        Assert.That(stub.SetManyOpIds.Distinct().Count(), Is.EqualTo(1),
            "an unchanged (full-batch) resend must reuse the same operation id");

        KvTransaction readTx = await BeginTransaction(stub, "setid_full_r");
        ReadOnlyMemory<byte>? result = await store.GetRow(readTx, rowId);
        ObjectIdValue? found = await store.LookupUnique(readTx, IndexId, indexKey);
        await CommitTransaction(stub, readTx);

        Assert.AreEqual(rowData, result!.Value.ToArray(), "row must be readable after the resend");
        Assert.AreEqual(rowId, found, "unique index entry must map to the row exactly once");
    }

    /// <summary>
    /// Partial transient: the first set-many confirms the unique index key and faults only the
    /// (non-unique) row key. The retry resends a STRICTLY SMALLER batch — a genuinely new declaration —
    /// so a fresh operation id must be minted. Reusing the id would fold the shrunk batch into the
    /// original registration; this asserts the "shrank ⟹ new id" half of the identity rule.
    /// </summary>
    [Test]
    public async Task WriteRowsBatch_SetManyPartialTransient_ShrinkMintsNewId()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_shrink_set");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();
        CompositeColumnValue indexKey = new([new ColumnValue(ColumnType.Integer64, 88L)]);
        const string IndexId = "idx_shrink_unique";

        KvTableStore.RowWrite row = new() { RowId = rowId, RowData = BranchKvCodec.EncodeValue([4, 5]), IndexEntries = [new KvTableStore.IndexWrite(IndexId, indexKey, Unique: true)] };

        // First call SETs the unique index key and faults only the row key; the retry carries just the
        // row key (a smaller batch), which must trigger a fresh operation id.
        stub.SetManyPartialFaultPredicate = key => !key.Contains(":i:", StringComparison.Ordinal);

        KvTransaction tx = await BeginTransaction(stub, "shrink_set_w");
        await store.WriteRowsBatch(tx, [row]);
        await CommitTransaction(stub, tx);

        Assert.That(stub.SetManyOpIds.Count, Is.GreaterThanOrEqualTo(2), "the partial fault forced a resend");
        Assert.That(stub.SetManyOpIds[0], Is.Not.EqualTo(stub.SetManyOpIds[1]),
            "a shrinking (partial) resend must mint a fresh operation id");
    }

    /// <summary>
    /// A whole-batch lock acquisition transient (every key <c>MustRetry</c>) is resent unchanged: the
    /// pending set did not shrink, so the retry must reuse the same operation id. Re-acquiring the same
    /// locks under the same id lets the coordinator fold the acquisition once rather than double-fold it.
    /// </summary>
    [Test]
    public async Task WriteRowsBatch_AcquireManyFullRetry_ReusesSameId()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_lockid_full");
        await using EmbeddedKahuna __ = node;

        List<(ObjectIdValue rowId, byte[] data)> expected = Enumerable.Range(0, 3)
            .Select(i => (Generate(), new byte[] { (byte)i }))
            .ToList();
        List<KvTableStore.RowWrite> batch = expected
            .Select(e => new KvTableStore.RowWrite { RowId = e.rowId, RowData = BranchKvCodec.EncodeValue(e.data) })
            .ToList();

        stub.InjectAcquireManyFaults = 1; // whole batch MustRetry once; retry resends the same keys

        KvTransaction tx = await BeginTransaction(stub, "lockid_full_w");
        await store.WriteRowsBatch(tx, batch);
        await CommitTransaction(stub, tx);

        Assert.That(stub.AcquireManyOpIds.Count, Is.GreaterThanOrEqualTo(2), "the transient forced an acquire-many resend");
        Assert.That(stub.AcquireManyOpIds.Distinct().Count(), Is.EqualTo(1),
            "an unchanged (full-batch) lock resend must reuse the same operation id");

        KvTransaction readTx = await BeginTransaction(stub, "lockid_full_r");
        foreach ((ObjectIdValue rowId, byte[] data) in expected)
            Assert.AreEqual(data, (await store.GetRow(readTx, rowId))!.Value.ToArray(), $"row {rowId} must be readable after the resend");
        await CommitTransaction(stub, readTx);
    }

    /// <summary>
    /// A range-lock acquisition retried through transient <c>MustRetry</c> must retain the same
    /// operation id across every attempt. A lost completion ack on a range lock would otherwise strand
    /// the original registration and double-fold the lock into the session working set. Only a confirmed
    /// <c>AlreadyLocked</c> denial (not exercised here) starts a fresh attempt id.
    /// </summary>
    [Test]
    public async Task AcquireRowRangeLock_MustRetry_RetainsSameId()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_rangeid");
        await using EmbeddedKahuna __ = node;

        (KeyValueResponseType type, TransactionHandle handle) = await stub.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = "rangeid_w", Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Set, type);

        // Serializable + read-write is the only shape that acquires range locks with an isolation-
        // critical expiry, so this drives AcquireRangeLockAsync down its real acquisition path.
        KvTransaction tx = new(handle.TransactionId, "rangeid_w", isReadOnly: false,
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        stub.InjectRangeLockFaults = 2; // two transient MustRetry before the real acquire

        await store.AcquireRowRangeLockAsync(tx, exclusive: true);

        Assert.That(stub.RangeLockOpIds.Count, Is.EqualTo(3), "two MustRetry retries plus the successful acquire");
        Assert.That(stub.RangeLockOpIds.Distinct().Count(), Is.EqualTo(1),
            "a range-lock retried through transient MustRetry must retain the same operation id");

        await stub.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);
    }

    // -----------------------------------------------------------------------
    // Diagnostics: a conflict that outlives the deadline must name the object and the waiter
    // -----------------------------------------------------------------------

    /// <summary>
    /// Builds a store whose database and table carry user-facing names, so the conflict messages
    /// can be asserted against what an operator would actually see in a log.
    /// </summary>
    private static async Task<(EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store)> CreateNamedStoreAsync(
        string tableId, string dbName, string tableName)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        FaultInjectingKahuna stub = new(node.Kahuna);
        return (node, stub, new KvTableStore(stub, CamusDBOptions.Default, "testdb", tableId, tableName, null, null, dbName));
    }

    [Test]
    public async Task WriteRow_LockWaitDeadline_ReportsDatabaseTableKeyAndTransaction()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) =
            await CreateNamedStoreAsync("tbl_diag_row", "inventory", "robots");
        await using EmbeddedKahuna __ = node;

        // Never stops faulting, so the wall-clock deadline — not the retry budget — ends the loop.
        stub.InjectSetKeyValueFaults = int.MaxValue;

        ObjectIdValue rowId = Generate();
        KvTransaction tx = await BeginTransaction(stub, "diag_row_w");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await store.InsertRow(tx, rowId, [1, 2, 3]))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code);
        Assert.That(ex.Message, Does.Contain("Lock-wait deadline exceeded"));
        Assert.That(ex.Message, Does.Contain("row write"), "the failing operation is named");
        Assert.That(ex.Message, Does.Contain("table 'robots'"));
        Assert.That(ex.Message, Does.Contain("database 'inventory'"));
        Assert.That(ex.Message, Does.Contain("testdb:tbl_diag_row"), "the raw key prefix ties the message to KV traces");
        Assert.That(ex.Message, Does.Contain(rowId.ToString()), "the contended row is identified");
        Assert.That(ex.Message, Does.Contain("diag_row_w"), "the waiting transaction is identified");

        await stub.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);
    }

    [Test]
    public async Task WriteRowsBatch_LockWaitDeadline_ReportsConflictingKeyCount()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) =
            await CreateNamedStoreAsync("tbl_diag_batch", "inventory", "robots");
        await using EmbeddedKahuna __ = node;

        List<KvTableStore.RowWrite> batch = Enumerable.Range(0, 5)
            .Select(i => new KvTableStore.RowWrite
            {
                RowId = Generate(),
                RowData = BranchKvCodec.EncodeValue([(byte)i]),
            })
            .ToList();

        stub.InjectSetManyFaults = int.MaxValue;

        KvTransaction tx = await BeginTransaction(stub, "diag_batch_w");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await store.WriteRowsBatch(tx, batch))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code);
        Assert.That(ex.Message, Does.Contain("batched write"));
        Assert.That(ex.Message, Does.Contain("5 key(s) still conflicting"));
        Assert.That(ex.Message, Does.Contain("table 'robots'"));
        Assert.That(ex.Message, Does.Contain("database 'inventory'"));
        Assert.That(ex.Message, Does.Contain("diag_batch_w"));

        await stub.LocateAndRollbackTransaction(tx.Handle, CancellationToken.None);
    }
}
