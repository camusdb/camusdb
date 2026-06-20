
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

namespace CamusDB.Tests.Transactions;

/// <summary>
/// KvTransaction status + KvTransactionsManager.
///
/// Tests verify:
///   - Begin creates an active KvTransaction.
///   - Commit marks the transaction Committed; the written data is visible.
///   - Rollback marks the transaction RolledBack; the written data is not visible.
///   - Double-commit and double-rollback throw CamusDBException.
///   - RollbackIfNotCompletedAsync is a no-op for already-completed transactions.
///   - Interleaved transactions from the same manager are independent.
/// </summary>
[TestFixture]
public sealed class TestKvTransactionsManager
{
    // ---- schema helpers ---------------------------------------------------

    private static TableColumnSchema Col(string name, ColumnType type) =>
        new(name, name, type, false, null);

    private static TableSchema SingleCol(ColumnType type)
    {
        List<TableColumnSchema> cols = [Col("v", type)];
        return new TableSchema
        {
            Id = "t", Name = "t", Version = 0,
            Columns = cols,
            SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = cols }]
        };
    }

    // ---- node / manager factory -------------------------------------------

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);

        KvTransactionsManager mgr = new(node.Kahuna);
        KvTableStore store = new(node.Kahuna, tableId);
        return (node, mgr, store);
    }

    // ---- tests ------------------------------------------------------------

    [Test]
    public async Task BeginAsync_CreatesActiveTransaction()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m1");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.AreEqual(KvTransactionStatus.Active, tx.Status);
        Assert.IsFalse(string.IsNullOrEmpty(tx.UniqueId));
    }

    [Test]
    public async Task CommitAsync_MarksTxCommitted()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m2");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.CommitAsync(tx);

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);
    }

    [Test]
    public async Task RollbackAsync_MarksTxRolledBack()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m3");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.RollbackAsync(tx);

        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    [Test]
    public async Task InsertAndCommit_DataIsReadable()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("m4");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(1, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 99L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);
        await mgr.CommitAsync(tx);

        byte[]? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNotNull(got);
        Assert.AreEqual(data, got);
    }

    [Test]
    public async Task InsertAndRollback_DataIsNotVisible()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("m5");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(2, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 42L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);
        await mgr.RollbackAsync(tx);

        byte[]? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNull(got, "Rolled-back data must not be visible");
    }

    [Test]
    public async Task DoubleCommit_ThrowsCamusDBException()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m6");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.CommitAsync(tx);

        Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
    }

    [Test]
    public async Task DoubleRollback_ThrowsCamusDBException()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m7");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.RollbackAsync(tx);

        Assert.ThrowsAsync<CamusDBException>(() => mgr.RollbackAsync(tx));
    }

    [Test]
    public async Task RollbackIfNotCompletedAsync_IsNoOp_WhenAlreadyCommitted()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m8");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.CommitAsync(tx);

        // Must not throw.
        await mgr.RollbackIfNotCompletedAsync(tx);
        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);
    }

    [Test]
    public async Task RollbackIfNotCompletedAsync_IsNoOp_WhenAlreadyRolledBack()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m9");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.RollbackAsync(tx);

        await mgr.RollbackIfNotCompletedAsync(tx);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    [Test]
    public async Task RollbackIfNotCompletedAsync_RollsBack_WhenActive()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m10");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    [Test]
    public async Task TwoIndependentTransactions_DoNotInterfere()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("m11");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = SingleCol(ColumnType.Integer64);

        CamusDB.Core.Util.ObjectIds.ObjectIdValue id1 = new(10, 0, 0);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue id2 = new(20, 0, 0);

        byte[] data1 = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 1L) }, id1);
        byte[] data2 = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 2L) }, id2);

        // tx1 commits; tx2 rolls back.
        KvTransaction tx1 = await mgr.BeginAsync();
        await store.InsertRow(tx1, id1, data1);

        KvTransaction tx2 = await mgr.BeginAsync();
        await store.InsertRow(tx2, id2, data2);

        await mgr.CommitAsync(tx1);
        await mgr.RollbackAsync(tx2);

        byte[]? got1 = await store.GetRow(KvTransaction.CreateReadOnly(), id1);
        byte[]? got2 = await store.GetRow(KvTransaction.CreateReadOnly(), id2);

        Assert.IsNotNull(got1, "Committed row must be visible");
        Assert.IsNull(got2, "Rolled-back row must not be visible");
    }

    [Test]
    public async Task FinallyPattern_RollsBackOnException()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("m12");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(99, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 7L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        try
        {
            await store.InsertRow(tx, rowId, data);
            throw new InvalidOperationException("simulated failure");
        }
        catch (InvalidOperationException)
        {
            await mgr.RollbackIfNotCompletedAsync(tx);
        }

        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        byte[]? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNull(got, "Row written in rolled-back transaction must not be visible");
    }

    [Test]
    public async Task RollbackAllActiveAsync_RollsBackEveryOpenTransaction()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("m13");
        await using EmbeddedKahuna __ = node;

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(100, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 9L) }, rowId);

        KvTransaction tx1 = await mgr.BeginAsync();
        KvTransaction tx2 = await mgr.BeginAsync();
        await store.InsertRow(tx1, rowId, data);

        await mgr.RollbackAllActiveAsync();

        Assert.AreEqual(KvTransactionStatus.RolledBack, tx1.Status);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx2.Status);
        byte[]? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNull(got, "Rows from rolled-back transactions must not be visible");
    }

    // error-code contract: once a transaction is committed, a second commit attempt
    // must throw CADB0501 (TransactionAlreadyCompleted, permanent), NOT the retryable
    // CADB0504 (TransactionMustRetry). The distinction lets callers decide:
    //   CADB0504 = routing failure → retry the whole operation from BeginAsync
    //   CADB0501 = permanent failure → do not retry
    [Test]
    public async Task CommitAsync_ThrowsTransactionAlreadyCompleted_NotMustRetry_OnDoubleCommit()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m14");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.CommitAsync(tx);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, ex!.Code,
            "Double-commit must throw permanent CADB0501, not the retryable CADB0504");
        Assert.AreNotEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code,
            "CADB0504 is reserved for transient routing failures; double-commit is permanent");
    }

    // error-code contract: a rolled-back transaction cannot be committed;
    // the error must be CADB0501 (permanent), never CADB0504 (retryable).
    [Test]
    public async Task CommitAsync_ThrowsTransactionAlreadyCompleted_NotMustRetry_AfterRollback()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore _) = await CreateAsync("m15");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.RollbackAsync(tx);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, ex!.Code,
            "Committing a rolled-back tx must throw permanent CADB0501");
        Assert.AreNotEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code);
    }

    // commit MustRetry fault injection: when LocateAndCommitTransaction returns
    // MustRetry fewer times than MaxCommitRetries (5), CommitAsync internally retries and
    // ultimately succeeds. The written row must be visible and the tx is Committed.
    [Test]
    public async Task CommitAsync_WithTransientMustRetry_RetriesAndSucceeds()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fault-a/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        // 3 MustRetry responses before delegating to the real Kahuna (< MaxCommitRetries = 5).
        CommitFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: 3);
        KvTransactionsManager mgr = new(faultKahuna);
        KvTableStore store = new(node.Kahuna, "fault-a");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(200, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 42L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);
        await mgr.CommitAsync(tx);

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status,
            "CommitAsync must succeed after retrying through transient MustRetry responses");

        byte[]? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNotNull(got, "Row written in a successfully committed tx must be visible");
        Assert.AreEqual(data, got);
    }

    // commit MustRetry fault injection: when LocateAndCommitTransaction returns
    // MustRetry more times than MaxCommitRetries (5), CommitAsync exhausts all retries and
    // throws CADB0504 (TransactionMustRetry), never CADB0501 (TransactionAlreadyCompleted).
    [Test]
    public async Task CommitAsync_WithPersistentMustRetry_ThrowsTransactionMustRetry()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fault-b/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        // 6 MustRetry responses — exhausts all MaxCommitRetries = 5 attempts.
        CommitFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: 6);
        KvTransactionsManager mgr = new(faultKahuna);
        KvTableStore store = new(node.Kahuna, "fault-b");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(201, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new() { ["v"] = new(ColumnType.Integer64, 99L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex!.Code,
            "Exhausted MustRetry must surface as CADB0504, not the permanent CADB0501");
        Assert.AreNotEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, ex.Code,
            "CADB0501 is reserved for permanent failures; a routing retry-exhaustion is CADB0504");
    }

    // Delegating IKahuna wrapper that injects MustRetry responses from
    // LocateAndCommitTransaction for the first mustRetryCount calls, then delegates to the
    // real inner IKahuna. All other methods forward directly.
    private sealed class CommitFaultKahuna(IKahuna inner, int mustRetryCount) : IKahuna
    {
        private int mustRetryRemaining = mustRetryCount;

        public Task<KeyValueResponseType> LocateAndCommitTransaction(
            string uniqueId, HLCTimestamp timestamp,
            List<KeyValueTransactionModifiedKey> acquiredLocks,
            List<KeyValueTransactionModifiedKey> modifiedKeys,
            List<KeyValueTransactionReadKey> readKeys,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref mustRetryRemaining) >= 0)
                return Task.FromResult(KeyValueResponseType.MustRetry);
            return inner.LocateAndCommitTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, readKeys, cancellationToken);
        }

        // --- all remaining methods delegate to inner ---

        public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryLock(resource, owner, expiresMs, durability, cancellationToken);

        public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryExtendLock(resource, owner, expiresMs, durability, cancellationToken);

        public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryUnlock(resource, owner, durability, cancellationToken);

        public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndGetLock(resource, durability, cancellationToken);

        public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
            => inner.TryLock(resource, owner, expiresMs, durability);

        public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
            => inner.TryExtendLock(resource, owner, expiresMs, durability);

        public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability)
            => inner.TryUnlock(resource, owner, durability);

        public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability)
            => inner.GetLock(resource, durability);

        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0)
            => inner.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);

        public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken)
            => inner.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);

        public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken)
            => inner.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);

        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

        public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);

        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryGetManyValues(transactionId, keys, cancellationToken);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryExistsManyValues(transactionId, keys, cancellationToken);

        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);

        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);

        public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);

        public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);

        public IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct)
            => inner.LocateAndScanRange(txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct);

        public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration = 0)
            => inner.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, routedGeneration);

        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability)
            => inner.TryExtendKeyValue(transactionId, key, expiresMs, durability);

        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability)
            => inner.TryDeleteKeyValue(transactionId, key, durability);

        public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items)
            => inner.DeleteManyNodeKeyValue(items);

        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability)
            => inner.TryGetValue(transactionId, key, revision, readTimestamp, durability);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys)
            => inner.TryGetManyValues(transactionId, keys);

        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability)
            => inner.TryExistsValue(transactionId, key, revision, readTimestamp, durability);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(HLCTimestamp transactionId, List<(string key, long revision, KeyValueDurability durability)> keys)
            => inner.TryExistsManyValues(transactionId, keys);

        public Task<KeyValueResponseType> TryCheckWriteIntentValue(HLCTimestamp transactionId, string key, KeyValueDurability durability)
            => inner.TryCheckWriteIntentValue(transactionId, key, durability);

        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancellationToken);

        public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);

        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, cancellationToken);

        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken)
            => inner.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);

        public Task<(KeyValueResponseType, HLCTimestamp)> TryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode)
            => inner.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode);

        public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace)
            => inner.GetRangeLocks(keySpace);

        public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks)
            => inner.ImportRangeLocks(keySpace, locks);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancellationToken);

        public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);

        public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);

        public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancellationToken);

        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0)
            => inner.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancellationToken, routedGeneration);

        public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancellationToken);

        public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancellationToken);

        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryCommitManyMutations(transactionId, keys, cancellationToken);

        public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancellationToken);

        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
            => inner.LocateAndTryRollbackManyMutations(transactionId, keys, cancellationToken);

        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
            => inner.LocateAndStartTransaction(options, cancellationToken);

        public Task<KeyValueResponseType> LocateAndRollbackTransaction(string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
            => inner.LocateAndRollbackTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, cancellationToken);

        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability)
            => inner.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);

        public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability)
            => inner.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);

        public Task<(KeyValueResponseType, HLCTimestamp)> TryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability)
            => inner.TryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability);

        public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability)
            => inner.TryReleaseExclusiveLock(transactionId, key, durability);

        public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability)
            => inner.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);

        public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability)
            => inner.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration = 0)
            => inner.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration);

        public Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)
            => inner.TryCommitMutations(transactionId, key, proposalTicketId, durability);

        public Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)
            => inner.TryRollbackMutations(transactionId, key, proposalTicketId, durability);

        public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters)
            => inner.TryExecuteTransactionScript(script, hash, parameters);

        public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
            => inner.GetByBucket(transactionId, prefixKeyName, readTimestamp, durability);

        public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
            => inner.ScanByPrefix(prefixKeyName, readTimestamp, durability);

        public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
            => inner.ScanAllByPrefix(prefixKeyName, readTimestamp, durability, cancellationToken);

        public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options)
            => inner.StartTransaction(options);

        public Task<KeyValueResponseType> CommitTransaction(HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, List<KeyValueTransactionReadKey> readKeys)
            => inner.CommitTransaction(timestamp, acquiredLocks, modifiedKeys, readKeys);

        public Task<KeyValueResponseType> RollbackTransaction(HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys)
            => inner.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);

        public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndGetSequence(name, durability, cancellationToken);

        public Task<(SequenceResponseType, long)> LocateAndCreateSequence(string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndCreateSequence(name, initialValue, increment, maxValue, durability, cancellationToken);

        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndNextSequenceValue(name, idempotencyKey, durability, cancellationToken);

        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndReserveSequenceRange(name, count, idempotencyKey, durability, cancellationToken);

        public Task<SequenceResponseType> LocateAndDeleteSequence(string name, SequenceDurability durability, CancellationToken cancellationToken)
            => inner.LocateAndDeleteSequence(name, durability, cancellationToken);

        public Task<bool> OnLogRestored(int partitionId, RaftLog log)
            => inner.OnLogRestored(partitionId, log);

        public Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
            => inner.OnReplicationReceived(partitionId, log);

        public void OnReplicationError(int partitionId, RaftLog log)
            => inner.OnReplicationError(partitionId, log);

        public Task FlushPersistenceAsync()
            => inner.FlushPersistenceAsync();

        public void RegisterKeyRange(string keySpace)
            => inner.RegisterKeyRange(keySpace);

        public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
            => inner.RegisterKeyRangeAsync(keySpace, cancellationToken);

        public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default)
            => inner.TriggerAutoSplitAsync(ct);

        public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default)
            => inner.TriggerAutoMergeAsync(ct);

        public bool IsBackupConfigured => inner.IsBackupConfigured;

        public Task BootstrapFromPitrBackupAsync(string backupDir, Guid leafBackupId, HLCTimestamp targetTime, IWAL walAdapter, TimeSpan pitrWindow, TimeSpan baseSnapshotInterval)
            => inner.BootstrapFromPitrBackupAsync(backupDir, leafBackupId, targetTime, walAdapter, pitrWindow, baseSnapshotInterval);

        public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken ct = default)
            => inner.TakeFullBackupAsync(ct);

        public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken ct = default)
            => inner.TakeIncrementalBackupAsync(parentBackupId, ct);

        public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default)
            => inner.TakeCoordinatedBackupAsync(ct);

        public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken ct = default)
            => inner.ListBackupsAsync(ct);

        public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken ct = default)
            => inner.GetBackupChainAsync(leafBackupId, ct);

        public Task<KahunaRestoreResponse> RestoreToAsync(Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken ct = default)
            => inner.RestoreToAsync(leafBackupId, targetDir, targetTimeMs, ct);
    }
}
