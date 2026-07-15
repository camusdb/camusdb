
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
        KvTableStore store = new(node.Kahuna, "testdb", tableId);
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
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 99L) }, rowId);

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
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 42L) }, rowId);

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

        byte[] data1 = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 1L) }, id1);
        byte[] data2 = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 2L) }, id2);

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
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 7L) }, rowId);

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
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 9L) }, rowId);

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
        KvTableStore store = new(node.Kahuna, "testdb", "fault-a");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(200, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 42L) }, rowId);

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
        KvTableStore store = new(node.Kahuna, "testdb", "fault-b");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(201, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 99L) }, rowId);

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
    // Wraps a real IKahuna and forces LocateAndCommitTransaction to return MustRetry for the first
    // mustRetryCount calls, then delegates to the real inner node. Only the commit is intercepted;
    // every other member forwards through DelegatingKahuna.
    private sealed class CommitFaultKahuna(IKahuna inner, int mustRetryCount)
        : CamusDB.Tests.Storage.DelegatingKahuna(inner)
    {
        private int mustRetryRemaining = mustRetryCount;

        public override Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref mustRetryRemaining) >= 0)
                return Task.FromResult<(KeyValueResponseType, string?)>((KeyValueResponseType.MustRetry, null));
            return inner.LocateAndCommitTransaction(handle, cancellationToken);
        }
    }
}