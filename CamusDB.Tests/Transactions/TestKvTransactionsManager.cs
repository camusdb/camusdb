
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
using CamusDB.Core.Cache;
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

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNotNull(got);
        Assert.AreEqual(data, got!.Value.ToArray());
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

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
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

        ReadOnlyMemory<byte>? got1 = await store.GetRow(KvTransaction.CreateReadOnly(), id1);
        ReadOnlyMemory<byte>? got2 = await store.GetRow(KvTransaction.CreateReadOnly(), id2);

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
        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
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
        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
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

    // commit MustRetry fault injection: when LocateAndCommitTransaction returns the non-terminal
    // MustRetry fewer times than the finalize retry bound, CommitAsync internally retries the same
    // handle and ultimately succeeds. The written row must be visible and the tx is Committed.
    [Test]
    public async Task CommitAsync_WithTransientMustRetry_RetriesAndSucceeds()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fault-a/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        // 3 MustRetry responses before delegating to the real Kahuna (well under the finalize bound).
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

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNotNull(got, "Row written in a successfully committed tx must be visible");
        Assert.AreEqual(data, got!.Value.ToArray());
    }

    // Enough consecutive MustRetry responses to exhaust the bounded finalize retry loop (attempts
    // 0..MaxFinalizeRetries inclusive), so the loop gives up while every attempt is still MustRetry.
    private const int PersistentMustRetryInjections = 13;

    // commit MustRetry is NON-terminal: when LocateAndCommitTransaction keeps returning MustRetry past
    // the bounded finalize retries, CommitAsync must NOT fabricate a rollback. It throws the
    // non-terminal CADB0509 (TransactionFinalizeUnresolved), leaves the tx Finalizing (never
    // RolledBack/Committed) and tracked, and rejects any further data operation — because the write may
    // in fact have committed, so replaying the business op from BeginAsync could double-apply.
    [Test]
    public async Task CommitAsync_WithPersistentMustRetry_ThrowsFinalizeUnresolved_StaysFinalizing()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fault-b/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        CommitFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: PersistentMustRetryInjections);
        KvTransactionsManager mgr = new(faultKahuna);
        KvTableStore store = new(node.Kahuna, "testdb", "fault-b");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(201, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 99L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code,
            "Unresolved commit outcome must surface as the non-terminal CADB0509, not CADB0504/CADB0501");
        Assert.AreNotEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code,
            "CADB0504 is the pre-write 'replay from BeginAsync' signal; an unresolved finalize must not use it");
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status,
            "A non-terminal MustRetry must leave the tx Finalizing — never fabricate a RolledBack");

        // No further data operation may register once finalizing (the fence is installed).
        CamusDBException? opEx = Assert.ThrowsAsync<CamusDBException>(() =>
            store.InsertRow(tx, new(202, 0, 0), data));
        Assert.AreEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, opEx!.Code,
            "A finalizing transaction must reject new data operations");
    }

    // The resume path: a commit that returns MustRetry (unresolved) can be retried on the SAME handle,
    // and once the coordinator resolves it the transaction commits — without ever re-running the
    // business operation. This is the behaviour that prevents the double-apply the old code risked.
    [Test]
    public async Task CommitAsync_MustRetryThenResolves_CommitsOnSameHandle()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fault-c/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        // First CommitAsync exhausts its bounded retries on injected MustRetry; the injections are then
        // spent, so the second CommitAsync on the same handle reaches the real coordinator and commits.
        CommitFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: PersistentMustRetryInjections);
        KvTransactionsManager mgr = new(faultKahuna);
        KvTableStore store = new(node.Kahuna, "testdb", "fault-c");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(203, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 7L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code);
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status);

        // Retry the SAME commit on the SAME handle — no re-execution of the INSERT.
        await mgr.CommitAsync(tx);
        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status,
            "A retried commit on the same handle must reach a terminal Committed once the coordinator resolves");

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNotNull(got, "The row must be visible after the resumed commit");
        Assert.AreEqual(data, got!.Value.ToArray());
    }

    // rollback MustRetry is likewise non-terminal: RollbackAsync must inspect the coordinator result
    // and retry the same handle, only reporting terminal RolledBack once the coordinator does — never
    // marking RolledBack (and dropping the entry) before the coordinator acknowledges the release.
    [Test]
    public async Task RollbackAsync_MustRetryThenResolves_RollsBackOnSameHandle()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fault-d/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        RollbackFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: PersistentMustRetryInjections);
        KvTransactionsManager mgr = new(faultKahuna);
        KvTableStore store = new(node.Kahuna, "testdb", "fault-d");

        TableSchema schema = SingleCol(ColumnType.Integer64);
        CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId = new(204, 0, 0);
        byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue>() { ["v"] = new(ColumnType.Integer64, 5L) }, rowId);

        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.RollbackAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code,
            "An unresolved rollback must surface as CADB0509 — not report success while intents may remain");
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status,
            "A non-terminal rollback MustRetry must leave the tx Finalizing, not RolledBack");

        // Retry the SAME rollback on the SAME handle — the injections are spent, so it resolves.
        await mgr.RollbackAsync(tx);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status,
            "A retried rollback on the same handle must reach terminal RolledBack once the coordinator acknowledges");

        ReadOnlyMemory<byte>? got = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.IsNull(got, "A rolled-back write must not be visible");
    }

    // durable record anchor: the coordinator returns the record anchor from commit — including
    // alongside a non-terminal MustRetry — and it must be folded onto the handle so every subsequent
    // finalize attempt carries it. Without this, a commit retried after the live coordinator session is
    // lost could only return unknown Errored instead of consulting the durable decision.
    [Test]
    public async Task CommitAsync_CapturesRecordAnchor_AndCarriesItOnFinalizeRetry()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("anchor-a/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        const string anchor = "testdb:1:r/00000000000000000000dead";
        // MustRetry (with the anchor) for the whole first finalize loop, then Committed on resume.
        AnchorCommitFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: PersistentMustRetryInjections, anchor);
        KvTransactionsManager mgr = new(faultKahuna);

        KvTransaction tx = await mgr.BeginAsync();

        // First commit exhausts the bounded finalize retries on the injected MustRetry, but the anchor
        // must already be folded onto the handle.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code);
        Assert.AreEqual(anchor, tx.Handle.RecordAnchorKey,
            "The coordinator-returned record anchor must be folded onto the handle, even on MustRetry");

        // The very first attempt could not carry an anchor yet; every attempt after the coordinator
        // first returned it must carry the captured anchor.
        Assert.IsNull(faultKahuna.ReceivedAnchors[0], "the first commit attempt carries no anchor yet");
        Assert.That(faultKahuna.ReceivedAnchors.Skip(1), Has.All.EqualTo(anchor),
            "every subsequent finalize attempt must carry the captured anchor");

        // Resume the SAME commit on the SAME (now anchored) handle — the injections are spent, so it
        // commits, and the resuming attempt carried the anchor.
        await mgr.CommitAsync(tx);
        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);
        Assert.AreEqual(anchor, faultKahuna.ReceivedAnchors[^1],
            "the resuming commit must carry the anchor to the coordinator");
    }

    // best-effort guard: when the coordinator returns a null anchor (the best-effort path), no anchor
    // is fabricated on the handle — the durable-recovery plumbing stays inert.
    [Test]
    public async Task CommitAsync_BestEffortNullAnchor_LeavesHandleAnchorNull()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("anchor-b/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        AnchorCommitFaultKahuna faultKahuna = new(node.Kahuna, mustRetryCount: 0, anchor: null);
        KvTransactionsManager mgr = new(faultKahuna);

        KvTransaction tx = await mgr.BeginAsync();
        await mgr.CommitAsync(tx);

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);
        Assert.IsNull(tx.Handle.RecordAnchorKey,
            "A best-effort commit (null coordinator anchor) must leave the handle anchor null");
    }

    // cache invalidation must be sourced from the coordinator's FROZEN server-owned working set
    // (captured by closing the transaction), not the client-side modified-key mirror. Here no client
    // write is tracked on the transaction, yet the server set (returned by Close) names a modified key;
    // the commit must still bump that keyspace's cache generation, fencing out a stale publish that
    // raced the write. Under the old client-mirror sourcing, the empty mirror would have invalidated
    // nothing and the stale entry would survive.
    [Test]
    public async Task CommitAsync_SourcesCacheInvalidationFromFrozenServerWorkingSet()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("closeset/warmup", CancellationToken.None);
        await using EmbeddedKahuna __ = node;

        using QueryResultCache cache = new(sweepIntervalMs: -1);

        const string keyspace = "testdb:1:r";
        TransactionWorkingSet serverSet = new()
        {
            ModifiedKeys =
            [
                new KeyValueTransactionModifiedKey
                {
                    Key = keyspace + "/00000000000000000000dead",
                    Durability = KeyValueDurability.Persistent
                }
            ]
        };

        CloseSetKahuna spy = new(node.Kahuna, serverSet);
        KvTransactionsManager mgr = new(spy, cache: cache);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        // A cacheable query snapshots the keyspace generation BEFORE the write commits. No client-side
        // write is tracked on tx, so only the frozen server set (returned by Close) names the modified key.
        CacheGenerationToken token = cache.PublishGate.SnapshotGenerations([keyspace]);

        await mgr.CommitAsync(tx);
        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);

        // Because invalidation was sourced from the frozen server set, the keyspace generation moved, so a
        // publish under the pre-commit token is now fenced out — no stale entry survives the write.
        string fp = ResultFingerprintBuilder.Build("testdb", "c", "s", null, null, new CacheHintOptions("c"));
        CachedQueryResult result = new(
            CacheName: "c",
            DatabaseId: "testdb",
            Rows: [],
            ResultFingerprint: fp,
            CachedAt: default,
            Status: QueryCacheStatus.Miss);

        (QueryCacheStatus status, QueryCacheBypassReason reason) = await cache.TryPublishAsync(result, token);

        Assert.AreEqual(QueryCacheStatus.EvictedBeforePublish, status,
            "a publish that raced the committed write must be fenced by the bumped generation");
        Assert.AreEqual(QueryCacheBypassReason.InFlightWrite, reason,
            "the fence must fire because invalidation was sourced from the frozen server working set");
    }

    // Delegating IKahuna wrapper that returns a fixed frozen working set from LocateAndCloseTransaction,
    // letting a test control the exact server-owned modified-key set the commit sources invalidation from.
    // Every other member (including the commit itself) forwards to the real inner node.
    private sealed class CloseSetKahuna(IKahuna inner, TransactionWorkingSet closeSet)
        : CamusDB.Tests.Storage.DelegatingKahuna(inner)
    {
        public override Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(
            string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
            => Task.FromResult<(KeyValueResponseType, TransactionWorkingSet?)>((KeyValueResponseType.Set, closeSet));
    }

    // Delegating IKahuna wrapper that injects MustRetry responses from LocateAndCommitTransaction for
    // the first mustRetryCount calls, then delegates to the real inner node. Only the commit is
    // intercepted; every other member forwards through DelegatingKahuna.
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

    // Delegating IKahuna wrapper that injects MustRetry responses from LocateAndRollbackTransaction for
    // the first mustRetryCount calls, then delegates to the real inner node.
    private sealed class RollbackFaultKahuna(IKahuna inner, int mustRetryCount)
        : CamusDB.Tests.Storage.DelegatingKahuna(inner)
    {
        private int mustRetryRemaining = mustRetryCount;

        public override Task<KeyValueResponseType> LocateAndRollbackTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref mustRetryRemaining) >= 0)
                return Task.FromResult(KeyValueResponseType.MustRetry);
            return inner.LocateAndRollbackTransaction(handle, cancellationToken);
        }
    }

    // Delegating IKahuna wrapper that fully controls the commit outcome and always returns the supplied
    // <paramref name="anchor"/>: MustRetry for the first mustRetryCount calls, then Committed. It records
    // the RecordAnchorKey carried by each handle it receives, so a test can assert the anchor was folded
    // onto the handle and carried on subsequent finalize attempts. The commit is never delegated to the
    // inner node, keeping the outcome deterministic and independent of real 2PC.
    private sealed class AnchorCommitFaultKahuna(IKahuna inner, int mustRetryCount, string? anchor)
        : CamusDB.Tests.Storage.DelegatingKahuna(inner)
    {
        private int mustRetryRemaining = mustRetryCount;

        public List<string?> ReceivedAnchors { get; } = [];

        public override Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            lock (ReceivedAnchors)
                ReceivedAnchors.Add(handle.RecordAnchorKey);

            KeyValueResponseType type = Interlocked.Decrement(ref mustRetryRemaining) >= 0
                ? KeyValueResponseType.MustRetry
                : KeyValueResponseType.Committed;

            return Task.FromResult<(KeyValueResponseType, string?)>((type, anchor));
        }
    }
}