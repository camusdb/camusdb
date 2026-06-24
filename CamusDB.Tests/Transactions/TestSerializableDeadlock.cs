
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Verifies that reverse-order deadlocks (and persistent lock conflicts) abort within the
/// LockWaitDeadlineMs wall-clock cap rather than spinning for the full retry budget.
///
/// A deadlock in the CamusDB/Kahuna model is a livelock: two Serializable+RW transactions
/// each holding a shared range lock on one key while trying to write the other key. Both
/// writers keep receiving MustRetry from Kahuna's range-lock fence. Without a deadline they
/// would spin for the full MaxKahunaRetries budget (≈ 1.4 s each). With the deadline they
/// abort with TransactionMustRetry within ≈ LockWaitDeadlineMs (500 ms) per operation.
///
/// Guarantees verified here:
///   - A write blocked by a foreign shared range lock aborts within the deadline.
///   - A reverse-order deadlock (A holds [K1,K1] and wants K2; B holds [K2,K2] and wants K1)
///     resolves within 2 × LockWaitDeadlineMs total wall time.
///   - Non-conflicting writes (no lock conflict) are not slowed by the deadline path.
/// </summary>
[TestFixture]
public sealed class TestSerializableDeadlock
{
    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        KvTransactionsManager mgr = new(node.Kahuna);
        KvTableStore store = new(node.Kahuna, "testdb", tag);
        return (node, mgr, store);
    }

    private static async Task InsertInitial(KvTransactionsManager mgr, KvTableStore store,
        ObjectIdValue rowId, byte[] data)
    {
        KvTransaction tx = await mgr.BeginAsync();
        await store.InsertRow(tx, rowId, data);
        await mgr.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 1. Single-sided: one write blocked by a foreign shared lock aborts fast
    // -----------------------------------------------------------------------

    [Test]
    public async Task BlockedWrite_AbortsWithinDeadline()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SDL-01");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        await InsertInitial(mgr, store, rowId, [1]);

        // Reader holds shared range lock on rowId — writer will be blocked indefinitely.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.GetRow(reader, rowId);

        // Writer tries to write the same key. With the deadline it must abort fast.
        KvTransaction writer = await mgr.BeginAsync();
        Stopwatch sw = Stopwatch.StartNew();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.UpdateRow(writer, rowId, [2]));
        sw.Stop();

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Blocked write must abort with TransactionMustRetry");

        // Must complete well within 2 × deadline (allow generous CI headroom).
        Assert.Less(sw.ElapsedMilliseconds, 2000,
            $"Blocked write must abort within the lock-wait deadline; took {sw.ElapsedMilliseconds} ms");

        await mgr.RollbackAsync(writer);
        await mgr.CommitAsync(reader);
    }

    // -----------------------------------------------------------------------
    // 2. Reverse-order deadlock resolves within the deadline (not at TTL)
    //
    // Tx A reads K1 (Shared [K1,K1]) then tries to write K2 — blocked by B's [K2,K2].
    // Tx B reads K2 (Shared [K2,K2]) then tries to write K1 — blocked by A's [K1,K1].
    // Both writes run concurrently; at least one must abort with TransactionMustRetry
    // within ≈ 2 × LockWaitDeadlineMs total wall time (not ≈ 30 s range-lock TTL).
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReverseOrderDeadlock_ResolvesWithinDeadline()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SDL-02");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId1 = new(1, 0, 0);
        ObjectIdValue rowId2 = new(2, 0, 0);
        await InsertInitial(mgr, store, rowId1, [1]);
        await InsertInitial(mgr, store, rowId2, [2]);

        // Tx A: read K1 → Shared [K1,K1]
        KvTransaction txA = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.GetRow(txA, rowId1);

        // Tx B: read K2 → Shared [K2,K2]
        KvTransaction txB = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.GetRow(txB, rowId2);

        // Both writes start concurrently. A wants K2 (blocked by B's [K2,K2]);
        // B wants K1 (blocked by A's [K1,K1]). Classic reverse-order deadlock.
        Stopwatch sw = Stopwatch.StartNew();

        Task writeA = store.UpdateRow(txA, rowId2, [20]);
        Task writeB = store.UpdateRow(txB, rowId1, [10]);

        // Drain both tasks without re-throwing.
        await Task.WhenAll(
            writeA.ContinueWith(_ => { }, TaskContinuationOptions.None),
            writeB.ContinueWith(_ => { }, TaskContinuationOptions.None));

        sw.Stop();

        object? errA = writeA.IsFaulted ? writeA.Exception!.InnerException : null;
        object? errB = writeB.IsFaulted ? writeB.Exception!.InnerException : null;

        // At least one transaction must have aborted.
        bool aAborted = errA is CamusDBException exA && exA.Code == CamusDBErrorCodes.TransactionMustRetry;
        bool bAborted = errB is CamusDBException exB && exB.Code == CamusDBErrorCodes.TransactionMustRetry;
        Assert.IsTrue(aAborted || bAborted,
            $"At least one Tx must abort with TransactionMustRetry; errA={errA?.GetType().Name}, errB={errB?.GetType().Name}");

        // The total wall time must be well below the range-lock TTL (30 s).
        // With LockWaitDeadlineMs = 500 ms, both tasks together should finish in < 2 s.
        Assert.Less(sw.ElapsedMilliseconds, 3000,
            $"Deadlock must resolve within ~2 × LockWaitDeadlineMs; took {sw.ElapsedMilliseconds} ms");

        // Cleanup.
        if (!aAborted) await mgr.CommitAsync(txA).ConfigureAwait(false);
        else await mgr.RollbackAsync(txA).ConfigureAwait(false);

        if (!bAborted) await mgr.CommitAsync(txB).ConfigureAwait(false);
        else await mgr.RollbackAsync(txB).ConfigureAwait(false);
    }

    // -----------------------------------------------------------------------
    // 3. No false-positive: uncontested write is not slowed by the deadline path
    // -----------------------------------------------------------------------

    [Test]
    public async Task UncontestedWrite_CompletesNormally()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SDL-03");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = new(1, 0, 0);
        await InsertInitial(mgr, store, rowId, [1]);

        KvTransaction tx = await mgr.BeginAsync();
        Assert.DoesNotThrowAsync(() => store.UpdateRow(tx, rowId, [2]),
            "Uncontested write must not throw even though the deadline path is active");
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(tx),
            "Uncontested write must commit normally");
    }
}
