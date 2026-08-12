
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
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// A coordinator finalize that answers the non-terminal <c>MustRetry</c> means "the outcome is not
/// known yet — retry the same handle", never "this failed". CamusDB retries it for a wall-clock
/// budget rather than a fixed number of attempts, because every condition behind that answer (a
/// leadership flip, an in-progress drain, a participant write shed after ageing in the storage
/// layer's queue) takes longer on a saturated node without taking more attempts.
///
/// <para>These tests hold the coordinator permanently unresolved and assert on the budget's edges:
/// that it bounds the wait, that the configured value is what bounds it, that a non-positive budget
/// still attempts once, and that what the caller is left holding afterwards is a live transaction
/// they can re-finalize — not a dead one.</para>
/// </summary>
[TestFixture]
public sealed class TestFinalizeRetryBudget
{
    /// <summary>
    /// Wraps a real node but never lets a finalize resolve: commit, rollback and close all answer the
    /// non-terminal <c>MustRetry</c> forever. Counts the attempts so a test can distinguish "retried
    /// until the budget ran out" from "gave up after one call".
    /// </summary>
    private sealed class UnresolvedFinalizeKahuna : DelegatingKahuna
    {
        internal int CommitAttempts;
        internal int RollbackAttempts;

        internal UnresolvedFinalizeKahuna(IKahuna inner) : base(inner) { }

        public override Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref CommitAttempts);
            return Task.FromResult<(KeyValueResponseType, string?)>((KeyValueResponseType.MustRetry, null));
        }

        public override Task<KeyValueResponseType> LocateAndRollbackTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref RollbackAttempts);
            return Task.FromResult(KeyValueResponseType.MustRetry);
        }

        public override Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(
            string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
            => Task.FromResult<(KeyValueResponseType, TransactionWorkingSet?)>((KeyValueResponseType.MustRetry, null));
    }

    /// <summary>
    /// Builds an engine whose finalizes never resolve. The budget is fixed at construction — a manager
    /// captures its options — so a test that compares two budgets must build two of these.
    /// </summary>
    private static async Task<(EmbeddedKahuna node, UnresolvedFinalizeKahuna kahuna, KvTransactionsManager mgr)> CreateAsync(
        string tag, int finalizeRetryBudgetMs)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        UnresolvedFinalizeKahuna kahuna = new(node.Kahuna);
        CamusDBOptions options = CamusDBOptions.Default with { TransactionFinalizeRetryBudgetMs = finalizeRetryBudgetMs };

        return (node, kahuna, new KvTransactionsManager(kahuna, options));
    }

    [Test]
    [NonParallelizable]
    public async Task AnUnresolvedCommitSurfacesTheRetrySameFinalizeError()
    {
        (EmbeddedKahuna node, UnresolvedFinalizeKahuna kahuna, KvTransactionsManager mgr) =
            await CreateAsync("FB-01", finalizeRetryBudgetMs: 300);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await mgr.CommitAsync(tx));

        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code);
        Assert.Greater(kahuna.CommitAttempts, 1, "an unresolved commit must be retried, not surfaced on the first answer");
    }

    [Test]
    [NonParallelizable]
    public async Task AnUnresolvedCommitLeavesTheTransactionFinalizableRatherThanDead()
    {
        (EmbeddedKahuna node, UnresolvedFinalizeKahuna _, KvTransactionsManager mgr) =
            await CreateAsync("FB-02", finalizeRetryBudgetMs: 200);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        Assert.ThrowsAsync<CamusDBException>(async () => await mgr.CommitAsync(tx));

        // The outcome is unknown, so the transaction is neither committed nor rolled back: it stays
        // finalizing, and the caller is expected to come back and retry the same commit.
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status);

        // Idle eviction reads exactly this: a transaction the client will return to finish is not idle,
        // and disposing the database under it would strand the very thing they are coming back for.
        Assert.IsTrue(mgr.HasUnfinishedTransactions,
            "a transaction awaiting a finalize retry must count as unfinished so its database is not evicted");
    }

    [Test]
    [NonParallelizable]
    public async Task ACommittedTransactionLeavesNothingUnfinished()
    {
        EmbeddedKahuna node = new();
        await using EmbeddedKahuna __ = node;
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("FB-03/warmup", CancellationToken.None);

        KvTransactionsManager mgr = new(node.Kahuna, CamusDBOptions.Default);

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await mgr.CommitAsync(tx);

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);
        Assert.IsFalse(mgr.HasUnfinishedTransactions);
    }

    [Test]
    [NonParallelizable]
    public async Task TheConfiguredBudgetIsWhatBoundsTheWait()
    {
        // Two budgets means two engines: a manager captures its options at construction, so setting the
        // budget on one engine after the fact would compare a result against itself.
        (EmbeddedKahuna shortNode, UnresolvedFinalizeKahuna shortKahuna, KvTransactionsManager shortMgr) =
            await CreateAsync("FB-04a", finalizeRetryBudgetMs: 300);
        await using EmbeddedKahuna _ = shortNode;

        (EmbeddedKahuna longNode, UnresolvedFinalizeKahuna longKahuna, KvTransactionsManager longMgr) =
            await CreateAsync("FB-04b", finalizeRetryBudgetMs: 1_500);
        await using EmbeddedKahuna __ = longNode;

        long shortMs = await TimeUnresolvedCommitAsync(shortMgr);
        long longMs = await TimeUnresolvedCommitAsync(longMgr);

        Assert.Less(shortMs, 1_000, $"a 300 ms budget must not spend {shortMs} ms retrying");
        Assert.Greater(longMs, 900, $"a 1500 ms budget must keep retrying well past the short one, spent only {longMs} ms");
        Assert.Greater(longKahuna.CommitAttempts, shortKahuna.CommitAttempts,
            "a larger budget must buy more attempts, not merely a longer sleep");
    }

    [Test]
    [NonParallelizable]
    public async Task ANonPositiveBudgetAttemptsTheFinalizeExactlyOnce()
    {
        (EmbeddedKahuna node, UnresolvedFinalizeKahuna kahuna, KvTransactionsManager mgr) =
            await CreateAsync("FB-05", finalizeRetryBudgetMs: 0);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        Assert.ThrowsAsync<CamusDBException>(async () => await mgr.CommitAsync(tx));

        Assert.AreEqual(1, kahuna.CommitAttempts, "a disabled budget must still attempt the finalize once");
    }

    [Test]
    [NonParallelizable]
    public async Task AnUnresolvedRollbackIsRetriedOnTheSameBudget()
    {
        (EmbeddedKahuna node, UnresolvedFinalizeKahuna kahuna, KvTransactionsManager mgr) =
            await CreateAsync("FB-06", finalizeRetryBudgetMs: 300);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await mgr.RollbackAsync(tx));

        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code);
        Assert.Greater(kahuna.RollbackAttempts, 1, "rollback returns MustRetry until every release is acknowledged, so it must be retried too");
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status);
    }

    private static async Task<long> TimeUnresolvedCommitAsync(KvTransactionsManager mgr)
    {
        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        Stopwatch watch = Stopwatch.StartNew();
        try
        {
            await mgr.CommitAsync(tx);
        }
        catch (CamusDBException)
        {
            // Expected: the coordinator never resolves.
        }
        watch.Stop();

        return watch.ElapsedMilliseconds;
    }
}
