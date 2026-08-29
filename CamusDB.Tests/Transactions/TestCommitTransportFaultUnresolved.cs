
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// A transport or unexpected fault thrown AFTER the commit request left this node says nothing
/// about the transaction's outcome: the storage layer may already have decided and durably applied
/// the commit while the response path died (a coordinator node SIGKILLed mid-commit). Surfacing the
/// raw exception told clients a definite-looking generic error for a commit that had landed — the
/// leaked-write signature of the fault-injection soaks. The truthful answer is the same contract as
/// an exhausted MustRetry: <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/>, with the
/// transaction left Finalizing so the caller retries the SAME commit on the SAME handle.
/// </summary>
[TestFixture]
public sealed class TestCommitTransportFaultUnresolved
{
    /// <summary>
    /// Wraps a real node but makes the first <paramref name="failures"/> commit calls throw the
    /// given exception — the shape of an internode transport failure surfacing through the
    /// embedded storage API — then delegates to the real coordinator.
    /// </summary>
    private sealed class ThrowingCommitKahuna : DelegatingKahuna
    {
        private int failuresRemaining;

        private readonly Func<Exception> exceptionFactory;

        internal int CommitAttempts;

        internal ThrowingCommitKahuna(IKahuna inner, int failures, Func<Exception> exceptionFactory) : base(inner)
        {
            failuresRemaining = failures;
            this.exceptionFactory = exceptionFactory;
        }

        public override Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref CommitAttempts);

            if (Interlocked.Decrement(ref failuresRemaining) >= 0)
                throw exceptionFactory();

            return base.LocateAndCommitTransaction(handle, cancellationToken);
        }
    }

    private static async Task<(EmbeddedKahuna node, ThrowingCommitKahuna kahuna, KvTransactionsManager mgr)> CreateAsync(
        string tag, int failures, Func<Exception>? exceptionFactory = null)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        ThrowingCommitKahuna kahuna = new(
            node.Kahuna,
            failures,
            exceptionFactory ?? (static () => new InvalidOperationException("simulated internode transport failure")));

        return (node, kahuna, new KvTransactionsManager(kahuna, CamusDBOptions.Default));
    }

    [Test]
    [NonParallelizable]
    public async Task ATransportFaultDuringCommitSurfacesTheRetrySameFinalizeError()
    {
        (EmbeddedKahuna node, ThrowingCommitKahuna kahuna, KvTransactionsManager mgr) =
            await CreateAsync("TF-01", failures: int.MaxValue);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await mgr.CommitAsync(tx));

        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code,
            "an exception after the commit was submitted is an unknown outcome, never a definite failure");
        Assert.AreEqual(1, kahuna.CommitAttempts);

        // Unknown outcome: the transaction is neither committed nor rolled back — the caller must
        // come back and retry the same commit on the same handle.
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status);
        Assert.IsTrue(mgr.HasUnfinishedTransactions);
    }

    [Test]
    [NonParallelizable]
    public async Task TheSameHandleRetriesAfterATransportFaultAndCommits()
    {
        (EmbeddedKahuna node, ThrowingCommitKahuna kahuna, KvTransactionsManager mgr) =
            await CreateAsync("TF-02", failures: 1);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await mgr.CommitAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, ex!.Code);

        // The resume path: a Finalizing transaction falls through the pre-finalize checks and
        // retries the same coordinator handle, which now resolves.
        await mgr.CommitAsync(tx);

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status);
        Assert.AreEqual(2, kahuna.CommitAttempts);
        Assert.IsFalse(mgr.HasUnfinishedTransactions);
    }

    [Test]
    [NonParallelizable]
    public async Task CancellationDuringCommitPropagatesUnchanged()
    {
        (EmbeddedKahuna node, ThrowingCommitKahuna _, KvTransactionsManager mgr) =
            await CreateAsync("TF-03", failures: int.MaxValue, static () => new OperationCanceledException());
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        // Cancellation carries its own contract (the client's transport surfaces it code-less, which
        // the caller already treats as outcome-unknown); it must not be re-labelled.
        Assert.CatchAsync<OperationCanceledException>(async () => await mgr.CommitAsync(tx));
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status);
    }
}
