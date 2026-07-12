
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;

using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the per-transaction finalize guard in <see cref="HttpTransactionCoordinator"/>: a duplicate,
/// concurrent commit/rollback for the same transaction id (e.g. a client that timed out on a slow
/// commit and re-issued it) must never drive two overlapping Kahuna finalize calls. Exactly one call
/// finalizes; the others are rejected deterministically with
/// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> instead of racing.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestHttpCommitIdempotencyGuard : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        HttpTransactionCoordinator coord = new(executor);
        return (dbname, db, coord);
    }

    private static async Task<KvTransaction> StartSerializableRwAsync(HttpTransactionCoordinator coord, string dbname) =>
        await coord.StartAsync(
            dbname,
            CamusIsolationLevel.Serializable,
            CamusTransactionMode.ReadWrite,
            CancellationToken.None);

    /// <summary>Runs <paramref name="action"/> and captures the thrown exception (or null on success).</summary>
    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Test]
    public async Task TwoConcurrentCommitsSameTxId_OnlyOneDrivesCommit()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // Fire several commits for the same transaction concurrently. A second successful commit is
        // structurally impossible (the winner unregisters the tx and the manager's Status check
        // rejects a repeat), so "exactly one success" is proof that only one call drove finalization.
        const int callers = 8;
        Task<Exception?>[] calls = Enumerable.Range(0, callers)
            .Select(_ => Task.Run(() => CaptureAsync(() => coord.CommitAsync(db, tx, CancellationToken.None))))
            .ToArray();

        Exception?[] outcomes = await Task.WhenAll(calls);

        int successes = outcomes.Count(o => o is null);
        List<Exception> failures = outcomes.Where(o => o is not null).Cast<Exception>().ToList();

        Assert.That(successes, Is.EqualTo(1), "exactly one commit must drive finalization");
        Assert.That(failures, Has.Count.EqualTo(callers - 1));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));

        // Every rejected duplicate is the deterministic "already finalizing/completed" error, never a
        // raw 500 or a conflicting outcome.
        foreach (Exception ex in failures)
        {
            Assert.That(ex, Is.TypeOf<CamusDBException>());
            Assert.That(((CamusDBException)ex).Code, Is.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted));
        }

        // The transaction is gone from the in-flight map after finalization.
        Assert.Throws<CamusDBException>(() => coord.GetState(tx.TransactionId.L, tx.TransactionId.C));
    }

    [Test]
    public async Task SequentialRetryAfterSuccess_IsRejectedNotReDriven()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        await coord.CommitAsync(db, tx, CancellationToken.None);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));

        // A second commit issued after the first fully returned must fail deterministically rather
        // than re-drive 2PC on an already-committed transaction.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await coord.CommitAsync(db, tx, CancellationToken.None));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));
    }

    [Test]
    public async Task ConcurrentCommitAndRollback_SameTxId_OneWins()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        Task<Exception?> commit = Task.Run(() => CaptureAsync(() => coord.CommitAsync(db, tx, CancellationToken.None)));
        Task<Exception?> rollback = Task.Run(() => CaptureAsync(() => coord.RollbackAsync(tx, CancellationToken.None)));

        Exception?[] outcomes = await Task.WhenAll(commit, rollback);

        // Exactly one finalize wins; the loser is rejected. (The loser's exact code depends on the
        // interleaving — a lost claim gives TransactionAlreadyCompleted; a resolve-after-removal gives
        // "Unknown transaction" — so we only assert one success + one CamusDBException failure.)
        int successes = outcomes.Count(o => o is null);
        Assert.That(successes, Is.EqualTo(1), "exactly one of commit/rollback must win");
        Assert.That(outcomes.Single(o => o is not null), Is.TypeOf<CamusDBException>());

        // The transaction reached a single terminal state, not a torn/half-finalized one.
        Assert.That(
            tx.Status,
            Is.EqualTo(KvTransactionStatus.Committed).Or.EqualTo(KvTransactionStatus.RolledBack));
        Assert.Throws<CamusDBException>(() => coord.GetState(tx.TransactionId.L, tx.TransactionId.C));
    }

    [Test]
    public async Task ReaperConcurrentWithCommit_NoDoubleFinalize()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // The reaper (Zero threshold = everything idle) and a client commit race for the same tx.
        // Whichever claims the finalize gate first drives it; the other must not also finalize.
        Task<Exception?> commit = Task.Run(() => CaptureAsync(() => coord.CommitAsync(db, tx, CancellationToken.None)));
        Task<Exception?> reap = Task.Run(() => CaptureAsync(() => coord.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None)));

        Exception?[] outcomes = await Task.WhenAll(commit, reap);

        // The reaper is best-effort and never throws; the commit either wins (success) or loses the
        // claim (TransactionAlreadyCompleted). No other failure is acceptable.
        Assert.That(outcomes[1], Is.Null, "reaper must not throw");

        Exception? commitOutcome = outcomes[0];
        if (commitOutcome is not null)
        {
            Assert.That(commitOutcome, Is.TypeOf<CamusDBException>());
            Assert.That(((CamusDBException)commitOutcome).Code, Is.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted));
        }

        // Exactly one terminal state, and the transaction is gone from the map — never double-finalized.
        Assert.That(
            tx.Status,
            Is.EqualTo(KvTransactionStatus.Committed).Or.EqualTo(KvTransactionStatus.RolledBack));
        Assert.Throws<CamusDBException>(() => coord.GetState(tx.TransactionId.L, tx.TransactionId.C));
    }
}
