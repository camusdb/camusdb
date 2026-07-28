
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

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;

using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the idempotent client-facing ROLLBACK in <see cref="HttpTransactionCoordinator"/>.
/// A statement that fails inside an explicit transaction is rolled back and untracked by the
/// request handler that caught it, and the idle reaper does the same to an abandoned one — so the
/// client's own follow-up ROLLBACK (the correct thing for it to send after an error) must report
/// success rather than "Unknown transaction". The transaction is already in the state it asked for.
/// </summary>
[TestFixture]
public sealed class TestHttpRollbackIdempotency : SharedNodeBaseTest
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
            cancellationToken: CancellationToken.None);

    [Test]
    public async Task RollbackOfNeverStartedTransaction_IsNoOpSuccess()
    {
        (_, _, HttpTransactionCoordinator coord) = await SetupAsync();

        bool rolledBack = await coord.RollbackByIdAsync(txnIdPT: 424242, txnIdCounter: 7, CancellationToken.None);

        Assert.That(rolledBack, Is.False, "an id that was never tracked reports no work done, not an error");
    }

    [Test]
    public async Task SecondRollbackAfterFirst_IsNoOpSuccess()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        bool first = await coord.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C, CancellationToken.None);
        Assert.That(first, Is.True, "the first rollback drives the finalize");
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));

        // This is the shape the retrying client produces: rollback again for the same id. It must not
        // throw — the transaction is already rolled back, which is exactly what was asked for.
        bool second = await coord.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C, CancellationToken.None);
        Assert.That(second, Is.False);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
    }

    [Test]
    public async Task RollbackAfterServerSideCleanupRolledItBack_IsNoOpSuccess()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // Stand in for the failed-statement cleanup path: the server rolls the transaction back and
        // drops it from the in-flight map without the client knowing.
        await coord.RollbackIfNotCompletedAsync(tx, CancellationToken.None);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));

        Assert.That(
            await coord.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C, CancellationToken.None),
            Is.False);
    }

    [Test]
    public async Task RollbackAfterReaperReclaimedIt_IsNoOpSuccess()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // Zero threshold: everything is idle, so the reaper reclaims this transaction.
        int reaped = await coord.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None);
        Assert.That(reaped, Is.GreaterThanOrEqualTo(1));

        Assert.That(
            await coord.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C, CancellationToken.None),
            Is.False);
    }

    [Test]
    public async Task RollbackAfterCommit_DoesNotReDriveFinalize()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        await coord.CommitAsync(db, tx, CancellationToken.None);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));

        // Idempotence must not reach into a committed transaction: the no-op path only drops the
        // request, it never rolls back work that was already committed.
        bool rolledBack = await coord.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C, CancellationToken.None);

        Assert.That(rolledBack, Is.False);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));
    }

    [Test]
    public async Task ConcurrentRollbacks_OnlyOneDrivesFinalize()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // Duplicate concurrent rollbacks (a client that timed out and re-sent) must still resolve to a
        // single finalize. The losers either lose the claim — rejected with TransactionAlreadyCompleted
        // so an in-flight finalize of unknown outcome is never reported as done — or find the entry
        // already gone and no-op.
        const int callers = 8;
        Task<object>[] calls = new Task<object>[callers];
        for (int i = 0; i < callers; i++)
        {
            calls[i] = Task.Run<object>(async () =>
            {
                try
                {
                    return await coord.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    return ex;
                }
            });
        }

        object[] outcomes = await Task.WhenAll(calls);

        int drove = 0;
        foreach (object outcome in outcomes)
        {
            switch (outcome)
            {
                case bool didWork:
                    if (didWork)
                        drove++;
                    break;

                case CamusDBException ex:
                    Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted));
                    break;

                default:
                    Assert.Fail($"unexpected rollback outcome: {outcome}");
                    break;
            }
        }

        Assert.That(drove, Is.EqualTo(1), "exactly one rollback must drive finalization");
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
    }
}
