
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
/// Covers the abandoned-transaction reaper: an explicit (client-driven) transaction that is opened
/// and never committed or rolled back must be reclaimed by <see cref="HttpTransactionCoordinator.ReapIdleAsync"/>
/// so its locks — renewed indefinitely by the Serializable+RW range-lock heartbeat — cannot be held
/// forever. Also verifies the reaper leaves actively-used transactions alone and never double-finalizes.
/// </summary>
[TestFixture]
public sealed class TestAbandonedTransactionReaper : SharedNodeBaseTest
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
    public async Task ReapsIdleTransactionAndRollsItBack()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Active));
        // A real, heartbeat-backed transaction — this is the leak vector.
        Assert.That(tx.TransactionId, Is.Not.EqualTo(Kommander.Time.HLCTimestamp.Zero));

        // Zero idle threshold: every tracked transaction is considered abandoned.
        int reaped = await coord.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None);

        Assert.That(reaped, Is.EqualTo(1));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));

        // It must be gone from the in-flight map, so a later statement can no longer resolve it.
        // The map keys on ClientId (the wire identity), not on the Kahuna session id.
        Assert.Throws<CamusDBException>(() => coord.GetState(tx.ClientId.L, tx.ClientId.C));
    }

    [Test]
    public async Task DoesNotReapRecentlyActiveTransaction()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // A generous idle window: the transaction was just started, so it is nowhere near idle.
        int reaped = await coord.ReapIdleAsync(TimeSpan.FromMinutes(10), CancellationToken.None);

        Assert.That(reaped, Is.EqualTo(0));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Active));
        // Still resolvable — GetState both proves it survived and refreshes its idle timer.
        Assert.That(coord.GetState(tx.ClientId.L, tx.ClientId.C), Is.SameAs(tx));

        // Clean up so the shared node is not left with a live heartbeat.
        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task GetStateRefreshesIdleSoActiveTransactionSurvives()
    {
        (string dbname, _, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);

        // Let a little time pass, then simulate a statement touching the transaction.
        await Task.Delay(60, CancellationToken.None);
        coord.GetState(tx.ClientId.L, tx.ClientId.C);

        // Idle threshold above the elapsed-since-touch time: the touch must keep it alive.
        int reaped = await coord.ReapIdleAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.That(reaped, Is.EqualTo(0));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Active));

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task ReapDoesNotDoubleFinalizeCommittedTransaction()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord) = await SetupAsync();

        KvTransaction tx = await StartSerializableRwAsync(coord, dbname);
        await coord.CommitAsync(db, tx, CancellationToken.None);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));

        // The commit already unregistered it, so a Zero-threshold sweep finds nothing to reap and
        // must not touch the committed transaction.
        int reaped = await coord.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None);

        Assert.That(reaped, Is.EqualTo(0));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));
    }
}
