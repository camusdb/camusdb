/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// <c>transaction_idle_timeout_ms</c> and <c>transaction_reaper_interval_ms</c> are runtime
/// settings, so a live <c>SET CLUSTER SETTING</c> must retune the running
/// <see cref="AbandonedTransactionReaper"/> — the hosted singleton the host builds once at startup.
///
/// <para>Each test drives the real path: the SQL statement, the settings service, the options
/// holder, and the reaper running as a started <see cref="Microsoft.Extensions.Hosting.BackgroundService"/>
/// over a real <see cref="HttpTransactionCoordinator"/>. The boot values (a 5-minute timeout swept
/// every 30 s) are far from the live values, so a reaper that kept its boot configuration fails
/// every wait here by a wide margin.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestAbandonedTransactionReaperLiveSettings : BaseTest
{
    private const int BootIdleTimeoutMs = 300_000;

    private const int BootIntervalMs = 30_000;

    private CamusDBOptionsHolder holder = null!;

    private ClusterSettingsService settings = null!;

    private CamusDBOptions savedAmbient = null!;

    [SetUp]
    public async Task SetUpLiveSettings()
    {
        savedAmbient = CamusDBConfig.Ambient;

        holder = new CamusDBOptionsHolder(Options with
        {
            TransactionIdleTimeoutMs = BootIdleTimeoutMs,
            TransactionReaperIntervalMs = BootIntervalMs,
        });

        // The base definition carries the same boot values, so re-resolution after a SET changes
        // only the key the test names.
        string dataDir = Options.DataDirectory;
        settings = new ClusterSettingsService(
            TestNode!,
            holder,
            () => new ConfigDefinition
            {
                DataDir = dataDir,
                TransactionIdleTimeoutMs = BootIdleTimeoutMs,
                TransactionReaperIntervalMs = BootIntervalMs,
            },
            postResolve: null,
            isClusterMode: false,
            forwarder: null,
            logger);

        await settings.StartAsync();
    }

    [TearDown]
    public async Task TearDownLiveSettings()
    {
        if (settings is not null)
            await settings.DisposeAsync();

        CamusDBConfig.SetAmbient(savedAmbient);
    }

    private async Task<(string Db, CommandExecutor Executor, HttpTransactionCoordinator Coordinator)> CreateEngine()
    {
        CommandExecutor executor = CreateCommandExecutor(holder.Current, holder, settings);
        (string db, _, _) = await CreateDatabaseWith(executor);
        return (db, executor, new HttpTransactionCoordinator(executor));
    }

    private static async Task NonQueryAsync(CommandExecutor executor, string db, string sql)
        => await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

    /// <summary>The leak vector the reaper exists for: a heartbeat-backed explicit transaction.</summary>
    private static Task<KvTransaction> StartSerializableRwAsync(HttpTransactionCoordinator coordinator, string db)
        => coordinator.StartAsync(
            db,
            CamusIsolationLevel.Serializable,
            CamusTransactionMode.ReadWrite,
            cancellationToken: CancellationToken.None);

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int deadlineMs)
    {
        Stopwatch watch = Stopwatch.StartNew();

        while (watch.ElapsedMilliseconds < deadlineMs)
        {
            if (condition())
                return true;

            await Task.Delay(25);
        }

        return condition();
    }

    /// <summary>
    /// Runs <paramref name="body"/> against a started reaper, and always stops it: a reaper left
    /// running would keep sweeping a coordinator whose engine the fixture disposes.
    /// </summary>
    private async Task WithReaperAsync(HttpTransactionCoordinator coordinator, Func<Task> body)
    {
        AbandonedTransactionReaper reaper = new(coordinator, NullLogger<AbandonedTransactionReaper>.Instance, holder);
        await reaper.StartAsync(CancellationToken.None);

        try
        {
            await body();
        }
        finally
        {
            await reaper.StopAsync(CancellationToken.None);
            reaper.Dispose();
        }
    }

    /// <summary>
    /// The incident case: an operator shortens the timeout and the interval on a running node to
    /// free an abandoned transaction's locks now. The reaper started with the boot values, so its
    /// first tick was 30 s away when the interval changed; only a re-armed timer and a timeout read
    /// per tick reap the transaction within the deadline.
    /// </summary>
    [Test]
    public async Task ShorteningTheTimeoutAndIntervalLiveReapsAnAbandonedTransaction()
    {
        (string db, CommandExecutor executor, HttpTransactionCoordinator coordinator) = await CreateEngine();

        await WithReaperAsync(coordinator, async () =>
        {
            KvTransaction tx = await StartSerializableRwAsync(coordinator, db);
            Assert.AreEqual(KvTransactionStatus.Active, tx.Status);

            await NonQueryAsync(executor, db, "SET CLUSTER SETTING transaction_idle_timeout_ms = 200");
            await NonQueryAsync(executor, db, "SET CLUSTER SETTING transaction_reaper_interval_ms = 50");
            Assert.AreEqual(200, holder.Current.TransactionIdleTimeoutMs);
            Assert.AreEqual(50, holder.Current.TransactionReaperIntervalMs);

            bool reaped = await WaitUntilAsync(() => tx.Status == KvTransactionStatus.RolledBack, deadlineMs: 10_000);

            Assert.IsTrue(reaped,
                "the transaction must be reaped about 200 ms after it went idle, not after the 5-minute boot timeout");
            Assert.AreEqual(0, coordinator.ActiveCount);
        });
    }

    /// <summary>
    /// Off and on again, live. With the idle reap switched off, an idle transaction survives many
    /// sweeps; switching it back on reaps that same transaction. Both directions of the switch are
    /// read per tick, so neither needs a restart.
    /// </summary>
    [Test]
    public async Task DisablingAndReEnablingTheIdleReapLiveTakesEffect()
    {
        (string db, CommandExecutor executor, HttpTransactionCoordinator coordinator) = await CreateEngine();

        await WithReaperAsync(coordinator, async () =>
        {
            await NonQueryAsync(executor, db, "SET CLUSTER SETTING transaction_reaper_interval_ms = 50");
            await NonQueryAsync(executor, db, "SET CLUSTER SETTING transaction_idle_timeout_ms = 0");

            KvTransaction tx = await StartSerializableRwAsync(coordinator, db);

            // About 20 sweeps at 50 ms. A reaper that still applied a short timeout would reap here.
            await Task.Delay(1_000);
            Assert.AreEqual(KvTransactionStatus.Active, tx.Status, "a disabled idle reap must not roll anything back");
            Assert.AreEqual(1, coordinator.ActiveCount);

            await NonQueryAsync(executor, db, "SET CLUSTER SETTING transaction_idle_timeout_ms = 200");

            bool reaped = await WaitUntilAsync(() => tx.Status == KvTransactionStatus.RolledBack, deadlineMs: 10_000);
            Assert.IsTrue(reaped, "re-enabling the idle reap live must reap the transaction that was left idle");
        });
    }
}
