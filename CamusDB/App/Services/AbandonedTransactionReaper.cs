
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Config;
using Microsoft.Extensions.Hosting;

namespace CamusDB.App.Services;

/// <summary>
/// Background sweep that reclaims abandoned explicit transactions. A client that opens a
/// transaction via <c>/start-transaction</c> and then disconnects, crashes, or forgets to commit or
/// roll back leaves a tracked <see cref="Core.Transactions.KvTransaction"/> that is never finalized.
/// For a Serializable+ReadWrite transaction that is especially damaging: its range-lock heartbeat
/// keeps renewing the locks every interval, so absent this reaper the locks are held until the
/// process restarts and every conflicting transaction aborts indefinitely.
///
/// <para>A single periodic timer scans the <see cref="HttpTransactionCoordinator"/> for
/// transactions idle longer than <see cref="CamusDBOptions.TransactionIdleTimeoutMs"/> and rolls
/// them back. One timer covers all transactions — the cost is one pass per
/// <see cref="CamusDBOptions.TransactionReaperIntervalMs"/>, independent of transaction count.</para>
///
/// <para>The same tick also releases the key mirrors parked by a rollback that met the
/// coordinator-unknown outcome before its transaction was old enough to release. Those keys have no
/// other owner left — the transaction is terminal and untracked — so this sweep is what turns "not
/// yet safe to release" into "released" instead of "never released". It runs even when the idle reap
/// itself is switched off, because a mirror can also be parked by the DML cleanup path.</para>
///
/// <para>The idle reap is disabled when <see cref="CamusDBOptions.TransactionIdleTimeoutMs"/> is
/// <c>&lt;= 0</c>. Even disabled, an abandoned Serializable+RW transaction's locks remain bounded by
/// <see cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/> (the session lease is reclaimed
/// past that cap), so the reaper is prompt cleanup, not the sole safety net.</para>
///
/// <para><b>Both settings are runtime-mutable, so this service reads the options holder, never an
/// injected snapshot.</b> It is a singleton: a <see cref="CamusDBOptions"/> injected into it is the
/// boot snapshot for the life of the process, and a <c>SET CLUSTER SETTING</c> would show in
/// <c>SHOW VARIABLES</c> and change nothing here. The idle timeout, including the switch between
/// enabled and disabled, is read at each tick. A new sweep interval re-arms the timer from the
/// publish itself, so shortening it during an incident does not first wait out one tick of the old,
/// longer interval.</para>
/// </summary>
public sealed class AbandonedTransactionReaper : BackgroundService
{
    private readonly HttpTransactionCoordinator coordinator;

    /// <summary>
    /// Source of the current configuration snapshot; injected, never ambient. Read once per tick,
    /// and the snapshot read is used for the whole sweep.
    /// </summary>
    private readonly CamusDBOptionsHolder optionsHolder;

    private readonly ILogger<AbandonedTransactionReaper> logger;

    public AbandonedTransactionReaper(
        HttpTransactionCoordinator coordinator,
        ILogger<AbandonedTransactionReaper> logger,
        CamusDBOptionsHolder optionsHolder)
    {
        this.coordinator = coordinator;
        this.logger = logger;
        this.optionsHolder = optionsHolder;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        CamusDBOptions startup = optionsHolder.Current;
        int idleTimeoutMs = startup.TransactionIdleTimeoutMs;

        if (logger.IsEnabled(LogLevel.Information))
        {
            if (idleTimeoutMs > 0)
                logger.LogInformation(
                    "Abandoned-transaction reaper started: idle timeout {IdleMs} ms, sweep interval {IntervalMs} ms",
                    idleTimeoutMs, startup.TransactionReaperIntervalMs);
            else
                logger.LogInformation(
                    "Abandoned-transaction reaper started with the idle reap disabled (transaction_idle_timeout_ms <= 0); " +
                    "it still releases parked transaction holdings every {IntervalMs} ms",
                    startup.TransactionReaperIntervalMs);
        }

        using PeriodicTimer timer = new(TimeSpan.FromMilliseconds(startup.TransactionReaperIntervalMs));

        // Declared after the timer, so it is disposed first: once Dispose returns, the holder runs no
        // callback that could touch a disposed timer. The catch-up call covers a publish that landed
        // between the read of the startup snapshot and the subscription.
        using IDisposable subscription = optionsHolder.Subscribe(next => RetuneInterval(timer, next));
        RetuneInterval(timer, optionsHolder.Current);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    int currentIdleTimeoutMs = optionsHolder.Current.TransactionIdleTimeoutMs;
                    if (currentIdleTimeoutMs != idleTimeoutMs)
                    {
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation(
                                "Abandoned-transaction reaper idle timeout changed from {OldMs} ms to {NewMs} ms (<= 0 disables the idle reap)",
                                idleTimeoutMs, currentIdleTimeoutMs);

                        idleTimeoutMs = currentIdleTimeoutMs;
                    }

                    if (idleTimeoutMs > 0)
                    {
                        int reaped = await coordinator.ReapIdleAsync(TimeSpan.FromMilliseconds(idleTimeoutMs), stoppingToken).ConfigureAwait(false);
                        if (reaped > 0 && logger.IsEnabled(LogLevel.Warning))
                            logger.LogWarning(
                                "Reaped {Count} abandoned transaction(s) idle for >= {IdleMs} ms",
                                reaped, idleTimeoutMs);
                    }

                    // Independent of the reap above: that one finishes transactions, this one releases
                    // the keys of transactions finished on an earlier tick whose release age has since
                    // been reached. Second in the tick so live reclamation is never delayed by cleanup.
                    int released = await coordinator.ReleaseDueMirroredHoldingsAsync(stoppingToken).ConfigureAwait(false);
                    if (released > 0 && logger.IsEnabled(LogLevel.Warning))
                        logger.LogWarning(
                            "Released {Count} key(s) held by abandoned transaction(s) whose coordinator session was unknown",
                            released);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Never let a sweep failure kill the reaper loop; the next tick retries.
                    logger.LogWarning(ex, "Abandoned-transaction reaper sweep failed");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    /// <summary>
    /// Adopts a published sweep interval. Setting <see cref="PeriodicTimer.Period"/> re-arms the
    /// timer from now, so the next tick comes one new interval after the change. Runs inline under
    /// the holder's publish lock, so it does no more than swap the period and write one log line,
    /// and only when the interval changed. A non-positive interval is ignored and
    /// the timer keeps its current period: configuration validation rejects one, and a timer cannot
    /// run with it.
    /// </summary>
    private void RetuneInterval(PeriodicTimer timer, CamusDBOptions next)
    {
        if (next.TransactionReaperIntervalMs <= 0)
            return;

        TimeSpan interval = TimeSpan.FromMilliseconds(next.TransactionReaperIntervalMs);
        if (timer.Period == interval)
            return;

        timer.Period = interval;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Abandoned-transaction reaper sweep interval changed to {IntervalMs} ms", next.TransactionReaperIntervalMs);
    }
}
