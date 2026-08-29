
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
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
/// </summary>
public sealed class AbandonedTransactionReaper : BackgroundService
{
    private readonly HttpTransactionCoordinator coordinator;

    /// <summary>Configuration for the engine this service serves; injected, never ambient.</summary>
    private readonly CamusDBOptions options;
    private readonly ILogger<AbandonedTransactionReaper> logger;

    public AbandonedTransactionReaper(
        HttpTransactionCoordinator coordinator,
        ILogger<AbandonedTransactionReaper> logger,
        CamusDBOptions options)
    {
        this.coordinator = coordinator;
        this.logger = logger;
        this.options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int idleTimeoutMs = options.TransactionIdleTimeoutMs;
        bool reapIdle = idleTimeoutMs > 0;

        TimeSpan idleTimeout = TimeSpan.FromMilliseconds(idleTimeoutMs);
        TimeSpan interval = TimeSpan.FromMilliseconds(options.TransactionReaperIntervalMs);

        if (logger.IsEnabled(LogLevel.Information))
        {
            if (reapIdle)
                logger.LogInformation(
                    "Abandoned-transaction reaper started: idle timeout {IdleMs} ms, sweep interval {IntervalMs} ms",
                    idleTimeoutMs, options.TransactionReaperIntervalMs);
            else
                logger.LogInformation(
                    "Abandoned-transaction reaper started with the idle reap disabled (transaction_idle_timeout_ms <= 0); " +
                    "it still releases parked transaction holdings every {IntervalMs} ms",
                    options.TransactionReaperIntervalMs);
        }

        using PeriodicTimer timer = new(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    if (reapIdle)
                    {
                        int reaped = await coordinator.ReapIdleAsync(idleTimeout, stoppingToken).ConfigureAwait(false);
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
}
