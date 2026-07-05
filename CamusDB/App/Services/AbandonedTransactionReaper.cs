
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
/// transactions idle longer than <see cref="CamusDBConfig.TransactionIdleTimeoutMs"/> and rolls
/// them back. One timer covers all transactions — the cost is one pass per
/// <see cref="CamusDBConfig.TransactionReaperIntervalMs"/>, independent of transaction count.</para>
///
/// <para>Disabled when <see cref="CamusDBConfig.TransactionIdleTimeoutMs"/> is <c>&lt;= 0</c>. Even
/// disabled, an abandoned Serializable+RW transaction's locks remain bounded by
/// <see cref="CamusDBConfig.MaxSerializableTransactionLifetimeMs"/> (the heartbeat self-terminates
/// past that cap), so the reaper is prompt cleanup, not the sole safety net.</para>
/// </summary>
public sealed class AbandonedTransactionReaper : BackgroundService
{
    private readonly HttpTransactionCoordinator coordinator;
    private readonly ILogger<AbandonedTransactionReaper> logger;

    public AbandonedTransactionReaper(
        HttpTransactionCoordinator coordinator,
        ILogger<AbandonedTransactionReaper> logger)
    {
        this.coordinator = coordinator;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int idleTimeoutMs = CamusDBConfig.TransactionIdleTimeoutMs;
        if (idleTimeoutMs <= 0)
        {
            logger.LogInformation("Abandoned-transaction reaper disabled (transaction_idle_timeout_ms <= 0)");
            return;
        }

        TimeSpan idleTimeout = TimeSpan.FromMilliseconds(idleTimeoutMs);
        TimeSpan interval = TimeSpan.FromMilliseconds(CamusDBConfig.TransactionReaperIntervalMs);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Abandoned-transaction reaper started: idle timeout {IdleMs} ms, sweep interval {IntervalMs} ms",
                idleTimeoutMs, CamusDBConfig.TransactionReaperIntervalMs);

        using PeriodicTimer timer = new(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    int reaped = await coordinator.ReapIdleAsync(idleTimeout, stoppingToken).ConfigureAwait(false);
                    if (reaped > 0 && logger.IsEnabled(LogLevel.Warning))
                        logger.LogWarning(
                            "Reaped {Count} abandoned transaction(s) idle for >= {IdleMs} ms",
                            reaped, idleTimeoutMs);
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
