/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using Microsoft.Extensions.Hosting;

namespace CamusDB.App.Services;

/// <summary>
/// Background sweep that deletes session records whose absolute expiry has passed.
///
/// <para>A session key has no storage TTL, and the only other deletions are an explicit logout and a
/// drop of the owning user. Because the token lifetime is short and re-login is the only refresh, a
/// client that reconnects on a timer leaves one dead record per reconnection and never removes it.
/// Without this sweep the auth bucket grows for the life of the deployment.</para>
///
/// <para>The growth costs more than storage. Dropping a user scans that whole bucket to find the
/// account's sessions, so a statement unrelated to any of these records gets slower as they
/// accumulate.</para>
///
/// <para><b>Every node runs it, with no leader election.</b> A record is removed only once its own
/// expiry has passed, so it can no longer authenticate anyone, and the delete is idempotent. Two nodes
/// sweeping at the same instant therefore reach the same end state as one — which is a far simpler
/// property to hold than electing a sweeper and handling its failover.</para>
///
/// <para>Switched off by a <see cref="CamusDBOptions.SessionReaperIntervalMs"/> of zero or less. The
/// interval is read once here, which is why that setting is restart-class.</para>
/// </summary>
public sealed class ExpiredSessionReaper : BackgroundService
{
    private readonly CommandExecutor executor;

    /// <summary>Configuration for the engine this service serves; injected, never ambient.</summary>
    private readonly CamusDBOptions options;

    private readonly ILogger<ExpiredSessionReaper> logger;

    public ExpiredSessionReaper(
        CommandExecutor executor,
        ILogger<ExpiredSessionReaper> logger,
        CamusDBOptions options)
    {
        this.executor = executor;
        this.logger = logger;
        this.options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int intervalMs = options.SessionReaperIntervalMs;

        if (intervalMs <= 0)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Expired-session reaper is disabled (session_reaper_interval_ms <= 0); " +
                    "session records will accumulate until a user is dropped or logs out");
            return;
        }

        // Nothing to sweep with authentication off: no session is ever written. Checked here rather
        // than per tick so a node that will never have work does not hold a timer for the process's
        // whole life.
        if (!options.AuthenticationEnabled)
            return;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Expired-session reaper started: sweep interval {IntervalMs} ms", intervalMs);

        using PeriodicTimer timer = new(TimeSpan.FromMilliseconds(intervalMs));

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    int reaped = await executor.ReapExpiredSessionsAsync().ConfigureAwait(false);

                    if (reaped > 0 && logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug("Reaped {Count} expired session record(s)", reaped);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Never let one failed sweep kill the loop; the next tick retries. Logged rather
                    // than swallowed, because a sweep that fails every tick is a growing bucket.
                    logger.LogWarning(ex, "Expired-session reaper sweep failed");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }
}
