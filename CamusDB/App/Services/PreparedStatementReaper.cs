
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
/// Background sweep that drops REST prepared statements nobody has used for a while.
///
/// <para>A gRPC handle dies with its stream, so the gRPC surface needs no reaper. A REST handle has
/// no such event to hang its lifetime on: a client that prepares statements and then disappears —
/// crashed, redeployed, or simply pointed at another node by a load balancer — would otherwise leave
/// its entries in memory until the process restarts. Entries are small, so this is about a slow leak
/// across many clients rather than any single offender.</para>
///
/// <para>Disabled when <see cref="CamusDBOptions.PreparedStatementIdleTimeoutMs"/> is <c>&lt;= 0</c>,
/// in which case handles live until they are closed or the process exits. Expiry is also checked
/// when a handle is resolved, so this sweep controls memory rather than correctness — an entry past
/// its timeout is never executed even if the sweep has not reached it yet.</para>
/// </summary>
public sealed class PreparedStatementReaper : BackgroundService
{
    private readonly PreparedStatementRegistry registry;

    /// <summary>Configuration for the engine this service serves; injected, never ambient.</summary>
    private readonly CamusDBOptions options;
    private readonly ILogger<PreparedStatementReaper> logger;

    public PreparedStatementReaper(
        PreparedStatementRegistry registry,
        ILogger<PreparedStatementReaper> logger,
        CamusDBOptions options)
    {
        this.registry = registry;
        this.logger = logger;
        this.options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int idleTimeoutMs = options.PreparedStatementIdleTimeoutMs;
        if (idleTimeoutMs <= 0)
        {
            logger.LogInformation("Prepared-statement reaper disabled (prepared_statement_idle_timeout_ms <= 0)");
            return;
        }

        // Not clamped: the sweep interval is validated at startup, so a bad value from config is a
        // startup error rather than something to paper over here. Clamping a typo like -1 to 1 ms
        // would turn it into a thousand full-registry scans per second, silently. A non-positive
        // value can still reach this point if the static was set directly (tests do that), and the
        // honest response is to stop rather than invent a cadence.
        int sweepIntervalMs = options.PreparedStatementSweepIntervalMs;
        if (sweepIntervalMs <= 0)
        {
            logger.LogWarning(
                "Prepared-statement reaper not started: sweep interval is {IntervalMs} ms", sweepIntervalMs);
            return;
        }

        TimeSpan interval = TimeSpan.FromMilliseconds(sweepIntervalMs);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Prepared-statement reaper started: idle timeout {IdleMs} ms, sweep interval {IntervalMs} ms",
                idleTimeoutMs, options.PreparedStatementSweepIntervalMs);

        using PeriodicTimer timer = new(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    int reaped = registry.ReapExpired();
                    if (reaped > 0 && logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug("Reaped {Count} idle prepared statement(s)", reaped);
                }
                catch (Exception ex)
                {
                    // A failed sweep must never end the loop; the next tick tries again.
                    logger.LogWarning(ex, "Prepared-statement reaper sweep failed");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }
}
