
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.AspNetCore.Http;

namespace CamusDB.App.Services;

/// <summary>
/// Counts in-flight foreground SQL/API requests as a load signal for background work (auto-analyze).
///
/// <para>The explicit-transaction coordinator only tracks long-lived <c>/start-transaction</c>
/// sessions; the common foreground workload is autocommit SQL and single-statement API calls that
/// never open an explicit transaction and so are invisible to it. This gauge closes that gap by
/// counting concurrent requests to the data endpoints for their full lifetime (including result
/// streaming), giving the auto-analyze scheduler a metric that actually reflects a busy node.</para>
/// </summary>
public sealed class ForegroundRequestGauge
{
    private int inFlight;

    /// <summary>Number of foreground data requests currently executing on this node.</summary>
    public int InFlight => Volatile.Read(ref inFlight);

    internal void Increment() => Interlocked.Increment(ref inFlight);
    internal void Decrement() => Interlocked.Decrement(ref inFlight);
}

/// <summary>
/// Middleware that maintains <see cref="ForegroundRequestGauge"/> for the duration of each request to
/// a data endpoint (paths beginning <c>/execute</c>, e.g. <c>/execute-sql-query</c>). Non-data
/// requests (Razor pages, static assets, health checks) are ignored so the gauge reflects DB load
/// rather than incidental HTTP traffic.
/// </summary>
public sealed class ForegroundRequestGaugeMiddleware
{
    private readonly RequestDelegate next;
    private readonly ForegroundRequestGauge gauge;

    public ForegroundRequestGaugeMiddleware(RequestDelegate next, ForegroundRequestGauge gauge)
    {
        this.next = next;
        this.gauge = gauge;
    }

    public async Task Invoke(HttpContext context)
    {
        string? path = context.Request.Path.Value;
        bool isDataRequest = path is not null && path.StartsWith("/execute", StringComparison.OrdinalIgnoreCase);

        if (!isDataRequest)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        gauge.Increment();
        try
        {
            await next(context).ConfigureAwait(false);
        }
        finally
        {
            gauge.Decrement();
        }
    }
}
