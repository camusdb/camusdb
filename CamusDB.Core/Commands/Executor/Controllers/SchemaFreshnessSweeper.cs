/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Microsoft.Extensions.Logging;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Periodically probes every open database for schema staleness: a durable checkpoint version
/// ahead of the in-memory version means committed schema deltas were never delivered to this node,
/// and the in-memory schema is reloaded from the checkpoint.
///
/// <para><b>Why a sweep is needed on top of the open-time and miss-triggered probes.</b> Those two
/// probes fire only when a request misses a table. A missed DROP TABLE, ADD COLUMN or index delta
/// produces no miss — the node silently serves the old shape — and a missed CREATE TABLE whose
/// checkpoint persisted after the open-time probe stays invisible until a request happens to touch
/// it. The sweep bounds every such episode to one tick instead of forever.</para>
///
/// <para><b>Cost.</b> One KV read of the version key per open database per tick when nothing is
/// stale. The sweep runs only in cluster mode: a standalone node applies its own deltas in-process
/// and cannot fall behind its own checkpoint.</para>
/// </summary>
internal sealed class SchemaFreshnessSweeper : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    private readonly int intervalMs;

    private readonly CancellationTokenSource cts = new();

    private Task? loop;

    public SchemaFreshnessSweeper(
        DatabaseDescriptors databaseDescriptors,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        int intervalMs)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.catalogs = catalogs;
        this.logger = logger;
        this.intervalMs = intervalMs;
    }

    /// <summary>Number of stale schemas this sweeper has repaired, for diagnostics and tests.</summary>
    internal int ReconciledCount;

    /// <summary>
    /// Starts the periodic probe loop. A no-op when the interval is non-positive, so a deployment
    /// can disable the sweep entirely.
    /// </summary>
    public void Start()
    {
        if (intervalMs <= 0)
            return;

        // Serialize concurrent Start calls: an unsynchronized `loop ??=` is check-then-act, and two
        // racing callers would each launch an independent loop for the process lifetime.
        lock (this)
        {
            loop ??= SweepLoopAsync(cts.Token);
        }
    }

    private async Task SweepLoopAsync(CancellationToken ct)
    {
        // try/catch INSIDE the loop: one failed sweep is logged and retried on the next tick, and
        // only cancellation ends the loop. The sweep is a repair mechanism — a sweep that throws
        // must never take the loop down and leave staleness permanent thereafter.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(intervalMs, ct).ConfigureAwait(false);
                await SweepOnceAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // normal shutdown
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Schema freshness sweep failed; retrying next tick");
            }
        }
    }

    /// <summary>
    /// Probes every open, fully loaded descriptor once and returns how many were repaired.
    /// Public surface for tests, which drive ticks directly instead of waiting out the timer.
    /// </summary>
    internal async Task<int> SweepOnceAsync()
    {
        int reconciled = 0;

        foreach (AsyncLazy<DatabaseDescriptor> lazy in databaseDescriptors.Descriptors.Values)
        {
            // A descriptor still loading runs its own open-time probe; one that failed to load has
            // nothing to probe. Neither is this sweep's business.
            if (!lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
                continue;

            DatabaseDescriptor descriptor = lazy.Task.Result;

            if (descriptor.IsDropped)
                continue;

            try
            {
                if (await catalogs.ReconcileSchemaFreshnessAsync(descriptor).ConfigureAwait(false))
                {
                    reconciled++;
                    Interlocked.Increment(ref ReconciledCount);
                }
            }
            catch (ObjectDisposedException)
            {
                // Idle eviction or a drop released the descriptor between the snapshot of the value
                // collection and the probe. The next open reloads from the checkpoint anyway.
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Schema freshness probe failed for database '{Db}'; retrying next tick",
                    descriptor.Name);
            }
        }

        return reconciled;
    }

    public async ValueTask DisposeAsync()
    {
        await cts.CancelAsync().ConfigureAwait(false);

        if (loop is not null)
        {
            try { await loop.ConfigureAwait(false); }
            catch { /* the loop swallows its own errors */ }
        }

        cts.Dispose();
    }
}
