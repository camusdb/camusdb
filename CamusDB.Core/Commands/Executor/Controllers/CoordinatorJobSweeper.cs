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
/// Periodically resumes, on every open database this node is the schema leader of, the staged
/// schema changes whose driver is gone: an index build, a column fill or a constraint validation
/// that recorded its job and was then cut by a crash, a kill or a leadership change.
///
/// <para><b>Why a sweep on top of the leader callback.</b> A job is resumed when this node wins the
/// schema leadership, through a callback the database registers when it opens. That reaches the job
/// only when the election happens after the open and the record is visible when the callback runs.
/// A node that restarts and wins the schema partition seconds later, before any request has opened
/// the database, registers the callback after the election it needed; a node still applying the
/// committed log can read no record at all; and a resume that fails on a transient, such as a peer
/// down during the ack wait, is not asked again. In each case the job sat until the next leadership
/// change, which may never come, and the element stayed half-built: an index "already exists" to
/// <c>CREATE</c> and missing to every query. The sweep bounds every such episode to one tick.</para>
///
/// <para><b>Cost.</b> One local leadership check per open database per tick, and on the databases
/// this node leads one small KV scan of the job records; nothing is driven when there is none. A job
/// a statement is driving on this node right now holds its table and is skipped; a job that failed
/// its last attempt here waits its backoff out (<see cref="SchemaChangeCoordinator"/>). Runs in both
/// modes: a standalone node is its own schema leader.</para>
/// </summary>
internal sealed class CoordinatorJobSweeper : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly SchemaChangeCoordinator coordinator;

    private readonly ILogger<ICamusDB> logger;

    private readonly int intervalMs;

    private readonly CancellationTokenSource cts = new();

    private Task? loop;

    public CoordinatorJobSweeper(
        DatabaseDescriptors databaseDescriptors,
        SchemaChangeCoordinator coordinator,
        ILogger<ICamusDB> logger,
        int intervalMs)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.coordinator = coordinator;
        this.logger = logger;
        this.intervalMs = intervalMs;
    }

    /// <summary>Number of sweeps that ran to the end, for diagnostics and tests.</summary>
    internal int SweepCount;

    /// <summary>
    /// Starts the periodic loop. A no-op when the interval is non-positive, so a deployment or a test
    /// can disable the sweep and drive it by hand.
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
        // only cancellation ends the loop. The sweep is a repair mechanism; a sweep that throws must
        // never take the loop down and leave a half-built element permanent thereafter.
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
                logger.LogWarning(ex, "Coordinator job sweep failed; retrying next tick");
            }
        }
    }

    /// <summary>
    /// One sweep over every open, fully loaded descriptor. Returns how many databases this node leads
    /// and therefore resumed the jobs of. Public surface for tests, which drive ticks directly instead
    /// of waiting out the timer.
    /// </summary>
    internal async Task<int> SweepOnceAsync()
    {
        int swept = 0;

        foreach (AsyncLazy<DatabaseDescriptor> lazy in databaseDescriptors.Descriptors.Values)
        {
            // A descriptor still loading runs its own open-time resume; one that failed to load has
            // nothing to resume. Neither is this sweep's business.
            if (!lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
                continue;

            DatabaseDescriptor descriptor = lazy.Task.Result;

            if (descriptor.IsDropped)
                continue;

            try
            {
                if (!await descriptor.Kahuna.AmISchemaLeaderAsync(descriptor.Id).ConfigureAwait(false))
                    continue;

                await coordinator.ResumeJobsAsync(descriptor, CoordinatorResumeTrigger.Sweep).ConfigureAwait(false);
                swept++;
            }
            catch (ObjectDisposedException)
            {
                // Idle eviction or a drop released the descriptor between the snapshot of the value
                // collection and the resume. The next open resumes what is left anyway.
            }
            catch (Exception ex)
            {
                Log.LogCoordinatorJobSweepFailed(logger, ex, descriptor.Name);
            }
        }

        Interlocked.Increment(ref SweepCount);
        return swept;
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
