
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Storage.Kv;
using Kahuna;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Owns a single Kahuna snapshot-floor hold for the lifetime of one long read, and keeps it alive.
///
/// <para>A hold is <b>leased</b>, not permanent: Kahuna stops honoring it once the lease lapses, and
/// revision GC is then free to reclaim past the held timestamp. Acquiring one and never renewing it
/// therefore protects a read only for as long as the lease — after that the pin is gone while the
/// reader carries on believing it is reading a fixed snapshot. That failure is silent and produces a
/// <em>partial</em> result rather than an error, which is the worst shape a bug can take here: a
/// materialized view that quietly holds some of the rows it should.</para>
///
/// <para>So this renews in the background at a fraction of the lease, and — the part that matters —
/// <b>fails closed</b>. The moment renewal is refused, or cannot be confirmed before the lease would
/// lapse, <see cref="Lost"/> fires and <see cref="ThrowIfLost"/> starts throwing. A caller that
/// publishes results must check before publishing; a caller that is still scanning should observe the
/// token and stop.</para>
///
/// <para>Elapsed time is measured with a monotonic stopwatch rather than the wall clock, because the
/// question being asked is "how long since renewal was last confirmed" — a local duration. It is
/// deliberately not an HLC comparison: HLC orders distributed events, and this orders nothing, it
/// only measures a lease against its own clock.</para>
/// </summary>
internal sealed class SnapshotHoldLease : IAsyncDisposable
{
    private readonly IKahuna kahuna;
    private readonly ILogger<ICamusDB> logger;
    private readonly int leaseMs;
    private readonly CancellationTokenSource lost = new();
    private readonly CancellationTokenSource stop = new();
    private readonly Stopwatch sinceConfirmed = Stopwatch.StartNew();
    private Task? loop;
    private int disposed;

    /// <summary>
    /// How much of the lease may pass without a confirmed renewal before the hold is treated as gone.
    /// Below 1.0 so the reader gives up while the hold is still (just) valid rather than after it has
    /// already lapsed — declaring it lost a moment early is harmless, a moment late is the bug.
    /// </summary>
    private const double LostAfterLeaseFraction = 0.8;

    /// <summary>
    /// Test-only seam: when set, every newly acquired lease reports its hold as immediately lost, so a
    /// test can prove that a read which loses its pin refuses to publish rather than publishing a
    /// partial result. Always false in production. Set it inside a <c>try</c>/<c>finally</c> from a
    /// non-parallelizable test — it is process-wide.
    /// </summary>
    internal static bool LoseEveryHoldForTesting { get; set; }

    /// <summary>The pinned timestamp this hold protects.</summary>
    internal HLCTimestamp Snapshot { get; }

    /// <summary>Kahuna's id for the hold, used to renew and release it.</summary>
    private readonly string holdId;

    /// <summary>Fires when the hold can no longer be relied on. Never fires while it is healthy.</summary>
    internal CancellationToken Lost => lost.Token;

    /// <summary>True once the hold has been lost; latches, and never returns to false.</summary>
    internal bool IsLost => lost.IsCancellationRequested;

    private SnapshotHoldLease(
        IKahuna kahuna, ILogger<ICamusDB> logger, string holdId, HLCTimestamp snapshot, int leaseMs)
    {
        this.kahuna = kahuna;
        this.logger = logger;
        this.holdId = holdId;
        this.leaseMs = leaseMs;
        Snapshot = snapshot;
    }

    /// <summary>
    /// Acquires a hold at <paramref name="snapshot"/> and starts renewing it. Throws when the hold
    /// cannot be taken — a read that needs a pinned snapshot must not start without one.
    /// </summary>
    internal static async Task<SnapshotHoldLease> AcquireAsync(
        IKahuna kahuna,
        ILogger<ICamusDB> logger,
        string holderId,
        HLCTimestamp snapshot,
        int leaseMs,
        string statementName)
    {
        (KeyValueResponseType type, string holdId, _) = await kahuna
            .LocateAndAcquireSnapshotHold(holderId, snapshot, leaseMs, CancellationToken.None)
            .ConfigureAwait(false);

        if (type != KeyValueResponseType.Set || string.IsNullOrEmpty(holdId))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidAsOfSystemTime,
                $"Could not pin history at the requested snapshot (status {type}); the {statementName} was " +
                "not started because its source could not be guaranteed to stay readable for the whole copy");

        SnapshotHoldLease lease = new(kahuna, logger, holdId, snapshot, leaseMs);

        if (LoseEveryHoldForTesting)
        {
            // Test-only: stand in for a lease that lapsed mid-read. Renewal cannot be made to fail
            // against a healthy embedded Kahuna, and the behavior worth proving is not the renew RPC
            // but what the readers above do once the pin is gone.
            lease.MarkLost();
            return lease;
        }

        lease.loop = lease.RenewLoopAsync();
        return lease;
    }

    /// <summary>
    /// Throws if the hold has been lost, naming what is being refused. Call before acting on anything
    /// the pinned read produced — publishing rows read without a live hold is exactly the silent
    /// partial result this class exists to prevent.
    /// </summary>
    internal void ThrowIfLost(string what)
    {
        if (!IsLost)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidAsOfSystemTime,
            $"The snapshot pinned for {what} could not be kept alive for the whole read, so the rows it " +
            "produced may be incomplete and were not used. Retry; if this recurs, the read is taking longer " +
            "than the configured snapshot-hold lease.");
    }

    private async Task RenewLoopAsync()
    {
        // Renew well inside the lease so a single missed tick — an election, a transient transport
        // error — does not cost the hold.
        int intervalMs = Math.Max(250, leaseMs / 3);

        try
        {
            while (!stop.IsCancellationRequested)
            {
                await Task.Delay(intervalMs, stop.Token).ConfigureAwait(false);

                if (stop.IsCancellationRequested)
                    return;

                try
                {
                    (KeyValueResponseType type, _) = await kahuna
                        .LocateAndRenewSnapshotHold(holdId, leaseMs, stop.Token).ConfigureAwait(false);

                    if (type == KeyValueResponseType.Set)
                    {
                        sinceConfirmed.Restart();
                        continue;
                    }

                    // Anything else means Kahuna no longer recognizes the hold — it has already expired
                    // or was never registered. There is nothing to wait for.
                    logger.LogWarning(
                        "Snapshot hold {HoldId} was refused renewal (status {Status}); the pinned read can no longer be trusted",
                        holdId, type);

                    MarkLost();
                    return;
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // A transport failure is worth retrying — but only for as long as the lease can
                    // still be assumed live. Past that the hold has to be presumed gone.
                    if (sinceConfirmed.Elapsed.TotalMilliseconds >= leaseMs * LostAfterLeaseFraction)
                    {
                        logger.LogWarning(
                            ex,
                            "Snapshot hold {HoldId} could not be renewed within its lease; the pinned read can no longer be trusted",
                            holdId);

                        MarkLost();
                        return;
                    }

                    if (logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug(ex, "Transient failure renewing snapshot hold {HoldId}; will retry", holdId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Disposed while waiting — the ordinary way this loop ends.
        }
    }

    private void MarkLost()
    {
        try
        {
            lost.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Raced with disposal; the reader is already finished and nothing is left to warn.
        }
    }

    /// <summary>
    /// Stops renewing and releases the hold. Idempotent — a statement may dispose on both its success
    /// and its failure path, and a release that has already happened must not turn a real error into a
    /// confusing second one.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        await stop.CancelAsync().ConfigureAwait(false);

        if (loop is not null)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "Snapshot hold renew loop for {HoldId} ended with an error", holdId);
            }
        }

        try
        {
            await kahuna.LocateAndReleaseSnapshotHold(holdId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Best effort: a hold that outlives its statement only over-retains revisions until its
            // lease lapses, which is far better than failing a read that already succeeded.
            logger.LogWarning(ex, "Failed to release the snapshot-floor hold {HoldId}", holdId);
        }

        lost.Dispose();
        stop.Dispose();
    }
}
