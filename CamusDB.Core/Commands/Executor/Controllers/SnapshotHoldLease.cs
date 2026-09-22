
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
/// <para>A hold is <b>leased</b>, not permanent. A lapsed lease does not by itself end the pin: a
/// hold that is still registered keeps constraining reclamation, and a renew revives it. What the
/// lapse does is expose the hold to Kahuna's reaper, whose purge removes it and frees revision GC to
/// reclaim past the held timestamp. Acquiring one and never renewing it therefore protects a read
/// only until that purge — after it the pin is gone while the reader carries on believing it is
/// reading a fixed snapshot. That failure is silent and produces a
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

    /// <summary>
    /// When false, <see cref="DisposeAsync"/> only stops the renew loop and leaves the hold alive —
    /// the keep-alive mode used by branch creation, where the hold must outlive this object (the
    /// registry-driven renewer takes over once the branch is published) and release decisions
    /// belong to the creation flow's own cleanup paths.
    /// </summary>
    private readonly bool releaseOnDispose;

    private SnapshotHoldLease(
        IKahuna kahuna, ILogger<ICamusDB> logger, string holdId, HLCTimestamp snapshot, int leaseMs,
        bool releaseOnDispose = true)
    {
        this.kahuna = kahuna;
        this.logger = logger;
        this.holdId = holdId;
        this.leaseMs = leaseMs;
        this.releaseOnDispose = releaseOnDispose;
        Snapshot = snapshot;
    }

    /// <summary>
    /// Acquires a hold at <paramref name="snapshot"/> and starts renewing it. Throws when the hold
    /// cannot be taken — a read that needs a pinned snapshot must not start without one.
    ///
    /// <para>A transient answer is ridden out for up to <paramref name="retryBudgetMs"/> first: the
    /// acquire commits on Kahuna's snapshot-hold partition, so an election in flight there answers
    /// <c>MustRetry</c> without refusing anything. Past the budget the read is refused as retryable
    /// (the hold may well be grantable a moment later), which is a different statement from a
    /// snapshot that genuinely cannot be pinned.</para>
    /// </summary>
    internal static async Task<SnapshotHoldLease> AcquireAsync(
        IKahuna kahuna,
        ILogger<ICamusDB> logger,
        string holderId,
        HLCTimestamp snapshot,
        int leaseMs,
        int retryBudgetMs,
        string statementName)
    {
        (KeyValueResponseType type, string holdId, _) = await SnapshotHoldRetry
            .AcquireAsync(kahuna, holderId, snapshot, leaseMs, retryBudgetMs, CancellationToken.None)
            .ConfigureAwait(false);

        if (SnapshotHoldRetry.IsTransient(type))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Could not pin history at the requested snapshot: Kahuna's snapshot-hold partition reported no " +
                $"confirmed leader for the whole {retryBudgetMs} ms retry budget (status {type}). The " +
                $"{statementName} was not started and nothing was changed; retry the statement.");

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
    /// Wraps an <em>already acquired</em> hold in a keep-alive: renews it in the background and
    /// fails closed exactly like an acquired lease, but never releases it — disposal only stops
    /// the renew loop. Branch creation uses this to keep its fresh fork-point hold alive through
    /// the (unbounded-duration) metadata copy and to validate, via <see cref="ThrowIfLost"/>, that
    /// the hold survived before and after the branch is published; ownership of the hold then
    /// passes to the registry-driven <see cref="SnapshotHoldRenewer"/>. Release on the abort paths
    /// stays with the caller, which already owns that cleanup.
    /// </summary>
    internal static SnapshotHoldLease AdoptForKeepAlive(
        IKahuna kahuna,
        ILogger<ICamusDB> logger,
        string holdId,
        HLCTimestamp snapshot,
        int leaseMs)
    {
        SnapshotHoldLease lease = new(kahuna, logger, holdId, snapshot, leaseMs, releaseOnDispose: false);

        if (LoseEveryHoldForTesting)
        {
            // Same test seam as AcquireAsync: stands in for a lease that lapsed mid-operation, so a
            // test can prove branch creation aborts rather than publishing an unprotected branch.
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

        // Counts consecutive attempts that ended without an authoritative answer. Non-zero replaces
        // the ordinary interval with a short back-off, so an election lasting a few seconds is
        // re-probed several times rather than once per third of the lease.
        int unanswered = 0;

        try
        {
            while (!stop.IsCancellationRequested)
            {
                int waitMs = unanswered == 0 ? intervalMs : SnapshotHoldRetry.DelayMs(unanswered - 1);

                await Task.Delay(waitMs, stop.Token).ConfigureAwait(false);

                if (stop.IsCancellationRequested)
                    return;

                try
                {
                    (KeyValueResponseType type, _) = await kahuna
                        .LocateAndRenewSnapshotHold(holdId, leaseMs, stop.Token).ConfigureAwait(false);

                    if (type == KeyValueResponseType.Set)
                    {
                        sinceConfirmed.Restart();
                        unanswered = 0;
                        continue;
                    }

                    if (type == KeyValueResponseType.DoesNotExist)
                    {
                        // The one definitive refusal: the hold is gone from Kahuna's registry — it was
                        // released, or the reaper purged it after its lease lapsed. Removal is
                        // replicated and permanent, so this answer is the same on every node, forever.
                        logger.LogWarning(
                            "Snapshot hold {HoldId} no longer exists (released, or purged after its lease lapsed); " +
                            "the pinned read can no longer be trusted",
                            holdId);

                        MarkLost();
                        return;
                    }

                    // Every other status is not authoritative either way. MustRetry in particular means
                    // Kahuna's snapshot-hold partition had no confirmed leader at that instant — a
                    // routine election, which says nothing about whether this hold is alive. Treating
                    // it as a refusal discards a live hold and aborts the read that depends on it, so
                    // it takes the same rule as a transport failure: keep probing while the lease can
                    // still be assumed live, and only then presume the hold gone.
                    if (!TryContinueUnanswered($"renew returned {type}", ex: null))
                        return;

                    unanswered++;
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // A transport failure is worth retrying — but only for as long as the lease can
                    // still be assumed live. Past that the hold has to be presumed gone.
                    if (!TryContinueUnanswered("renew failed at the transport", ex))
                        return;

                    unanswered++;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Disposed while waiting — the ordinary way this loop ends.
        }
    }

    /// <summary>
    /// Decides what one unanswered renew attempt means. Returns true to keep probing, false once the
    /// lease can no longer be assumed live — in which case the hold is marked lost before returning.
    ///
    /// <para>Fails closed by design: nothing here proves the hold is gone, but a lease that has run
    /// most of its length without a confirmed renewal can no longer be relied on, and a reader that
    /// carries on would publish rows it cannot prove complete.</para>
    /// </summary>
    private bool TryContinueUnanswered(string detail, Exception? ex)
    {
        if (sinceConfirmed.Elapsed.TotalMilliseconds >= leaseMs * LostAfterLeaseFraction)
        {
            logger.LogWarning(
                ex,
                "Snapshot hold {HoldId} could not be renewed within its lease ({Detail}); the pinned read can no longer be trusted",
                holdId, detail);

            MarkLost();
            return false;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug(ex, "Unanswered renewal of snapshot hold {HoldId} ({Detail}); will retry", holdId, detail);

        return true;
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

        if (releaseOnDispose)
        {
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
        }

        lost.Dispose();
        stop.Dispose();
    }
}
