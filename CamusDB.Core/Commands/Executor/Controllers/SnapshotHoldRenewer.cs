
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Keeps every branch database's Kahuna snapshot-floor hold alive for as long as the branch exists
/// in the <see cref="DatabaseRegistry"/> — not merely while it is open.
///
/// A branch acquires a leased hold on its immediate parent at fork time so its as-of-<c>forkT</c>
/// reads stay correct under revision reclamation. The lease lapses if it is not renewed, so some node
/// must renew it periodically regardless of whether any client currently has the branch open. This
/// renewer is that owner.
///
/// <b>Single-owner election.</b> The renew RPC auto-routes to the system-partition leader, so running
/// it on every node would only multiply Raft churn. To sweep from exactly one node, the loop acts only
/// while this node leads the database-registry key's partition (<see cref="EmbeddedKahuna.AmILeaderForKeyAsync"/>).
/// Because that check is re-evaluated every tick, a failover naturally hands the sweep to the new
/// leader; a standalone node always leads and always sweeps. Renewing every <c>lease/3</c> keeps a wide
/// margin below expiry so a single missed tick (election, transient error) does not drop a hold.
///
/// This is best-effort and idempotent: renewing an already-live hold simply extends its lease, and a
/// transient renew failure is logged and retried on the next tick. A non-<c>Set</c> renew result is
/// surfaced as a warning because it means a branch's frozen view may be losing protection.
/// </summary>
internal sealed class SnapshotHoldRenewer : IAsyncDisposable
{
    private readonly EmbeddedKahuna sharedNode;
    private readonly DatabaseRegistry registry;
    private readonly ILogger<ICamusDB> logger;
    private readonly int leaseMs;
    private int intervalMs;
    private readonly CancellationTokenSource cts = new();
    private Task? loop;

    /// <summary>
    /// HLC timestamp of the last sweep that completed without throwing (leader check + registry scan
    /// succeeded, regardless of how many individual holds were due). Stays <see cref="HLCTimestamp.Zero"/>
    /// until the first successful sweep. Exposed as a liveness signal so a stalled renewal pipeline —
    /// e.g. one that keeps failing the leader check or the registry scan — is observable rather than
    /// silent. Ordered by HLC, never wall clock, so the value is comparable across nodes.
    /// </summary>
    internal HLCTimestamp LastSuccessfulSweep { get; private set; } = HLCTimestamp.Zero;

    public SnapshotHoldRenewer(
        EmbeddedKahuna sharedNode,
        DatabaseRegistry registry,
        ILogger<ICamusDB> logger,
        int leaseMs)
    {
        this.sharedNode = sharedNode;
        this.registry = registry;
        this.logger = logger;
        this.leaseMs = leaseMs;
        // Renew well inside the lease; never spin faster than once a second.
        this.intervalMs = Math.Max(1000, leaseMs / 3);
    }

    /// <summary>
    /// Test-only seam. When set, the background loop invokes this instead of <see cref="RenewDueHoldsAsync"/>,
    /// so a test can inject a one-shot sweep failure and assert the loop survives it (the production fix is
    /// that a thrown sweep is caught inside the loop and retried on the next tick). Null in production —
    /// the loop then sweeps via <see cref="RenewDueHoldsAsync"/> directly.
    /// </summary>
    internal Func<CancellationToken, Task<int>>? SweepForTesting { get; set; }

    /// <summary>Uses a short renew interval so the loop's per-tick behavior is testable without waiting a full lease/3.</summary>
    internal int IntervalMsForTesting
    {
        get => intervalMs;
        set => intervalMs = value;
    }

    /// <summary>Starts the background renew loop. Idempotent: a second call is a no-op.</summary>
    public void Start()
    {
        loop ??= RenewLoopAsync(cts.Token);
    }

    private async Task RenewLoopAsync(CancellationToken ct)
    {
        // A sweep-level failure (leader check or registry scan throwing) must never terminate the
        // loop: if it did, every branch hold on this node would lapse after its lease and Kahuna
        // could reclaim the revisions branch as-of reads depend on, with nothing to restart the
        // sweep. So the catch lives INSIDE the loop — one failed sweep is logged and the loop keeps
        // sweeping on the next tick. Only cancellation (shutdown) ends the loop.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(intervalMs, ct).ConfigureAwait(false);
                await (SweepForTesting is null
                    ? RenewDueHoldsAsync(ct)
                    : SweepForTesting(ct)).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown.
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Snapshot-hold renewer sweep failed; will retry on the next tick");
            }
        }
    }

    /// <summary>
    /// Runs one sweep: if this node leads the registry partition, renews the immediate-parent hold of
    /// every registered branch. Returns the number of holds that were successfully renewed (0 when this
    /// node is not the sweeping leader or no branch holds exist). Exposed to tests so a sweep can be
    /// forced without waiting a full tick and its effect asserted.
    ///
    /// <para>The sweep reads from the persistent KV registry (not only the local in-memory cache) so
    /// that branches registered on other nodes after this node started are not missed. A branch absent
    /// from the local cache would never have its hold renewed if the sweep used the cache alone, and the
    /// hold would eventually lapse while the branch is still live.</para>
    /// </summary>
    internal async Task<int> RenewDueHoldsAsync(CancellationToken ct)
    {
        if (!await sharedNode.AmILeaderForKeyAsync(registry.RegistryBucket, ct).ConfigureAwait(false))
        {
            // Reaching here without throwing means the pipeline is alive even though this node does
            // not currently sweep; record it so the liveness signal reflects a healthy no-op.
            MarkSweepSucceeded();
            return 0;
        }

        int renewed = 0;
        IReadOnlyList<DatabaseRegistryEntry> entries = await registry.ScanAllEntriesAsync().ConfigureAwait(false);
        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (string.IsNullOrEmpty(entry.ImmediateParentHoldId))
                continue;

            try
            {
                (KeyValueResponseType type, _) = await sharedNode.Kahuna
                    .LocateAndRenewSnapshotHold(entry.ImmediateParentHoldId, leaseMs, ct)
                    .ConfigureAwait(false);

                if (type == KeyValueResponseType.Set)
                    renewed++;
                else
                    logger.LogWarning(
                        "Renew of snapshot hold {HoldId} for branch '{Database}' returned {Type}; its frozen view may lose protection",
                        entry.ImmediateParentHoldId, entry.Name, type);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Failed to renew snapshot hold {HoldId} for branch '{Database}'", entry.ImmediateParentHoldId, entry.Name);
            }
        }

        // The full sweep completed (leader check + registry scan succeeded); individual per-entry
        // renew failures above are contained and do not count against liveness.
        MarkSweepSucceeded();
        return renewed;
    }

    /// <summary>Stamps <see cref="LastSuccessfulSweep"/> with a fresh local HLC event.</summary>
    private void MarkSweepSucceeded()
    {
        LastSuccessfulSweep = sharedNode.Raft.HybridLogicalClock
            .SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
    }

    public async ValueTask DisposeAsync()
    {
        await cts.CancelAsync().ConfigureAwait(false);
        if (loop is not null)
        {
            try { await loop.ConfigureAwait(false); }
            catch { /* loop swallows its own errors; nothing to propagate on shutdown */ }
        }
        cts.Dispose();
    }
}
