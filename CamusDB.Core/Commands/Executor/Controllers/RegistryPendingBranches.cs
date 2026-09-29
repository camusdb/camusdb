/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Pending-create tracking for orphan-namespace recovery.
///
/// <para>Branch creation writes metadata before publishing the registry entry; a crash between the two
/// leaves an orphaned namespace. Tracking the allocated id in a persistent pending-set lets a
/// startup scrubber find and purge such orphans. The pending key is deleted on success and on the
/// abort path. A crash between <see cref="TrackPendingBranchAsync"/> and the finally cleanup leaves the
/// marker. A failed best-effort <see cref="ClearPendingBranchAsync"/> after a SUCCESSFUL create also
/// leaves it, so a marker can point at a live, registered branch. A marker alone is never proof of
/// abandonment.</para>
///
/// <para>Keys: <c>_system/dbregistry/pending:{branchId}</c> (value is <c>"{nodeId}:{epoch}"</c>, the same
/// owner stamp as the drop-lifecycle markers; markers written before owner stamping carry one 0x01
/// byte).</para>
/// </summary>
public sealed class RegistryPendingBranches
{
    private const string PendingSuffix = "pending:";

    private readonly RegistryKeyspace keyspace;

    private readonly RegistryMarkerOwner owner;

    // The registry's authoritative persistent scan of registered entries. Orphanhood is decided against
    // it, never against the registry's local cache — see LoadOrphanBranchIdsAsync.
    private readonly Func<Task<IReadOnlyList<DatabaseRegistryEntry>>> scanAllEntries;

    internal RegistryPendingBranches(
        RegistryKeyspace keyspace,
        RegistryMarkerOwner owner,
        Func<Task<IReadOnlyList<DatabaseRegistryEntry>>> scanAllEntries)
    {
        this.keyspace = keyspace;
        this.owner = owner;
        this.scanAllEntries = scanAllEntries;
    }

    private string PendingKey(string branchId) => keyspace.Key(PendingSuffix + branchId);

    /// <summary>
    /// True when this run's startup recovery may act on a pending-create marker. Two cases qualify.
    /// The marker is this node's from a prior run: its create died with that run, so no live creator
    /// can exist. Or the marker is a legacy anonymous one (single 0x01 byte) from before owner
    /// stamping: it is reclaimed only under the scrubber's fence and registration re-checks.
    /// A current-epoch marker is a live in-flight create in this process. A different node's marker
    /// can be a live in-flight create on that node. Recovery must never touch either of those.
    /// </summary>
    private bool IsReclaimablePendingMarker(byte[]? value) =>
        owner.IsOwnStaleMarker(value) || value is [0x01];

    /// <summary>
    /// Writes a persistent pending-create marker for <paramref name="branchId"/> so a startup
    /// scrubber can find orphaned branch metadata if the process crashes mid-creation.
    ///
    /// <para><b>This write is mandatory, not best-effort.</b> Callers must invoke this method
    /// inside a try block whose catch releases any resources allocated so far (snapshot hold,
    /// etc.) and must copy the branch metadata only if this method returns without throwing. The
    /// invariant this preserves: every meta namespace written by the branch metadata copy is either
    /// registered in the persistent registry, or it has a pending-create marker that the startup
    /// scrubber can use to find and purge it. Without this guarantee a failed marker write followed
    /// by a crash after metadata copy would leave an unreachable orphan namespace with no recovery
    /// path.</para>
    ///
    /// <para>Kahuna errors are propagated to the caller; <see cref="ClearPendingBranchAsync"/>
    /// is best-effort and may be called even when no marker was written (idempotent delete).</para>
    ///
    /// <para>The marker value is the <c>{nodeId}:{epoch}</c> owner stamp, like the drop-lifecycle
    /// markers. Startup recovery uses it to reclaim only this node's prior-run markers, so a live
    /// in-flight create — in this process or on another node — is never scrubbed.</para>
    /// </summary>
    public Task TrackPendingBranchAsync(string branchId) =>
        keyspace.SetAsync(
            PendingKey(branchId), owner.Value,
            $"Failed to write pending-create marker for branch id '{branchId}'");

    /// <summary>
    /// Removes the pending-create marker for <paramref name="branchId"/>. Called on both the
    /// success path (after <see cref="DatabaseRegistry.RegisterAsync"/>) and the abort path so a
    /// successful or cleanly-aborted creation does not leave a spurious pending entry that the next
    /// startup would try to scrub.
    /// Best-effort: a failure is silently ignored.
    /// </summary>
    public Task ClearPendingBranchAsync(string branchId) => keyspace.DeleteBestEffortAsync(PendingKey(branchId));

    /// <summary>
    /// Test-only: reads whether the pending-create marker for <paramref name="branchId"/> is still
    /// present in the persistent registry. Used to assert that an indeterminate branch-create abort
    /// retained its recovery handle rather than clearing it.
    /// </summary>
    internal async Task<bool> PendingMarkerExistsForTestingAsync(string branchId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? _) = await keyspace.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, PendingKey(branchId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);
        return type == KeyValueResponseType.Get;
    }

    /// <summary>
    /// Scans the registry bucket and returns every pending-create marker as a
    /// <c>(branchId, markerValue)</c> pair. Shared by the orphan visibility scan and the startup
    /// reclaim scan, which apply different filters to the same raw marker set.
    /// </summary>
    private async Task<List<(string BranchId, byte[]? MarkerValue)>> ScanPendingBranchMarkersAsync()
    {
        string pendingPrefix = keyspace.Key(PendingSuffix);
        List<(string, byte[]?)> markers = [];

        foreach ((string key, byte[]? value) in await keyspace.ScanPrefixAsync(pendingPrefix).ConfigureAwait(false))
            markers.Add((key[pendingPrefix.Length..], value));

        return markers;
    }

    /// <summary>
    /// Scans the registry bucket for pending-create markers and returns the ids of any that are
    /// not registered in the PERSISTENT registry. These represent branch namespaces written before
    /// the registry entry was committed — typically from a process crash during branch creation.
    ///
    /// <para>Registration is decided against <see cref="DatabaseRegistry.ScanAllEntriesAsync"/>, never
    /// against the local in-memory cache. A registry loaded before a branch was created (another
    /// cluster node, or a long-lived instance) legitimately has no cached entry for a live branch.
    /// Judging orphanhood from that cache absence once let the startup scrubber destroy a published
    /// branch's schema namespace.</para>
    ///
    /// <para>This method reports; it is not an authority to destroy. The startup scrubber acts only
    /// on <see cref="LoadReclaimablePendingBranchIdsAsync"/> and re-confirms each id under a
    /// per-id fence before it purges anything.</para>
    /// </summary>
    public async Task<List<string>> LoadOrphanBranchIdsAsync()
    {
        List<(string BranchId, byte[]? MarkerValue)> markers = await ScanPendingBranchMarkersAsync().ConfigureAwait(false);
        if (markers.Count == 0)
            return [];

        // Read the registered set AFTER the marker scan: a create that registered between the two
        // scans is then seen here and correctly excluded, instead of reported as an orphan.
        HashSet<string> registered = new(StringComparer.Ordinal);
        foreach (DatabaseRegistryEntry entry in await scanAllEntries().ConfigureAwait(false))
            registered.Add(entry.Id);

        List<string> orphans = new(markers.Count);
        foreach ((string branchId, _) in markers)
        {
            if (!registered.Contains(branchId))
                orphans.Add(branchId);
        }

        return orphans;
    }

    /// <summary>
    /// Returns the pending-create marker ids that THIS run's startup recovery may act on: this
    /// node's markers from a prior run (their creates died with that run) and legacy anonymous
    /// markers. Markers from the current run (a live in-flight create in this process) and markers
    /// owned by other nodes (possibly a live in-flight create there) are excluded, so recovery can
    /// never reclaim a namespace out from under a running create.
    ///
    /// <para>Registered ids are NOT filtered out here. A published branch whose best-effort marker
    /// cleanup failed must still be visited, so the scrubber can clear its obsolete marker after it
    /// re-confirms the registration under the fence.</para>
    /// </summary>
    public async Task<List<string>> LoadReclaimablePendingBranchIdsAsync()
    {
        List<(string BranchId, byte[]? MarkerValue)> markers = await ScanPendingBranchMarkersAsync().ConfigureAwait(false);
        List<string> reclaimable = [];

        foreach ((string branchId, byte[]? value) in markers)
        {
            if (IsReclaimablePendingMarker(value))
                reclaimable.Add(branchId);
        }

        return reclaimable;
    }
}
