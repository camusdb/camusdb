/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Storage.Kv;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The database-lifecycle markers the <see cref="DatabaseRegistry"/> keeps beside its entries:
/// the drop-intent fence (cross-node drop-vs-branch-create and relink-vs-GC exclusion), the
/// drop-in-progress markers that make a keyspace purge crash-resumable, and the
/// lost-snapshot-protection markers that fail a branch closed.
/// </summary>
public sealed class RegistryDropMarkers : IAsyncDisposable
{
    private readonly RegistryKeyspace keyspace;

    private readonly RegistryMarkerOwner owner;

    // A drop-intent fence carries a bounded lease (its KV key's native expiry, CamusDBOptions.FenceLeaseMs).
    // If the owner crashes without releasing it, the lease lapses and any node can then re-acquire — a
    // dead owner can no longer block relink/GC of an id forever. A live owner keeps the fence by renewing
    // the lease in the background (below) for as long as it holds it, so an operation that legitimately
    // outlives one lease period (e.g. a large keyspace purge) never has the fence stolen mid-flight.

    // The lease mechanism itself lives in KeyLeaseFence: acquire with SetIfNotExists + a native expiry,
    // renew with a compare-and-set on this acquisition's token. This fence is its only remaining user —
    // row-level TTL span claims moved to Kahuna's distributed locks (TtlSpanLease), which enforce owner
    // equality server-side and hand out a monotonic fencing token. Prefer those locks for anything new;
    // what keeps this one here is that startup recovery enumerates fence markers by range scan, which
    // the lock API cannot do. Assigned in the constructor; see the note there on why not lazily.
    private readonly KeyLeaseFence dropIntentFence;

    // Fence id → the token of the acquisition this process currently holds. The public acquire/release
    // surface stays bool/void for its many call sites; the token is bookkeeping those callers should not
    // have to carry, but without which a release cannot tell our own live claim from a successor's.
    private readonly ConcurrentDictionary<string, string> dropIntentTokens = new(StringComparer.Ordinal);

    internal RegistryDropMarkers(RegistryKeyspace keyspace, RegistryMarkerOwner owner, CamusDBOptions options)
    {
        this.keyspace = keyspace;
        this.owner = owner;

        // Constructed eagerly, not lazily: a `??=` here would be check-then-act, and two concurrent
        // acquires could each build a fence while only one landed in the field — so a later release
        // would cancel the wrong instance's renewer and keep re-stamping a lease it had "released".
        dropIntentFence = new KeyLeaseFence(
            keyspace.Kahuna, owner.Value, options.FenceLeaseMs, options.FenceLeaseRenewIntervalMs);
    }

    // -----------------------------------------------------------------------
    // Drop-intent fence for cross-node drop-vs-branch-create atomicity
    // -----------------------------------------------------------------------

    // A DROP DATABASE on node A and a CREATE ... BRANCH FROM ... on node B can race: if A's
    // descendant scan completes before B registers the new child, A sees no descendants and
    // proceeds to purge the parent's keyspace, orphaning the child.
    //
    // The fence works via a persistent KV key per database id:
    //   A sets the key (SetIfNotExists) before its descendant scan and holds it through purge.
    //   B checks the key after RegisterAsync (not before — the Raft-linearized ordering means
    //   either A's set happened before B's register and B will observe it here, or B's register
    //   happened before A's set in which case A's subsequent descendant scan sees B's child and
    //   A aborts instead).
    // Those two checks alone do not cover a third ordering: A's descendant scan runs before B
    // registers, A finishes the whole drop, and A releases the key — all while B is stalled between
    // its metadata copy and RegisterAsync. B then reads an absent key, which only proves that no
    // drop is in flight, not that the source still exists. B therefore runs a second gate right
    // after this one: it re-reads the source's liveness by its immutable id from the persistent
    // registry (TryResolveNameByIdAsync) and aborts when the id is gone. See
    // DatabaseLifecycleService.CreateBranchDatabaseAsync. The two reads in that order close the
    // window, because a drop that missed the child holds this key from before the child's
    // RegisterAsync until after its own UnregisterAsync.
    //
    // Keys: _system/dbregistry/drop-intent:{dbId}  (value is the owner stamp "{nodeId}:{epoch}",
    // followed by the ":{acquisitionToken}" the lease fence appends per acquisition — see
    // RegistryMarkerOwner. Startup recovery parses it to reclaim only this node's own prior-run
    // remnants, never a live drop another node holds.)

    private const string DropIntentSuffix = "drop-intent:";

    private string DropIntentKey(string dbId) => keyspace.Key(DropIntentSuffix + dbId);

    /// <summary>
    /// Composite drop-intent fence id for a table orphan (<c>{dbId}:{tableId}</c>), used with
    /// <see cref="AcquireDropIntentAsync"/>. Both <c>CREATE TABLE ... RELINK</c> and the orphan
    /// reclaimer take this key so a relink and a GC purge of the same table id never interleave. It
    /// cannot collide with a database fence id: a bare database id contains no colon.
    /// </summary>
    public static string TableFenceId(string dbId, string tableId) => $"{dbId}:{tableId}";

    /// <summary>
    /// Atomically acquires the drop-intent fence for <paramref name="dbId"/> with a bounded lease
    /// (<see cref="CamusDBOptions.FenceLeaseMs"/>) via <c>SetIfNotExists</c>. Returns <c>true</c> if this node now owns
    /// the fence; <c>false</c> if another node's <em>live</em> (non-expired) lease already holds it.
    ///
    /// <para><b>Lease.</b> The marker's KV key carries a native expiry, so a holder that crashes without
    /// releasing frees the fence automatically once the lease lapses — a dead owner can no longer block
    /// relink/GC of the id forever (the failure this fixes). On success a background renewer keeps the
    /// lease alive for as long as this node holds the fence, so a long operation (a large keyspace purge)
    /// is never interrupted by a competing acquire. The marker value is <c>{nodeId}:{epoch}</c> so
    /// startup recovery reclaims only this node's own prior-run remnants.</para>
    ///
    /// <para><b>Transient vs. genuine contention.</b> Only a real <c>SetIfNotExists</c> conflict
    /// (<c>NotSet</c> — a live lease is present) reports the fence as held; transient replication/retry
    /// statuses (<c>MustRetry</c>/<c>WaitingForReplication</c>) are retried with bounded backoff rather
    /// than mistaken for contention.</para>
    ///
    /// <para>The caller must call <see cref="ReleaseDropIntentAsync"/> on every exit path so the renewer
    /// stops and the fence frees immediately rather than only when its lease lapses.</para>
    /// </summary>
    public async Task<bool> AcquireDropIntentAsync(string dbId)
    {
        string? token = await dropIntentFence.TryAcquireAsync(DropIntentKey(dbId)).ConfigureAwait(false);
        if (token is null)
            return false;

        // Remember which acquisition this is so the release can be fenced against it. A fence id has at
        // most one live acquisition in this process, so a plain map is sufficient — the token exists to
        // distinguish acquisitions across *time* (ours vs. a successor's after our lease lapsed), which
        // is precisely what an unconditional release cannot see.
        dropIntentTokens[dbId] = token;
        return true;
    }

    /// <summary>
    /// Returns <c>true</c> if a drop-intent marker is set for <paramref name="sourceId"/>, meaning a
    /// concurrent <c>DROP DATABASE</c> is actively processing the source and its keyspace may be
    /// purged at any moment. A branch-create that detects this after registering must unregister the
    /// newly-created branch and abort.
    ///
    /// <para><b>An absent marker does not prove the source still exists.</b> It proves only that no
    /// drop is in flight right now: a drop that already finished released its marker. The create path
    /// therefore follows this read with an authoritative liveness re-read of the source's immutable id
    /// (<see cref="DatabaseRegistry.TryResolveNameByIdAsync"/>), and the two reads in that order close
    /// the window.</para>
    ///
    /// <para>This read is part of the cross-node drop/create fence, so an <em>indeterminate</em>
    /// result must never be reported as "no drop". Only an authoritative key-absent response
    /// (<see cref="KeyValueResponseType.DoesNotExist"/>) returns <c>false</c>; a present marker
    /// (<see cref="KeyValueResponseType.Get"/>) returns <c>true</c>; transient statuses
    /// (<c>MustRetry</c>/<c>WaitingForReplication</c>) are retried with bounded backoff; and any other
    /// status, an exhausted retry, or an exception <b>throws</b> a retryable
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> so the create path keeps its published-child
    /// recovery state rather than treating the fence as clear. Mapping an unconfirmed read to "absent"
    /// would let a branch publish while its parent is being purged.</para>
    /// </summary>
    public async Task<bool> HasDropIntentAsync(string sourceId)
    {
        int retries = 0;
        while (true)
        {
            KeyValueResponseType type;
            try
            {
                (type, _) = await keyspace.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, DropIntentKey(sourceId), -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"Could not confirm drop-intent state for '{sourceId}' ({ex.Message}); retry the operation");
            }

            if (type == KeyValueResponseType.Get)
                return true;

            // Authoritative absence is the ONLY result that clears the fence.
            if (type == KeyValueResponseType.DoesNotExist)
                return false;

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < RegistryKeyspace.MaxRetries)
            {
                await Task.Delay(retries * 10).ConfigureAwait(false);
                continue;
            }

            // Any other status, or exhausted transient retries: the read is indeterminate. Do NOT report
            // "no drop" — surface a retryable error so the fence is re-evaluated rather than bypassed.
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Drop-intent read for '{sourceId}' was indeterminate (status {type}); retry the operation");
        }
    }

    /// <summary>
    /// Releases the drop-intent fence for <paramref name="dbId"/>: stops its background lease renewer and
    /// frees the marker. Called after the fenced operation completes on every exit path.
    /// The marker write retries transient replication statuses with a bounded budget, so an orderly
    /// release normally frees the fence immediately. If the budget is exhausted the marker is left, and
    /// its bounded lease frees it once the lease lapses rather than stranding forever (the pre-lease
    /// behavior).
    /// </summary>
    public async Task ReleaseDropIntentAsync(string dbId)
    {
        // TryRemove, not a read-then-remove: two callers releasing the same fence must not both proceed
        // to the conditional expire with the same token.
        if (!dropIntentTokens.TryRemove(dbId, out string? token))
            return; // not held by this process — releasing would be someone else's fence to free

        await dropIntentFence.ReleaseAsync(DropIntentKey(dbId), token).ConfigureAwait(false);
    }

    /// <summary>
    /// Scans the registry bucket for drop-intent keys owned by <em>this</em> node and deletes them.
    /// Called once at startup: a drop never spans a process restart, so any drop-intent stamped with
    /// this node's id that survived a restart is a crash remnant — left when a crash hit between
    /// <see cref="AcquireDropIntentAsync"/> and the release.
    ///
    /// <para><b>This is the prompt path, not the only one.</b> A remnant blocks a later drop or relink
    /// of that database id only until its lease (<c>CamusDBOptions.FenceLeaseMs</c>) lapses, because
    /// the dead owner no longer renews it. Clearing it here frees the id immediately instead of making
    /// the first drop after a restart fail for up to one lease.</para>
    ///
    /// <para><b>Owner-scoped on purpose.</b> In a cluster the drop-intent key is Raft-replicated and
    /// visible on every node. Deleting <em>all</em> drop-intents at startup would let a restarting
    /// node wipe a drop-intent that a different node currently holds for an in-flight drop, reopening
    /// the cross-node drop/create race this fence exists to close. A node only ever writes markers
    /// under its own id, and its own in-flight drops die with its crash, so clearing only own-owned
    /// markers is always safe and never touches another live node's fence.</para>
    ///
    /// Returns the number of own stale markers deleted. Best-effort: individual delete failures are
    /// swallowed.
    /// </summary>
    public async Task<int> ClearOwnStaleDropIntentsAsync()
    {
        List<string> keys = [];

        foreach ((string key, byte[]? value) in await keyspace.ScanPrefixAsync(keyspace.Key(DropIntentSuffix)).ConfigureAwait(false))
        {
            // Only reclaim markers this node owns; leave another live node's fence untouched.
            if (owner.IsOwnStaleMarker(value))
                keys.Add(key);
        }

        foreach (string key in keys)
            await keyspace.DeleteBestEffortAsync(key).ConfigureAwait(false);

        return keys.Count;
    }

    // ── Drop-in-progress markers (crash-resumable keyspace purge) ──────────────────────────────
    //
    // DROP DATABASE unregisters the entry and then purges its keyspace with per-key autocommit
    // deletes — not one transaction. A crash mid-purge would orphan row/index/stats/meta data with
    // no reclaim. A "dropping" marker written before the unregister and cleared only after the purge
    // completes lets startup resume any interrupted purge. Owner-scoped (value = this node's id) so a
    // restarting node never resumes a drop another live node is actively running.
    //
    // Keys: _system/dbregistry/dropping:{dbId}  (value is the owning node id)

    private const string DroppingSuffix = "dropping:";

    private string DroppingKey(string dbId) => keyspace.Key(DroppingSuffix + dbId);

    /// <summary>
    /// Marks database <paramref name="dbId"/> as drop-in-progress before its keyspace purge begins.
    /// Stamped with this node's id so startup recovery resumes only its own interrupted drops. Cleared
    /// via <see cref="ClearDroppingAsync"/> only after the purge fully completes.
    /// </summary>
    public Task MarkDroppingAsync(string dbId) =>
        keyspace.SetAsync(
            DroppingKey(dbId), owner.Value,
            $"Failed to write drop-in-progress marker for database id '{dbId}'");

    /// <summary>Removes the drop-in-progress marker for <paramref name="dbId"/> after a completed purge. Best-effort.</summary>
    public Task ClearDroppingAsync(string dbId) => keyspace.DeleteBestEffortAsync(DroppingKey(dbId));

    /// <summary>
    /// Scans for drop-in-progress markers owned by <em>this</em> node and returns their database ids.
    /// Each is an interrupted drop this node started before a crash: the caller resumes the keyspace
    /// purge for any id no longer registered, then clears the marker via <see cref="ClearDroppingAsync"/>.
    /// A marker whose id is still registered means the crash preceded
    /// <see cref="DatabaseRegistry.UnregisterAsync"/> (no data was purged); the caller clears it without
    /// resuming. Owner-scoped so another live node's in-flight drop is never disturbed.
    /// </summary>
    public async Task<List<string>> LoadOwnDroppingIdsAsync()
    {
        string droppingPrefix = keyspace.Key(DroppingSuffix);
        List<string> ids = [];

        foreach ((string key, byte[]? value) in await keyspace.ScanPrefixAsync(droppingPrefix).ConfigureAwait(false))
        {
            if (owner.IsOwnStaleMarker(value))
                ids.Add(key[droppingPrefix.Length..]);
        }

        return ids;
    }

    // ── Lost-snapshot-protection markers (branch fail-closed state) ────────────────────────────
    //
    // A branch whose snapshot-floor hold is REMOVED from Kahuna's registry (released, or purged by
    // the reaper after its lease lapsed without renewal) can never regain its frozen ancestor view:
    // Kahuna refuses to renew a removed hold, and re-acquiring at the old timestamp does not bring
    // reclaimed history back. (A bare lapse is recoverable — a registered hold keeps constraining
    // reclamation and the next renew revives it — so no marker is written for that.) The marker
    // durably records the removed state so every node — including one
    // that opens the branch after a restart or failover — fails the branch closed with a definitive
    // message instead of rediscovering the loss through a refused renew. Correctness does not
    // depend on the marker (the refused renew is itself permanent and visible everywhere); the
    // marker is the fast, well-explained path.
    //
    // Keys: _system/dbregistry/holdlost:{dbId}  (value is a human-readable reason)

    private string HoldLostKey(string dbId) => keyspace.Key($"holdlost:{dbId}");

    /// <summary>
    /// Durably marks branch <paramref name="dbId"/> as having lost its snapshot protection.
    /// Idempotent — a repeat overwrites the reason. Throws when the write cannot be confirmed;
    /// callers treat the marker as best-effort and log, because the fail-closed behavior itself
    /// comes from the refused renew, not from this record.
    /// </summary>
    public Task MarkSnapshotProtectionLostAsync(string dbId, string reason) =>
        keyspace.SetAsync(
            HoldLostKey(dbId), System.Text.Encoding.UTF8.GetBytes(reason),
            $"Failed to write lost-snapshot-protection marker for database id '{dbId}'");

    /// <summary>
    /// Reads the lost-snapshot-protection marker for <paramref name="dbId"/>: the recorded reason,
    /// or <c>null</c> when no marker exists. An indeterminate read (exhausted transient retries)
    /// also returns <c>null</c> — deliberately lenient, because the marker only accelerates the
    /// error at open time; the guard's own refused renew still fails a genuinely lost branch closed.
    /// </summary>
    public async Task<string?> TryGetSnapshotProtectionLostAsync(string dbId)
    {
        int retries = 0;
        while (true)
        {
            KeyValueResponseType type;
            ReadOnlyKeyValueEntry? entry;
            try
            {
                (type, entry) = await keyspace.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, HoldLostKey(dbId), -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);
            }
            catch
            {
                return null;
            }

            if (type == KeyValueResponseType.Get && entry?.Value is not null)
                return System.Text.Encoding.UTF8.GetString(entry.Value);

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < RegistryKeyspace.MaxRetries)
            {
                await Task.Delay(retries * 10).ConfigureAwait(false);
                continue;
            }

            return null;
        }
    }

    /// <summary>
    /// Removes the lost-snapshot-protection marker for <paramref name="dbId"/>. Called when the
    /// branch is dropped — ids are never reused, so a stale marker is only clutter, but dropping
    /// the branch is the one action that genuinely resolves the state. Best-effort.
    /// </summary>
    public Task ClearSnapshotProtectionLostAsync(string dbId) => keyspace.DeleteBestEffortAsync(HoldLostKey(dbId));

    /// <summary>
    /// Stops every background fence-lease renewer this node still holds. Their leases then lapse on
    /// their own, freeing the fences for another node without leaving a live renewer rooted here.
    /// </summary>
    public ValueTask DisposeAsync() => dropIntentFence.DisposeAsync();
}
