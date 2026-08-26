
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Background garbage collector for deferred-dropped (orphaned) databases and tables. Once an orphan's
/// age exceeds <see cref="CamusDBOptions.OrphanRetentionMs"/> it is no longer recoverable and its
/// keyspace is physically reclaimed.
///
/// <para><b>Single-owner election.</b> Like <see cref="SnapshotHoldRenewer"/>, a sweep runs only while
/// this node leads the database-registry key's partition
/// (<see cref="EmbeddedKahuna.AmILeaderForKeyAsync"/>), re-checked every tick, so exactly one node
/// reclaims and a failover hands the work to the new leader.</para>
///
/// <para><b>Fencing &amp; the relink race.</b> A database orphan is purged under its per-id drop-intent
/// fence — the same fence <c>CREATE DATABASE ... RELINK</c> takes — and a table orphan under the
/// composite <see cref="DatabaseRegistry.TableFenceId"/> fence that <c>CREATE TABLE ... RELINK</c>
/// takes, so a reclamation and a recovery of the same id never interleave. Under the fence the reclaimer
/// re-confirms the object is still an orphan and <em>not</em> currently live (a database id absent from
/// the registry; a table whose per-object meta key is absent). If it finds the object live — a relink
/// that crashed after re-registering but before deleting its stale orphan record — it deletes the stale
/// record instead of purging.</para>
///
/// <para>Best-effort and idempotent: purges are autocommit deletes safe to run twice, a per-orphan
/// failure is logged and retried next tick, and the orphan record is deleted last so an interrupted
/// purge is finished by a later pass or by startup recovery.</para>
/// </summary>
internal sealed class OrphanReclaimer : IAsyncDisposable
{
    private readonly EmbeddedKahuna sharedNode;
    private readonly DatabaseRegistry registry;
    private readonly DatabaseDropper databaseDropper;
    private readonly ILogger<ICamusDB> logger;

    /// <summary>
    /// Configuration for this engine; injected, never ambient. Swapped by <see cref="ApplyOptions"/>:
    /// the loop pins the field once per iteration, so a published change takes effect at the next
    /// wake-up.
    /// </summary>
    private CamusDBOptions options;

    private readonly CancellationTokenSource cts = new();
    private Task? loop;

    /// <summary>
    /// How long the loop sleeps between re-checks while the reclaim interval is non-positive
    /// (disabled). Short enough that enabling the interval at runtime starts sweeping promptly.
    /// </summary>
    private const int DisabledProbeMs = 5_000;

    public OrphanReclaimer(
        EmbeddedKahuna sharedNode,
        DatabaseRegistry registry,
        DatabaseDropper databaseDropper,
        ILogger<ICamusDB> logger,
        CamusDBOptions options)
    {
        this.options = options;
        this.sharedNode = sharedNode;
        this.registry = registry;
        this.databaseDropper = databaseDropper;
        this.logger = logger;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic; the loop
    /// pins the field once per iteration, so a shortened interval takes effect only after the
    /// currently running delay elapses — accepted, the loop is never interrupted mid-sleep.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Starts the background reclaim loop. The loop always runs — while the interval is non-positive
    /// it sleeps a short probe interval and re-checks, so an interval configured at runtime starts
    /// reclamation on an engine where it was disabled at construction.
    /// </summary>
    public void Start()
    {
        // Serialize concurrent Start calls: an unsynchronized `loop ??=` is check-then-act, and two
        // racing callers would each launch an independent reclaim loop for the process lifetime.
        lock (this)
        {
            loop ??= ReclaimLoopAsync(cts.Token);
        }
    }

    private async Task ReclaimLoopAsync(CancellationToken ct)
    {
        // The try/catch is INSIDE the loop so a single failed sweep (a transient scan error, a leadership
        // flip) is logged and retried next interval — it never terminates the loop. Only cancellation ends it.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Pin one snapshot per iteration so a runtime configuration change re-arms the loop.
                int intervalMs = options.OrphanReclaimIntervalMs;

                if (intervalMs <= 0)
                {
                    await Task.Delay(DisabledProbeMs, ct).ConfigureAwait(false);
                    continue;
                }

                await Task.Delay(intervalMs, ct).ConfigureAwait(false);
                await ReclaimDueAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // normal shutdown
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Orphan reclaimer sweep failed; retrying next interval");
            }
        }
    }

    private HLCTimestamp Now() =>
        sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());

    /// <summary>
    /// True once an orphan dropped at <paramref name="droppedAt"/> is past the retention window. Always
    /// false when retention is disabled (<see cref="CamusDBOptions.OrphanRetentionMs"/> &lt;= 0), which
    /// keeps orphans indefinitely.
    /// </summary>
    private bool IsExpired(HLCTimestamp droppedAt, HLCTimestamp now)
    {
        long retentionMs = options.OrphanRetentionMs;
        if (retentionMs <= 0)
            return false;
        return droppedAt + TimeSpan.FromMilliseconds(retentionMs) <= now;
    }

    /// <summary>
    /// Runs one reclamation sweep on the elected node and returns the number of orphans physically
    /// reclaimed. Exposed to tests so a sweep can be forced without waiting a full tick.
    /// </summary>
    internal async Task<int> ReclaimDueAsync(CancellationToken ct)
    {
        if (!await sharedNode.AmILeaderForKeyAsync(registry.RegistryBucket, ct).ConfigureAwait(false))
            return 0;

        // Retention disabled → nothing is ever eligible; skip the scans entirely.
        if (options.OrphanRetentionMs <= 0)
            return 0;

        HLCTimestamp now = Now();
        int reclaimed = 0;

        reclaimed += await ReclaimDatabaseOrphansAsync(now, ct).ConfigureAwait(false);
        reclaimed += await ReclaimTableOrphansAsync(now, ct).ConfigureAwait(false);
        reclaimed += await ReclaimAbandonedRefreshesAsync(ct).ConfigureAwait(false);

        return reclaimed;
    }

    /// <summary>
    /// Reclaims the staging storage of materialized-view refreshes that never finished.
    ///
    /// <para>This is the path that covers what a later refresh cannot. Reclaiming on the next refresh
    /// only helps a view that is refreshed again, and reclaiming on drop only helps one that is
    /// dropped; a view that is neither — the ordinary case after a crash on a view refreshed on a
    /// schedule that has since been turned off — kept its abandoned storage registered indefinitely.
    /// </para>
    ///
    /// <para>Eligibility is decided by the <b>fence</b>, not by elapsed time. A refresh that is still
    /// running holds it for its whole duration, so anything whose fence can be acquired belongs to a
    /// run that is gone. That is exact where a staleness timeout would only be a guess, and a guess
    /// that fired early would delete storage out from under a slow but healthy rebuild.</para>
    /// </summary>
    private async Task<int> ReclaimAbandonedRefreshesAsync(CancellationToken ct)
    {
        if (ReclaimRefreshJobsForDatabaseAsync is null)
            return 0;

        int reclaimed = 0;

        foreach (DatabaseRegistryEntry db in await registry.GetBackgroundSnapshotAsync().ConfigureAwait(false))
        {
            if (ct.IsCancellationRequested)
                break;

            if (!await sharedNode.AmILeaderForKeyAsync(registry.RegistryBucket, ct).ConfigureAwait(false))
                return reclaimed; // leadership lost mid-sweep

            try
            {
                reclaimed += await ReclaimRefreshJobsForDatabaseAsync(db.Name, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                // One database's failure must not stop the sweep for the others; the next tick retries.
                logger.LogWarning(ex, "Failed to reclaim abandoned materialized-view refreshes in database {DbName}", db.Name);
            }
        }

        return reclaimed;
    }

    /// <summary>
    /// Reclaims abandoned refresh jobs for one database by name, returning how many it removed. Set by
    /// the engine, which owns the database opener and the refresher; null leaves the sweep inert.
    /// </summary>
    internal Func<string, CancellationToken, Task<int>>? ReclaimRefreshJobsForDatabaseAsync { get; set; }

    private async Task<int> ReclaimDatabaseOrphansAsync(HLCTimestamp now, CancellationToken ct)
    {
        List<OrphanDatabaseRecord> orphans = await registry.LoadDatabaseOrphansAsync().ConfigureAwait(false);
        if (orphans.Count == 0)
            return 0;

        int reclaimed = 0;
        foreach (OrphanDatabaseRecord orphan in orphans)
        {
            if (ct.IsCancellationRequested)
                break;
            if (!IsExpired(orphan.DroppedAt, now))
                continue;

            // Leadership can flip mid-sweep; stop acting the moment this node is no longer the elected
            // sweeper so an old and new leader never purge concurrently.
            if (!await sharedNode.AmILeaderForKeyAsync(registry.RegistryBucket, ct).ConfigureAwait(false))
                break;

            // Acquire the fence FIRST, then make every decision from authoritative persistent state read
            // under it — never from a pre-fence snapshot (that is the TOCTOU the review flagged).
            if (!await registry.AcquireDropIntentAsync(orphan.Id).ConfigureAwait(false))
                continue; // a relink or another reclaimer holds the fence

            try
            {
                // Still an orphan? (A relink deletes the record under this same fence.)
                OrphanDatabaseRecord? current = await registry.TryGetDatabaseOrphanAsync(orphan.Id).ConfigureAwait(false);
                if (current is null || !IsExpired(current.DroppedAt, now))
                    continue;

                // Live again? Authoritatively (KV, cross-node) — a relink that re-registered the id but
                // crashed before deleting its orphan record. Clean the stale record; never purge live data.
                string? liveName = await registry.TryResolveNameByIdAsync(orphan.Id).ConfigureAwait(false);
                if (liveName is not null)
                {
                    await registry.DeleteDatabaseOrphanAsync(orphan.Id).ConfigureAwait(false);
                    continue;
                }

                await registry.MarkDroppingAsync(orphan.Id).ConfigureAwait(false);

                // Only remove the recovery record + drop marker if the purge VERIFIABLY completed.
                // Otherwise leave both so a later sweep / startup finishes it — never abandon leaked keys.
                if (await databaseDropper.PurgeKeyspaceByIdAsync(sharedNode.Kahuna, orphan.Id, null, ct).ConfigureAwait(false))
                {
                    await registry.DeleteDatabaseOrphanAsync(orphan.Id).ConfigureAwait(false);
                    await registry.ClearDroppingAsync(orphan.Id).ConfigureAwait(false);
                    reclaimed++;

                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation("Reclaimed orphaned database id {DbId} (former name '{Name}')", orphan.Id, orphan.FormerName);
                }
                else
                {
                    logger.LogWarning("Purge of orphaned database id {DbId} is incomplete; keeping orphan record for the next sweep", orphan.Id);
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Failed to reclaim orphaned database id {DbId}", orphan.Id);
            }
            finally
            {
                await registry.ReleaseDropIntentAsync(orphan.Id).ConfigureAwait(false);
            }
        }

        return reclaimed;
    }

    private async Task<int> ReclaimTableOrphansAsync(HLCTimestamp now, CancellationToken ct)
    {
        int reclaimed = 0;

        // One authoritative registry snapshot per sweep for the db list (down from a second scan).
        foreach (DatabaseRegistryEntry db in await registry.GetBackgroundSnapshotAsync().ConfigureAwait(false))
        {
            if (ct.IsCancellationRequested)
                break;

            foreach (OrphanTableRecord orphan in await ScanTableOrphansAsync(db.Id, ct).ConfigureAwait(false))
            {
                if (ct.IsCancellationRequested)
                    break;
                if (!IsExpired(orphan.DroppedAt, now))
                    continue;

                if (!await sharedNode.AmILeaderForKeyAsync(registry.RegistryBucket, ct).ConfigureAwait(false))
                    return reclaimed; // leadership lost mid-sweep

                string fenceId = DatabaseRegistry.TableFenceId(db.Id, orphan.TableId);
                if (!await registry.AcquireDropIntentAsync(fenceId).ConfigureAwait(false))
                    continue; // a relink or another reclaimer holds the fence

                try
                {
                    // Re-confirm the orphan record still exists under the fence.
                    if (!await TableOrphanExistsAsync(db.Id, orphan.TableId, ct).ConfigureAwait(false))
                        continue; // reclaimed meanwhile

                    if (orphan.Kind == OrphanKind.RetiredContents)
                    {
                        if (await ReclaimRetiredContentsAsync(db.Id, orphan, ct).ConfigureAwait(false))
                            reclaimed++;

                        continue;
                    }

                    // The table is live again (relink re-persisted its meta key) but a crash left the
                    // orphan record — clean it instead of purging live data. The per-table meta key is the
                    // authoritative liveness signal.
                    if (await MetaTableExistsAsync(db.Id, orphan.TableId, ct).ConfigureAwait(false))
                    {
                        await DeleteTableOrphanRecordAsync(db.Id, orphan.TableId, ct).ConfigureAwait(false);
                        continue;
                    }

                    // PurgeTableKeyspaceAsync deletes the orphan record last, and only if all data is
                    // verified gone; a false return means it left the record for a later sweep.
                    // The captured schema is what knows where the rows live: a refreshed materialized
                    // view owns a key-space that is not its id, so purging by id alone would delete an
                    // empty prefix and leak the real contents indefinitely.
                    string? orphanStorageId = orphan.Schema?.StorageId;

                    if (await databaseDropper.PurgeTableKeyspaceAsync(sharedNode.Kahuna, db.Id, orphan.TableId, orphanStorageId, ct).ConfigureAwait(false))
                    {
                        reclaimed++;
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("Reclaimed orphaned table id {TableId} (former name '{Name}') in database {DbId}", orphan.TableId, orphan.FormerName, db.Id);
                    }
                    else
                    {
                        logger.LogWarning("Purge of orphaned table id {TableId} in database {DbId} is incomplete; keeping orphan record for the next sweep", orphan.TableId, db.Id);
                    }
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    logger.LogWarning(ex, "Failed to reclaim orphaned table id {TableId} in database {DbId}", orphan.TableId, db.Id);
                }
                finally
                {
                    await registry.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
                }
            }
        }

        return reclaimed;
    }

    /// <summary>
    /// Reclaims one retired contents generation, or recognises that a recovery already took it.
    /// </summary>
    /// <remarks>
    /// <para><b>The liveness test used for a dropped table is wrong here and must not be reused.</b> A
    /// dropped table is gone from the catalog, so a live meta key at its id proves a recovery finished.
    /// Retired contents belong to a relation that never stopped being live, and on a first truncate the
    /// retired key-space is named by that relation's own id — so that test would fire on every truncate
    /// and delete the record without reclaiming a byte, leaking a full copy of the table each time.</para>
    ///
    /// <para>The only proof of a recovery is <see cref="OrphanTableRecord.RelinkTargetId"/>, and it is
    /// believed only when the relation it names is live <em>and</em> actually reads this key-space. A
    /// target recorded but never published (a recovery that crashed) leaves the storage reclaimable,
    /// which is correct: the recovery can be retried until retention runs out.</para>
    /// </remarks>
    private async Task<bool> ReclaimRetiredContentsAsync(string dbId, OrphanTableRecord orphan, CancellationToken ct)
    {
        string retiredStorageId = orphan.RetiredStorageId;

        if (string.IsNullOrEmpty(retiredStorageId))
        {
            logger.LogWarning(
                "Retired-contents record {TableId} in database {DbId} names no storage generation; leaving it for inspection",
                orphan.TableId, dbId);
            return false;
        }

        if (orphan.RelinkTargetId is { Length: > 0 } target
            && await RelationReadsStorageAsync(dbId, target, retiredStorageId, ct).ConfigureAwait(false))
        {
            // Recovered: the storage belongs to a live relation now, so only the record is stale.
            await DeleteTableOrphanRecordAsync(dbId, orphan.TableId, ct).ConfigureAwait(false);
            return false;
        }

        if (await databaseDropper.PurgeRetiredContentsAsync(sharedNode.Kahuna, dbId, retiredStorageId, ct).ConfigureAwait(false))
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Reclaimed retired contents {StorageId} of table {SourceTableId} (name '{Name}') in database {DbId}",
                    retiredStorageId, orphan.SourceTableId, orphan.FormerName, dbId);

            return true;
        }

        logger.LogWarning(
            "Purge of retired contents {StorageId} in database {DbId} is incomplete; keeping the record for the next sweep",
            retiredStorageId, dbId);

        return false;
    }

    /// <summary>
    /// Whether a live relation with this id exists and reads <paramref name="storageId"/>.
    /// </summary>
    /// <remarks>
    /// Both halves are required. A relation can exist at the recorded target id without owning this
    /// key-space — the id was reallocated, or a recovery was rolled back and the id reused — and
    /// treating that as "recovered" would abandon the storage permanently.
    /// </remarks>
    private async Task<bool> RelationReadsStorageAsync(string dbId, string tableId, string storageId, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await sharedNode.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{dbId}/meta/table:{tableId}", -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, ct).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is not { Length: > 0 })
            return false;

        TableSchema live = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchema);
        return string.Equals(live.EffectiveStorageId, storageId, StringComparison.Ordinal);
    }

    // ── Raw KV helpers for the per-database meta namespace ({dbId}/meta/orphan:{tableId}) ──────────

    private async Task<List<OrphanTableRecord>> ScanTableOrphansAsync(string dbId, CancellationToken ct)
    {
        string metaBucket = $"{dbId}/meta";
        string orphanPrefix = $"{dbId}/meta/orphan:";
        List<OrphanTableRecord> orphans = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in sharedNode.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).ConfigureAwait(false))
        {
            if (!key.StartsWith(orphanPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            orphans.Add(MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.OrphanTableRecord));
        }

        return orphans;
    }

    private async Task<bool> TableOrphanExistsAsync(string dbId, string tableId, CancellationToken ct) =>
        await KeyExistsAsync($"{dbId}/meta/orphan:{tableId}", ct).ConfigureAwait(false);

    private async Task<bool> MetaTableExistsAsync(string dbId, string tableId, CancellationToken ct) =>
        await KeyExistsAsync($"{dbId}/meta/table:{tableId}", ct).ConfigureAwait(false);

    private async Task<bool> KeyExistsAsync(string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await sharedNode.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, ct).ConfigureAwait(false);
        return type == KeyValueResponseType.Get && entry?.Value is not null;
    }

    private async Task DeleteTableOrphanRecordAsync(string dbId, string tableId, CancellationToken ct)
    {
        try
        {
            await sharedNode.Kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, $"{dbId}/meta/orphan:{tableId}",
                KeyValueDurability.Persistent, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch { }
    }

    public async ValueTask DisposeAsync()
    {
        await cts.CancelAsync().ConfigureAwait(false);
        if (loop is not null)
        {
            try { await loop.ConfigureAwait(false); }
            catch { /* loop swallows its own errors */ }
        }
        cts.Dispose();
    }
}
