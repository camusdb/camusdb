
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Background garbage collector for deferred-dropped (orphaned) databases and tables. Once an orphan's
/// age exceeds <see cref="CamusDBConfig.OrphanRetentionMs"/> it is no longer recoverable and its
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
    private readonly int intervalMs;
    private readonly CancellationTokenSource cts = new();
    private Task? loop;

    public OrphanReclaimer(
        EmbeddedKahuna sharedNode,
        DatabaseRegistry registry,
        DatabaseDropper databaseDropper,
        ILogger<ICamusDB> logger,
        int intervalMs)
    {
        this.sharedNode = sharedNode;
        this.registry = registry;
        this.databaseDropper = databaseDropper;
        this.logger = logger;
        this.intervalMs = intervalMs;
    }

    /// <summary>Starts the background reclaim loop. A non-positive interval disables the loop entirely.</summary>
    public void Start()
    {
        if (intervalMs <= 0)
            return;
        loop ??= ReclaimLoopAsync(cts.Token);
    }

    private async Task ReclaimLoopAsync(CancellationToken ct)
    {
        // The try/catch is INSIDE the loop so a single failed sweep (a transient scan error, a leadership
        // flip) is logged and retried next interval — it never terminates the loop. Only cancellation ends it.
        while (!ct.IsCancellationRequested)
        {
            try
            {
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
    /// false when retention is disabled (<see cref="CamusDBConfig.OrphanRetentionMs"/> &lt;= 0), which
    /// keeps orphans indefinitely.
    /// </summary>
    private bool IsExpired(HLCTimestamp droppedAt, HLCTimestamp now)
    {
        long retentionMs = CamusDBConfig.OrphanRetentionMs;
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
        if (CamusDBConfig.OrphanRetentionMs <= 0)
            return 0;

        HLCTimestamp now = Now();
        int reclaimed = 0;

        reclaimed += await ReclaimDatabaseOrphansAsync(now, ct).ConfigureAwait(false);
        reclaimed += await ReclaimTableOrphansAsync(now, ct).ConfigureAwait(false);

        return reclaimed;
    }

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
        foreach (DatabaseRegistryEntry db in await registry.ScanAllEntriesAsync().ConfigureAwait(false))
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
                    if (await databaseDropper.PurgeTableKeyspaceAsync(sharedNode.Kahuna, db.Id, orphan.TableId, ct).ConfigureAwait(false))
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
