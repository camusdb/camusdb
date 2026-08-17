
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
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Maintenance;

/// <summary>
/// Reclaims what a crashed or dead run left behind: this node's own stale drop-intent markers, a
/// <c>DROP DATABASE</c> keyspace purge that was interrupted part-way, the metadata namespace of a
/// branch whose creation never committed, and materialized-view refreshes whose owning run died.
///
/// <para>All four share one shape — a non-transactional multi-step operation that can be cut in
/// half by a process death, leaving state no ordinary code path will ever revisit. Each recovery is
/// therefore idempotent, fenced against a live operation on the same object, and safe to run
/// concurrently with normal traffic.</para>
///
/// <para><b>Fencing, not timing, is what makes this safe.</b> Nothing here decides an operation is
/// dead because it has taken too long. A recovery acts only once it has acquired the fence the live
/// operation would still be holding, so a slow-but-healthy run can never be collected out from
/// under itself.</para>
/// </summary>
internal sealed class StartupRecoveryService
{
    private readonly ExecutorContext context;

    private readonly CatalogsManager catalogs;

    private readonly DatabaseDropper databaseDropper;

    internal StartupRecoveryService(
        ExecutorContext context,
        CatalogsManager catalogs,
        DatabaseDropper databaseDropper
    )
    {
        this.context = context;
        this.catalogs = catalogs;
        this.databaseDropper = databaseDropper;
    }

    /// <summary>
    /// Takes over the unfinished materialized-view refreshes in one database — reclaiming what each
    /// dead run was building into and restarting its rebuild — and returns how many it handled.
    /// </summary>
    /// <remarks>
    /// Gated on the refresh fence, per job: a rebuild that is still running holds it, so acquiring it
    /// is proof the run that wrote the record is gone. Nothing here is time-based, which is what keeps
    /// a slow but healthy rebuild safe from being collected out from under itself.
    ///
    /// <para>A restarted rebuild runs to completion <b>inside</b> this sweep, holding the fence, which
    /// is what makes "exactly one node finishes it" true. One tick can therefore take as long as a
    /// rebuild does; that only delays the next sweep, and the cancellation token ends it at shutdown.</para>
    ///
    /// <para>Takes the executor because restarting a rebuild runs a full statement pipeline; the
    /// refresher reaches back through the facade for the same reason it does on the foreground path.</para>
    /// </remarks>
    internal async Task<int> ReclaimAbandonedRefreshesAsync(
        CommandExecutor executor, string databaseName, CancellationToken ct)
    {
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);

        if (!registry.TryResolveId(databaseName, out string databaseId) || context.SharedNode is null)
            return 0;

        // Asked before the database is opened: on the overwhelming majority of ticks there is nothing
        // to reclaim, and opening every database each time to discover that is pure cost — and, because
        // opening one competes for the same locks real statements use, avoidable contention.
        List<MaterializedViewRefreshJob> jobs =
            await CatalogsManager.ListRefreshJobsAsync(context.SharedNode.Kahuna, databaseId).ConfigureAwait(false);

        if (jobs.Count == 0)
            return 0;

        DatabaseDescriptor database = await context.DatabaseOpener.Open(databaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        int reclaimed = 0;

        foreach (MaterializedViewRefreshJob job in jobs)
        {
            if (ct.IsCancellationRequested)
                break;

            string fenceId = DatabaseRegistry.TableFenceId(database.Id, job.ViewTableId);

            if (!await registry.AcquireDropIntentAsync(fenceId).ConfigureAwait(false))
                continue; // a refresh is running right now — this storage is not abandoned

            try
            {
                if (await DDL.MaterializedViewRefresher.TakeOverAbandonedRefreshAsync(
                    executor, catalogs, registry, database, job.ViewTableId, fenceId, context.Logger, ct).ConfigureAwait(false))
                    reclaimed++;
            }
            finally
            {
                await registry.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
            }
        }

        return reclaimed;
    }

    /// <summary>
    /// Runs one orphan-reclamation sweep at startup so databases/tables whose retention window elapsed
    /// while the node was down (and any purge interrupted by a crash) are reclaimed promptly instead of
    /// waiting a full <see cref="CamusDBOptions.OrphanReclaimIntervalMs"/>. Best-effort and gated by
    /// leader election inside the reclaimer; failures are logged and swallowed so startup never blocks.
    /// </summary>
    internal async Task ReclaimExpiredOrphansOnStartupAsync(OrphanReclaimer reclaimer)
    {
        try
        {
            await reclaimer.ReclaimDueAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(ex, "Startup orphan reclamation sweep failed");
        }
    }

    /// <summary>
    /// Purges the <c>{branchId}/meta/…</c> namespace written by <c>CopyMetaForBranchAsync</c>
    /// when a branch-creation attempt is abandoned — either by a process crash (startup scrubber
    /// path) or by an in-process abort after the copy but before <c>RegisterAsync</c> commits.
    /// Uses a 3-round retry to absorb transient scan misses; each key is deleted idempotently.
    /// </summary>
    internal async Task PurgeBranchMetaNamespaceAsync(string branchId, IKahuna kahuna)
    {
        string metaBucket = $"{branchId}/meta";
        string metaPrefix = $"{branchId}/";

        for (int round = 0; round < 3; round++)
        {
            List<string> keys = [];
            await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
            {
                if (key.StartsWith(metaPrefix, StringComparison.Ordinal))
                    keys.Add(key);
            }

            if (keys.Count == 0)
                break;

            foreach (string key in keys)
            {
                try
                {
                    await kahuna.LocateAndTryDeleteKeyValue(
                        HLCTimestamp.Zero, key,
                        KeyValueDurability.Persistent, CancellationToken.None
                    ).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    context.Logger.LogWarning(ex, "Failed to delete orphan key '{Key}' for branch id {BranchId}", key, branchId);
                }
            }

            if (context.Logger.IsEnabled(LogLevel.Information))
                context.Logger.LogInformation("Purged {Count} meta key(s) for branch id {BranchId}", keys.Count, branchId);
        }
    }

    /// <summary>
    /// Purges KV namespace entries for branch ids that appear in the pending-create set but
    /// are not registered in the database registry. These orphans are left when a process crash
    /// occurs after metadata is written but before the registry entry is published during
    /// <c>CreateBranchDatabaseAsync</c>.
    ///
    /// <para>Safe to call concurrently with normal operations: orphan ids are not registered,
    /// so no live transaction or table-open path can reference them. The purge is idempotent.</para>
    ///
    /// <para>Also clears <em>this node's own</em> stale drop-intent markers left by a crash during
    /// <c>DropDatabase</c>. A node's own drop-intent can never legitimately survive its restart
    /// (drops do not span restarts), so any own-owned marker at startup is a crash remnant; without
    /// this cleanup the affected database would be permanently undroppable because
    /// <c>AcquireDropIntentAsync</c> uses <c>SetIfNotExists</c> and would always find the key present.
    /// The cleanup is owner-scoped so a restarting node never deletes a drop-intent another live node
    /// currently holds for an in-flight drop (which would reopen the cross-node drop/create race).</para>
    ///
    /// <para>Reachable from the facade because tests invoke it directly to verify the production
    /// scrub path rather than reimplementing the same logic inline.</para>
    /// </summary>
    internal async Task ScrubOrphanBranchNamespacesAsync(EmbeddedKahuna node, DatabaseRegistry registry)
    {
        try
        {
            // Clear this node's own stale drop-intent markers first so that a marker left by a crash
            // does not permanently block future drops. Owner-scoped: a restarting node must not delete
            // a drop-intent another live node currently holds for an in-flight drop.
            int intentsCleared = await registry.ClearOwnStaleDropIntentsAsync().ConfigureAwait(false);
            if (intentsCleared > 0 && context.Logger.IsEnabled(LogLevel.Information))
                context.Logger.LogInformation("Cleared {Count} stale drop-intent marker(s) on startup", intentsCleared);

            // Resume any DROP DATABASE this node started but did not finish before a crash (prior-epoch
            // dropping markers only — LoadOwnDroppingIdsAsync excludes this run's live markers). The
            // keyspace purge is per-key and non-transactional, so an interrupted drop can leave orphaned
            // row/index/stats data with no other reclaim. Each resume takes the id's fence and rechecks
            // AUTHORITATIVE registry state under it (not the local cache) so a concurrent relink/GC of the
            // same id can never interleave with the resumed purge.
            foreach (string droppingId in await registry.LoadOwnDroppingIdsAsync().ConfigureAwait(false))
            {
                try
                {
                    // Authoritative: if the id is still registered (live), the crash preceded
                    // UnregisterAsync — nothing was purged — so just clear the stale marker.
                    if (await registry.TryResolveNameByIdAsync(droppingId).ConfigureAwait(false) is not null)
                    {
                        await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
                        continue;
                    }

                    // Fence the id for the resumed purge. If we cannot take it, another operation holds
                    // it (a relink/GC on this or another node); leave the marker for a later resume.
                    if (!await registry.AcquireDropIntentAsync(droppingId).ConfigureAwait(false))
                        continue;

                    try
                    {
                        // Re-check liveness under the fence before destroying anything.
                        if (await registry.TryResolveNameByIdAsync(droppingId).ConfigureAwait(false) is not null)
                        {
                            await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
                            continue;
                        }

                        if (context.Logger.IsEnabled(LogLevel.Information))
                            context.Logger.LogInformation("Resuming interrupted DROP DATABASE keyspace purge for id {DbId} on startup", droppingId);

                        // Clear the marker only if the resumed purge verifiably completed; otherwise
                        // leave it so the NEXT startup resumes again (never abandon leaked keys).
                        if (await databaseDropper.PurgeKeyspaceByIdAsync(node.Kahuna, droppingId, null).ConfigureAwait(false))
                            await registry.ClearDroppingAsync(droppingId).ConfigureAwait(false);
                        else
                            context.Logger.LogWarning("Resumed purge for id {DbId} is still incomplete; leaving marker for the next startup", droppingId);
                    }
                    finally
                    {
                        await registry.ReleaseDropIntentAsync(droppingId).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogWarning(ex, "Failed to resume interrupted drop for id {DbId}", droppingId);
                }
            }

            List<string> orphanIds = await registry.LoadOrphanBranchIdsAsync().ConfigureAwait(false);
            if (orphanIds.Count == 0)
                return;

            if (context.Logger.IsEnabled(LogLevel.Information))
                context.Logger.LogInformation("Found {Count} orphan branch namespace(s) to scrub on startup", orphanIds.Count);

            IKahuna kahuna = node.Kahuna;
            foreach (string orphanId in orphanIds)
            {
                try
                {
                    await PurgeBranchMetaNamespaceAsync(orphanId, kahuna).ConfigureAwait(false);
                    await registry.ClearPendingBranchAsync(orphanId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    context.Logger.LogWarning(ex, "Failed to scrub orphan branch namespace for id {BranchId}", orphanId);
                }
            }
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(ex, "Startup orphan-branch scrub failed");
        }
    }
}
