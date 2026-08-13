
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;


namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseDropper
{
    // Max rounds a single keyspace bucket is re-scanned during purge to absorb a transient
    // scan miss of a just-committed entry (delete is idempotent, so re-scanning is safe).
    private const int MaxPurgeScanRounds = 3;

    // Bounded retries for a single key delete that returns a retryable (non-terminal) status.
    private const int MaxDeleteRetries = 10;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly ILogger<ICamusDB> logger;

    /// <summary>Configuration for this engine; injected, never ambient.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    public DatabaseDropper(DatabaseDescriptors databaseDescriptors, ILogger<ICamusDB> logger, CamusDBOptions options)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.logger = logger;
        this.options = options;
    }

    /// <summary>
    /// Deletes one key and <b>verifies the outcome</b>: returns <c>true</c> only on a terminal
    /// <see cref="KeyValueResponseType.Deleted"/> / <see cref="KeyValueResponseType.DoesNotExist"/>,
    /// retrying retryable statuses (<c>MustRetry</c> / <c>WaitingForReplication</c>) with bounded
    /// backoff. Any other status, an exhausted retry, or an exception returns <c>false</c> so the caller
    /// treats the enclosing purge as <em>incomplete</em> and keeps the recovery markers in place.
    /// </summary>
    private async Task<bool> DeleteExactVerifiedAsync(IKahuna kahuna, string key, CancellationToken ct)
    {
        int retries = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            KeyValueResponseType type;
            try
            {
                (type, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to delete key '{Key}' during purge", key);
                return false;
            }

            if (type is KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist)
                return true;

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < MaxDeleteRetries)
            {
                await Task.Delay(retries * 10, ct).ConfigureAwait(false);
                continue;
            }

            logger.LogWarning("Delete of key '{Key}' returned {Type}; purge treated as incomplete", key, type);
            return false;
        }
    }

    /// <summary>
    /// Closes database <paramref name="id"/>: evicts its descriptor, drains in-flight operations, and
    /// (when <paramref name="purge"/> is <c>true</c>) physically deletes its entire keyspace.
    ///
    /// <para>A deferred (non-<c>FORCE</c>) <c>DROP DATABASE</c> of a root database passes
    /// <paramref name="purge"/> <c>= false</c>: the database is closed but every row/index/meta key is
    /// left on disk so <c>CREATE DATABASE ... RELINK TO {id}</c> can recover it, or the garbage
    /// collector can reclaim it after the retention window. The caller has already written the orphan
    /// record and unregistered the name.</para>
    /// </summary>
    /// <param name="headlessKahuna">
    /// Shared node used to purge the keyspace by id alone when there is no usable descriptor on this node
    /// (absent from the cache, or a faulted load lazy). Required to purge in those cases — without it the
    /// purge cannot be verified and the recovery marker is kept. Ignored when a live descriptor is found.
    /// </param>
    /// <param name="drainTimeout">
    /// How long to wait for in-flight operations to drain before aborting their transactions and retrying.
    /// Defaults to 30 s; exposed so tests can drive the timeout path quickly.
    /// </param>
    /// <returns>
    /// <c>true</c> if the keyspace was fully and verifiably purged (or <paramref name="purge"/> was
    /// <c>false</c>, i.e. nothing to purge). <c>false</c> means the purge was incomplete — the caller
    /// must <b>not</b> clear the drop-in-progress marker so a later startup/GC pass resumes it. A missing
    /// or faulted descriptor no longer returns a phantom <c>true</c>: it falls back to a headless purge,
    /// and a drain that never completes returns <c>false</c> without purging or disposing.
    /// </returns>
    public async Task<bool> Drop(string id, bool purge = true, IKahuna? headlessKahuna = null, TimeSpan? drainTimeout = null)
    {
        if (!databaseDescriptors.Descriptors.TryRemove(id, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
        {
            // No live descriptor on this node. There is nothing to drain or dispose, but the keyspace may
            // still exist in shared KV, so a purge must still run headlessly rather than reporting success.
            return await PurgeHeadlessAsync(id, purge, headlessKahuna).ConfigureAwait(false);
        }

        DatabaseDescriptor databaseDescriptor;
        try
        {
            databaseDescriptor = await databaseDescriptorLazy;
        }
        catch
        {
            // The cached lazy is faulted (e.g. LoadDatabase threw during a prior Open attempt). There is no
            // usable descriptor to drain or dispose, but its keyspace may still be on disk — purge by id
            // rather than declaring the drop complete and leaking the keyspace while clearing its marker.
            return await PurgeHeadlessAsync(id, purge, headlessKahuna).ConfigureAwait(false);
        }

        // Signal all new AddRef calls to fail immediately with DatabaseDoesntExist.
        databaseDescriptor.MarkDropped();

        TimeSpan timeout = drainTimeout ?? TimeSpan.FromSeconds(30);

        // Wait for any operations already holding a use-reference to finish.
        bool drained = await WaitForDrainAsync(databaseDescriptor, timeout).ConfigureAwait(false);

        if (!drained)
        {
            // Writers are still in-flight. Abort their transactions so none can commit row/index writes
            // AFTER the purge scan has passed their bucket (which would recreate keys behind the drop),
            // then wait a bounded second time for the now-failing operations to release their refs.
            logger.LogWarning(
                "DropDatabase for id={Id}: in-flight operations did not drain within {Timeout}; aborting active transactions and retrying drain",
                id, timeout);
            try
            {
                await databaseDescriptor.Transactions.RollbackAllActiveAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to abort active transactions for id={Id} during drop drain", id);
            }

            drained = await WaitForDrainAsync(databaseDescriptor, timeout).ConfigureAwait(false);
        }

        if (!drained)
        {
            // Still not drained: a writer may yet commit into the keyspace. Do NOT purge (a recreated key
            // could outlive the drop) and do NOT dispose the descriptor (in-flight operations would then
            // run against a disposed descriptor). Report the drop incomplete so the caller keeps the
            // drop-in-progress marker durable; a startup/GC resume finishes the purge once writers drain.
            logger.LogWarning(
                "DropDatabase for id={Id}: operations still in-flight after abort; deferring purge and disposal and leaving the recovery marker",
                id);
            return false;
        }

        // Purge all key-value entries that belong to this database from the shared node.
        // The shared node is NOT disposed here — it's owned by the process, not this database.
        //
        // Purged namespaces (scan bucket → stored key prefix):
        //   {id}/meta                    → {id}/                           — meta keys (system, version, schemas, history, coordinator jobs)
        //   {id}:                        → {id}:                           — statistics keys ({id}:stats:{tableId})
        //   {id}:{tableId}:r             → {id}:{tableId}:r/               — row data for every table
        //   {id}:{tableId}:i:{indexId}   → {id}:{tableId}:i:{indexId}/     — index data for every index
        //
        //   Schema-log entries in the Raft WAL are append-only and cannot be removed.
        //
        // On a deferred drop (purge == false) the keyspace is intentionally left intact for recovery;
        // the descriptor is still evicted and disposed so the database is closed on this node.
        bool purged = true;
        if (purge)
            purged = await PurgeClusterKeyspaceAsync(databaseDescriptor, id).ConfigureAwait(false);

        databaseDescriptor.Dispose();
        Log.LogDatabaseDropped(logger, databaseDescriptor.Name);
        return purged;
    }

    /// <summary>
    /// Awaits the descriptor's drain up to <paramref name="timeout"/>. Returns <c>true</c> if every
    /// in-flight use-reference was released in time, <c>false</c> on timeout.
    /// </summary>
    private static async Task<bool> WaitForDrainAsync(DatabaseDescriptor descriptor, TimeSpan timeout)
    {
        try
        {
            await descriptor.WhenDrainedAsync().WaitAsync(timeout).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// Purge path taken when there is no usable descriptor on this node (absent from cache or faulted
    /// load). A deferred drop (<paramref name="purge"/> <c>= false</c>) has nothing to reclaim and returns
    /// <c>true</c>. Otherwise the keyspace is purged by id via <paramref name="headlessKahuna"/>; if none
    /// was supplied the purge cannot be verified, so it returns <c>false</c> to keep the recovery marker.
    /// </summary>
    private async Task<bool> PurgeHeadlessAsync(string id, bool purge, IKahuna? headlessKahuna)
    {
        if (!purge)
            return true;

        if (headlessKahuna is null)
        {
            logger.LogWarning(
                "DropDatabase for id={Id}: no descriptor and no shared node available to purge the keyspace; leaving the recovery marker",
                id);
            return false;
        }

        return await PurgeKeyspaceByIdAsync(headlessKahuna, id, null).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes all KV entries belonging to <paramref name="descriptor"/>'s database, using the
    /// descriptor's live schema as a safety net over the persisted keyspace catalog. Returns whether the
    /// purge verifiably completed.
    /// </summary>
    private Task<bool> PurgeClusterKeyspaceAsync(DatabaseDescriptor descriptor, string id)
        => PurgeKeyspaceByIdAsync(descriptor.Kahuna.Kahuna, id, [.. descriptor.Schema.Tables.Values]);

    /// <summary>
    /// Purges the full keyspace of database <paramref name="id"/>, driven by the persisted keyspace
    /// catalog under <c>{id}/meta/keyspace:{tableId}</c> (which records every index id ever allocated,
    /// so historically-dropped indexes are still purged), plus an optional
    /// <paramref name="safetyNetTables"/> pass for live tables not yet in the catalog.
    ///
    /// <para><b>Meta is deleted LAST</b> (row/index/stats first). The keyspace catalog lives in the
    /// meta namespace, and a crash-resumed purge rebuilds the row/index target list from it. Deleting
    /// meta last guarantees the catalog is intact until the data it describes is already gone, so a
    /// resume can re-run this method from the id alone (no live descriptor, <c>safetyNetTables = null</c>)
    /// and is fully idempotent — already-deleted keys are harmless no-op deletes.</para>
    ///
    /// Scan bucket = the string before the last '/' of the stored keys. Each key is deleted as a
    /// standalone autocommit operation (HLCTimestamp.Zero). Errors are logged and skipped so a partial
    /// failure never wedges the drop.
    /// </summary>
    internal async Task<bool> PurgeKeyspaceByIdAsync(IKahuna kahuna, string id, IReadOnlyList<TableSchema>? safetyNetTables, CancellationToken ct = default)
    {
        string metaBucket = $"{id}/meta";
        string metaKeyPrefix = $"{id}/meta";
        string catalogPrefix = $"{id}/meta/keyspace:";

        // Phase A: read the keyspace catalog. Read-only — meta (including the catalog) is deleted last
        // (Phase E) so the catalog survives for every crash-resume. The scan is repeated up to
        // MaxPurgeScanRounds times, accumulating entries idempotently (TryAdd), and stops once a full
        // round adds nothing new. A catalog key transiently missed on one round would otherwise leak
        // its table's entire row/index overlay: on a crashed drop the resume re-scans, but on a
        // *successful* drop the marker is cleared and there is no resume, so re-scanning here is the
        // only guard for that case.
        Dictionary<string, List<string>> catalogByTableId = [];
        for (int round = 0; round < MaxPurgeScanRounds; round++)
        {
            CatalogScanRoundsForTesting++;
            int before = catalogByTableId.Count;
            try
            {
                await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                    HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
                {
                    if (!key.StartsWith(catalogPrefix, StringComparison.Ordinal) || entry.Value is not { Length: > 0 })
                        continue;

                    string tableId = key[catalogPrefix.Length..];
                    try
                    {
                        string[] indexIds = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.StringArray);
                        catalogByTableId.TryAdd(tableId, [.. indexIds]);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Failed to parse keyspace catalog for table {TableId} in database {Id}", tableId, id);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to scan meta bucket for catalog while purging database (id={Id})", id);
                break;
            }

            // A confirming round that adds nothing new means the catalog is fully collected.
            if (round > 0 && catalogByTableId.Count == before)
                break;
        }

        // Phase B: build the row/index bucket prefixes and exact stats keys.
        List<(string bucket, string keyPrefix)> rowIndexPrefixes = [];
        List<string> exactKeys = [];
        HashSet<string> coveredTableIds = [];

        foreach ((string tableId, List<string> indexIds) in catalogByTableId)
        {
            coveredTableIds.Add(tableId);
            rowIndexPrefixes.Add(($"{id}:{tableId}:r", $"{id}:{tableId}:r/"));
            exactKeys.Add($"{id}:stats:{tableId}");
            foreach (string indexId in indexIds)
                rowIndexPrefixes.Add(($"{id}:{tableId}:i:{indexId}", $"{id}:{tableId}:i:{indexId}/"));
        }

        // Safety net: live tables not yet in the catalog (unavailable on a headless resume).
        if (safetyNetTables is not null)
        {
            foreach (TableSchema table in safetyNetTables)
            {
                if (table.Id is null || coveredTableIds.Contains(table.Id))
                    continue;
                rowIndexPrefixes.Add(($"{id}:{table.Id}:r", $"{id}:{table.Id}:r/"));
                exactKeys.Add($"{id}:stats:{table.Id}");
                if (table.Indexes is not null)
                    foreach (TableIndexSchema index in table.Indexes)
                        if (!string.IsNullOrEmpty(index.KvId))
                            rowIndexPrefixes.Add(($"{id}:{table.Id}:i:{index.KvId}", $"{id}:{table.Id}:i:{index.KvId}/"));
            }
        }

        // Track verified completion across every phase — the caller only clears the drop-in-progress
        // marker (and, for a table, the recovery record) when this is true.
        bool complete = true;

        // Phase C: purge rows and index entries.
        foreach ((string bucket, string keyPrefix) in rowIndexPrefixes)
            complete &= await PurgeBucketAsync(kahuna, id, bucket, keyPrefix, ct).ConfigureAwait(false);

        // Phase D: delete exact stats keys (no '/' suffix so unreachable by bucket scan).
        foreach (string key in exactKeys)
            complete &= await DeleteExactVerifiedAsync(kahuna, key, ct).ConfigureAwait(false);

        // Phase E: delete the meta namespace LAST (catalog included) so it survived for every resume.
        complete &= await PurgeBucketAsync(kahuna, id, metaBucket, metaKeyPrefix, ct).ConfigureAwait(false);

        return complete;
    }

    /// <summary>
    /// Physically reclaims a single orphaned table's keyspace: its row bucket, every index bucket named
    /// in the persisted keyspace catalog (so historically-dropped indexes are covered), its exact stats
    /// key, the keyspace-catalog key, the per-table meta key, and finally its orphan record. Driven by
    /// <c>(dbId, tableId)</c> alone so it is idempotent and resumable — every delete is an autocommit
    /// no-op if already gone. The orphan record is deleted <b>last</b> so a crash mid-purge leaves the
    /// record for the next reclamation pass to finish.
    ///
    /// <para>The caller (the reclaimer / manual purge) is responsible for having confirmed, under the
    /// table fence, that the table is not live (its per-table meta key is absent) before purging — this
    /// method does not re-check.</para>
    /// </summary>
    /// <param name="tableId">
    /// The relation's immutable identity. Its meta, stats, orphan and keyspace-catalog keys are all
    /// filed under this.
    /// </param>
    /// <param name="storageId">
    /// The key-space the rows and index entries actually occupy, when that differs from the identity —
    /// which it does for a materialized view that has been refreshed, because a refresh replaces the
    /// storage while deliberately keeping the identity. Null means the two are the same, as for every
    /// ordinary table. Purging the identity's key-space in that case would delete nothing and silently
    /// leave a full copy of the relation behind forever.
    /// </param>
    internal async Task<bool> PurgeTableKeyspaceAsync(
        IKahuna kahuna, string dbId, string tableId, string? storageId = null, CancellationToken ct = default)
    {
        string catalogKey = $"{dbId}/meta/keyspace:{tableId}";
        string dataId = string.IsNullOrEmpty(storageId) ? tableId : storageId;

        // Collect index ids from the keyspace catalog (grow-only; survives DROP TABLE). A failed read
        // means index buckets may be missed, so it counts against completion.
        List<string> indexIds = [];
        bool catalogRead = false;
        try
        {
            (KeyValueResponseType catType, ReadOnlyKeyValueEntry? catEntry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, catalogKey, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct).ConfigureAwait(false);

            if (catType == KeyValueResponseType.Get && catEntry?.Value is { Length: > 0 })
                indexIds = [.. MetaJsonSerializer.Deserialize(catEntry.Value, MetaJsonContext.Default.StringArray)];

            catalogRead = catType is KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to read keyspace catalog for orphan table {TableId} in database {Id}", tableId, dbId);
        }

        // Purge row + index buckets, and delete the stats/catalog/meta keys — all verified. The orphan
        // record is NOT touched here; it is the recovery marker and is deleted only if everything else
        // is confirmed gone, so an incomplete purge leaves the record for a later sweep to finish.
        bool complete = catalogRead;
        complete &= await PurgeBucketAsync(kahuna, dbId, $"{dbId}:{dataId}:r", $"{dbId}:{dataId}:r/", ct).ConfigureAwait(false);
        foreach (string indexId in indexIds)
            complete &= await PurgeBucketAsync(kahuna, dbId, $"{dbId}:{dataId}:i:{indexId}", $"{dbId}:{dataId}:i:{indexId}/", ct).ConfigureAwait(false);

        foreach (string key in new[]
        {
            $"{dbId}:stats:{tableId}",
            catalogKey,
            $"{dbId}/meta/table:{tableId}",
        })
            complete &= await DeleteExactVerifiedAsync(kahuna, key, ct).ConfigureAwait(false);

        // Only remove the recovery record once all data is verified gone.
        if (!complete)
            return false;

        return await DeleteExactVerifiedAsync(kahuna, $"{dbId}/meta/orphan:{tableId}", ct).ConfigureAwait(false);
    }

    // Test-only: counts the number of delete batches the purge has issued, so a test with a low
    // KeyspacePurgeBatchSize can prove the purge actually pages (multiple batches) rather than
    // materialising the whole bucket in one shot.
    internal static long PurgeBatchesForTesting;

    // Test-only: counts the keyspace-catalog scan rounds, so a test can prove the collection re-scans
    // (a confirming round) rather than reading the catalog once — the guard against a transient miss.
    internal static long CatalogScanRoundsForTesting;

    /// <summary>
    /// Deletes every key under <paramref name="keyPrefix"/> in <paramref name="bucket"/> in bounded
    /// batches of <see cref="CamusDBOptions.KeyspacePurgeBatchSize"/>: scan one batch, delete it,
    /// re-scan. Peak memory is one batch regardless of how large the overlay is — a <c>DROP DATABASE</c>
    /// can span an entire database. The loop ends after <see cref="MaxPurgeScanRounds"/> consecutive
    /// scans make no progress (all-empty or all-failed), which also absorbs a transient scan miss; a
    /// batch that deletes nothing (persistent delete errors) counts as no-progress so the loop can't
    /// spin. Each delete is an idempotent autocommit operation.
    /// </summary>
    private async Task<bool> PurgeBucketAsync(IKahuna kahuna, string id, string bucket, string keyPrefix, CancellationToken ct)
    {
        int batchSize = options.KeyspacePurgeBatchSize;
        if (batchSize < 1) batchSize = 1;

        int failStreak = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            List<string> batch = [];
            try
            {
                await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                    HLCTimestamp.Zero, bucket, null, true, null, true, batchSize,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).ConfigureAwait(false))
                {
                    if (!key.StartsWith(keyPrefix, StringComparison.Ordinal))
                        continue;

                    batch.Add(key);
                    if (batch.Count >= batchSize)
                        break; // bound memory; the next iteration re-scans for the remainder
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to scan bucket '{Bucket}' while purging database (id={Id})", bucket, id);
                return false; // scan failed → cannot prove the bucket is empty
            }

            // Empty scan → the bucket is verified drained.
            if (batch.Count == 0)
                return true;

            PurgeBatchesForTesting++;

            int deleted = 0;
            foreach (string key in batch)
                if (await DeleteExactVerifiedAsync(kahuna, key, ct).ConfigureAwait(false))
                    deleted++;

            if (deleted == batch.Count)
            {
                failStreak = 0; // real progress; re-scan for the remainder
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("Purged {Count} key(s) under bucket '{Bucket}' for dropped database (id={Id})", batch.Count, bucket, id);
                continue;
            }

            // Some keys were not confirmed deleted. Bound the retries so a persistently failing key
            // does not spin forever; give up and report the bucket as not-yet-empty (the recovery
            // markers stay in place and a later sweep / startup resume retries).
            if (++failStreak >= MaxPurgeScanRounds)
                return false;

            await Task.Delay(failStreak * 20, ct).ConfigureAwait(false);
        }
    }
}
