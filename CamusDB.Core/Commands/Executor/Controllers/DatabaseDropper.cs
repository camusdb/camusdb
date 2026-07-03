
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

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly ILogger<ICamusDB> logger;

    public DatabaseDropper(DatabaseDescriptors databaseDescriptors, ILogger<ICamusDB> logger)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.logger = logger;
    }

    public async Task Drop(string id)
    {
        if (!databaseDescriptors.Descriptors.TryRemove(id, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
            return;

        DatabaseDescriptor databaseDescriptor;
        try
        {
            databaseDescriptor = await databaseDescriptorLazy;
        }
        catch
        {
            // The cached lazy is faulted (e.g. LoadDatabase threw during a prior Open attempt).
            // The name is already unregistered by the caller before Drop is invoked — nothing more to do.
            return;
        }

        // Signal all new AddRef calls to fail immediately with DatabaseDoesntExist.
        databaseDescriptor.MarkDropped();

        // Wait for any operations already holding a use-reference to finish.
        // Cap at 30 s to avoid blocking forever on a stuck in-flight operation.
        try
        {
            await databaseDescriptor.WhenDrainedAsync()
                .WaitAsync(TimeSpan.FromSeconds(30))
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            logger.LogWarning(
                "DropDatabase for id={Id} timed out waiting for in-flight operations; forcing disposal",
                id);
        }

        // Purge all key-value entries that belong to this database from the shared node.
        // The shared node is NOT disposed here — it's owned by the process, not this database.
        //
        // Purged namespaces (scan bucket → stored key prefix):
        //   {id}/meta                    → {id}/                           — meta keys (system, version, schemas, history, coordinator jobs)
        //   {id}:                        → {id}:                           — statistics keys ({id}:stats:{tableId})
        //   {id}:{tableId}:r             → {id}:{tableId}:r/               — row data for every table (Task 4 prefix)
        //   {id}:{tableId}:i:{indexId}   → {id}:{tableId}:i:{indexId}/     — index data for every index (Task 4 prefix)
        //
        //   Schema-log entries in the Raft WAL are append-only and cannot be removed.
        await PurgeClusterKeyspaceAsync(databaseDescriptor, id).ConfigureAwait(false);

        databaseDescriptor.Dispose();
        Log.LogDatabaseDropped(logger, databaseDescriptor.Name);
    }

    /// <summary>
    /// Deletes all KV entries belonging to <paramref name="descriptor"/>'s database, using the
    /// descriptor's live schema as a safety net over the persisted keyspace catalog.
    /// </summary>
    private Task PurgeClusterKeyspaceAsync(DatabaseDescriptor descriptor, string id)
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
    internal async Task PurgeKeyspaceByIdAsync(IKahuna kahuna, string id, IReadOnlyList<TableSchema>? safetyNetTables)
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

        // Phase C: purge rows and index entries.
        foreach ((string bucket, string keyPrefix) in rowIndexPrefixes)
            await PurgeBucketAsync(kahuna, id, bucket, keyPrefix).ConfigureAwait(false);

        // Phase D: delete exact stats keys (no '/' suffix so unreachable by bucket scan).
        foreach (string key in exactKeys)
        {
            try
            {
                await kahuna.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, key, KeyValueDurability.Persistent, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to delete stats key '{Key}' while purging database (id={Id})", key, id);
            }
        }

        // Phase E: delete the meta namespace LAST (catalog included) so it survived for every resume.
        await PurgeBucketAsync(kahuna, id, metaBucket, metaKeyPrefix).ConfigureAwait(false);
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
    /// batches of <see cref="CamusDBConfig.KeyspacePurgeBatchSize"/>: scan one batch, delete it,
    /// re-scan. Peak memory is one batch regardless of how large the overlay is — a <c>DROP DATABASE</c>
    /// can span an entire database. The loop ends after <see cref="MaxPurgeScanRounds"/> consecutive
    /// scans make no progress (all-empty or all-failed), which also absorbs a transient scan miss; a
    /// batch that deletes nothing (persistent delete errors) counts as no-progress so the loop can't
    /// spin. Each delete is an idempotent autocommit operation.
    /// </summary>
    private async Task PurgeBucketAsync(IKahuna kahuna, string id, string bucket, string keyPrefix)
    {
        int batchSize = CamusDBConfig.KeyspacePurgeBatchSize;
        if (batchSize < 1) batchSize = 1;

        int dryRounds = 0;
        while (dryRounds < MaxPurgeScanRounds)
        {
            List<string> batch = [];
            try
            {
                await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                    HLCTimestamp.Zero, bucket, null, true, null, true, batchSize,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
                {
                    if (!key.StartsWith(keyPrefix, StringComparison.Ordinal))
                        continue;

                    batch.Add(key);
                    if (batch.Count >= batchSize)
                        break; // bound memory; the next iteration re-scans for the remainder
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to scan bucket '{Bucket}' while purging database (id={Id})", bucket, id);
                break;
            }

            if (batch.Count == 0)
            {
                dryRounds++;
                continue;
            }

            PurgeBatchesForTesting++;

            bool progressed = false;
            foreach (string key in batch)
            {
                try
                {
                    await kahuna.LocateAndTryDeleteKeyValue(
                        HLCTimestamp.Zero, key, KeyValueDurability.Persistent, CancellationToken.None)
                        .ConfigureAwait(false);
                    progressed = true;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to delete key '{Key}' while purging database (id={Id})", key, id);
                }
            }

            // Reset only on real progress; an all-failed batch counts as a dry round so a persistently
            // failing key cannot spin the loop forever (the startup drop resume is the backstop).
            dryRounds = progressed ? 0 : dryRounds + 1;

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Purged {Count} key(s) under bucket '{Bucket}' for dropped database (id={Id})", batch.Count, bucket, id);
        }
    }
}
