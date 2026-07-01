
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
    /// Deletes all KV entries belonging to the database from the shared node.
    ///
    /// Phase 1 scans the meta bucket to delete all schema keys and simultaneously reads keyspace
    /// catalog entries (<c>{id}/meta/keyspace:{tableId}</c>) that record every index id ever
    /// allocated for each table. Using the catalog ensures that indexes dropped before the
    /// database was dropped are still purged — the in-memory schema only reflects live indexes.
    ///
    /// Phase 2 uses the collected catalog data (plus a safety-net pass over live in-memory
    /// tables) to build the full set of row and index bucket prefixes to purge.
    ///
    /// Phase 3 purges all row and index entries. Phase 4 deletes exact statistics keys that
    /// contain no '/' and therefore cannot be reached by a bucket scan.
    ///
    /// Scan bucket = the string before the last '/' of the stored keys, matching KvTableStore
    /// convention. Each key is deleted as a standalone autocommit operation (HLCTimestamp.Zero).
    /// Errors are logged and skipped so a partial failure never wedges the drop.
    /// </summary>
    private async Task PurgeClusterKeyspaceAsync(DatabaseDescriptor descriptor, string id)
    {
        IKahuna kahuna = descriptor.Kahuna.Kahuna;
        string metaBucket = $"{id}/meta";
        string metaKeyPrefix = $"{id}/meta";
        string catalogPrefix = $"{id}/meta/keyspace:";

        // Phase 1: Purge the meta namespace and collect keyspace catalog data.
        // Catalog entries live under {id}/meta/keyspace:{tableId} and are reached
        // by the same meta scan that purges version/system/schema/table/history entries.
        Dictionary<string, List<string>> catalogByTableId = [];

        for (int round = 0; round < MaxPurgeScanRounds; round++)
        {
            List<string> keys = [];
            try
            {
                await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                    HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
                {
                    if (!key.StartsWith(metaKeyPrefix, StringComparison.Ordinal))
                        continue;
                    keys.Add(key);

                    // Collect catalog entries on the first pass before their values are deleted.
                    if (round == 0 && key.StartsWith(catalogPrefix, StringComparison.Ordinal) && entry.Value is { Length: > 0 })
                    {
                        string tableId = key[catalogPrefix.Length..];
                        try
                        {
                            string[] indexIds = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.StringArray);
                            if (!catalogByTableId.ContainsKey(tableId))
                                catalogByTableId[tableId] = [.. indexIds];
                        }
                        catch (Exception ex)
                        {
                            logger.LogWarning(ex, "Failed to parse keyspace catalog for table {TableId} in database {Id}", tableId, id);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to scan meta bucket while purging database (id={Id})", id);
                break;
            }

            if (keys.Count == 0) break;

            foreach (string key in keys)
            {
                try
                {
                    await kahuna.LocateAndTryDeleteKeyValue(
                        HLCTimestamp.Zero, key, KeyValueDurability.Persistent, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to delete meta key '{Key}' while purging database (id={Id})", key, id);
                }
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Purged {Count} meta key(s) for dropped database (id={Id})", keys.Count, id);
        }

        // Phase 2: Build purge list from keyspace catalog (includes historically-dropped tables/indexes)
        // plus any live tables not yet in the catalog (safety net).
        List<(string bucket, string keyPrefix)> rowIndexPrefixes = [];
        List<string> exactKeys = [];
        HashSet<string> coveredTableIds = [];

        // From keyspace catalog (historically complete).
        foreach ((string tableId, List<string> indexIds) in catalogByTableId)
        {
            coveredTableIds.Add(tableId);
            rowIndexPrefixes.Add(($"{id}:{tableId}:r", $"{id}:{tableId}:r/"));
            exactKeys.Add($"{id}:stats:{tableId}");
            foreach (string indexId in indexIds)
                rowIndexPrefixes.Add(($"{id}:{tableId}:i:{indexId}", $"{id}:{tableId}:i:{indexId}/"));
        }

        // Safety net: live tables not yet in the catalog.
        foreach (TableSchema table in descriptor.Schema.Tables.Values)
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

        // Phase 3: Purge rows and index entries.
        foreach ((string bucket, string keyPrefix) in rowIndexPrefixes)
        {
            for (int round = 0; round < MaxPurgeScanRounds; round++)
            {
                List<string> keys = [];
                try
                {
                    await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                        HLCTimestamp.Zero, bucket, null, true, null, true, 512,
                        HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
                    {
                        if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                            keys.Add(key);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to scan bucket '{Bucket}' while purging database (id={Id})", bucket, id);
                    break;
                }

                if (keys.Count == 0) break;

                foreach (string key in keys)
                {
                    try
                    {
                        await kahuna.LocateAndTryDeleteKeyValue(
                            HLCTimestamp.Zero, key, KeyValueDurability.Persistent, CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Failed to delete key '{Key}' while purging database (id={Id})", key, id);
                    }
                }

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Purged {Count} key(s) under bucket '{Bucket}' for dropped database (id={Id})", keys.Count, bucket, id);
            }
        }

        // Phase 4: Delete exact stats keys (no '/' suffix so unreachable by bucket scan).
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
    }
}
