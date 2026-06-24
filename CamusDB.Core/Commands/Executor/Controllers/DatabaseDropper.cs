
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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;


namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseDropper
{
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
    /// Covers four key namespaces (scan bucket → matching key prefix):
    ///   <c>{id}/meta</c>               → <c>{id}/</c>           — all meta keys (system, version, table schemas, history, coordinator jobs)
    ///   <c>{id}:</c>                   → <c>{id}:</c>           — all statistics keys (<c>{id}:stats:{tableId}</c>)
    ///   <c>{id}:{tableId}:r</c>        → <c>{id}:{tableId}:r/</c>  — row data for every table (Task 4 prefix)
    ///   <c>{id}:{tableId}:i:{indexId}</c> → <c>{id}:{tableId}:i:{indexId}/</c> — index data for every index (Task 4 prefix)
    /// Scan bucket = the string before the last '/' of stored keys, matching KvTableStore convention.
    /// Each key is deleted as a standalone autocommit operation (HLCTimestamp.Zero).
    /// Errors are logged and skipped so a partial failure never wedges the drop.
    /// </summary>
    private async Task PurgeClusterKeyspaceAsync(DatabaseDescriptor descriptor, string id)
    {
        IKahuna kahuna = descriptor.Kahuna.Kahuna;

        // Build the full set of prefixes to scan: db-level meta/stats + per-table row+index.
        // Bucket prefix = the string before the last '/' of the stored keys — e.g. "{id}:{tableId}:r"
        // not "{id}:{tableId}:r/".  After Task 4, rows live under "{id}:{tableId}:r/{rowId}" so the
        // bucket is "{id}:{tableId}:r" (no trailing slash). Same convention for index buckets.
        //
        // All meta keys share the single bucket "{id}/meta" (see CatalogsManager: table/history/
        // coordinator use ':' sub-separators so their last-'/' prefix stays "{id}/meta"). So one
        // scan of "{id}/meta" reaches version/system/schema/table/history/coordinator together —
        // no per-sub-bucket enumeration needed, and scan+delete agree on the partition.
        List<(string bucket, string keyPrefix)> prefixes =
        [
            ($"{id}/meta", $"{id}/meta"),    // all schema meta: version, system, schema, table, history, coordinator
            ($"{id}:",     $"{id}:stats:"),  // statistics: {id}:stats:{tableId}
        ];

        foreach (TableSchema table in descriptor.Schema.Tables.Values)
        {
            if (table.Id is null)
                continue;

            prefixes.Add(($"{id}:{table.Id}:r", $"{id}:{table.Id}:r/"));

            if (table.Indexes is not null)
            {
                foreach (TableIndexSchema index in table.Indexes)
                {
                    if (index.Id is not null)
                        prefixes.Add(($"{id}:{table.Id}:i:{index.Id}", $"{id}:{table.Id}:i:{index.Id}/"));
                }
            }
        }

        foreach ((string bucket, string keyPrefix) in prefixes)
        {
            List<string> keys = [];

            try
            {
                await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                    HLCTimestamp.Zero,
                    bucket,
                    null, true,
                    null, true,
                    512,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent,
                    CancellationToken.None).ConfigureAwait(false))
                {
                    if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                        keys.Add(key);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Failed to scan bucket '{Bucket}' while purging cluster keyspace for dropped database (id={Id}); some keys may be orphaned",
                    bucket, id);
                continue;
            }

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
                    logger.LogWarning(ex,
                        "Failed to delete key '{Key}' while purging cluster keyspace for dropped database (id={Id})",
                        key, id);
                }
            }

            if (keys.Count > 0 && logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Purged {Count} key(s) under bucket '{Bucket}' for dropped database (id={Id})",
                    keys.Count, bucket, id);
        }
    }
}
