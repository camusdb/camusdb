
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Maintenance;

/// <summary>
/// Finds the databases and tables that background maintenance has work to do on, by reading
/// <em>authoritative</em> KV metadata rather than this node's open-object list. A table configured
/// or mutated only on another node is invisible to a node's own descriptor cache, so discovery
/// driven by that cache would silently skip it.
///
/// <para><b>The scan stays strictly ahead of the open.</b> Every discovery path here reads a
/// database's per-object meta keys first and opens the database only once that metadata has proved
/// there is work in it. Opening first would force every registered database resident on every tick
/// — on every node, since the TTL sweep is not leader-gated — which defeats lazy opening and holds
/// every catalog in memory for the life of the process.</para>
///
/// <para>Owned by <see cref="CommandExecutor"/> and handed to the background schedulers as
/// callbacks. It holds the discovery memos, which is why it is a live object rather than a set of
/// static helpers: the memos are what keep a steady-state tick from re-scanning every database's
/// metadata bucket forever.</para>
/// </summary>
internal sealed class MetadataDiscoveryService
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// One database's discovery result, and the schema version it was derived from.
    /// </summary>
    private readonly record struct MetaDiscoveryResult(long SchemaVersion, List<(string tableId, string tableName)> Tables);

    /// <summary>
    /// Per-database memo of what the last metadata scan found, keyed by the database schema version it
    /// was taken at. Every DDL — including <c>ALTER TABLE … SET</c>, which is what turns TTL on and off
    /// — advances that version, so an unchanged version means the answer cannot have changed.
    ///
    /// <para>This is what keeps a steady-state tick from re-scanning every database's metadata bucket
    /// forever. Without it, discovery's cost is a range scan per registered database per tick whether
    /// or not anything has changed, which is most of the idle cost of running many databases. With it,
    /// an unchanged database costs one point read.</para>
    ///
    /// <para>Bounded by pruning against the registry snapshot on every pass: a memo is state that grows
    /// with the number of databases, so left unpruned it would recreate in miniature the very problem
    /// discovery is being fixed to avoid.</para>
    /// </summary>
    private readonly ConcurrentDictionary<string, MetaDiscoveryResult> ttlMetaDiscoveryCache = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, MetaDiscoveryResult> tableMetaDiscoveryCache = new(StringComparer.Ordinal);

    private int metaDiscoveryScans;

    /// <summary>
    /// How many times background discovery has range-scanned a database's metadata bucket. The count a
    /// test needs to tell "the memo was used" from "the memo happened to produce the same answer" —
    /// the two are indistinguishable from the discovery result alone.
    /// </summary>
    internal int MetaDiscoveryScanCount => Volatile.Read(ref metaDiscoveryScans);

    internal MetadataDiscoveryService(ExecutorContext context, CamusDBOptions options)
    {
        this.context = context;
        this.options = options;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; each discovery pass pins the field once at its start, so an
    /// in-flight pass keeps the snapshot it began with and a change takes effect at the next tick.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Cluster-visible candidate discovery for row-level TTL, mirroring
    /// <see cref="DiscoverStaleTablesAsync"/>: enumerates authoritative metadata rather than this node's
    /// open-object list, so a table configured and written only on another node is still swept.
    ///
    /// <para>The TTL configuration is read straight from the per-object meta blob, so a table with TTL
    /// off costs discovery one deserialization and is never opened.</para>
    ///
    /// <para><b>A database is opened only once its metadata has already proved it has TTL work.</b>
    /// The meta scan needs nothing but the database id and the shared node, so opening first would
    /// force every registered database resident on every tick — on every node, since the sweep half of
    /// TTL is not leader-gated — which defeats lazy opening entirely and holds every catalog in memory
    /// for the life of the process. Keep the scan strictly ahead of the open.</para>
    /// </summary>
    internal async Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>> DiscoverTtlTablesAsync(CancellationToken ct)
    {
        List<(DatabaseDescriptor, TableDescriptor)> result = new List<(DatabaseDescriptor, TableDescriptor)>();
        if (context.SharedNode is null)
            return result;

        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        IReadOnlyList<DatabaseRegistryEntry> entries = await registry.GetBackgroundSnapshotAsync().ConfigureAwait(false);

        HashSet<string> liveDatabaseIds = new(StringComparer.Ordinal);

        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (ct.IsCancellationRequested)
                break;

            liveDatabaseIds.Add(entry.Id);

            List<(string tableId, string tableName)> ttlTables;

            try
            {
                ttlTables = await ScanTableMetaCachedAsync(
                    ttlMetaDiscoveryCache, entry.Id, ScanTtlTableMetaAsync, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(ex, "TTL discovery: could not read table metadata for database {Db}", entry.Name);
                continue;
            }

            if (ttlTables.Count == 0)
                continue; // nothing configured here: leave the database closed

            DatabaseDescriptor database;

            try
            {
                database = await context.DatabaseOpener.Open(entry.Name).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(ex, "TTL discovery: could not open database {Db}", entry.Name);
                continue;
            }

            foreach ((string _, string tableName) in ttlTables)
            {
                if (ct.IsCancellationRequested)
                    break;

                try
                {
                    TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);
                    result.Add((database, table));
                }
                catch (Exception ex)
                {
                    context.Logger.LogWarning(ex, "TTL discovery: could not open table {Table} in database {Db}", tableName, entry.Name);
                }
            }
        }

        PruneMetaDiscoveryCache(ttlMetaDiscoveryCache, liveDatabaseIds);

        return result;
    }

    /// <summary>
    /// Every registered database's id and name, straight from the registry — nothing is opened. Used by
    /// the TTL metadata reaper, which must look at databases whose tables have <em>stopped</em> being
    /// swept: the metadata it reclaims belongs to tables TTL discovery no longer returns, so a reaper
    /// driven by the sweep's own candidate list could never find it.
    ///
    /// <para>Ids and names are all the reaper needs — the run keys it enumerates and deletes are
    /// addressed by database id, and the name only ever reaches a log message. It therefore has no
    /// reason to hold a descriptor, and materializing one per registered database was the single
    /// largest source of unbounded descriptor growth on an idle node.</para>
    /// </summary>
    internal async Task<IReadOnlyList<(string Id, string Name)>> DiscoverRegisteredDatabasesAsync(CancellationToken ct)
    {
        List<(string, string)> result = [];
        if (context.SharedNode is null)
            return result;

        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);

        foreach (DatabaseRegistryEntry entry in await registry.GetBackgroundSnapshotAsync().ConfigureAwait(false))
        {
            if (ct.IsCancellationRequested)
                break;

            result.Add((entry.Id, entry.Name));
        }

        return result;
    }

    /// <summary>
    /// Reads a database's schema version straight from its meta key, or null when it cannot be read.
    /// A null answer means "do not trust a memo" — the caller rescans rather than guessing.
    /// </summary>
    private async Task<long?> TryReadSchemaVersionAsync(string dbId, CancellationToken ct)
    {
        try
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
                await context.SharedNode!.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero,
                    $"{dbId}/meta/version",
                    -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent,
                    ct).ConfigureAwait(false);

            if (type != KeyValueResponseType.Get || entry?.Value is null)
                return null;

            return MetaJsonSerializer.DeserializeCompat(entry.Value, MetaJsonContext.Default.Int64);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            context.Logger.LogWarning(ex, "Discovery could not read the schema version for database id {DbId}", dbId);
            return null;
        }
    }

    /// <summary>
    /// Runs <paramref name="scan"/> for a database only when its schema version has moved since the
    /// memo was taken. An unreadable version is treated as "changed", so a failure to read the cheap
    /// signal degrades to today's behavior rather than to a stale answer.
    /// </summary>
    private async Task<List<(string tableId, string tableName)>> ScanTableMetaCachedAsync(
        ConcurrentDictionary<string, MetaDiscoveryResult> cache,
        string dbId,
        Func<string, CancellationToken, Task<List<(string tableId, string tableName)>>> scan,
        CancellationToken ct)
    {
        long? version = await TryReadSchemaVersionAsync(dbId, ct).ConfigureAwait(false);

        if (version is not null &&
            cache.TryGetValue(dbId, out MetaDiscoveryResult memo) &&
            memo.SchemaVersion == version.Value)
            return memo.Tables;

        List<(string tableId, string tableName)> tables = await scan(dbId, ct).ConfigureAwait(false);

        if (version is not null)
            cache[dbId] = new MetaDiscoveryResult(version.Value, tables);

        return tables;
    }

    /// <summary>
    /// Drops memos for databases that are no longer registered, so the cache tracks the live set rather
    /// than every database this node has ever swept.
    /// </summary>
    private static void PruneMetaDiscoveryCache(
        ConcurrentDictionary<string, MetaDiscoveryResult> cache,
        HashSet<string> liveDatabaseIds
    )
    {
        if (cache.Count == liveDatabaseIds.Count)
            return;

        foreach (string dbId in cache.Keys)
        {
            if (!liveDatabaseIds.Contains(dbId))
                cache.TryRemove(dbId, out _);
        }
    }

    /// <summary>
    /// Enumerates a database's tables that have row-level TTL configured (paused or not), by reading the
    /// authoritative per-object meta keys — so it sees tables this node has never opened.
    /// </summary>
    private async Task<List<(string tableId, string tableName)>> ScanTtlTableMetaAsync(string dbId, CancellationToken ct)
    {
        Interlocked.Increment(ref metaDiscoveryScans);

        // Pinned once for the whole scan: a swap published mid-scan must not make two tables in the
        // same database resolve their TTL configuration against different defaults.
        CamusDBOptions currentOptions = options;

        string metaBucket = $"{dbId}/meta";
        string tablePrefix = $"{dbId}/meta/table:";
        List<(string, string)> tables = new();

        await foreach ((string key, ReadOnlyKeyValueEntry kvEntry) in context.SharedNode!.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).ConfigureAwait(false))
        {
            if (!key.StartsWith(tablePrefix, StringComparison.Ordinal) || kvEntry.Value is null)
                continue;

            TableSchema schema = MetaJsonSerializer.Deserialize(
                kvEntry.Value, MetaJsonContext.Default.TableSchema);

            if (schema.Id is null || schema.Name is null)
                continue;

            // Configured, not necessarily active: a paused table must still be visited so it can be
            // reported as paused. Filtering on IsActive here makes "paused" indistinguishable from
            // "gone" — the table simply disappears from every surface. The tick re-checks IsActive
            // before doing any work.
            if (TtlSettings.Resolve(schema.Settings, currentOptions).ExpirationColumn is not null)
                tables.Add((schema.Id, schema.Name));
        }

        return tables;
    }

    /// <summary>
    /// Cluster-visible candidate discovery for auto-analyze. Runs on the elected leader and enumerates
    /// <em>authoritative</em> metadata — every database in the registry and every table's per-object
    /// meta key — rather than this node's open-object list, so a hot table opened and mutated only on a
    /// follower is still found. Staleness is read from cluster-wide persisted state (which another
    /// node's flush writes), and the database is opened only once some table in it has been found
    /// stale.
    ///
    /// <para><b>Probing must leave no residue.</b> Both the descriptor and the statistics cache entry
    /// are created only for a table actually selected for analysis. Checking a table used to open its
    /// database and materialize a statistics entry for it, which meant a periodic freshness poll
    /// eventually pulled every database and every table in the cluster into memory and kept them
    /// there — the poll's own cost grew with what it had already inspected.</para>
    /// </summary>
    internal async Task<IReadOnlyList<(DatabaseDescriptor db, TableDescriptor table)>> DiscoverStaleTablesAsync(CancellationToken ct)
    {
        List<(DatabaseDescriptor, TableDescriptor)> result = new();
        if (context.SharedNode is null)
            return result;

        CamusDBOptions currentOptions = options;
        double fraction = currentOptions.AutoAnalyzeFractionStaleRows;
        long minRows = currentOptions.AutoAnalyzeMinStaleRows;

        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        IReadOnlyList<DatabaseRegistryEntry> entries = await registry.GetBackgroundSnapshotAsync().ConfigureAwait(false);

        HashSet<string> liveDatabaseIds = new(StringComparer.Ordinal);

        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (ct.IsCancellationRequested)
                break;

            liveDatabaseIds.Add(entry.Id);

            List<(string tableId, string tableName)> tables;

            try
            {
                tables = await ScanTableMetaCachedAsync(
                    tableMetaDiscoveryCache,
                    entry.Id,
                    ScanTableMetaAsync,
                    ct
                ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(ex, "Auto-analyze discovery: could not read table metadata for database {Db}", entry.Name);
                continue;
            }

            // Deferred so a database whose tables are all fresh is never opened. Resolved at most once
            // per database, on the first stale table found in it.
            DatabaseDescriptor? database = null;

            foreach ((string tableId, string tableName) in tables)
            {
                if (ct.IsCancellationRequested)
                    break;

                if (!await context.Statistics.IsStaleWithoutCachingAsync(context.SharedNode, entry.Id, tableId, fraction, minRows, ct).ConfigureAwait(false))
                    continue;

                if (database is null)
                {
                    try
                    {
                        database = await context.DatabaseOpener.Open(entry.Name).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        context.Logger.LogWarning(ex, "Auto-analyze discovery: could not open database {Db}", entry.Name);
                        break; // every remaining table in this database would fail the same way
                    }
                }

                try
                {
                    TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);
                    result.Add((database, table));
                }
                catch (Exception ex)
                {
                    context.Logger.LogWarning(ex, "Auto-analyze discovery: could not open table {Table} in database {Db}", tableName, entry.Name);
                }
            }
        }

        PruneMetaDiscoveryCache(tableMetaDiscoveryCache, liveDatabaseIds);

        return result;
    }

    /// <summary>
    /// Enumerates a database's live tables by scanning its per-object meta keys
    /// (<c>{dbId}/meta/table:{tableId}</c>) and returns each table's id and current name. Reads the
    /// authoritative KV catalog directly, so it sees tables this node has never opened.
    /// </summary>
    private async Task<List<(string tableId, string tableName)>> ScanTableMetaAsync(string dbId, CancellationToken ct)
    {
        Interlocked.Increment(ref metaDiscoveryScans);

        string metaBucket = $"{dbId}/meta";
        string tablePrefix = $"{dbId}/meta/table:";
        List<(string, string)> tables = new();

        await foreach ((string key, ReadOnlyKeyValueEntry kvEntry) in context.SharedNode!.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, metaBucket, null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).ConfigureAwait(false))
        {
            if (!key.StartsWith(tablePrefix, StringComparison.Ordinal) || kvEntry.Value is null)
                continue;

            TableSchema schema = MetaJsonSerializer.Deserialize(
                kvEntry.Value, MetaJsonContext.Default.TableSchema);

            // Honor the per-table opt-out (ALTER TABLE ... SET (sql_stats_automatic_collection_enabled
            // = false)) straight from the authoritative meta blob, so a disabled table costs discovery
            // nothing — it is never opened or stats-loaded.
            if (schema.Id is not null && schema.Name is not null && schema.AutoStatsCollectionEnabled)
                tables.Add((schema.Id, schema.Name));
        }

        return tables;
    }
}
