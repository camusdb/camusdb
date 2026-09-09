
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Kommander.Time;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Models;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Opens a database and wires the <see cref="KvTransactionsManager"/> into the returned
/// <see cref="DatabaseDescriptor"/>. Both standalone and cluster modes use the single
/// process-level shared <see cref="EmbeddedKahuna"/> node; opening a database is a pure
/// metadata load — no per-database node is constructed or started.
/// </summary>
internal sealed class DatabaseOpener
{
    private readonly CommandExecutor commandExecutor;

    /// <summary>Configuration for the engine whose databases this opens; injected, never ambient.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly SchemaReplicator schemaReplicator;

    private readonly SchemaChangeCoordinator coordinator;

    private readonly ILogger<ICamusDB> logger;

    private readonly EmbeddedKahuna sharedNode;

    private readonly bool isClusterMode;

    private readonly Task<DatabaseRegistry> registryTask;

    private readonly IQueryResultCache? cache;

    /// <summary>Evicts a relation's cached statistics; supplied by the engine that owns them.</summary>
    private readonly Action<DatabaseDescriptor, string>? evictTableStatistics;

    public DatabaseOpener(
        CommandExecutor commandExecutor,
        DatabaseDescriptors databaseDescriptors,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        CamusDBOptions options,
        EmbeddedKahuna? sharedNode,
        Task<DatabaseRegistry> registryTask,
        bool isClusterMode = false,
        IQueryResultCache? cache = null,
        Action<DatabaseDescriptor, string>? evictTableStatistics = null)
    {
        this.commandExecutor = commandExecutor;
        this.options = options;
        this.databaseDescriptors = databaseDescriptors;
        this.catalogs = catalogs;
        this.schemaReplicator = new(catalogs, logger);
        this.coordinator = new(catalogs, logger)
        {
            // Wire column backfill: leader-change resume re-runs it at WriteOnly before
            // advancing to Public so existing rows carry the default before the column
            // becomes visible.
            BackfillAsync = (db, tableName, column) =>
                commandExecutor.BackfillColumnDefaultsAsync(db, tableName, column),

            // Wire index backfill: same guarantee for the index add sequence — backfill
            // existing rows with the index entries before the index is published.
            IndexBackfillAsync = (db, tableName, indexInfo, startOffset, onCheckpoint) =>
                commandExecutor.BackfillIndexEntriesAsync(db, tableName, indexInfo, startOffset, onCheckpoint),
        };
        this.logger = logger;
        this.sharedNode = sharedNode ?? throw new ArgumentNullException(nameof(sharedNode), "A shared Kahuna node is required");
        this.isClusterMode = isClusterMode;
        this.registryTask = registryTask;
        this.cache = cache;
        this.evictTableStatistics = evictTableStatistics;
    }

    public async ValueTask<DatabaseDescriptor> Open(string name, bool recoveryMode = false)
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        // Resolve the full registry entry in one call so the id and ancestry come from
        // the same consistent snapshot.  TryResolveEntryAsync checks the in-memory cache
        // first (synchronous fast path) and falls back to a live KV read only on a miss,
        // providing both cross-node visibility and freedom from a TryResolveId→Get TOCTOU
        // window where a concurrent drop could evict the entry between the two lookups.
        DatabaseRegistryEntry? entry = await registry.TryResolveEntryAsync(name).ConfigureAwait(false);
        if (entry is null)
            throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, $"Database '{name}' does not exist");

        string id = entry.Id;

        AsyncLazy<DatabaseDescriptor> lazy = databaseDescriptors.Descriptors.GetOrAdd(
            id, _ => new(() => LoadDatabase(registry, entry, name)));

        DatabaseDescriptor descriptor = await lazy;

        // Guard: if the database was dropped after we resolved the lazy, fail fast
        // with a clean error rather than letting callers hit a disposed Kahuna node.
        if (descriptor.IsDropped)
        {
            databaseDescriptors.Descriptors.TryRemove(id, out _);
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseDoesntExist,
                $"Database '{name}' does not exist");
        }

        // Stamp before handing the descriptor out, not after the caller takes a use-reference. This
        // is what makes idle eviction safe: a descriptor being resolved right now cannot look idle to
        // a concurrent sweep, so the sweep can never dispose one in the instant between a caller
        // resolving it and referencing it.
        descriptor.Touch();

        return descriptor;
    }

    private async Task<DatabaseDescriptor> LoadDatabase(DatabaseRegistry registry, DatabaseRegistryEntry entry, string name)
    {
        string id = entry.Id;
        IReadOnlyList<DatabaseBranchAncestor> ancestors = entry.Ancestors;

        // Both modes use the single shared node. Opening a database is a pure metadata
        // load — no per-database node is constructed, started, or flushed here.
        HLCTimestamp mintLocalT(HLCTimestamp? floor)
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);

            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        }

        KvTransactionsManager transactions = new(sharedNode.Kahuna, options, (Func<HLCTimestamp?, HLCTimestamp>)mintLocalT, logger, cache);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new(StringComparer.OrdinalIgnoreCase);

        BranchSnapshotHoldGuard? snapshotProtection = ancestors.Count > 0
            ? await BuildBranchSnapshotGuardAsync(registry, entry).ConfigureAwait(false)
            : null;

        DatabaseDescriptor databaseDescriptor = new(
            id: id,
            name: name,
            kahuna: sharedNode,
            transactions: transactions,
            tableDescriptors: tableDescriptors,
            options: options,
            ancestors: ancestors,
            snapshotProtection: snapshotProtection
        )
        {
            Cache = cache,
        };

        // Bound after construction because the callback closes over the descriptor it belongs to.
        if (evictTableStatistics is not null)
            databaseDescriptor.EvictTableStatistics = tableId => evictTableStatistics(databaseDescriptor, tableId);

        await catalogs.LoadMetaAsync(databaseDescriptor).ConfigureAwait(false);

        // All DDL goes through ReplicateAndWaitLocalApplyAsync (Raft commit + local apply callback).
        // The SchemaReplicator must be registered in both standalone and cluster modes so that
        // ApplyAsync fires after the Raft commit and updates database.Schema.SchemaVersion.
        schemaReplicator.Register(databaseDescriptor, coordinator);

        // A committed schema delta delivered between the LoadMetaAsync read above and the Register
        // call is consumed with no subscriber and never redelivered, so this node would keep the
        // pre-delta schema with nothing to correct it. Re-probe the durable checkpoint now that the
        // subscription is live; a delta whose checkpoint was persisted in the gap is installed here,
        // and one whose checkpoint lands later is caught by the miss-triggered and periodic probes.
        // Best-effort: the probe repairs a rare race and must not fail an otherwise good open.
        try
        {
            await catalogs.ReconcileSchemaFreshnessAsync(databaseDescriptor).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Post-open schema freshness probe failed for database '{Db}'; the periodic probe will retry",
                name);
        }

        Log.LogDatabaseOpened(logger, name);

        return databaseDescriptor;
    }

    /// <summary>
    /// Builds the fail-closed snapshot-protection guard a branch descriptor carries for its
    /// lifetime (see <see cref="BranchSnapshotHoldGuard"/>).
    ///
    /// <para>The chain of hold ids the guard verifies — the branch's own hold on its immediate
    /// parent plus each non-root ancestor's hold on <em>its</em> parent — is resolved lazily on the
    /// guard's first verify, not here: a transient registry failure at open time must not poison
    /// the descriptor's <c>AsyncLazy</c>, and every id in the chain is immutable after
    /// registration, so late resolution loses nothing. A durable lost-protection marker found now
    /// pre-latches the guard so the first read fails fast with the recorded reason.</para>
    ///
    /// <para>A branch created before snapshot-floor durability landed has no hold id anywhere in
    /// its chain; its guard then verifies an empty chain (a warning is logged once at open). That
    /// preserves the pre-existing behavior for legacy branches instead of bricking them.</para>
    /// </summary>
    private async Task<BranchSnapshotHoldGuard> BuildBranchSnapshotGuardAsync(
        DatabaseRegistry registry, DatabaseRegistryEntry entry)
    {
        string branchId = entry.Id;
        string branchName = entry.Name;
        string selfHoldId = entry.ImmediateParentHoldId;
        string[] ancestorIds = new string[entry.Ancestors.Count];
        for (int i = 0; i < entry.Ancestors.Count; i++)
            ancestorIds[i] = entry.Ancestors[i].DatabaseId;

        if (string.IsNullOrEmpty(selfHoldId))
            logger.LogWarning(
                "Branch '{Branch}' has no snapshot-hold id (created before snapshot-floor durability); " +
                "its frozen ancestor view is NOT protected against revision reclamation",
                branchName);

        async Task<IReadOnlyList<string>> ResolveChainHoldIds(CancellationToken ct)
        {
            List<string> holdIds = new(ancestorIds.Length);

            if (!string.IsNullOrEmpty(selfHoldId))
                holdIds.Add(selfHoldId);

            // Each non-root ancestor is itself a registered branch owning a hold on ITS parent;
            // this branch's deeper frozen levels stay readable only while those holds live too.
            // The root's entry has an empty hold id and drops out naturally.
            foreach (string ancestorId in ancestorIds)
            {
                DatabaseRegistryEntry? ancestorEntry = await registry.TryResolveEntryByIdAsync(ancestorId).ConfigureAwait(false);
                if (ancestorEntry is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionMustRetry,
                        $"Could not resolve ancestor database id '{ancestorId}' of branch '{branchName}' " +
                        "to verify its snapshot protection; retry the operation");

                if (!string.IsNullOrEmpty(ancestorEntry.ImmediateParentHoldId))
                    holdIds.Add(ancestorEntry.ImmediateParentHoldId);
            }

            return holdIds;
        }

        BranchSnapshotHoldGuard guard = new(
            sharedNode.Kahuna,
            logger,
            branchName,
            options.BranchSnapshotHoldLeaseMs,
            ResolveChainHoldIds,
            persistLostAsync: reason => registry.MarkSnapshotProtectionLostAsync(branchId, reason));

        // Fast-fail path only: a marker written by the renewer (or by another node's guard) makes
        // the very first read fail with the definitive recorded reason. A transient miss here is
        // harmless — a genuinely lost chain still fails closed through the guard's refused renew.
        string? lostReason = await registry.TryGetSnapshotProtectionLostAsync(branchId).ConfigureAwait(false);
        if (lostReason is not null)
            guard.LatchLostFromDurableMarker(lostReason);

        return guard;
    }
}
