
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
    private readonly CamusDBOptions options;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly SchemaReplicator schemaReplicator;

    private readonly SchemaChangeCoordinator coordinator;

    private readonly ILogger<ICamusDB> logger;

    private readonly EmbeddedKahuna sharedNode;

    private readonly bool isClusterMode;

    private readonly Task<DatabaseRegistry> registryTask;

    private readonly IQueryResultCache? cache;

    public DatabaseOpener(
        CommandExecutor commandExecutor,
        DatabaseDescriptors databaseDescriptors,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        CamusDBOptions options,
        EmbeddedKahuna? sharedNode,
        Task<DatabaseRegistry> registryTask,
        bool isClusterMode = false,
        IQueryResultCache? cache = null)
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
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseDoesntExist,
                $"Database '{name}' does not exist");

        string id = entry.Id;
        IReadOnlyList<DatabaseBranchAncestor> ancestors = entry.Ancestors;

        AsyncLazy<DatabaseDescriptor> lazy = databaseDescriptors.Descriptors.GetOrAdd(
            id, _ => new(() => LoadDatabase(id, name, ancestors)));

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

        return descriptor;
    }

    private async Task<DatabaseDescriptor> LoadDatabase(
        string id, string name, IReadOnlyList<DatabaseBranchAncestor> ancestors)
    {
        // Both modes use the single shared node. Opening a database is a pure metadata
        // load — no per-database node is constructed, started, or flushed here.
        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager transactions = new(sharedNode.Kahuna, options, mintLocalT, logger, cache);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new(StringComparer.OrdinalIgnoreCase);

        DatabaseDescriptor databaseDescriptor = new(
            id: id,
            name: name,
            kahuna: sharedNode,
            transactions: transactions,
            tableDescriptors: tableDescriptors,
            options: options,
            ancestors: ancestors
        )
        {
            Cache = cache,
        };

        await catalogs.LoadMetaAsync(databaseDescriptor).ConfigureAwait(false);

        // All DDL goes through ReplicateAndWaitLocalApplyAsync (Raft commit + local apply callback).
        // The SchemaReplicator must be registered in both standalone and cluster modes so that
        // ApplyAsync fires after the Raft commit and updates database.Schema.SchemaVersion.
        schemaReplicator.Register(databaseDescriptor, coordinator);

        Log.LogDatabaseOpened(logger, name);

        return databaseDescriptor;
    }
}
