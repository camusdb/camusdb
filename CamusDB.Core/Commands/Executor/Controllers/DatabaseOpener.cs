
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Kommander.Time;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Models;
using System.Collections.Concurrent;
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

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly SchemaReplicator schemaReplicator;

    private readonly SchemaChangeCoordinator coordinator;

    private readonly ILogger<ICamusDB> logger;

    private readonly EmbeddedKahuna sharedNode;

    private readonly bool isClusterMode;

    private readonly Task<DatabaseRegistry> registryTask;

    public DatabaseOpener(
        CommandExecutor commandExecutor,
        DatabaseDescriptors databaseDescriptors,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        EmbeddedKahuna? sharedNode,
        Task<DatabaseRegistry> registryTask,
        bool isClusterMode = false)
    {
        this.commandExecutor = commandExecutor;
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
    }

    public async ValueTask<DatabaseDescriptor> Open(string name, bool recoveryMode = false)
    {
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

        string id;
        if (!registry.TryResolveId(name, out id))
        {
            // Cache miss — fall back to a live KV read so that databases created on other
            // cluster nodes (whose Raft-replicated write is visible in the shared store but
            // not yet in this node's in-memory cache) are found without requiring a restart.
            string? liveId = await registry.TryResolveIdAsync(name).ConfigureAwait(false);
            if (liveId is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{name}' does not exist");
            id = liveId;
        }

        AsyncLazy<DatabaseDescriptor> lazy = databaseDescriptors.Descriptors.GetOrAdd(
            id, _ => new(() => LoadDatabase(id, name)));

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

    private async Task<DatabaseDescriptor> LoadDatabase(string id, string name)
    {
        // Both modes use the single shared node. Opening a database is a pure metadata
        // load — no per-database node is constructed, started, or flushed here.
        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager transactions = new(sharedNode.Kahuna, mintLocalT, logger);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new();

        DatabaseDescriptor databaseDescriptor = new(
            id: id,
            name: name,
            kahuna: sharedNode,
            transactions: transactions,
            tableDescriptors: tableDescriptors
        );

        await catalogs.LoadMetaAsync(databaseDescriptor).ConfigureAwait(false);

        // All DDL goes through ReplicateAndWaitLocalApplyAsync (Raft commit + local apply callback).
        // The SchemaReplicator must be registered in both standalone and cluster modes so that
        // ApplyAsync fires after the Raft commit and updates database.Schema.SchemaVersion.
        schemaReplicator.Register(databaseDescriptor, coordinator);

        Log.LogDatabaseOpened(logger, name);

        return databaseDescriptor;
    }
}
