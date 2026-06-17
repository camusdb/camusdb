
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
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Opens a database and wires the <see cref="KvTransactionsManager"/> into the returned
/// <see cref="DatabaseDescriptor"/>.
/// In standalone mode, creates a per-database SQLite-backed <see cref="EmbeddedKahuna"/> node.
/// In cluster mode, reuses the process-level <see cref="EmbeddedKahuna"/> injected at construction.
/// Schema and row data are persisted to the Kahuna KV store and restored on reopen via WAL replay.
/// </summary>
internal sealed class DatabaseOpener
{
    private readonly CommandExecutor commandExecutor;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly SchemaReplicator schemaReplicator;

    private readonly SchemaChangeCoordinator coordinator;

    private readonly ILogger<ICamusDB> logger;

    private readonly ILoggerFactory? loggerFactory;

    private readonly EmbeddedKahuna? clusterNode;

    private readonly Task<DatabaseRegistry> registryTask;

    public DatabaseOpener(
        CommandExecutor commandExecutor,
        DatabaseDescriptors databaseDescriptors,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        EmbeddedKahuna? clusterNode,
        ILoggerFactory? loggerFactory,
        Task<DatabaseRegistry> registryTask)
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
        this.clusterNode = clusterNode;
        this.loggerFactory = loggerFactory;
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
        EmbeddedKahuna node;

        if (clusterNode is not null)
        {
            // Cluster mode: data lives in the shared node; no per-database directory.
            node = clusterNode;
        }
        else
        {
            // Standalone mode: data directory must already exist (created by CreateDatabase).
            string dataPath = Path.Combine(CamusConfig.DataDirectory, id);
            if (!Directory.Exists(Path.Combine(dataPath, "kv")))
                throw new CamusDBException(
                    CamusDBErrorCodes.SystemSpaceCorrupt,
                    $"Data directory for database '{name}' (id={id}) is missing or incomplete. " +
                    $"The database may not have finished creating (possible crash during setup), " +
                    $"or its data directory may have been deleted or corrupted. " +
                    $"Drop the database and recreate it to recover.");

            node = EmbeddedKahuna.CreateSqlite(dataPath, loggerFactory);
        }

        // Only start/wait if we own the node (standalone per-database instance).
        // The cluster node is started once at process startup in Program.cs.
        if (clusterNode is null)
        {
            await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await node.WaitForLeaderAsync($"{id}/warmup", CancellationToken.None).ConfigureAwait(false);

            // WAL replay queues dirty writes to the background writer. Flush them now so
            // SQLite is fully populated before LoadMetaAsync reads schema keys from storage.
            await node.FlushAsync().ConfigureAwait(false);
        }

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return node.Raft.HybridLogicalClock.ReceiveEvent(node.Raft.GetLocalNodeId(), floor.Value);
            return node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        };

        KvTransactionsManager transactions = new(node.Kahuna, mintLocalT);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new();

        DatabaseDescriptor databaseDescriptor = new(
            id: id,
            name: name,
            kahuna: node,
            transactions: transactions,
            tableDescriptors: tableDescriptors,
            ownsKahuna: clusterNode is null
        );

        await catalogs.LoadMetaAsync(databaseDescriptor).ConfigureAwait(false);

        if (!databaseDescriptor.OwnsKahuna)
            schemaReplicator.Register(databaseDescriptor, coordinator);

        Log.LogDatabaseOpened(logger, name);

        return databaseDescriptor;
    }
}
