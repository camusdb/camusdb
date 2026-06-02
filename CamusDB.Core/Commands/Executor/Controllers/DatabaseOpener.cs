
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
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

    private readonly ILogger<ICamusDB> logger;

    private readonly ILoggerFactory? loggerFactory;

    private readonly EmbeddedKahuna? clusterNode;

    public DatabaseOpener(
        CommandExecutor commandExecutor,
        DatabaseDescriptors databaseDescriptors,
        CatalogsManager catalogs,
        ILogger<ICamusDB> logger,
        EmbeddedKahuna? clusterNode = null,
        ILoggerFactory? loggerFactory = null)
    {
        this.commandExecutor = commandExecutor;
        this.databaseDescriptors = databaseDescriptors;
        this.catalogs = catalogs;
        this.schemaReplicator = new(catalogs, logger);
        this.logger = logger;
        this.clusterNode = clusterNode;
        this.loggerFactory = loggerFactory;
    }

    public async ValueTask<DatabaseDescriptor> Open(string name, bool recoveryMode = false)
    {
        AsyncLazy<DatabaseDescriptor> openDatabaseLazy = databaseDescriptors.Descriptors.GetOrAdd(name, LoadOrCreateDatabaseLazy);
        return await openDatabaseLazy;
    }

    private AsyncLazy<DatabaseDescriptor> LoadOrCreateDatabaseLazy(string name)
    {
        return new(() => LoadDatabase(name));
    }

    private async Task<DatabaseDescriptor> LoadDatabase(string name)
    {
        string dataPath = Path.Combine(CamusConfig.DataDirectory, name);
        Directory.CreateDirectory(Path.Combine(dataPath, "kv"));
        Directory.CreateDirectory(Path.Combine(dataPath, "wal"));

        EmbeddedKahuna node = clusterNode ?? EmbeddedKahuna.CreateSqlite(dataPath, loggerFactory);

        // Only start/wait if we own the node (standalone per-database instance).
        // The cluster node is started once at process startup in Program.cs.
        if (clusterNode is null)
        {
            await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await node.WaitForLeaderAsync($"{name}/warmup", CancellationToken.None).ConfigureAwait(false);

            // WAL replay queues dirty writes to the background writer. Flush them now so
            // SQLite is fully populated before LoadMetaAsync reads schema keys from storage.
            await node.FlushAsync().ConfigureAwait(false);
        }

        KvTransactionsManager transactions = new(node.Kahuna);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new();

        DatabaseDescriptor databaseDescriptor = new(
            name: name,
            kahuna: node,
            transactions: transactions,
            tableDescriptors: tableDescriptors,
            ownsKahuna: clusterNode is null
        );

        await catalogs.LoadMetaAsync(databaseDescriptor).ConfigureAwait(false);

        if (!databaseDescriptor.OwnsKahuna)
            schemaReplicator.Register(databaseDescriptor);

        logger.LogInformation("Database {DbName} opened", name);

        return databaseDescriptor;
    }
}
