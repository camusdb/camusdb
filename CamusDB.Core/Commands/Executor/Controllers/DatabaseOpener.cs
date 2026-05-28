
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
/// Opens a database, creating an <see cref="EmbeddedKahuna"/> node (SQLite-backed) and wiring
/// the <see cref="KvTransactionsManager"/> into the returned <see cref="DatabaseDescriptor"/>.
/// Schema and row data are persisted to the Kahuna KV store and restored on reopen via WAL replay.
/// </summary>
internal sealed class DatabaseOpener
{
    private readonly CommandExecutor commandExecutor;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    private readonly ILoggerFactory? loggerFactory;

    public DatabaseOpener(CommandExecutor commandExecutor, DatabaseDescriptors databaseDescriptors, CatalogsManager catalogs, ILogger<ICamusDB> logger, ILoggerFactory? loggerFactory = null)
    {
        this.commandExecutor = commandExecutor;
        this.databaseDescriptors = databaseDescriptors;
        this.catalogs = catalogs;
        this.logger = logger;
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

        EmbeddedKahuna node = EmbeddedKahuna.CreateSqlite(dataPath, loggerFactory);
        await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
        await node.WaitForLeaderAsync($"{name}/warmup", CancellationToken.None).ConfigureAwait(false);

        // WAL replay queues dirty writes to the background writer. Flush them now so
        // SQLite is fully populated before LoadMetaAsync reads schema keys from storage.
        await node.FlushAsync().ConfigureAwait(false);

        KvTransactionsManager transactions = new(node.Kahuna);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new();

        DatabaseDescriptor databaseDescriptor = new(
            name: name,
            kahuna: node,
            transactions: transactions,
            tableDescriptors: tableDescriptors
        );

        await catalogs.LoadMetaAsync(databaseDescriptor).ConfigureAwait(false);

        logger.LogInformation("Database {DbName} opened", name);

        return databaseDescriptor;
    }
}
