
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

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Opens a database, creating an <see cref="EmbeddedKahuna"/> node and wiring the
/// <see cref="KvTransactionsManager"/> into the returned <see cref="DatabaseDescriptor"/>.
/// Schema and system-space data are kept in-memory for Phase 4; Phase 5 will persist
/// them to Kahuna KV.
/// </summary>
internal sealed class DatabaseOpener
{
    private readonly CommandExecutor commandExecutor;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public DatabaseOpener(CommandExecutor commandExecutor, DatabaseDescriptors databaseDescriptors, CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.commandExecutor = commandExecutor;
        this.databaseDescriptors = databaseDescriptors;
        this.catalogs = catalogs;
        this.logger = logger;
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
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
        await node.WaitForLeaderAsync($"{name}/warmup", CancellationToken.None).ConfigureAwait(false);

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
