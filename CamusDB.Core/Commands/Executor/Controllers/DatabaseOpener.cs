
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.GC;
using CamusDB.Core.Storage;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusConfig = CamusDB.Core.CamusDBConfig;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Opens a database.
/// 
/// A unique descriptor in memory is created that has references to core systems like the buffer pool manager, 
/// storage manager, and GC (garbage collection) manager. Having multiple databases open means having these systems running in parallel.
/// To free up resources, it is possible to close the databases.
/// </summary>
internal sealed class DatabaseOpener
{
    private readonly CommandExecutor commandExecutor;

    private readonly HybridLogicalClock hlc;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly ILogger<ICamusDB> logger;

    public DatabaseOpener(CommandExecutor commandExecutor, HybridLogicalClock hlc, DatabaseDescriptors databaseDescriptors, ILogger<ICamusDB> logger)
    {
        this.commandExecutor = commandExecutor;
        this.hlc = hlc;
        this.databaseDescriptors = databaseDescriptors;
        this.logger = logger;
    }

    public async ValueTask<DatabaseDescriptor> Open(string name, bool recoveryMode = false)
    {
        AsyncLazy<DatabaseDescriptor> openDatabaseLazy = databaseDescriptors.Descriptors.GetOrAdd(
                                                            name,
                                                            (_) => new AsyncLazy<DatabaseDescriptor>(() => LoadDatabase(name))
                                                         );
        return await openDatabaseLazy;
    }

    private async Task<DatabaseDescriptor> LoadDatabase(string name)
    {
        //if (!Directory.Exists(path))
        //    throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

        LC logicalClock = new();
        StorageManager storage = new(name);
        BufferPoolManager bufferPool = new(storage, logicalClock, logger);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new();

        DatabaseDescriptor databaseDescriptor = new(
            name: name,
            storage: storage,
            bufferPool: bufferPool,
            gc: new GCManager(bufferPool, hlc, logicalClock, tableDescriptors, logger),
            tableDescriptors: tableDescriptors
        );

        await Task.WhenAll(new Task[]
        {
            LoadDatabaseSchema(databaseDescriptor),
            LoadDatabaseSystemSpace(databaseDescriptor),
        }).ConfigureAwait(false);

        logger.LogInformation("Database {DbName} opened", name);

        return databaseDescriptor;
    }

    private Task LoadDatabaseSchema(DatabaseDescriptor database)
    {
        byte[]? data = database.Storage.Get(CamusConfig.SchemaKey); //SchemaSpace!.GetDataFromPage(Config.SchemaHeaderPage);

        if (data is not null && data.Length > 0)
            database.Schema.Tables = Serializator.Unserialize<Dictionary<string, TableSchema>>(data);
        else
            database.Schema.Tables = new();

        logger.LogInformation("Schema tablespaces read. Loaded {Count} tables", database.Schema.Tables.Count);

        return Task.CompletedTask;
    }

    private Task LoadDatabaseSystemSpace(DatabaseDescriptor database)
    {
        byte[]? data = database.Storage.Get(CamusConfig.SystemKey);

        if (data is not null && data.Length > 0)
            database.SystemSchema = Serializator.Unserialize<SystemSchema>(data);
        else
            database.SystemSchema = new();

        logger.LogInformation("System tablespaces read. Found {Count} objects", database.SystemSchema.Tables.Count);

        return Task.CompletedTask;
    }
}
