
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
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseOpener(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async ValueTask<DatabaseDescriptor> Open(CommandExecutor executor, HybridLogicalClock hybridLogicalClock, string name, bool recoveryMode = false)
    {
        AsyncLazy<DatabaseDescriptor> openDatabaseLazy = databaseDescriptors.Descriptors.GetOrAdd(
                                                            name,
                                                            (_) => new AsyncLazy<DatabaseDescriptor>(() => LoadDatabase(hybridLogicalClock, name))
                                                         );
        return await openDatabaseLazy;
    }

    private static async Task<DatabaseDescriptor> LoadDatabase(HybridLogicalClock hybridLogicalClock, string name)
    {
        //if (!Directory.Exists(path))
        //    throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

        LC logicalClock = new();
        StorageManager storage = new(name);
        BufferPoolManager bufferPool = new(storage, logicalClock);
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors = new();

        DatabaseDescriptor databaseDescriptor = new(
            name: name,
            storage: storage,
            bufferPool: bufferPool,
            gc: new GCManager(bufferPool, hybridLogicalClock, logicalClock, tableDescriptors),
            tableDescriptors: tableDescriptors
        );

        await Task.WhenAll(new Task[]
        {
            LoadDatabaseSchema(databaseDescriptor),
            LoadDatabaseSystemSpace(databaseDescriptor),
        });

        Console.WriteLine("Database {0} opened", name);

        return databaseDescriptor;
    }

    private static Task LoadDatabaseSchema(DatabaseDescriptor database)
    {
        byte[]? data = database.Storage.Get(CamusConfig.SchemaKey); //SchemaSpace!.GetDataFromPage(Config.SchemaHeaderPage);

        if (data is not null && data.Length > 0)
            database.Schema.Tables = Serializator.Unserialize<Dictionary<string, TableSchema>>(data);
        else
            database.Schema.Tables = new();

        Console.WriteLine("Schema tablespaces read. Loaded {0} tables", database.Schema.Tables.Count);

        return Task.CompletedTask;
    }

    private static Task LoadDatabaseSystemSpace(DatabaseDescriptor database)
    {
        byte[]? data = database.Storage.Get(CamusConfig.SystemKey);

        if (data is not null && data.Length > 0)
            database.SystemSchema = Serializator.Unserialize<SystemSchema>(data);
        else
            database.SystemSchema = new();

        Console.WriteLine("System tablespaces read. Found {0} objects", database.SystemSchema.Tables.Count);

        return Task.CompletedTask;
    }
}
