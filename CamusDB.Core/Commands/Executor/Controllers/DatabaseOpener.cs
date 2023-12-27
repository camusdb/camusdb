
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RocksDbSharp;
using Nito.AsyncEx;
using CamusDB.Core.Storage;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseOpener
{
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseOpener(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async ValueTask<DatabaseDescriptor> Open(CommandExecutor executor, string name, bool recoveryMode = false)
    {
        AsyncLazy<DatabaseDescriptor> openDatabaseLazy = databaseDescriptors.Descriptors.GetOrAdd(
                                                            name,
                                                            (x) => new AsyncLazy<DatabaseDescriptor>(() => LoadDatabase(name))
                                                         );
        return await openDatabaseLazy;
    }
  
    private static async Task<DatabaseDescriptor> LoadDatabase(string name)
    {                    
        string path = Path.Combine(CamusConfig.DataDirectory, name);

        DbOptions options = new DbOptions()
                                .SetCreateIfMissing(true)
                                .SetWalDir(path) // using WAL
                                .SetWalRecoveryMode(Recovery.AbsoluteConsistency) // setting recovery mode to Absolute Consistency
                                .SetAllowConcurrentMemtableWrite(true);

        RocksDb dbHandler = RocksDb.Open(options, path);

        //if (!Directory.Exists(path))
        //    throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

        StorageManager tablespaceStorage = new(dbHandler);

        DatabaseDescriptor databaseDescriptor = new(
            name: name,
            dbHandler,
            tableSpace: new BufferPoolHandler(tablespaceStorage)
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
        byte[]? data = database.DbHandler.Get(CamusConfig.SchemaKey); //SchemaSpace!.GetDataFromPage(Config.SchemaHeaderPage);

        if (data is not null && data.Length > 0)
            database.Schema.Tables = Serializator.Unserialize<Dictionary<string, TableSchema>>(data);
        else
            database.Schema.Tables = new();

        Console.WriteLine("Schema tablespaces read. Loaded {0} tables", database.Schema.Tables.Count);

        return Task.CompletedTask;
    }

    private static Task LoadDatabaseSystemSpace(DatabaseDescriptor database)
    {
        byte[]? data = database.DbHandler.Get(CamusConfig.SystemKey);

        if (data is not null && data.Length > 0)
            database.SystemSchema.Objects = Serializator.Unserialize<Dictionary<string, DatabaseObject>>(data);
        else
            database.SystemSchema.Objects = new();

        Console.WriteLine("System tablespaces read. Found {0} objects", database.SystemSchema.Objects.Count);

        return Task.CompletedTask;
    }    
}
