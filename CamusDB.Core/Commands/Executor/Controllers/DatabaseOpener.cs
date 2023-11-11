
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RocksDbSharp;
using CamusDB.Core.Storage;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;

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
        if (databaseDescriptors.Descriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return databaseDescriptor;

        try
        {
            // This semamphore prevents multiple threads to open the same database
            await databaseDescriptors.Semaphore.WaitAsync();

            if (databaseDescriptors.Descriptors.TryGetValue(name, out databaseDescriptor))
                return databaseDescriptor;

            string path = Path.Combine(Config.DataDirectory, name);

            DbOptions options = new DbOptions()
                                    .SetCreateIfMissing(true)
                                    .SetWalDir(path) // using WAL
                                    .SetWalRecoveryMode(Recovery.AbsoluteConsistency) // setting recovery mode to Absolute Consistency
                                    .SetAllowConcurrentMemtableWrite(true);

            RocksDb dbHandler = RocksDb.Open(options, path);

            //if (!Directory.Exists(path))
            //    throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

            StorageManager tablespaceStorage = new(dbHandler);

            databaseDescriptor = new(
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

            // Only the data table space has a concurrent dirty page flusher
            //_ = Task.Factory.StartNew(databaseDescriptor.TableSpaceFlusher.PeriodicallyFlush);                        

            databaseDescriptors.Descriptors.Add(name, databaseDescriptor);
        }
        finally
        {
            databaseDescriptors.Semaphore.Release();
        }

        return databaseDescriptor;
    }

    private static Task LoadDatabaseSchema(DatabaseDescriptor database)
    {
        byte[]? data = database.DbHandler.Get(Config.SchemaKey); //SchemaSpace!.GetDataFromPage(Config.SchemaHeaderPage);

        if (data is not null && data.Length > 0)
            database.Schema.Tables = Serializator.Unserialize<Dictionary<string, TableSchema>>(data);
        else
            database.Schema.Tables = new();

        Console.WriteLine("Schema tablespaces read. Loaded {0} tables", database.Schema.Tables.Count);

        return Task.CompletedTask;
    }

    private static Task LoadDatabaseSystemSpace(DatabaseDescriptor database)
    {
        byte[]? data = database.DbHandler.Get(Config.SystemKey);

        if (data is not null && data.Length > 0)
            database.SystemSchema.Objects = Serializator.Unserialize<Dictionary<string, DatabaseObject>>(data);
        else
            database.SystemSchema.Objects = new();

        Console.WriteLine("System tablespaces read. Found {0} objects", database.SystemSchema.Objects.Count);

        return Task.CompletedTask;
    }    
}
