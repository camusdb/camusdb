
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage;

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

            if (!Directory.Exists(path))
                throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");
                        
            StorageManager schemaStorage = new(path, "schema");
            StorageManager systemStorage = new(path, "system");
            StorageManager tablespaceStorage = new(path, "tablespace");

            // Need to make sure tablespaces are initialized before using them
            await Task.WhenAll(new Task[]
            {
                tablespaceStorage.Initialize(),
                schemaStorage.Initialize(),
                systemStorage.Initialize()
            });

            databaseDescriptor = new(
                name: name,
                tableSpace: new BufferPoolHandler(tablespaceStorage),
                schemaSpace: new BufferPoolHandler(schemaStorage),
                systemSpace: new BufferPoolHandler(systemStorage)
            );

            await Task.WhenAll(new Task[]
            {
                LoadDatabaseSchema(databaseDescriptor),
                LoadDatabaseSystemSpace(databaseDescriptor),
                LoadDatabaseTableSpace(databaseDescriptor)
            });

            // Create this file when the database is open, remove it when closed
            // If the file exists when the server starts up then it might crashed
            path = Path.Combine(Config.DataDirectory, name, "camus.lock");

            await Task.WhenAll(new Task[]
            {
                databaseDescriptor.TableSpace.Initialize(),
                File.WriteAllBytesAsync(path, Array.Empty<byte>())
            });

            Console.WriteLine("Database {0} opened", name);

            // Only the data table space has a concurrent dirty page flusher
            _ = Task.Factory.StartNew(databaseDescriptor.TableSpaceFlusher.PeriodicallyFlush);

            // Initialize a new journal
            databaseDescriptor.Journal.Writer.Initialize();

            // Check journal for recovery
            if (recoveryMode)
                await databaseDescriptor.Journal.Writer.TryRecover(executor, databaseDescriptor);            

            databaseDescriptors.Descriptors.Add(name, databaseDescriptor);
        }
        finally
        {
            databaseDescriptors.Semaphore.Release();
        }

        return databaseDescriptor;
    }

    private static async Task LoadDatabaseSchema(DatabaseDescriptor databaseDescriptor)
    {
        byte[] data = await databaseDescriptor.SchemaSpace!.GetDataFromPage(Config.SchemaHeaderPage);

        if (data.Length > 0)
            databaseDescriptor.Schema.Tables = Serializator.Unserialize<Dictionary<string, TableSchema>>(data);
        else
            databaseDescriptor.Schema.Tables = new();

        Console.WriteLine("Schema tablespaces read");
    }

    private static async Task LoadDatabaseTableSpace(DatabaseDescriptor databaseDescriptor)
    {
        BufferPoolHandler tablespace = databaseDescriptor.TableSpace;

        bool initialized = await tablespace.IsInitialized(Config.TableSpaceHeaderPage);

        if (initialized) // tablespace is initialized?
            return;

        // write tablespace header
        BufferPage page = await databaseDescriptor.TableSpace.ReadPage(Config.TableSpaceHeaderPage);

        tablespace.WriteTableSpaceHeader(page.Buffer);
        await tablespace.FlushPage(page); // @todo make this atomic

        Console.WriteLine("Data tablespaces initialized");
    }

    private static async Task LoadDatabaseSystemSpace(DatabaseDescriptor database)
    {
        byte[] data = await database.SystemSpace!.GetDataFromPage(Config.SystemHeaderPage);

        if (data.Length > 0)
            database.SystemSchema.Objects = Serializator.Unserialize<Dictionary<string, DatabaseObject>>(data);
        else
            database.SystemSchema.Objects = new();

        Console.WriteLine("System tablespaces read");
    }
}
