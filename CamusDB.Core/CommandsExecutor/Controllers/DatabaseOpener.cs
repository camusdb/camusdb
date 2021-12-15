
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

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseOpener
{
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseOpener(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async ValueTask<DatabaseDescriptor> Open(CommandExecutor executor, string name)
    {
        if (databaseDescriptors.Descriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return databaseDescriptor;

        try
        {
            await databaseDescriptors.Semaphore.WaitAsync();

            if (databaseDescriptors.Descriptors.TryGetValue(name, out databaseDescriptor))
                return databaseDescriptor;

            string path = Path.Combine(Config.DataDirectory, name);

            if (!Directory.Exists(path))
                throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

            path = Path.Combine(Config.DataDirectory, name, "tablespace0");
            MemoryMappedFile tablespace = MemoryMappedFile.CreateFromFile(path, FileMode.Open);

            path = Path.Combine(Config.DataDirectory, name, "schema");
            MemoryMappedFile schema = MemoryMappedFile.CreateFromFile(path, FileMode.Open);

            path = Path.Combine(Config.DataDirectory, name, "system");
            MemoryMappedFile system = MemoryMappedFile.CreateFromFile(path, FileMode.Open);

            databaseDescriptor = new(
                name: name,
                tableSpace: new BufferPoolHandler(tablespace),
                schemaSpace: new BufferPoolHandler(schema),
                systemSpace: new BufferPoolHandler(system)
            );

            await Task.WhenAll(new Task[]
            {
                LoadDatabaseSchema(databaseDescriptor),
                LoadDatabaseSystemSpace(databaseDescriptor),
                LoadDatabaseTableSpace(databaseDescriptor)
            });

            path = Path.Combine(Config.DataDirectory, name, "camus.lock");

            await Task.WhenAll(new Task[]
            {
                databaseDescriptor.TableSpace.Initialize(),
                databaseDescriptor.Journal.Writer.Initialize(executor),
                File.WriteAllBytesAsync(path, Array.Empty<byte>())
            });

            Console.WriteLine("Database {0} opened", name);

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

        byte[] data = await tablespace.GetDataFromPage(Config.TableSpaceHeaderPage);

        if (data.Length != 0) // tablespace is initialized?
            return;

        // write tablespace header
        BufferPage page = await databaseDescriptor.TableSpace.ReadPage(Config.TableSpaceHeaderPage);

        tablespace.WriteTableSpaceHeader(page.Buffer);
        tablespace.FlushPage(page); // @todo make this atomic

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
