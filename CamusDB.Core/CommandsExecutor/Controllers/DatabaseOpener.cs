
using CamusDB.Core.Catalogs;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class DatabaseOpener
{
    private readonly SemaphoreSlim descriptorsSemaphore = new(1, 1);

    private readonly Dictionary<string, DatabaseDescriptor> databaseDescriptors = new();

    public async ValueTask<DatabaseDescriptor> Open(string name)
    {
        if (databaseDescriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return databaseDescriptor;

        try
        {
            await descriptorsSemaphore.WaitAsync();

            if (databaseDescriptors.TryGetValue(name, out databaseDescriptor))
                return databaseDescriptor;

            if (!Directory.Exists("Data/" + name))
                throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

            databaseDescriptor = new();

            string path = "Data/" + name + "/tablespace0";
            var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open);
            databaseDescriptor.TableSpace = new BufferPoolHandler(mmf);

            path = "Data/" + name + "/schema";
            mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open);
            databaseDescriptor.SchemaSpace = new BufferPoolHandler(mmf);

            path = "Data/" + name + "/system";
            mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open);
            databaseDescriptor.SystemSpace = new BufferPoolHandler(mmf);

            // @todo initialize in parallel

            await LoadDatabaseSchema(databaseDescriptor);
            await LoadDatabaseSystemSpace(databaseDescriptor);
            await LoadDatabaseTableSpace(databaseDescriptor);

            Console.WriteLine("Database {0} opened", name);

            databaseDescriptors.Add(name, databaseDescriptor);
        }
        finally
        {
            descriptorsSemaphore.Release();
        }

        return databaseDescriptor;
    }

    private static async Task LoadDatabaseSchema(DatabaseDescriptor databaseDescriptor)
    {
        byte[] data = await databaseDescriptor.SchemaSpace!.GetDataFromPage(0);
        if (data.Length > 0)
            databaseDescriptor.Schema.Tables = Serializator.Unserialize<Dictionary<string, TableSchema>>(data);
        else
            databaseDescriptor.Schema.Tables = new();

        Console.WriteLine("Schema tablespaces read");
    }

    private static async Task LoadDatabaseTableSpace(DatabaseDescriptor databaseDescriptor)
    {
        byte[] data = await databaseDescriptor.TableSpace!.GetDataFromPage(0);        

        if (data.Length == 0) // tablespace is not initialized
        {
            // write tablespace header
            BufferPage page = await databaseDescriptor.TableSpace.ReadPage(0);

            int pointer = 0;
            Serializator.WriteInt32(page.Buffer, 4, ref pointer); // size of data (4 integer)
            Serializator.WriteInt32(page.Buffer, 1, ref pointer); // next page offset

            databaseDescriptor.TableSpace!.FlushPage(page); // @todo make this atomic
        }

        Console.WriteLine("Data tablespaces read");
    }

    private static async Task LoadDatabaseSystemSpace(DatabaseDescriptor database)
    {
        byte[] data = await database.SystemSpace!.GetDataFromPage(0);
        if (data.Length > 0)
            database.SystemSchema.Objects = Serializator.Unserialize<Dictionary<string, DatabaseObject>>(data);
        else
            database.SystemSchema.Objects = new();

        Console.WriteLine("System tablespaces read");
    }
}
