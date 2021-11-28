
using System;
using CamusDB.Library.Catalogs;
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.BufferPool.Models;
using CamusDB.Library.CommandsExecutor.Models;

namespace CamusDB.Library.CommandsExecutor;

public class CommandExecutor
{
    private const int InitialTableSpaceSize = 1024 * 4096; // 1024 blocks

    private readonly SemaphoreSlim descriptorsSemaphore = new(1, 1);

    private readonly Dictionary<string, DatabaseDescriptor> databaseDescriptors = new();

    private CatalogsManager Catalogs { get; set; }

    public CommandExecutor(CatalogsManager catalogsManager)
    {
        Catalogs = catalogsManager;
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string name)
    {
        if (databaseDescriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return databaseDescriptor;

        try
        {
            await descriptorsSemaphore.WaitAsync();

            if (databaseDescriptors.TryGetValue(name, out databaseDescriptor))
                return databaseDescriptor;

            if (!Directory.Exists("Data/" + name))
                throw new CamusDBException("Database doesn't exist");

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

    public static async Task LoadDatabaseTableSpace(DatabaseDescriptor databaseDescriptor)
    {
        byte[] data = await databaseDescriptor.TableSpace!.GetDataFromPage(0);

        Console.WriteLine(data.Length);

        if (data.Length == 0)
        {
            MemoryPage page = await databaseDescriptor.TableSpace.ReadPage(0);

            int pointer = 0;
            Serializator.WriteInt32ToBuffer(page.Buffer, 4, ref pointer);
            Serializator.WriteInt32ToBuffer(page.Buffer, 1, ref pointer);

            await databaseDescriptor.TableSpace!.FlushPage(page);
        }

        Console.WriteLine("Data tablespaces read");
    }

    public static async Task LoadDatabaseSystemSpace(DatabaseDescriptor databaseDescriptor)
    {
        byte[] data = await databaseDescriptor.SystemSpace!.GetDataFromPage(0);
        if (data.Length > 0)
            databaseDescriptor.SystemSchema.Objects = Serializator.Unserialize<Dictionary<string, DatabaseObject>>(data);
        else
            databaseDescriptor.SystemSchema.Objects = new();

        Console.WriteLine("System tablespaces read");
    }

    public async Task<TableDescriptor> OpenTable(DatabaseDescriptor databaseDescriptor, string tableName)
    {
        if (databaseDescriptor.TableDescriptors.TryGetValue(tableName, out TableDescriptor? tableDescriptor))
            return tableDescriptor;

        try
        {
            await databaseDescriptor.DescriptorsSemaphore.WaitAsync();

            if (databaseDescriptor.TableDescriptors.TryGetValue(tableName, out tableDescriptor))
                return tableDescriptor;

            int tableOffset = await GetTablePage(databaseDescriptor, tableName);

            tableDescriptor = new();            

            byte[] data = await databaseDescriptor.TableSpace!.GetDataFromPage(tableOffset);
            //if (data.Length > 0)
            //    tableDescriptor.Rows = Serializator.Unserialize<Dictionary<int, int>>(data);
            
            tableDescriptor.Name = tableName;            
        }
        finally
        {
            databaseDescriptor.DescriptorsSemaphore.Release();
        }

        return tableDescriptor;
    }

    public async Task<bool> CreateTable(CreateTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await OpenDatabase(ticket.Database);
        return await Catalogs.CreateTable(descriptor, ticket);
    }

    public async Task CreateDatabase(string name)
    {
        name = name.ToLowerInvariant();

        if (Directory.Exists("Data/" + name))
            throw new CamusDBException("Database already exists");

        Directory.CreateDirectory("Data/" + name);

        await InitializeDatabaseFiles(name);
    }

    private static async Task InitializeDatabaseFiles(string name)
    {
        byte[] initialized = new byte[InitialTableSpaceSize];

        await Task.WhenAll(new Task[]
        {
            File.WriteAllBytesAsync("Data/" + name + "/tablespace0", initialized),
            File.WriteAllBytesAsync("Data/" + name + "/schema", initialized),
            File.WriteAllBytesAsync("Data/" + name + "/system", initialized)
        });

        Console.WriteLine("Database tablespaces created");
    }    

    private async Task<int> GetTablePage(DatabaseDescriptor database, string tableName)
    {
        int pageOffset = await database.TableSpace!.GetNextFreeOffset();
        Console.WriteLine(pageOffset);

        var objects = database.SystemSchema.Objects;

        if (!objects.TryGetValue(tableName, out DatabaseObject? databaseObject))
        {
            try
            {
                await database.SystemSchema.Semaphore.WaitAsync();

                databaseObject = new DatabaseObject();
                databaseObject.Type = DatabaseObjectType.Table;
                databaseObject.Name = tableName;
                databaseObject.StartOffset = pageOffset;
                objects.Add(tableName, databaseObject);

                await database.SystemSpace!.WritePages(0, Serializator.Serialize(database.SystemSchema.Objects));

                Console.WriteLine("Added table {0} to system", tableName);
            }
            finally
            {
                database.SystemSchema.Semaphore.Release();
            }
        }

        Console.WriteLine("TableOffset={0}", databaseObject.StartOffset);

        return databaseObject.StartOffset;
    }

    public async Task<bool> Insert(InsertTicket ticket)
    {
        DatabaseDescriptor datababase = await OpenDatabase(ticket.DatabaseName);
        //return await Catalogs.CreateTable(descriptor, ticket);

        TableDescriptor table = await OpenTable(datababase, ticket.TableName);

        



        return true;
    }
}
