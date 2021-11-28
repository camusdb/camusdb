
using System;
using CamusDB.Library.Catalogs;
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.BufferPool.Models;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.CommandsExecutor.Controllers;
using CamusDB.Library.Serializer.Models;

namespace CamusDB.Library.CommandsExecutor;

public class CommandExecutor
{
    private const int InitialTableSpaceSize = 1024 * 4096; // 1024 blocks
   
    private CatalogsManager Catalogs { get; set; }

    private readonly DatabaseOpener databaseOpener = new();

    private readonly TableOpener tableOpener = new();

    private readonly RowInserter rowInserter = new();

    public CommandExecutor(CatalogsManager catalogsManager)
    {
        Catalogs = catalogsManager;
    }
        
    public async Task<bool> CreateTable(CreateTableTicket ticket)
    {
        DatabaseDescriptor descriptor = await databaseOpener.Open(ticket.Database);
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
   
    public async Task<bool> Insert(InsertTicket ticket)
    {
        DatabaseDescriptor database = await databaseOpener.Open(ticket.DatabaseName);
        //return await Catalogs.CreateTable(descriptor, ticket);

        TableDescriptor table = await tableOpener.Open(database, Catalogs, ticket.TableName);

        await rowInserter.Insert(database, table, ticket);

        return true;
    }
}
