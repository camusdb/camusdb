

using System;
using CamusDB.Library.Catalogs;
using CamusDB.Library.Util.Trees;
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using System.Collections.Generic;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.BufferPool.Models;
using CamusDB.Library.CommandsExecutor.Models;

namespace CamusDB.Library.CommandsExecutor.Controllers;

public class TableOpener
{
    public async Task<TableDescriptor> Open(DatabaseDescriptor database, CatalogsManager catalogs, string tableName)
    {
        if (database.TableDescriptors.TryGetValue(tableName, out TableDescriptor? tableDescriptor))
            return tableDescriptor;

        try
        {
            await database.DescriptorsSemaphore.WaitAsync();

            if (database.TableDescriptors.TryGetValue(tableName, out tableDescriptor))
                return tableDescriptor;            

            tableDescriptor = new();            
            tableDescriptor.Name = tableName;
            tableDescriptor.Rows = await GetRowsIndex(database, tableName);
            tableDescriptor.Schema = catalogs.GetTableSchema(database, tableName);

            database.TableDescriptors.Add(tableName, tableDescriptor);
        }
        finally
        {
            database.DescriptorsSemaphore.Release();
        }

        return tableDescriptor;
    }

    private async Task<BTree> GetRowsIndex(DatabaseDescriptor database, string tableName)
    {
        int tableOffset = await GetTablePage(database, tableName);

        byte[] data = await database.TableSpace!.GetDataFromPage(tableOffset);
        //if (data.Length > 0)
        //    tableDescriptor.Rows = Serializator.Unserialize<Dictionary<int, int>>(data);

        return new BTree();
    }

    private async Task<int> GetTablePage(DatabaseDescriptor database, string tableName)
    {
        int pageOffset = await database.TableSpace!.GetNextFreeOffset();        

        var objects = database.SystemSchema.Objects;

        if (objects.TryGetValue(tableName, out DatabaseObject? databaseObject))
            return databaseObject.StartOffset;
        
        try
        {
            await database.SystemSchema.Semaphore.WaitAsync();

            databaseObject = new();
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

        Console.WriteLine("TableOffset={0}", databaseObject.StartOffset);

        return databaseObject.StartOffset;
    }
}
