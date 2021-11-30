
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableOpener
{
    private readonly IndexReader indexReader = new();    

    private CatalogsManager Catalogs { get; set; }

    public TableOpener(CatalogsManager catalogsManager)
    {
        Catalogs = catalogsManager;        
    }

    public async ValueTask<TableDescriptor> Open(DatabaseDescriptor database, string tableName)
    {
        if (database.TableDescriptors.TryGetValue(tableName, out TableDescriptor? tableDescriptor))
            return tableDescriptor;

        try
        {
            await database.DescriptorsSemaphore.WaitAsync();

            if (database.TableDescriptors.TryGetValue(tableName, out tableDescriptor))
                return tableDescriptor;

            DatabaseObject systemObject = GetSystemObject(database, tableName);

            tableDescriptor = new();
            tableDescriptor.Name = tableName;
            tableDescriptor.Schema = Catalogs.GetTableSchema(database, tableName);
            tableDescriptor.Rows = await GetIndexTree(database, systemObject.StartOffset);

            // @todo read indexes in parallel

            if (systemObject.Indexes is not null)
            {
                foreach (KeyValuePair<string, int> index in systemObject.Indexes)
                    tableDescriptor.Indexes.Add(index.Key, await GetIndexTree(database, index.Value));
            }

            database.TableDescriptors.Add(tableName, tableDescriptor);
        }
        finally
        {
            database.DescriptorsSemaphore.Release();
        }

        return tableDescriptor;
    }

    private Task<BTree> GetIndexTree(DatabaseDescriptor database, int offset)
    {
        return indexReader.Read(database.TableSpace!, offset);
    }

    private static DatabaseObject GetSystemObject(DatabaseDescriptor database, string tableName)
    {
        var objects = database.SystemSchema.Objects;

        if (!objects.TryGetValue(tableName, out DatabaseObject? databaseObject))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Table system data is corrupt");

        Console.WriteLine("Table {0} opened at offset={1}", tableName, databaseObject.StartOffset);

        return databaseObject;
    }

    private int GetTablePage(DatabaseDescriptor database, string tableName)
    {
        var objects = database.SystemSchema.Objects;

        if (!objects.TryGetValue(tableName, out DatabaseObject? databaseObject))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Table system data is corrupt");

        Console.WriteLine("Table {0} opened at offset={1}", tableName, databaseObject.StartOffset);

        return databaseObject.StartOffset;
    }
}
