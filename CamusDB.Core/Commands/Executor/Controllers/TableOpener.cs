
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The goal of this controller is to define a single point for opening tables. An open table maintains a descriptor 
/// in memory that will be subsequently used by all operations on the table and allows knowing the pages 
/// where the indices are located as well as the history of schemas.
/// </summary>
internal sealed class TableOpener
{
    private readonly IndexReader indexReader = new();

    private CatalogsManager Catalogs { get; }

    public TableOpener(CatalogsManager catalogsManager)
    {
        Catalogs = catalogsManager;
    }

    /// <summary>
    /// Opens the specified table and returns a descriptor that contains the table schema and pointers to the indexes
    /// </summary>
    /// <param name="database"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public async ValueTask<TableDescriptor> Open(DatabaseDescriptor database, string tableName)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, "Invalid or empty table name");

        TableSchema tableSchema = Catalogs.GetTableSchema(database, tableName);

        AsyncLazy<TableDescriptor> openTableLazy = database.TableDescriptors.GetOrAdd(
                                                        tableSchema.Name ?? "",
                                                        (_) => new AsyncLazy<TableDescriptor>(() => LoadTable(database, tableSchema))
                                                   );
        return await openTableLazy;
    }

    private async Task<TableDescriptor> LoadTable(DatabaseDescriptor database, TableSchema tableSchema)
    {                       
        BufferPoolManager tablespace = database.BufferPool;
        
        DatabaseObject systemObject = GetSystemObject(database, tableSchema.Name ?? "");

        TableDescriptor tableDescriptor = new(
            tableSchema.Id ?? "",
            tableSchema.Name ?? "",
            tableSchema,
            await indexReader.ReadOffsets(tablespace, ObjectId.ToValue(systemObject.StartOffset ?? ""))
        );

        // @todo read indexes in parallel

        if (systemObject.Indexes is not null)
        {
            foreach (KeyValuePair<string, DatabaseIndexObject> index in systemObject.Indexes)
            {
                switch (index.Value.Type)
                {
                    case IndexType.Unique:
                    case IndexType.Multi:
                        {
                            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> btree = await indexReader.Read(tablespace, ObjectId.ToValue(index.Value.StartOffset ?? ""));

                            tableDescriptor.Indexes.Add(
                                index.Key,
                                new TableIndexSchema(MapColumnsIdsToNames(tableSchema.Columns, index.Value.ColumnIds), index.Value.Type, btree)
                            );
                        }
                        break;                                           

                    default:
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot load invalid type of index");
                }
            }
        }

        return tableDescriptor;
    }

    private static string[] MapColumnsIdsToNames(List<TableColumnSchema>? columns, string[] columnIds)
    {
        string[] columNames = new string[columnIds.Length];

        for (int i  = 0; i < columnIds.Length;  i++)
        {
            foreach (TableColumnSchema column in columns!)
            {
                if (column.Id == columnIds[i])
                    columNames[i] = column.Name ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Table system data is corrupt");
            }
        }
       
        return columNames;
    }

    private static DatabaseObject GetSystemObject(DatabaseDescriptor database, string tableName)
    {
        Dictionary<string, DatabaseObject> objects = database.SystemSchema.Objects;

        if (!objects.TryGetValue(tableName, out DatabaseObject? databaseObject))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Table system data is corrupt");

        Console.WriteLine("Table {0} opened at offset={1}", tableName, databaseObject.StartOffset);

        return databaseObject;
    }
}
