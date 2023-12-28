﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

        if (database.TableDescriptors.TryGetValue(tableName, out TableDescriptor? tableDescriptor))
            return tableDescriptor;

        try
        {
            await database.DescriptorsSemaphore.WaitAsync(); // @todo block per table

            if (database.TableDescriptors.TryGetValue(tableName, out tableDescriptor))
                return tableDescriptor;

            BufferPoolHandler tablespace = database.TableSpace;

            TableSchema tableSchema = Catalogs.GetTableSchema(database, tableName);
            DatabaseObject systemObject = GetSystemObject(database, tableName);

            tableDescriptor = new(
                tableName,
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
                            {
                                BTree<ColumnValue, BTreeTuple?> rows = await indexReader.ReadUnique(tablespace, ObjectId.ToValue(index.Value.StartOffset ?? ""));

                                tableDescriptor.Indexes.Add(
                                    index.Key,
                                    new TableIndexSchema(index.Value.Column, index.Value.Type, rows)
                                );
                            }
                            break;

                        case IndexType.Multi:
                            {
                                BTreeMulti<ColumnValue> rows = await indexReader.ReadMulti(tablespace, ObjectId.ToValue(index.Value.StartOffset ?? ""));

                                tableDescriptor.Indexes.Add(
                                    index.Key,
                                    new TableIndexSchema(index.Value.Column, index.Value.Type, rows)
                                );

                                continue;
                            }

                        default:
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot load invalid type of index");
                    }
                }
            }

            database.TableDescriptors.Add(tableName, tableDescriptor);
        }
        finally
        {
            database.DescriptorsSemaphore.Release();
        }

        return tableDescriptor;
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
