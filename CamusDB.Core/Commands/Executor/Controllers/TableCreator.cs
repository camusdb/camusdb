
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Serializer;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableCreator
{
    private CatalogsManager Catalogs { get; set; }

    public TableCreator(CatalogsManager catalogsManager)
    {
        Catalogs = catalogsManager;
    }

    public async Task<bool> Create(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        TableSchema tableSchema = await Catalogs.CreateTable(database, ticket);

        await SetInitialTablePages(database, tableSchema);

        return true;
    }

    private async Task SetInitialTablePages(DatabaseDescriptor database, TableSchema tableSchema)
    {
        try
        {
            await database.SystemSchema.Semaphore.WaitAsync();

            Dictionary<string, DatabaseObject> objects = database.SystemSchema.Objects;

            BufferPoolHandler tablespace = database.TableSpace;            

            string tableName = tableSchema.Name!;            

            ObjectIdValue pageOffset = tablespace.GetNextFreeOffset();

            DatabaseObject databaseObject = new()
            {
                Type = DatabaseObjectType.Table,
                Name = tableName,
                StartOffset = pageOffset.ToString(),
                Indexes = new()
            };

            foreach (TableColumnSchema column in tableSchema.Columns!)
            {
                if (column.Primary)
                {
                    ObjectIdValue indexPageOffset = tablespace.GetNextFreeOffset();

                    Console.WriteLine("Primary key for {0} added to system, staring at {1}", tableName, indexPageOffset);

                    databaseObject.Indexes.Add(
                        CamusDBConfig.PrimaryKeyInternalName,
                        new DatabaseIndexObject(column.Name, IndexType.Unique, indexPageOffset.ToString())
                    );
                    continue;
                }

                if (column.Index != IndexType.None)
                {
                    ObjectIdValue indexPageOffset = tablespace.GetNextFreeOffset();

                    Console.WriteLine("Index {0}/{1} key for {2} added to system, staring at {3}", column.Name, column.Index, tableName, indexPageOffset);

                    databaseObject.Indexes.Add(
                        column.Name,
                        new DatabaseIndexObject(column.Name, column.Index, indexPageOffset.ToString())
                    );
                    continue;
                }
            }

            objects.Add(tableName, databaseObject);

            database.DbHandler.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema.Objects));

            Console.WriteLine("Added table {0} to system, data table staring at {1}", tableName, pageOffset);
        }
        finally
        {
            database.SystemSchema.Semaphore.Release();
        }
    }
}

