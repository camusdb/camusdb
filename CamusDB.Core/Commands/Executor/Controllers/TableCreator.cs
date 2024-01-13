
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
    private readonly CatalogsManager catalogs;

    public TableCreator(CatalogsManager catalogsManager)
    {
        catalogs = catalogsManager;
    }

    public async Task<bool> Create(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        if (ticket.IfNotExists && database.SystemSchema.Objects.ContainsKey(ticket.TableName))
            return false;

        TableSchema tableSchema = await catalogs.CreateTable(database, ticket);

        await SetInitialTablePages(database, tableSchema);

        return true;
    }

    private async Task SetInitialTablePages(DatabaseDescriptor database, TableSchema tableSchema)
    {
        try
        {
            await database.SystemSchema.Semaphore.WaitAsync();

            Dictionary<string, DatabaseObject> objects = database.SystemSchema.Objects;

            BufferPoolManager tablespace = database.BufferPool;

            string tableName = tableSchema.Name!;

            ObjectIdValue pageOffset = tablespace.GetNextFreeOffset();

            DatabaseObject databaseObject = new()
            {
                Type = DatabaseObjectType.Table,
                Id = tablespace.GetNextFreeOffset().ToString(),
                Name = tableName,
                StartOffset = pageOffset.ToString(),
                Indexes = new()
            };

            foreach (TableColumnSchema column in tableSchema.Columns!)
            {
                if (column.Primary)
                {
                    ObjectIdValue indexId = tablespace.GetNextFreeOffset();
                    ObjectIdValue indexPageOffset = tablespace.GetNextFreeOffset();

                    Console.WriteLine("Primary key for {0} added to system, staring at {1}", tableName, indexPageOffset);

                    databaseObject.Indexes.Add(
                        CamusDBConfig.PrimaryKeyInternalName,
                        new DatabaseIndexObject(indexId.ToString(), new string[] { column.Id }, IndexType.Unique, indexPageOffset.ToString())
                    );
                    continue;
                }

                if (column.Index != IndexType.None)
                {
                    ObjectIdValue indexId = tablespace.GetNextFreeOffset();
                    ObjectIdValue indexPageOffset = tablespace.GetNextFreeOffset();

                    Console.WriteLine("Index {0}/{1} key for {2} added to system, staring at {3}", column.Name, column.Index, tableName, indexPageOffset);

                    databaseObject.Indexes.Add(
                        column.Name + "_idx",
                        new DatabaseIndexObject(indexId.ToString(), new string[] { column.Id }, column.Index, indexPageOffset.ToString())
                    );
                    continue;
                }
            }

            objects.Add(tableName, databaseObject);

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema.Objects));

            Console.WriteLine("Added table {0} to system, data table staring at {1}", tableName, pageOffset);
        }
        finally
        {
            database.SystemSchema.Semaphore.Release();
        }
    }
}

