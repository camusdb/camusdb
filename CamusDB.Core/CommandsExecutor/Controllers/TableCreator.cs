
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
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

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
            var objects = database.SystemSchema.Objects;

            BufferPoolHandler tablespace = database.TableSpace!;
            BufferPoolHandler systemTablespace = database.SystemSpace!;

            string tableName = tableSchema.Name!;

            await database.SystemSchema.Semaphore.WaitAsync();
            
            int pageOffset = await tablespace.GetNextFreeOffset();

            DatabaseObject databaseObject = new();
            databaseObject.Type = DatabaseObjectType.Table;
            databaseObject.Name = tableName;
            databaseObject.StartOffset = pageOffset;

            databaseObject.Indexes = new();

            foreach (TableColumnSchema column in tableSchema.Columns!)
            {
                if (column.Primary)
                {
                    int indexPageOffset = await tablespace.GetNextFreeOffset();

                    Console.WriteLine("Primary key for {0} added to system, staring at {1}", tableName, indexPageOffset);

                    databaseObject.Indexes.Add(
                        CamusDBConfig.PrimaryKeyInternalName,
                        new DatabaseIndexObject(IndexType.Unique, indexPageOffset)
                    );
                    continue;
                }

                if (column.Index != IndexType.None)
                {
                    int indexPageOffset = await tablespace.GetNextFreeOffset();

                    Console.WriteLine("Index {0}/{1} key for {2} added to system, staring at {3}", column.Name, column.Index, tableName, indexPageOffset);

                    databaseObject.Indexes.Add(
                        column.Name,
                        new DatabaseIndexObject(column.Index, indexPageOffset)
                    );
                    continue;
                }
            }

            objects.Add(tableName, databaseObject);

            await systemTablespace.WriteDataToPage(
                CamusDBConfig.SystemHeaderPage,
                Serializator.Serialize(database.SystemSchema.Objects)
            );

            Console.WriteLine("Added table {0} to system, data table staring at {1}", tableName, pageOffset);
        }
        finally
        {
            database.SystemSchema.Semaphore.Release();
        }
    }
}

