
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Serializer;
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
        await Catalogs.CreateTable(database, ticket);

        await SetInitialTablePages(database, ticket);

        return true;
    }

    private async Task SetInitialTablePages(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        try
        {
            var objects = database.SystemSchema.Objects;

            string tableName = ticket.TableName;

            await database.SystemSchema.Semaphore.WaitAsync();
            
            int pageOffset = await database.TableSpace!.GetNextFreeOffset();

            DatabaseObject databaseObject = new();
            databaseObject.Type = DatabaseObjectType.Table;
            databaseObject.Name = tableName;
            databaseObject.StartOffset = pageOffset;

            databaseObject.Indexes = new();

            foreach (ColumnInfo column in ticket.Columns)
            {
                if (column.Primary)
                {
                    int indexPageOffset = await database.TableSpace!.GetNextFreeOffset();

                    Console.WriteLine("Primary key for {0} added to system, staring at {1}", tableName, indexPageOffset);

                    databaseObject.Indexes.Add("pk", indexPageOffset);
                }
            }

            objects.Add(tableName, databaseObject);

            await database.SystemSpace!.WriteDataToPage(
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

