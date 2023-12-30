
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableDropper
{
    private readonly CatalogsManager catalogs;

    public TableDropper(CatalogsManager catalogsManager)
    {
        catalogs = catalogsManager;
    }

    public async Task<bool> Drop(
        QueryExecutor queryExecutor,
        RowDeleter rowDeleter,
        DatabaseDescriptor database,
        TableDescriptor table,
        DropTableTicket ticket
    )
    {        
        DeleteTicket deleteTicket = new(
            txnId: ticket.TxnId,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            where: null,
            filters: null
        );

        await rowDeleter.Delete(queryExecutor, database, table, deleteTicket);

        try { 

            await database.Schema.Semaphore.WaitAsync();

            database.Schema.Tables.Remove(ticket.TableName);
                       
            database.DbHandler.Put(CamusDBConfig.SchemaKey, Serializator.Serialize(database.Schema.Tables));

            Console.WriteLine("Dropped table {0}", ticket.TableName);

            return true;
        }
        finally
        {
            database.Schema.Semaphore.Release();            
        }
    }
}