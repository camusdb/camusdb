
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
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
        TableIndexAlterer tableIndexAlterer,
        RowDeleter rowDeleter,
        DatabaseDescriptor database,
        TableDescriptor table,
        DropTableTicket ticket
    )
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            AlterIndexTicket alterIndexTicket = new(
                txnId: ticket.TxnId,
                databaseName: ticket.DatabaseName,
                tableName: ticket.TableName,
                indexName: index.Key,
                columnName: "",
                operation: index.Key == CamusDBConfig.PrimaryKeyInternalName ? AlterIndexOperation.DropPrimaryKey : AlterIndexOperation.DropIndex
            );

            await tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket);
        }

        DeleteTicket deleteTicket = new(
            txnId: ticket.TxnId,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            where: null,
            filters: null
        );

        await rowDeleter.Delete(queryExecutor, database, table, deleteTicket);        

        try
        {
            await database.Schema.Semaphore.WaitAsync();

            if (database.Schema.Tables.Remove(ticket.TableName))
                Console.WriteLine("Removed table {0} from database schema", ticket.TableName);

            database.Storage.Put(CamusDBConfig.SchemaKey, Serializator.Serialize(database.Schema.Tables));
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }

        try
        {
            await database.SystemSchema.Semaphore.WaitAsync();

            Dictionary<string, DatabaseObject> objects = database.SystemSchema.Objects;

            if (database.SystemSchema.Objects.Remove(ticket.TableName))
                Console.WriteLine("Removed table {0} from system schema", ticket.TableName);

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema.Objects));
        }
        finally
        {
            database.SystemSchema.Semaphore.Release();
        }

        Console.WriteLine("Dropped table {0}", ticket.TableName);

        return true;
    }
}