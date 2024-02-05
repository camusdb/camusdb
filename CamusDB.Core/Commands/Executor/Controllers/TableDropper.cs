
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
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableDropper
{
    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public TableDropper(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.logger = logger;
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
                txnState: ticket.TxnState,
                databaseName: ticket.DatabaseName,
                tableName: ticket.TableName,
                indexName: index.Key,
                columns: Array.Empty<ColumnIndexInfo>(),
                operation: index.Key == CamusDBConfig.PrimaryKeyInternalName ? AlterIndexOperation.DropPrimaryKey : AlterIndexOperation.DropIndex
            );

            await tableIndexAlterer.Alter(queryExecutor, database, table, alterIndexTicket);
        }

        DeleteTicket deleteTicket = new(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            where: null,
            filters: null
        );

        await rowDeleter.Delete(queryExecutor, database, table, deleteTicket).ConfigureAwait(false);

        try
        {
            await database.Schema.Semaphore.WaitAsync().ConfigureAwait(false);

            if (database.Schema.Tables.Remove(ticket.TableName))
                logger.LogInformation("Removed table {TableName} from database schema", ticket.TableName);

            database.Storage.Put(CamusDBConfig.SchemaKey, Serializator.Serialize(database.Schema.Tables));
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }

        try
        {
            await database.SystemSchemaSemaphore.WaitAsync().ConfigureAwait(false);

            Dictionary<string, DatabaseTableObject> objects = database.SystemSchema.Tables;

            if (database.SystemSchema.Tables.Remove(table.Id))
                logger.LogInformation("Removed table {TableName} from system schema", ticket.TableName);

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema));
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }

        database.TableDescriptors.TryRemove(ticket.TableName, out _);

        logger.LogInformation("Dropped table {TableName}", ticket.TableName);

        return true;
    }
}