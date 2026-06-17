
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
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TableCreator
{
    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public TableCreator(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.logger = logger;
    }

    public async Task<bool> Create(
        QueryExecutor queryExecutor,
        TableOpener tableOpener,
        TableIndexAlterer tableIndexAlterer,
        DatabaseDescriptor database,
        CreateTableTicket ticket,
        KvTransaction tx
    )
    {
        if (ticket.IfNotExists && catalogs.TableExists(database, ticket.TableName))
            return false;

        TableSchema tableSchema = await catalogs.CreateTable(database, ticket, tx).ConfigureAwait(false);

        RegisterTableObject(database, tableSchema);
        await catalogs.PersistSystemMetaAsync(database, tx).ConfigureAwait(false);

        await AddConstraints(queryExecutor, tableOpener, tableIndexAlterer, database, ticket, tx).ConfigureAwait(false);

        return true;
    }

    private void RegisterTableObject(DatabaseDescriptor database, TableSchema tableSchema)
    {
        try
        {
            database.SystemSchemaSemaphore.Wait();

            DatabaseTableObject tableObject = new(
                type: DatabaseObjectType.Table,
                id: tableSchema.Id ?? "",
                name: tableSchema.Name!,
                startOffset: ""
            );

            database.SystemSchema.Tables.TryAdd(tableObject.Id, tableObject);

            Log.LogTableRegisteredInSystemSpace(logger, tableSchema.Name!);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }
    }

    private async Task AddConstraints(
        QueryExecutor queryExecutor,
        TableOpener tableOpener,
        TableIndexAlterer tableIndexAlterer,
        DatabaseDescriptor database,
        CreateTableTicket ticket,
        KvTransaction tx
    )
    {
        if (ticket.Constraints.Length == 0)
            return;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            AlterIndexTicket indexTicket;

            switch (constraint.Type)
            {
                case ConstraintType.PrimaryKey:
                    indexTicket = new(
                        databaseName: database.Name,
                        tableName: ticket.TableName,
                        indexName: constraint.Name,
                        columns: constraint.Columns,
                        operation: AlterIndexOperation.AddPrimaryKey
                    );
                    break;

                case ConstraintType.IndexMulti:
                    indexTicket = new(
                        databaseName: database.Name,
                        tableName: ticket.TableName,
                        indexName: constraint.Name,
                        columns: constraint.Columns,
                        operation: AlterIndexOperation.AddIndex
                    );
                    break;

                case ConstraintType.IndexUnique:
                    indexTicket = new(
                        databaseName: database.Name,
                        tableName: ticket.TableName,
                        indexName: constraint.Name,
                        columns: constraint.Columns,
                        operation: AlterIndexOperation.AddUniqueIndex
                    );
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        "Unknown constraint: " + constraint.Type
                    );
            }

            await tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket, tx).ConfigureAwait(false);

            // On cluster nodes, replicate the index change via the schema log so that all
            // nodes apply it and the index survives restarts. For standalone nodes,
            // PublishIndex already calls PersistSchemaTableAsync directly.
            if (!database.OwnsKahuna)
                await catalogs.ReplicateIndexChangeAsync(database, indexTicket, table, tx).ConfigureAwait(false);
        }
    }
}
