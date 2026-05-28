
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
        CreateTableTicket ticket
    )
    {
        if (ticket.IfNotExists && catalogs.TableExists(database, ticket.TableName))
            return false;

        TableSchema tableSchema = await catalogs.CreateTable(database, ticket);

        RegisterTableObject(database, tableSchema);

        await AddConstraints(queryExecutor, tableOpener, tableIndexAlterer, database, ticket);

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

            logger.LogInformation("Registered table {TableName} in system space", tableSchema.Name);
        }
        finally
        {
            database.SystemSchemaSemaphore.Release();
        }
    }

    private static async Task AddConstraints(
        QueryExecutor queryExecutor,
        TableOpener tableOpener,
        TableIndexAlterer tableIndexAlterer,
        DatabaseDescriptor database,
        CreateTableTicket ticket
    )
    {
        if (ticket.Constraints.Length == 0)
            return;

        TableDescriptor table = await tableOpener.Open(database, ticket.TableName);

        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            switch (constraint.Type)
            {
                case ConstraintType.PrimaryKey:
                {
                    AlterIndexTicket indexTicket = new(
                        txnState: ticket.TxnState,
                        databaseName: database.Name,
                        tableName: ticket.TableName,
                        indexName: constraint.Name,
                        columns: constraint.Columns,
                        operation: AlterIndexOperation.AddPrimaryKey
                    );

                    await tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket);
                }
                    break;

                case ConstraintType.IndexMulti:
                {
                    AlterIndexTicket indexTicket = new(
                        txnState: ticket.TxnState,
                        databaseName: database.Name,
                        tableName: ticket.TableName,
                        indexName: constraint.Name,
                        columns: constraint.Columns,
                        operation: AlterIndexOperation.AddIndex
                    );

                    await tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket);
                }
                    break;

                case ConstraintType.IndexUnique:
                {
                    AlterIndexTicket indexTicket = new(
                        txnState: ticket.TxnState,
                        databaseName: database.Name,
                        tableName: ticket.TableName,
                        indexName: constraint.Name,
                        columns: constraint.Columns,
                        operation: AlterIndexOperation.AddUniqueIndex
                    );

                    await tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket);
                }
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        "Unknown constraint: " + constraint.Type
                    );
            }
        }
    }
}
