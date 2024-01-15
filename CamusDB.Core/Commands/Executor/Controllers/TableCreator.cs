
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

    public async Task<bool> Create(
        QueryExecutor queryExecutor,
        TableOpener tableOpener,
        TableIndexAlterer tableIndexAlterer,
        DatabaseDescriptor database,
        CreateTableTicket ticket
    )
    {
        if (ticket.IfNotExists && database.SystemSchema.Tables.ContainsKey(ticket.TableName))
            return false;

        TableSchema tableSchema = await catalogs.CreateTable(database, ticket);

        await SetInitialTablePages(database, tableSchema);

        await AddConstraints(queryExecutor, tableOpener, tableIndexAlterer, database, ticket);

        return true;
    }

    private static async Task SetInitialTablePages(DatabaseDescriptor database, TableSchema tableSchema)
    {
        try
        {
            await database.SystemSchemaSemaphore.WaitAsync();

            Dictionary<string, DatabaseTableObject> tables = database.SystemSchema.Tables;

            BufferPoolManager tablespace = database.BufferPool;

            string tableName = tableSchema.Name!;

            ObjectIdValue pageOffset = tablespace.GetNextFreeOffset();

            DatabaseTableObject tableObject = new(            
                type: DatabaseObjectType.Table,
                id: tableSchema.Id ?? "",
                name: tableName,
                startOffset:  pageOffset.ToString()
            );

            tables.Add(tableObject.Id, tableObject);

            database.Storage.Put(CamusDBConfig.SystemKey, Serializator.Serialize(database.SystemSchema));

            Console.WriteLine("Added table {0} to system, data table staring at {1}", tableName, pageOffset);
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

        for (int i = 0; i < ticket.Constraints.Length; i++)
        {
            ConstraintInfo constraint = ticket.Constraints[i];

            switch (constraint.Type)
            {
                case ConstraintType.PrimaryKey:
                    {
                        AlterIndexTicket indexTicket = new(
                            txnId: ticket.TxnId,
                            databaseName: database.Name,
                            tableName: ticket.TableName,
                            indexName: constraint.Name,
                            columnName: constraint.Columns[0].Name,
                            operation: AlterIndexOperation.AddPrimaryKey
                        );

                        await tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket);
                    }
                    break;

                case ConstraintType.IndexMulti:
                    {
                        AlterIndexTicket indexTicket = new(
                            txnId: ticket.TxnId,
                            databaseName: database.Name,
                            tableName: ticket.TableName,
                            indexName: constraint.Name,
                            columnName: constraint.Columns[0].Name,
                            operation: AlterIndexOperation.AddIndex
                        );

                        await tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket);
                    }
                    break;

                case ConstraintType.IndexUnique:
                    {
                        AlterIndexTicket indexTicket = new(
                            txnId: ticket.TxnId,
                            databaseName: database.Name,
                            tableName: ticket.TableName,
                            indexName: constraint.Name,
                            columnName: constraint.Columns[0].Name,
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
