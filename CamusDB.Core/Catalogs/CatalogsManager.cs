
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Catalogs;

public sealed class CatalogsManager
{
    public async Task<TableSchema> CreateTable(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        try
        {
            await database.Schema.Semaphore.WaitAsync();

            if (database.Schema.Tables.ContainsKey(ticket.TableName))
                throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, "Table already exists");

            TableSchema tableSchema = new();
            tableSchema.Version = 0;
            tableSchema.Name = ticket.TableName;

            tableSchema.Columns = new();

            foreach (ColumnInfo column in ticket.Columns)
            {
                tableSchema.Columns.Add(
                    new TableColumnSchema(
                        column.Name,
                        column.Type,
                        column.Primary,
                        column.NotNull,
                        column.Index
                    )
                );
            }            

            database.Schema.Tables.Add(ticket.TableName, tableSchema);

            await database.SchemaSpace!.WriteDataToPage(CamusDBConfig.SchemaHeaderPage, Serializator.Serialize(database.Schema.Tables));

            Console.WriteLine("Added table {0} to schema", ticket.TableName);

            return tableSchema;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    public TableSchema GetTableSchema(DatabaseDescriptor database, string tableName) // @todo return a snapshot instead of the schema
    {
        if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
            return tableSchema;

        throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, "Table doesn't exist");
    }
}
