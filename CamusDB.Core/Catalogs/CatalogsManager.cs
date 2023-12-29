
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
using System.Xml.Linq;

namespace CamusDB.Core.Catalogs;

public sealed class CatalogsManager
{
    public async Task<TableSchema> CreateTable(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        try
        {
            await database.Schema.Semaphore.WaitAsync();

            if (database.Schema.Tables.ContainsKey(ticket.TableName))
                throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{ticket.TableName}' already exists");

            TableSchema tableSchema = new()
            {
                Version = 0,
                Name = ticket.TableName,
                Columns = new(),
                SchemaHistory = new()
            };

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

            // Every time a change is made to the table schema, an instance is added
            // to the history that allows reading records with old schema versions.
            TableSchemaHistory schemaHistory = new()
            {
                Version = 0,
                Columns = tableSchema.Columns,
            };

            tableSchema.SchemaHistory.Add(schemaHistory);

            database.Schema.Tables.Add(ticket.TableName, tableSchema);

            database.DbHandler.Put(CamusDBConfig.SchemaKey, Serializator.Serialize(database.Schema.Tables));

            Console.WriteLine("Added table {0} to schema", ticket.TableName);

            return tableSchema;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    public async Task<TableSchema> AlterTable(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        try
        {
            await database.Schema.Semaphore.WaitAsync();

            if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
                throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{ticket.TableName}' already exists");

            tableSchema.Version++;

            switch (ticket.Operation)
            {
                case AlterTableOperation.AddColumn:
                    AddColumn(tableSchema, ticket.Column);
                    break;

                case AlterTableOperation.DropColumn:
                    DropColumn(tableSchema, ticket.Column.Name);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{ticket.Operation}'");
            }

            TableSchemaHistory schemaHistory = new()
            {
                Version = tableSchema.Version,
                Columns = tableSchema.Columns,
            };

            tableSchema.SchemaHistory!.Add(schemaHistory);

            database.DbHandler.Put(CamusDBConfig.SchemaKey, Serializator.Serialize(database.Schema.Tables));

            Console.WriteLine("Modifed table {0} schema", ticket.TableName);

            return tableSchema;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    private static void AddColumn(TableSchema tableSchema, ColumnInfo newColumn)
    {
        bool hasColumn = false;

        List<TableColumnSchema> tableColumns = new(tableSchema.Columns!.Count);

        foreach (TableColumnSchema column in tableSchema.Columns!)
        {
            if (newColumn.Name == column.Name)
                hasColumn = true;
            else
                tableColumns.Add(column);
        }

        if (hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.DuplicateColumn, $"Duplicate column '{newColumn.Name}'");

        tableColumns.Add(new TableColumnSchema(newColumn.Name, newColumn.Type, false, newColumn.NotNull, IndexType.None));

        tableSchema.Columns = tableColumns;
    }

    private static void DropColumn(TableSchema tableSchema, string columnName)
    {
        bool hasColumn = false;

        List<TableColumnSchema> tableColumns = new(tableSchema.Columns!.Count);

        foreach (TableColumnSchema column in tableSchema.Columns!)
        {
            if (columnName == column.Name)
                hasColumn = true;
            else
                tableColumns.Add(column);
        }

        if (!hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{columnName}'");

        tableSchema.Columns = tableColumns;
    }

    public TableSchema GetTableSchema(DatabaseDescriptor database, string tableName) // @todo return a snapshot instead of the schema
    {
        if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
            return tableSchema;

        throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{tableName}' doesn't exist");
    }
}
