
using System;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.Serializer;

namespace CamusDB.Library.Catalogs
{
    public class CatalogsManager
    {
        public CatalogsManager()
        {
        }

        public async Task<bool> CreateTable(DatabaseDescriptor database, CreateTableTicket ticket)
        {
            try
            {
                await database.Schema.Semaphore.WaitAsync();

                if (database.Schema.Tables.ContainsKey(ticket.Name))
                    throw new CamusDBException("Table already exists");

                TableSchema tableSchema = new();
                tableSchema.Version = 0;
                tableSchema.Name = ticket.Name;
                tableSchema.Columns = new();

                foreach (ColumnInfo column in ticket.Columns)                
                    tableSchema.Columns.Add(new TableColumnSchema(column.Name, column.Type, column.Primary, column.NotNull));

                database.Schema.Tables.Add(ticket.Name, tableSchema);

                await database.SchemaSpace!.WritePages(0, Serializator.Serialize(database.Schema.Tables));

                Console.WriteLine("Added table {0}", ticket.Name);

                return true;
            }
            finally
            {
                database.Schema.Semaphore.Release();
            }
        }
    }
}

