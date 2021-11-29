
using System.Diagnostics;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public class QueryExecutor
{
    public async Task<List<List<ColumnValue>>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        foreach (BTreeEntry entry in table.Rows.EntriesTraverse())
        {
            if (entry.Value is null)
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value.Value);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            rows.Add(UnserializeRow(table.Schema!, data));
        }

        return rows;
    }

    public async Task<List<List<ColumnValue>>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();
        
        int? pageOffset = table.Indexes["pk"].Get(ticket.Id);

        if (pageOffset is null)
        {
            Console.WriteLine("Index Pk={0} has an empty page data", ticket.Id);
            return rows;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.Value);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return rows;
        }

        rows.Add(UnserializeRow(table.Schema!, data));

        return rows;
    }

    private List<ColumnValue> UnserializeRow(TableSchema tableSchema, byte[] data)
    {
        //catalogs.GetTableSchema(database, tableName);

        int pointer = 0;

        int type = Serializator.ReadType(data, ref pointer);
        int schema = Serializator.ReadInt32(data, ref pointer);

        type = Serializator.ReadType(data, ref pointer);
        int rowId = Serializator.ReadInt32(data, ref pointer);

        List<ColumnValue> columns = new();

        foreach (TableColumnSchema columnSchema in tableSchema.Columns!)
        {
            int value;

            switch (columnSchema.Type)
            {
                case ColumnType.Id:
                    Serializator.ReadType(data, ref pointer);
                    value = Serializator.ReadInt32(data, ref pointer);
                    columns.Add(new(ColumnType.Id, value.ToString()));
                    break;

                case ColumnType.Integer:
                    Serializator.ReadType(data, ref pointer);
                    value = Serializator.ReadInt32(data, ref pointer);
                    columns.Add(new(ColumnType.Integer, value.ToString()));
                    break;

                case ColumnType.String:
                    Serializator.ReadType(data, ref pointer);
                    int length = Serializator.ReadInt32(data, ref pointer);                    
                    columns.Add(new(ColumnType.String, Serializator.ReadString(data, length, ref pointer)));
                    break;

                case ColumnType.Bool:
                    Serializator.ReadType(data, ref pointer);
                    columns.Add(new(ColumnType.String, Serializator.ReadBool(data, ref pointer) ? "true" : "false"));                    
                    break;
            }
        }

        return columns;
    }
}
