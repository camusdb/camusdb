
using System.Diagnostics;
using CamusDB.Library.Catalogs;
using CamusDB.Library.Util.Trees;
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.BufferPool.Models;
using CamusDB.Library.Serializer.Models;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.CommandsExecutor.Controllers;
using CamusDB.Library.CommandsExecutor.Models.Tickets;

namespace CamusDB.Library.CommandsExecutor.Controllers;

public class QueryExecutor
{
    public async Task<List<List<ColumnValue>>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        foreach (Entry entry in table.Rows.EntriesTraverse())
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
