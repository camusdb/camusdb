
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
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

            byte[] data = await tablespace.GetDataFromPageAsBytes(entry.Value.Value);
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

        byte[] data = await tablespace.GetDataFromPageAsBytes(pageOffset.Value);
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

        /*Console.WriteLine(data.Length);

        Console.WriteLine("***");

        for (int i = 0; i < data.Length; i++)
            Console.WriteLine(data[i]);

        Console.WriteLine("***");*/

        int pointer = 0;

        int type = Serializator.ReadType(data, ref pointer);
        int schema = Serializator.ReadInt32(data, ref pointer);

        type = Serializator.ReadType(data, ref pointer);
        int rowId = Serializator.ReadInt32(data, ref pointer);

        List<ColumnValue> columnValues = new();

        List<TableColumnSchema> columns = tableSchema.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            int value;
            TableColumnSchema column = columns[i];

            //Console.WriteLine("{0} {1}", column.Name, column.Type);

            switch (column.Type)
            {
                case ColumnType.Id:
                    //int r = pointer;
                    Serializator.ReadType(data, ref pointer);
                    value = Serializator.ReadInt32(data, ref pointer);
                    columnValues.Add(new(ColumnType.Id, value.ToString()));
                    //Console.WriteLine(pointer - r);
                    break;

                case ColumnType.Integer:
                    //int rx = pointer;
                    Serializator.ReadType(data, ref pointer);
                    value = Serializator.ReadInt32(data, ref pointer);
                    columnValues.Add(new(ColumnType.Integer, value.ToString()));
                    //Console.WriteLine(pointer - rx);
                    break;

                case ColumnType.String:
                    //Console.WriteLine("Type={0}", Serializator.ReadType(data, ref pointer));
                    int length = Serializator.ReadInt32(data, ref pointer);
                    columnValues.Add(new(ColumnType.String, Serializator.ReadString(data, length, ref pointer)));
                    break;

                case ColumnType.Bool:
                    Serializator.ReadType(data, ref pointer);
                    columnValues.Add(new(ColumnType.String, Serializator.ReadBool(data, ref pointer) ? "true" : "false"));
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownType,
                        "Unknown type " + column.Type
                    );
            }
        }

        return columnValues;
    }
}
