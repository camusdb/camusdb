
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class RowReader
{
    public List<ColumnValue> Unserialize(TableSchema tableSchema, byte[] data)
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
                    Serializator.ReadType(data, ref pointer);
                    int length = Serializator.ReadInt32(data, ref pointer);
                    //Console.WriteLine("Length={0}", length);
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
