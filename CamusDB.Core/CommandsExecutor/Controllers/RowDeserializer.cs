
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowDeserializer
{
    public Dictionary<string, ColumnValue> Deserialize(TableSchema tableSchema, byte[] data)
    {
        //catalogs.GetTableSchema(database, tableName);

        /*Console.WriteLine(data.Length);

        Console.WriteLine("***");

        for (int i = 0; i < data.Length; i++)
            Console.WriteLine(data[i]);

        Console.WriteLine("***");*/

        //throw new Exception(data);

        int pointer = 0;

        Serializator.ReadType(data, ref pointer); // type 
        Serializator.ReadInt32(data, ref pointer); // schema

        Serializator.ReadType(data, ref pointer); // type
        Serializator.ReadInt32(data, ref pointer); // row id

        Dictionary<string, ColumnValue> columnValues = new();

        List<TableColumnSchema> columns = tableSchema.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            int value;
            TableColumnSchema column = columns[i];

            //Console.WriteLine("{0} {1}", column.Name, column.Type);

            switch (column.Type)
            {
                case ColumnType.Id:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        if (columnType == SerializatorTypes.TypeInteger32)
                        {
                            value = Serializator.ReadInt32(data, ref pointer);
                            columnValues.Add(column.Name, new(ColumnType.Id, value.ToString()));
                        }
                        else
                        {
                            if (columnType != SerializatorTypes.TypeNull)
                                throw new Exception(columnType.ToString());
                        }                        
                    }
                    break;

                case ColumnType.Integer:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        if (columnType == SerializatorTypes.TypeInteger32)
                        {
                            value = Serializator.ReadInt32(data, ref pointer);
                            columnValues.Add(column.Name, new(ColumnType.Integer, value.ToString()));
                        }
                        else
                        {
                            if (columnType != SerializatorTypes.TypeNull)
                                throw new Exception(columnType.ToString());
                        }
                    }
                    //Console.WriteLine(pointer - rx);
                    break;

                case ColumnType.String:
                    Serializator.ReadType(data, ref pointer);
                    int length = Serializator.ReadInt32(data, ref pointer);
                    //Console.WriteLine("Length={0}", length);
                    columnValues.Add(column.Name, new(ColumnType.String, Serializator.ReadString(data, length, ref pointer)));
                    break;

                case ColumnType.Bool:
                    Serializator.ReadType(data, ref pointer);
                    columnValues.Add(column.Name, new(ColumnType.Bool, Serializator.ReadBool(data, ref pointer) ? "true" : "false"));
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
