
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

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
        Serializator.ReadObjectId(data, ref pointer); // row id

        Dictionary<string, ColumnValue> columnValues = new();

        List<TableColumnSchema> columns = tableSchema.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            //Console.WriteLine("{0} {1}", column.Name, column.Type);

            switch (column.Type)
            {
                case ColumnType.Id:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        if (columnType == SerializatorTypes.TypeId)
                        {
                            ObjectIdValue idValue = Serializator.ReadObjectId(data, ref pointer);
                            columnValues.Add(column.Name, new(ColumnType.Id, idValue.ToString()));
                        }
                        else
                        {
                            if (columnType == SerializatorTypes.TypeNull)
                                columnValues.Add(column.Name, new(ColumnType.Null, ""));
                            else
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;

                case ColumnType.Integer64:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        if (columnType == SerializatorTypes.TypeInteger64)
                        {
                            long value = Serializator.ReadInt64(data, ref pointer);
                            columnValues.Add(column.Name, new(ColumnType.Integer64, value.ToString()));
                        }
                        else
                        {
                            if (columnType == SerializatorTypes.TypeNull)
                                columnValues.Add(column.Name, new(ColumnType.Null, ""));
                            else
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    //Console.WriteLine(pointer - rx);
                    break;

                case ColumnType.String:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        if (columnType == SerializatorTypes.TypeString8 || columnType == SerializatorTypes.TypeString16 || columnType == SerializatorTypes.TypeString32)
                        {
                            string str = Serializator.ReadString(data, ref pointer);
                            columnValues.Add(column.Name, new(ColumnType.String, str));
                        }
                        else
                        {
                            if (columnType == SerializatorTypes.TypeNull)
                                columnValues.Add(column.Name, new(ColumnType.Null, ""));
                            else
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;

                case ColumnType.Bool:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        if (columnType == SerializatorTypes.TypeBool)
                            columnValues.Add(column.Name, new(ColumnType.Bool, Serializator.ReadBool(data, ref pointer) ? "true" : "false"));
                        else
                        {
                            if (columnType == SerializatorTypes.TypeNull)
                                columnValues.Add(column.Name, new(ColumnType.Null, ""));
                            else
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;


                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownType, "Unknown type " + column.Type
                    );
            }
        }

        return columnValues;
    }
}
