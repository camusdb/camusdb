
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

/// <summary>
/// Transforms the records from their disk format into a native C# representation, which is easier to handle and manipulate.
/// </summary>
internal sealed class RowDeserializer
{
    public Dictionary<string, ColumnValue> Deserialize(TableSchema tableSchema, ObjectIdValue slotOne, byte[] data)
    {
        //catalogs.GetTableSchema(database, tableName);

        /*Console.WriteLine(data.Length);

        Console.WriteLine("***");

        for (int i = 0; i < data.Length; i++)
            Console.WriteLine(data[i]);

        Console.WriteLine("***");*/

        //throw new Exception(data);

        int pointer = 0;

        Serializator.ReadType(data, ref pointer); // schema type
        int schemaVersion = Serializator.ReadInt32(data, ref pointer); // schema

        Serializator.ReadType(data, ref pointer); // row id type
        Serializator.ReadObjectId(data, ref pointer); // row id

        List<TableColumnSchema> columns = tableSchema.SchemaHistory![schemaVersion].Columns!;

        Dictionary<string, ColumnValue> columnValues = new(columns.Count + 1)
        {
            //{ "_id", new(ColumnType.Id, slotOne.ToString()) }
        };

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            //Console.WriteLine("{0} {1}", column.Name, column.Type);

            switch (column.Type)
            {
                case ColumnType.Id:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        switch (columnType)
                        {
                            case SerializatorTypes.TypeId:
                            {
                                ObjectIdValue idValue = Serializator.ReadObjectId(data, ref pointer);
                                columnValues.Add(column.Name, new(ColumnType.Id, idValue.ToString()));
                                break;
                            }
                            
                            case SerializatorTypes.TypeNull:
                                columnValues.Add(column.Name, new(ColumnType.Null, ""));
                                break;
                            
                            default:
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;

                case ColumnType.Integer64:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        switch (columnType)
                        {
                            case SerializatorTypes.TypeInteger64:
                            {
                                long value = Serializator.ReadInt64(data, ref pointer);
                                columnValues.Add(column.Name, new(ColumnType.Integer64, value));
                                break;
                            }
                            
                            case SerializatorTypes.TypeNull:
                                columnValues.Add(column.Name, new(ColumnType.Null, 0));
                                break;
                            
                            default:
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;

                case ColumnType.String:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        switch (columnType)
                        {
                            case SerializatorTypes.TypeString8:
                            case SerializatorTypes.TypeString16:
                            case SerializatorTypes.TypeString32:
                            {
                                string str = Serializator.ReadString(data, ref pointer);
                                columnValues.Add(column.Name, new(ColumnType.String, str));
                                break;
                            }
                            
                            case SerializatorTypes.TypeNull:
                                columnValues.Add(column.Name, new(ColumnType.Null, 0));
                                break;
                            
                            default:
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;

                case ColumnType.Bool:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        switch (columnType)
                        {
                            case SerializatorTypes.TypeBool:
                                columnValues.Add(column.Name, new(ColumnType.Bool, Serializator.ReadBool(data, ref pointer)));
                                break;
                            
                            case SerializatorTypes.TypeNull:
                                columnValues.Add(column.Name, new(ColumnType.Null, 0));
                                break;
                            
                            default:
                                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, columnType.ToString());
                        }
                    }
                    break;

                case ColumnType.Float64:
                    {
                        int columnType = Serializator.ReadType(data, ref pointer);
                        switch (columnType)
                        {
                            case SerializatorTypes.TypeDouble:
                            {
                                double value = Serializator.ReadDouble(data, ref pointer);
                                columnValues.Add(column.Name, new(ColumnType.Float64, value));
                                break;
                            }
                            
                            case SerializatorTypes.TypeNull:
                                columnValues.Add(column.Name, new(ColumnType.Null, 0));
                                break;
                            
                            default:
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
