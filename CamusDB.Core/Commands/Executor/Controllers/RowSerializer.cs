
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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowSerializer
{
    private int CalculateBufferLength(TableDescriptor table, InsertTicket ticket)
    {
        int length = 10; // 1 type + 4 schemaVersion + 1 type + 4 rowId

        List<TableColumnSchema> tableColumns = table.Schema!.Columns!;

        for (int i = 0; i < tableColumns.Count; i++)
        {
            TableColumnSchema column = tableColumns[i];

            if (!ticket.Values.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                length += SerializatorTypeSizes.TypeNull; // null (1 byte)
                continue;
            }

            if (column.Type != columnValue.Type)
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownType,
                    "Type " + columnValue.Type + " cannot be assigned to " + column.Name + " (" + column.Type + ")"
                );

            length += columnValue.Type switch
            {
                // type 1 byte + 3 * 4 byte int
                ColumnType.Id => SerializatorTypeSizes.TypeInteger8 + (SerializatorTypeSizes.TypeInteger32 * 3),

                // type 1 byte + 4 byte int
                ColumnType.Integer => SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32,

                // type 1 byte + 4 byte length + strLength
                ColumnType.String => SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32 + columnValue.Value.Length,

                // bool (1 byte)
                ColumnType.Bool => SerializatorTypeSizes.TypeInteger8,

                _ => throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnValue.Type),
            };
        }

        return length;
    }

    public byte[] Serialize(TableDescriptor table, InsertTicket ticket, int rowId)
    {
        int length = CalculateBufferLength(table, ticket);

        //throw new Exception(length.ToString());

        byte[] rowBuffer = new byte[length];

        int pointer = 0;

        Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(rowBuffer, table.Schema!.Version, ref pointer); // schema version

        Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(rowBuffer, rowId, ref pointer); // row Id

        List<TableColumnSchema> columns = table.Schema.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!ticket.Values.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                Serializator.WriteType(rowBuffer, SerializatorTypes.TypeNull, ref pointer);
                continue;
            }

            //Console.WriteLine("{0} {1}", column.Name, column.Type);

            switch (columnValue.Type)
            {
                case ColumnType.Id:
                    ObjectIdValue objectId = ObjectId.ToValue(columnValue.Value);
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeId, ref pointer);
                    Serializator.WriteObjectId(rowBuffer, objectId, ref pointer);
                    break;

                case ColumnType.Integer: // @todo use int.TryParse
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, int.Parse(columnValue.Value), ref pointer);
                    break;

                case ColumnType.String:
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeString32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, columnValue.Value.Length, ref pointer);
                    Serializator.WriteString(rowBuffer, columnValue.Value, ref pointer);
                    break;

                case ColumnType.Bool:
                    Serializator.WriteBool(rowBuffer, columnValue.Value == "true", ref pointer);
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownType,
                        "Unknown type " + columnValue.Type
                    );
            }
        }

        /*Console.WriteLine("***");

        for (int i = 0; i < rowBuffer.Length; i++)
            Console.WriteLine(rowBuffer[i]);

        Console.WriteLine("***");

        Console.WriteLine("Length={0} BUffer={1}", pointer, rowBuffer.Length);*/

        return rowBuffer;
    }
}
