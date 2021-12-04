
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
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowSerializer
{
    private int CalculateBufferLength(TableDescriptor table, InsertTicket ticket)
    {
        int length = 10; // 1 type + 4 schemaVersion + 1 type + 4 rowId

        for (int i = 0; i < table.Schema!.Columns!.Count; i++)
        {
            TableColumnSchema column = table.Schema!.Columns[i];

            if (!ticket.Values.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                length += 1; // null (1 byte)
                continue;
            }

            if (column.Type != columnValue.Type)
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownType,
                    "Type " + columnValue.Type + " cannot be assigned to " + column.Name + " (" + column.Type + ")"
                );

            switch (columnValue.Type) // @todo check if value is compatible with column
            {
                case ColumnType.Id:
                    length += 5; // type 1 byte + 4 byte int
                    break;

                case ColumnType.Integer:
                    length += 5; // type 1 byte + 4 byte int
                    break;

                case ColumnType.String:
                    length += 5 + columnValue.Value.Length; // type 1 byte + 4 byte length + strLength
                    break;

                case ColumnType.Bool:
                    length++; // bool (1 byte)
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownType,
                        "Unknown type " + columnValue.Type
                    );
            }
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

        List<TableColumnSchema> columns = table.Schema!.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!ticket.Values.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                //Console.WriteLine("here?");
                Serializator.WriteType(rowBuffer, SerializatorTypes.TypeNull, ref pointer);
                continue;
            }

            //Console.WriteLine("{0} {1}", column.Name, column.Type);

            switch (columnValue.Type)
            {
                case ColumnType.Id: // @todo use int.TryParse
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, int.Parse(columnValue.Value), ref pointer);
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
