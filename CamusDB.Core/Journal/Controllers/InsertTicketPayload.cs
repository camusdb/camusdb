
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Journal.Controllers;

public static class InsertTicketPayload
{
    private static int GetLogLength(InsertTicket insertTicket)
    {
        int length = insertTicket.TableName.Length;

        foreach (KeyValuePair<string, ColumnValue> columnValue in insertTicket.Values)
        {
            length += columnValue.Key.Length;

            switch (columnValue.Value.Type)
            {
                case ColumnType.Id:
                case ColumnType.Integer:
                    length += SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32;
                    break;

                case ColumnType.String:
                    length += SerializatorTypeSizes.TypeInteger8 + columnValue.Value.Value.Length;
                    break;

                case ColumnType.Bool:
                    length += SerializatorTypeSizes.TypeBool;
                    break;
            }
        }

        return length;
    }

    public static byte[] Generate(uint sequence, InsertTicket insertTicket)
    {
        int length = GetLogLength(insertTicket);

        byte[] journal = new byte[
            SerializatorTypeSizes.TypeInteger32 + // LSN (4 bytes)
            SerializatorTypeSizes.TypeInteger16 + // journal type (2 bytes)            
            SerializatorTypeSizes.TypeInteger32 + // length(4 bytes)
            SerializatorTypeSizes.TypeInteger16 + // number fields (2 bytes)
            length // payload
        ];

        int pointer = 0;
        Serializator.WriteUInt32(journal, sequence, ref pointer);
        Serializator.WriteInt16(journal, JournalScheduleTypes.InsertTicket, ref pointer);
        Serializator.WriteInt32(journal, length, ref pointer);
        Serializator.WriteInt16(journal, insertTicket.Values.Count, ref pointer);

        foreach (KeyValuePair<string, ColumnValue> columnValue in insertTicket.Values)
        {
            Serializator.WriteString(journal, columnValue.Key, ref pointer);

            switch (columnValue.Value.Type)
            {
                case ColumnType.Id:
                case ColumnType.Integer:
                    Serializator.WriteInt32(journal, int.Parse(columnValue.Value.Value), ref pointer);
                    break;

                case ColumnType.String:
                    Serializator.WriteString(journal, columnValue.Value.Value, ref pointer);
                    break;

                case ColumnType.Bool:
                    Serializator.WriteBool(journal, columnValue.Value.Value == "true", ref pointer);
                    break;

                default:
                    throw new Exception("here");
            }
        }

        return journal;
    }
}

