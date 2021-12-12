
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Journal.Utils;

public static class SerializatorHelper
{
    public static int GetColumnValueLength(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Id or ColumnType.Integer => SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32,
            ColumnType.String => SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32 + value.Value.Length,
            ColumnType.Bool => SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeBool,
            _ => throw new Exception("Unsupported column value type"),
        };
    }

    public static void WriteColumnValue(byte[] journal, ColumnValue value, ref int pointer)
    {
        Serializator.WriteInt8(journal, (int)value.Type, ref pointer);

        switch (value.Type)
        {
            case ColumnType.Id:
            case ColumnType.Integer:                
                Serializator.WriteInt32(journal, int.Parse(value.Value), ref pointer);
                break;

            case ColumnType.String:                
                Serializator.WriteInt32(journal, value.Value.Length, ref pointer);
                Serializator.WriteString(journal, value.Value, ref pointer);
                break;

            case ColumnType.Bool:
                Serializator.WriteBool(journal, value.Value == "true", ref pointer);
                break;

            default:
                throw new Exception("here");
        }
    }

    public static async Task<ColumnValue> ReadColumnValue(FileStream journal)
    {
        string value;
        ColumnType type = (ColumnType)(await ReadInt8(journal));

        switch (type)
        {
            case ColumnType.Id:
            case ColumnType.Integer:
                value = (await ReadInt32(journal)).ToString();
                break;

            case ColumnType.String:
                int length = await ReadInt32(journal);
                value = await ReadString(journal, length);
                break;

            case ColumnType.Bool:
                bool boolValue = await ReadBool(journal);
                value = boolValue ? "true" : "false";
                break;

            default:
                throw new Exception("here");
        }

        return new ColumnValue(type, value);
    }

    public static async Task<int> ReadInt8(FileStream journal)
    {
        byte[] buffer = new byte[
            SerializatorTypeSizes.TypeInteger8   // (1 byte)
        ];

        int readBytes = await journal.ReadAsync(buffer, 0, SerializatorTypeSizes.TypeInteger8);
        if (readBytes != SerializatorTypeSizes.TypeInteger8)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading logs"
            );

        int pointer = 0;
        return Serializator.ReadInt8(buffer, ref pointer);
    }

    public static async Task<short> ReadInt16(FileStream journal)
    {
        byte[] buffer = new byte[
            SerializatorTypeSizes.TypeInteger16   // (2 bytes)
        ];

        int readBytes = await journal.ReadAsync(buffer, 0, SerializatorTypeSizes.TypeInteger16);
        if (readBytes != SerializatorTypeSizes.TypeInteger16)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading logs"
            );

        int pointer = 0;
        return Serializator.ReadInt16(buffer, ref pointer);
    }

    public static async Task<int> ReadInt32(FileStream journal)
    {
        byte[] buffer = new byte[
            SerializatorTypeSizes.TypeInteger32   // (4 bytes)
        ];

        int readBytes = await journal.ReadAsync(buffer, 0, SerializatorTypeSizes.TypeInteger32);
        if (readBytes != SerializatorTypeSizes.TypeInteger32)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading logs"
            );

        int pointer = 0;
        return Serializator.ReadInt32(buffer, ref pointer);
    }

    public static async Task<string> ReadString(FileStream journal, int size)
    {
        byte[] buffer = new byte[size];

        int readBytes = await journal.ReadAsync(buffer, 0, size);
        if (readBytes != size)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading logs"
            );

        int pointer = 0;
        return Serializator.ReadString(buffer, size, ref pointer);
    }

    public static async Task<bool> ReadBool(FileStream journal)
    {
        byte[] buffer = new byte[
            SerializatorTypeSizes.TypeBool   // (1 byte)
        ];

        int readBytes = await journal.ReadAsync(buffer, 0, SerializatorTypeSizes.TypeBool);
        if (readBytes != SerializatorTypeSizes.TypeBool)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading logs"
            );

        int pointer = 0;
        return Serializator.ReadBoolAhead(buffer, ref pointer);
    }
}
