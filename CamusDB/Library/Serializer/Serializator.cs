
using System;
using System.Text;
using System.Text.Json;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.Serializer.Models;

namespace CamusDB.Library.Serializer;

public class Serializator
{
    public static void Serialize(byte[] buffer, TableSchema tableSchema)
    {
        int pointer = 0;
        PackInt(buffer, tableSchema.Version, ref pointer);
    }

    public static byte[] Serialize(Dictionary<string, TableSchema> tableSchema)
    {            
        string jsonSerialized = JsonSerializer.Serialize(tableSchema);
        return Encoding.UTF8.GetBytes(jsonSerialized);
    }

    public static byte[] Serialize(Dictionary<string, DatabaseObject> databaseObjects)
    {
        string jsonSerialized = JsonSerializer.Serialize(databaseObjects);
        return Encoding.UTF8.GetBytes(jsonSerialized);
    }

    public static T Unserialize<T>(byte[] serialized) where T : new()
    {
        string xp = Encoding.UTF8.GetString(serialized);
        Console.WriteLine(xp);

        /*for (int i = 0; i < serialized.Length; i++)
        {
            Console.WriteLine(serialized[i]);
        }*/

        T? deserialized = JsonSerializer.Deserialize<T>(xp);
        if (deserialized is null)
            return new T();

        return deserialized;
    }

    private static bool PackInt(byte[] buffer, int number, ref int pointer)
    {
        if (number < 0 || number >= 0x7FFF)
        {
            WriteTypeToBuffer(buffer, Types.TYPE_INTEGER_32, ref pointer);
            WriteInt32ToBuffer(buffer, number, ref pointer);
            return true;
        }

        if (number < 0x10)
        {
            int typedInt = 0;
            typedInt = (typedInt & 0xf) | (Types.TYPE_INTEGER_4 << 4);
            typedInt = (typedInt & 0xf0) | number;
            WriteInt8ToBuffer(buffer, typedInt, ref pointer);
            return true;
        }

        if (number < 0x100)
        {
            WriteTypeToBuffer(buffer, Types.TYPE_INTEGER_8, ref pointer);
            WriteInt8ToBuffer(buffer, number, ref pointer);
            return true;
        }

        if (number < 0x7FFF)
        {
            WriteTypeToBuffer(buffer, Types.TYPE_INTEGER_16, ref pointer);
            WriteInt16ToBuffer(buffer, number, ref pointer);
            return true;
        }

        return false;
    }

    public static void WriteTypeToBuffer(byte[] buffer, int type, ref int pointer)
    {
        int byteType = 0;

        if (type < 0x10)
        {
            byteType = (byteType & 0xf) | (type << 4);
        }
        else
        {
            byteType = (byteType & 0xf) | (Types.TYPE_EXTENDED << 4);
            byteType = (byteType & 0xf0) | (type - 0xf);
        }

        //CheckBufferOverflow(1);
        buffer[pointer++] = (byte)byteType;
    }

    public static void WriteInt8ToBuffer(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(1);
        buffer[pointer++] = (byte)((number >> 0) & 0xff);
    }

    public static void WriteInt16ToBuffer(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(2);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        pointer += 2;
    }

    public static void WriteInt32ToBuffer(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((number >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((number >> 24) & 0xff);
        pointer += 4;
    }

    public static int UnpackInt32(byte[] buffer, ref int pointer)
    {
        int number = buffer[pointer++];
        number += (buffer[pointer++] << 8);
        number += (buffer[pointer++] << 16);
        number += (buffer[pointer++] << 24);
        return number;
    }
}

