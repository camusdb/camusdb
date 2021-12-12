
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Serializer;

public sealed class Serializator
{
    public static void Serialize(byte[] buffer, TableSchema tableSchema)
    {
        int pointer = 0;
        WriteInt(buffer, tableSchema.Version, ref pointer);
    }

    public static byte[] Serialize(Dictionary<string, TableSchema> tableSchema)
    {
        string jsonSerialized = JsonSerializer.Serialize(tableSchema);
        return Encoding.UTF8.GetBytes(jsonSerialized);
    }

    public static byte[] Serialize(Dictionary<string, DatabaseObject> databaseObjects)
    {
        string jsonSerialized = JsonSerializer.Serialize(databaseObjects);
        var x = Encoding.UTF8.GetBytes(jsonSerialized);
        //Console.WriteLine(x.Length);
        return x;
    }

    public static T Unserialize<T>(byte[] buffer) where T : new()
    {
        string xp = Encoding.UTF8.GetString(buffer);
        Console.WriteLine(xp);

        T? deserialized = JsonSerializer.Deserialize<T>(xp);
        if (deserialized is null)
            return new T();

        return deserialized;
    }

    private static bool WriteInt(byte[] buffer, int number, ref int pointer)
    {
        if (number < 0 || number >= 0x7FFF)
        {
            WriteType(buffer, SerializatorTypes.TypeInteger32, ref pointer);
            WriteInt32(buffer, number, ref pointer);
            return true;
        }

        if (number < 0x10)
        {
            int typedInt = 0;
            typedInt = (typedInt & 0xf) | (SerializatorTypes.TypeInteger4 << 4);
            typedInt = (typedInt & 0xf0) | number;
            WriteInt8(buffer, typedInt, ref pointer);
            return true;
        }

        if (number < 0x100)
        {
            WriteType(buffer, SerializatorTypes.TypeInteger8, ref pointer);
            WriteInt8(buffer, number, ref pointer);
            return true;
        }

        if (number < 0x7FFF)
        {
            WriteType(buffer, SerializatorTypes.TypeInteger16, ref pointer);
            WriteInt16(buffer, number, ref pointer);
            return true;
        }

        return false;
    }

    public static void WriteType(byte[] buffer, int type, ref int pointer)
    {
        int byteType = 0;

        if (type < 0x10)
        {
            byteType = (byteType & 0xf) | (type << 4);
        }
        else
        {
            byteType = (byteType & 0xf) | (SerializatorTypes.TypeExtended << 4);
            byteType = (byteType & 0xf0) | (type - 0xf);
        }

        //CheckBufferOverflow(1);
        buffer[pointer++] = (byte)byteType;
    }

    public static void WriteInt8(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(1);        
        buffer[pointer++] = (byte)((number >> 0) & 0xff);
    }

    public static void WriteInt16(byte[] buffer, int number, ref int pointer)
    {
        short number16 = Convert.ToInt16(number);
        byte[] byteArray = BitConverter.GetBytes(number16);
        buffer[pointer + 0] = byteArray[0];
        buffer[pointer + 1] = byteArray[1];
        pointer += 2;
    }

    public static void WriteInt32(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((number >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((number >> 24) & 0xff);
        pointer += 4;
    }

    public static void WriteUInt32(byte[] buffer, uint number, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((number >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((number >> 24) & 0xff);
        pointer += 4;
    }

    public static void WriteString(byte[] buffer, string str, ref int pointer)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(str);
        Buffer.BlockCopy(bytes, 0, buffer, pointer, bytes.Length);
        pointer += bytes.Length;
    }

    public static void WriteByteArray(byte[] buffer, byte[] bytes, ref int pointer)
    {
        Buffer.BlockCopy(bytes, 0, buffer, pointer, bytes.Length);
        pointer += bytes.Length;
    }

    public static void WriteBool(byte[] buffer, bool value, ref int pointer)
    {
        int typedBool = 0;
        int bool8 = ((value ? 1 : 0) >> 0) & 0xff;
        typedBool = (typedBool & 0xf) | (SerializatorTypes.TypeBool << 4);
        typedBool = (typedBool & 0xf0) | bool8;
        WriteInt8(buffer, typedBool, ref pointer);
    }

    public static int ReadType(byte[] buffer, ref int pointer)
    {
        int typeByte = buffer[pointer++];
        int type = (typeByte & 0xf0) >> 4;
        if (type == SerializatorTypes.TypeExtended)
            return (typeByte & 0xf) + 0xf;
        return type;
    }

    public static int ReadInt8(byte[] buffer, ref int pointer)
    {
        return buffer[pointer++];
    }

    public static short ReadInt16(byte[] buffer, ref int pointer)
    {
        short number = BitConverter.ToInt16(buffer, pointer);
        pointer += 2;
        return number;
    }

    private static float ReadFloat(byte[] buffer, ref int pointer)
    {
        float number = BitConverter.ToSingle(buffer, pointer);
        pointer += 4;
        return number;
    }

    public static int ReadInt32(byte[] buffer, ref int pointer)
    {
        int number = buffer[pointer++];
        number += (buffer[pointer++] << 8);
        number += (buffer[pointer++] << 16);
        number += (buffer[pointer++] << 24);
        return number;
    }

    public static uint ReadUInt32(byte[] buffer, ref int pointer)
    {
        uint number = buffer[pointer++];
        number += (uint)(buffer[pointer++] << 8);
        number += (uint)(buffer[pointer++] << 16);
        number += (uint)(buffer[pointer++] << 24);
        return number;
    }

    public static string ReadString(byte[] buffer, int length, ref int pointer)
    {
        byte[] bytes = new byte[length];
        Buffer.BlockCopy(buffer, pointer, bytes, 0, length);

        string str = Encoding.UTF8.GetString(bytes);
        pointer += length;
        return str;
    }

    public static byte[] ReadByteArray(byte[] buffer, int length, ref int pointer)
    {
        byte[] bytes = new byte[length];
        Buffer.BlockCopy(buffer, pointer, bytes, 0, length);        
        pointer += length;
        return bytes;
    }

    public static bool ReadBool(byte[] buffer, ref int pointer)
    {
        return (buffer[pointer - 1] & 0xf) == 1;
    }

    public static bool ReadBoolAhead(byte[] buffer, ref int pointer)
    {
        return (buffer[pointer++] & 0xf) == 1;
    }
}

