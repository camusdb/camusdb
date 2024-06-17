
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using System.Runtime.CompilerServices;

using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.Serializer;

/// <summary>
/// Utility class for serializing and deserializing all kinds of data to and from a buffer.
///
/// The goal is for the serialization to be very fast and for most methods should be inlined where they are called.
/// </summary>
public sealed class Serializator
{
    public static byte[] Serialize<T>(T tableSchema)
    {
        string jsonSerialized = JsonSerializer.Serialize(tableSchema);
        return Encoding.Unicode.GetBytes(jsonSerialized);
    }

    public static T Unserialize<T>(ReadOnlySpan<byte> buffer) where T : new()
    {
        string str = Encoding.Unicode.GetString(buffer);

        T? deserialized = JsonSerializer.Deserialize<T>(str);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt8(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(1);        
        buffer[pointer++] = (byte)((number >> 0) & 0xff);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt16(byte[] buffer, int number, ref int pointer)
    {
        short number16 = Convert.ToInt16(number);
        byte[] byteArray = BitConverter.GetBytes(number16);
        buffer[pointer + 0] = byteArray[0];
        buffer[pointer + 1] = byteArray[1];
        pointer += 2;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt32(byte[] buffer, int number, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((number >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((number >> 24) & 0xff);
        pointer += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt64(byte[] buffer, long number, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((number >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((number >> 24) & 0xff);
        buffer[pointer + 4] = (byte)((number >> 32) & 0xff);
        buffer[pointer + 5] = (byte)((number >> 40) & 0xff);
        buffer[pointer + 6] = (byte)((number >> 48) & 0xff);
        buffer[pointer + 7] = (byte)((number >> 56) & 0xff);
        pointer += 8;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt32(byte[] buffer, uint number, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((number >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((number >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((number >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((number >> 24) & 0xff);
        pointer += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteDouble(byte[] buffer, double number, ref int pointer)
    {
        byte[] byteArray = BitConverter.GetBytes(number);
        buffer[pointer + 0] = byteArray[0];
        buffer[pointer + 1] = byteArray[1];
        buffer[pointer + 2] = byteArray[2];
        buffer[pointer + 3] = byteArray[3];
        buffer[pointer + 4] = byteArray[4];
        buffer[pointer + 5] = byteArray[5];
        buffer[pointer + 6] = byteArray[6];
        buffer[pointer + 7] = byteArray[7];
        pointer += 8;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteObjectId(byte[] buffer, ObjectIdValue id, ref int pointer)
    {
        //CheckBufferOverflow(4);
        buffer[pointer + 0] = (byte)((id.a >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((id.a >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((id.a >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((id.a >> 24) & 0xff);

        buffer[pointer + 4] = (byte)((id.b >> 0) & 0xff);
        buffer[pointer + 5] = (byte)((id.b >> 8) & 0xff);
        buffer[pointer + 6] = (byte)((id.b >> 16) & 0xff);
        buffer[pointer + 7] = (byte)((id.b >> 24) & 0xff);

        buffer[pointer + 8] = (byte)((id.c >> 0) & 0xff);
        buffer[pointer + 9] = (byte)((id.c >> 8) & 0xff);
        buffer[pointer + 10] = (byte)((id.c >> 16) & 0xff);
        buffer[pointer + 11] = (byte)((id.c >> 24) & 0xff);

        pointer += 12;
    }

    public static void WriteHLCTimestamp(byte[] buffer, HLCTimestamp timestamp, ref int pointer)
    {
        long physicalTime = timestamp.L;
        uint counter = timestamp.C;

        buffer[pointer + 0] = (byte)((physicalTime >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((physicalTime >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((physicalTime >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((physicalTime >> 24) & 0xff);
        buffer[pointer + 4] = (byte)((physicalTime >> 32) & 0xff);
        buffer[pointer + 5] = (byte)((physicalTime >> 40) & 0xff);
        buffer[pointer + 6] = (byte)((physicalTime >> 48) & 0xff);
        buffer[pointer + 7] = (byte)((physicalTime >> 56) & 0xff);
        pointer += 8;

        buffer[pointer + 0] = (byte)((counter >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((counter >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((counter >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((counter >> 24) & 0xff);
        pointer += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteString(byte[] buffer, string str, ref int pointer)
    {
        byte[] bytes = Encoding.Unicode.GetBytes(str);

        int length = bytes.Length;
        buffer[pointer + 0] = (byte)((length >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((length >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((length >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((length >> 24) & 0xff);
        pointer += 4;

        Buffer.BlockCopy(bytes, 0, buffer, pointer, bytes.Length);
        pointer += bytes.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteByteArray(byte[] buffer, byte[] bytes, ref int pointer)
    {
        Buffer.BlockCopy(bytes, 0, buffer, pointer, bytes.Length);
        pointer += bytes.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteBool(byte[] buffer, bool value, ref int pointer)
    {
        int typedBool = 0;
        int bool8 = ((value ? 1 : 0) >> 0) & 0xff;
        typedBool = (typedBool & 0xf) | (SerializatorTypes.TypeBool << 4);
        typedBool = (typedBool & 0xf0) | bool8;
        WriteInt8(buffer, typedBool, ref pointer);
    }

    public static void WriteTuple(byte[] buffer, BTreeTuple rowTuple, ref int pointer)
    {
        WriteObjectId(buffer, rowTuple.SlotOne, ref pointer);
        WriteObjectId(buffer, rowTuple.SlotTwo, ref pointer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadType(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        int typeByte = buffer[pointer++];
        int type = (typeByte & 0xf0) >> 4;
        if (type == SerializatorTypes.TypeExtended)
            return (typeByte & 0xf) + 0xf;
        return type;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt8(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        return buffer[pointer++];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static short ReadInt16(byte[] buffer, ref int pointer)
    {
        short number = BitConverter.ToInt16(buffer, pointer);
        pointer += 2;
        return number;
    }

    public static float ReadFloat(byte[] buffer, ref int pointer)
    {
        float number = BitConverter.ToSingle(buffer, pointer);
        pointer += 4;
        return number;
    }

    public static double ReadDouble(byte[] buffer, ref int pointer)
    {
        double number = BitConverter.ToDouble(buffer, pointer);
        pointer += 8;
        return number;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        int number = buffer[pointer];
        number += (buffer[pointer + 1] << 8);
        number += (buffer[pointer + 2] << 16);
        number += (buffer[pointer + 3] << 24);
        pointer += 4;
        return number;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64(byte[] buffer, ref int pointer)
    {
        long number = BitConverter.ToInt64(buffer, pointer);
        pointer += 8;
        return number;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ReadUInt32(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        uint number = buffer[pointer];
        number += (uint)(buffer[pointer + 1] << 8);
        number += (uint)(buffer[pointer + 2] << 16);
        number += (uint)(buffer[pointer + 3] << 24);
        pointer += 4;
        return number;
    }

    public static ObjectIdValue ReadObjectId(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        int a = buffer[pointer++];
        a += (buffer[pointer++] << 8);
        a += (buffer[pointer++] << 16);
        a += (buffer[pointer++] << 24);

        int b = buffer[pointer++];
        b += (buffer[pointer++] << 8);
        b += (buffer[pointer++] << 16);
        b += (buffer[pointer++] << 24);

        int c = buffer[pointer++];
        c += (buffer[pointer++] << 8);
        c += (buffer[pointer++] << 16);
        c += (buffer[pointer++] << 24);

        return new ObjectIdValue(a, b, c);
    }

    public static string ReadString(byte[] buffer, ref int pointer)
    {
        int length = ReadInt32(buffer, ref pointer);
        if (length == 0)
            return "";

        byte[] bytes = new byte[length];
        Buffer.BlockCopy(buffer, pointer, bytes, 0, length);

        string str = Encoding.Unicode.GetString(bytes);
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

    public static HLCTimestamp ReadHLCTimestamp(byte[] buffer, ref int pointer)
    {
        long pt = BitConverter.ToInt64(buffer, pointer);
        pointer += 8;

        uint counter = buffer[pointer];
        counter += (uint)(buffer[pointer + 1] << 8);
        counter += (uint)(buffer[pointer + 2] << 16);
        counter += (uint)(buffer[pointer + 3] << 24);
        pointer += 4;

        return new HLCTimestamp(pt, counter);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ReadBool(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        return (buffer[pointer - 1] & 0xf) == 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ReadBoolAhead(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        return (buffer[pointer++] & 0xf) == 1;
    }
}
