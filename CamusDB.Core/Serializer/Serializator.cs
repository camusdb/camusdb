
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Util.ObjectIds;

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
        if (number is < 0 or >= 0x7FFF)
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
        BinaryPrimitives.WriteInt16LittleEndian(buffer.AsSpan(pointer), number16);
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
    public static void WriteFloat(byte[] buffer, float number, ref int pointer)
    {
        BinaryPrimitives.WriteSingleLittleEndian(buffer.AsSpan(pointer), number);
        pointer += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteDouble(byte[] buffer, double number, ref int pointer)
    {
        BinaryPrimitives.WriteDoubleLittleEndian(buffer.AsSpan(pointer), number);
        pointer += 8;
    }

    /// <summary>Writes a 4-byte length prefix followed by the raw byte payload.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteBytesPayload(byte[] buffer, byte[] bytes, ref int pointer)
    {
        int length = bytes.Length;
        buffer[pointer + 0] = (byte)((length >>  0) & 0xff);
        buffer[pointer + 1] = (byte)((length >>  8) & 0xff);
        buffer[pointer + 2] = (byte)((length >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((length >> 24) & 0xff);
        pointer += 4;
        Buffer.BlockCopy(bytes, 0, buffer, pointer, length);
        pointer += length;
    }

    /// <summary>Reads a 4-byte length prefix and the following byte payload.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] ReadBytesPayload(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        int length = ReadInt32(buffer, ref pointer);
        return ReadByteArray(buffer, length, ref pointer);
    }

    /// <summary>
    /// Advances <paramref name="pointer"/> past a 4-byte length-prefixed payload (a String or Bytes
    /// value) without decoding or copying it. Used when a column is projected out of a row: the value
    /// is discarded, so materializing a managed string or a fresh byte[] only to throw it away is pure
    /// allocation waste. Validates the length so a corrupt frame fails loudly rather than moving the
    /// cursor out of range or backwards.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SkipLengthPrefixedPayload(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        int length = ReadInt32(buffer, ref pointer);
        if (length < 0 || (long)pointer + length > buffer.Length)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Invalid length-prefixed payload length {length}");
        pointer += length;
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

    /// <summary>Writes a Uuid as its 16 raw big-endian bytes (high half then low half).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUuid(byte[] buffer, long high, long low, ref int pointer)
    {
        BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(pointer), (ulong)high);
        BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(pointer + 8), (ulong)low);
        pointer += 16;
    }

    /// <summary>Reads the 16 raw big-endian bytes written by <see cref="WriteUuid"/> as two halves.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long High, long Low) ReadUuid(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        long high = (long)BinaryPrimitives.ReadUInt64BigEndian(buffer.Slice(pointer, 8));
        long low = (long)BinaryPrimitives.ReadUInt64BigEndian(buffer.Slice(pointer + 8, 8));
        pointer += 16;
        return (high, low);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteString(byte[] buffer, string str, ref int pointer)
    {
        // Encode the UTF-16 string straight into the destination buffer (no intermediate array),
        // reserving 4 bytes for the length prefix that we backfill afterwards.
        int length = Encoding.Unicode.GetBytes(str, buffer.AsSpan(pointer + 4));

        buffer[pointer + 0] = (byte)((length >> 0) & 0xff);
        buffer[pointer + 1] = (byte)((length >> 8) & 0xff);
        buffer[pointer + 2] = (byte)((length >> 16) & 0xff);
        buffer[pointer + 3] = (byte)((length >> 24) & 0xff);

        pointer += 4 + length;
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
    public static short ReadInt16(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        short number = BinaryPrimitives.ReadInt16LittleEndian(buffer.Slice(pointer));
        pointer += 2;
        return number;
    }

    public static float ReadFloat(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        float number = BinaryPrimitives.ReadSingleLittleEndian(buffer.Slice(pointer));
        pointer += 4;
        return number;
    }

    public static double ReadDouble(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        double number = BinaryPrimitives.ReadDoubleLittleEndian(buffer.Slice(pointer));
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
    public static long ReadInt64(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        long number = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(pointer, 8));
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

    public static string ReadString(ReadOnlySpan<byte> buffer, ref int pointer)
    {
        int length = ReadInt32(buffer, ref pointer);
        if (length == 0)
            return "";

        string str = Encoding.Unicode.GetString(buffer.Slice(pointer, length));
        pointer += length;
        return str;
    }

    public static byte[] ReadByteArray(ReadOnlySpan<byte> buffer, int length, ref int pointer)
    {
        byte[] bytes = buffer.Slice(pointer, length).ToArray();
        pointer += length;
        return bytes;
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
