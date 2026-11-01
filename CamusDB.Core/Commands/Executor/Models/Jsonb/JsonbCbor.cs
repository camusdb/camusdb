/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Numerics;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// The kind of a JSON value. The numeric order is the PostgreSQL <c>jsonb</c> sort order of kinds:
/// <c>Null &lt; String &lt; Number &lt; Boolean &lt; Array &lt; Object</c>.
/// </summary>
internal enum JsonbKind : byte
{
    Null = 0,
    String = 1,
    Number = 2,
    Boolean = 3,
    Array = 4,
    Object = 5,
}

/// <summary>
/// Reads the subset of CBOR (RFC 8949) that <see cref="JsonbEncoder"/> writes, straight from a span,
/// with no allocation. <c>System.Formats.Cbor</c> writes every document; this reader exists because
/// the operators run once per row and its <c>CborReader</c> is a class that allocates per call.
///
/// <para>The subset: unsigned and negative integers (major types 0 and 1), text strings (3), arrays
/// (4), maps with text keys (5), the bignum tags 2 and 3 over a byte string, the decimal-fraction tag
/// 4 over <c>[exponent, mantissa]</c>, and the simple values false, true and null. Lengths are always
/// definite. Anything else is <see cref="CamusDBErrorCodes.JsonbValueCorrupt"/>.</para>
///
/// <para>Every position is an offset into the CBOR item, without the format-version byte of the
/// cell. <see cref="SkipItem"/> is iterative: it keeps a count of the items still to read, so a deep
/// document cannot overflow the stack.</para>
/// </summary>
internal static class JsonbCbor
{
    public const int MajorUnsigned = 0;
    public const int MajorNegative = 1;
    public const int MajorBytes = 2;
    public const int MajorText = 3;
    public const int MajorArray = 4;
    public const int MajorMap = 5;
    public const int MajorTag = 6;
    public const int MajorSimple = 7;

    public const ulong TagPositiveBignum = 2;
    public const ulong TagNegativeBignum = 3;
    public const ulong TagDecimalFraction = 4;

    public const ulong SimpleFalse = 20;
    public const ulong SimpleTrue = 21;
    public const ulong SimpleNull = 22;

    public static CamusDBException Corrupt(string detail) =>
        new(CamusDBErrorCodes.JsonbValueCorrupt, "Corrupt jsonb value: " + detail);

    /// <summary>Reads one item header at <paramref name="pos"/> and moves past it.</summary>
    public static void ReadHeader(ReadOnlySpan<byte> data, ref int pos, out int major, out ulong argument)
    {
        if ((uint)pos >= (uint)data.Length)
            throw Corrupt("truncated item");

        byte initial = data[pos++];
        major = initial >> 5;
        int additional = initial & 0x1F;

        switch (additional)
        {
            case < 24:
                argument = (ulong)additional;
                return;

            case 24:
                Need(data, pos, 1);
                argument = data[pos];
                pos += 1;
                return;

            case 25:
                Need(data, pos, 2);
                argument = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(pos, 2));
                pos += 2;
                return;

            case 26:
                Need(data, pos, 4);
                argument = BinaryPrimitives.ReadUInt32BigEndian(data.Slice(pos, 4));
                pos += 4;
                return;

            case 27:
                Need(data, pos, 8);
                argument = BinaryPrimitives.ReadUInt64BigEndian(data.Slice(pos, 8));
                pos += 8;
                return;

            default:
                throw Corrupt("indefinite or reserved length");
        }
    }

    private static void Need(ReadOnlySpan<byte> data, int pos, int count)
    {
        if (data.Length - pos < count)
            throw Corrupt("truncated header");
    }

    /// <summary>A length or count from a header, checked against the bytes that remain.</summary>
    public static int CheckedLength(ReadOnlySpan<byte> data, int pos, ulong argument)
    {
        if (argument > (ulong)(data.Length - pos))
            throw Corrupt("length past the end of the value");
        return (int)argument;
    }

    /// <summary>The kind of the item at <paramref name="pos"/>, read from its first byte.</summary>
    public static JsonbKind KindAt(ReadOnlySpan<byte> data, int pos)
    {
        if ((uint)pos >= (uint)data.Length)
            throw Corrupt("truncated item");

        byte initial = data[pos];
        switch (initial >> 5)
        {
            case MajorUnsigned:
            case MajorNegative:
            case MajorTag:
                return JsonbKind.Number;

            case MajorText:
                return JsonbKind.String;

            case MajorArray:
                return JsonbKind.Array;

            case MajorMap:
                return JsonbKind.Object;

            case MajorSimple:
                return (ulong)(initial & 0x1F) switch
                {
                    SimpleFalse or SimpleTrue => JsonbKind.Boolean,
                    SimpleNull => JsonbKind.Null,
                    _ => throw Corrupt("unknown simple value"),
                };

            default:
                throw Corrupt("unexpected byte string");
        }
    }

    /// <summary>Returns the position just past the item that starts at <paramref name="pos"/>.</summary>
    public static int SkipItem(ReadOnlySpan<byte> data, int pos)
    {
        long pending = 1;
        while (pending > 0)
        {
            ReadHeader(data, ref pos, out int major, out ulong argument);
            pending--;

            switch (major)
            {
                case MajorBytes:
                case MajorText:
                    pos += CheckedLength(data, pos, argument);
                    break;

                case MajorArray:
                    pending += CheckedLength(data, pos, argument);
                    break;

                case MajorMap:
                    pending += 2L * CheckedLength(data, pos, argument);
                    break;

                case MajorTag:
                    pending++;
                    break;
            }
        }

        return pos;
    }

    /// <summary>Reads a container header at <paramref name="pos"/>: its kind and its element or pair count.</summary>
    public static int ReadContainerHeader(ReadOnlySpan<byte> data, ref int pos, out bool isObject)
    {
        ReadHeader(data, ref pos, out int major, out ulong argument);
        if (major is not (MajorArray or MajorMap))
            throw Corrupt("expected an array or an object");

        isObject = major == MajorMap;
        int count = CheckedLength(data, pos, argument);
        return count;
    }

    /// <summary>Reads a text item at <paramref name="pos"/> and returns its UTF-8 bytes.</summary>
    public static ReadOnlySpan<byte> ReadText(ReadOnlySpan<byte> data, scoped ref int pos)
    {
        ReadHeader(data, ref pos, out int major, out ulong argument);
        if (major != MajorText)
            throw Corrupt("expected a string");

        int length = CheckedLength(data, pos, argument);
        ReadOnlySpan<byte> text = data.Slice(pos, length);
        pos += length;
        return text;
    }

    /// <summary>Reads a boolean item at <paramref name="pos"/>.</summary>
    public static bool ReadBoolean(ReadOnlySpan<byte> data, ref int pos)
    {
        ReadHeader(data, ref pos, out int major, out ulong argument);
        if (major != MajorSimple || argument is not (SimpleFalse or SimpleTrue))
            throw Corrupt("expected a boolean");
        return argument == SimpleTrue;
    }

    /// <summary>
    /// True when the number item at <paramref name="pos"/> is a plain CBOR integer that fits a long,
    /// the fast path that needs no <see cref="BigInteger"/>.
    /// </summary>
    public static bool TryReadSmallInteger(ReadOnlySpan<byte> data, int pos, out long value, out int end)
    {
        int cursor = pos;
        ReadHeader(data, ref cursor, out int major, out ulong argument);
        end = cursor;

        if (major == MajorUnsigned && argument <= long.MaxValue)
        {
            value = (long)argument;
            return true;
        }

        if (major == MajorNegative && argument <= long.MaxValue)
        {
            value = -1 - (long)argument;
            return true;
        }

        value = 0;
        return false;
    }

    /// <summary>Reads any number item at <paramref name="pos"/>.</summary>
    public static JsonbNumber ReadNumber(ReadOnlySpan<byte> data, ref int pos)
    {
        ReadHeader(data, ref pos, out int major, out ulong argument);

        switch (major)
        {
            case MajorUnsigned:
                return new JsonbNumber(argument, 0);

            case MajorNegative:
                return new JsonbNumber(BigInteger.MinusOne - argument, 0);

            case MajorTag when argument is TagPositiveBignum or TagNegativeBignum:
                return new JsonbNumber(ReadBignumBody(data, ref pos, argument), 0);

            case MajorTag when argument == TagDecimalFraction:
            {
                ReadHeader(data, ref pos, out int arrayMajor, out ulong arrayCount);
                if (arrayMajor != MajorArray || arrayCount != 2)
                    throw Corrupt("malformed decimal fraction");

                ReadHeader(data, ref pos, out int exponentMajor, out ulong exponentArgument);
                if (exponentMajor != MajorNegative || exponentArgument >= JsonbLimits.MaxScale)
                    throw Corrupt("decimal fraction exponent");

                int scale = (int)exponentArgument + 1;
                BigInteger mantissa = ReadIntegerOrBignum(data, ref pos);
                return new JsonbNumber(mantissa, scale);
            }

            default:
                throw Corrupt("expected a number");
        }
    }

    private static BigInteger ReadIntegerOrBignum(ReadOnlySpan<byte> data, ref int pos)
    {
        ReadHeader(data, ref pos, out int major, out ulong argument);
        return major switch
        {
            MajorUnsigned => argument,
            MajorNegative => BigInteger.MinusOne - argument,
            MajorTag when argument is TagPositiveBignum or TagNegativeBignum => ReadBignumBody(data, ref pos, argument),
            _ => throw Corrupt("decimal fraction mantissa"),
        };
    }

    private static BigInteger ReadBignumBody(ReadOnlySpan<byte> data, ref int pos, ulong tag)
    {
        ReadHeader(data, ref pos, out int major, out ulong argument);
        if (major != MajorBytes)
            throw Corrupt("bignum without a byte string");

        int length = CheckedLength(data, pos, argument);
        BigInteger magnitude = new(data.Slice(pos, length), isUnsigned: true, isBigEndian: true);
        pos += length;
        return tag == TagPositiveBignum ? magnitude : BigInteger.MinusOne - magnitude;
    }
}
