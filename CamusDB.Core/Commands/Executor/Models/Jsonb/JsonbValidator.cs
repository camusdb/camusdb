/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Text.Unicode;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Checks that bytes from outside the engine are a <c>jsonb</c> cell that <see cref="JsonbEncoder"/>
/// could have written, byte for byte. The engine trusts its own cells: the readers take shortcuts
/// that only canonical bytes make correct. <see cref="JsonbDocument.TryGetField"/> stops at the first
/// key that sorts after the one it looks for, equality compares bytes first, and the recursive walks
/// are safe only because no document nests deeper than <see cref="JsonbLimits.MaxNestingDepth"/>. A
/// cell that breaks one of these rules is not "a different spelling" of a document. It is a value
/// that reads back wrong (a key that <c>-&gt;&gt;</c> cannot find) or that can overflow the stack.
///
/// <para>So every path that accepts cell bytes from a client or a peer calls
/// <see cref="ValidateCell"/> before the bytes become a <see cref="ColumnValue"/>. The rules are the
/// exact output of the encoder:</para>
/// <list type="bullet">
/// <item>The format-version byte, then exactly one CBOR item and no trailing bytes.</item>
/// <item>Every header in its shortest form, with definite lengths.</item>
/// <item>Text is valid UTF-8 without U+0000, so a lone surrogate is refused too.</item>
/// <item>Map keys are text, unique, and sorted shorter first, then by bytes.</item>
/// <item>An integer in the int64 range is a plain CBOR integer. An integer outside it is a bignum
/// (tag 2 or 3) whose magnitude has no leading zero byte. Any other number is a decimal fraction
/// (tag 4) <c>[-scale, mantissa]</c> with a scale from 1 to <see cref="JsonbLimits.MaxScale"/>, and
/// no number has more than <see cref="JsonbLimits.MaxIntegerDigits"/> digits before the point.</item>
/// <item>The only simple values are false, true and null. Byte strings, floats and other tags are
/// refused.</item>
/// <item>No more than <see cref="JsonbLimits.MaxNestingDepth"/> nested arrays and objects.</item>
/// </list>
///
/// <para>The walk is iterative, with an explicit stack, so a deep or hostile value cannot overflow
/// the stack while it is checked. It allocates nothing for a normal document; only a bignum near the
/// digit limit is converted to a <see cref="System.Numerics.BigInteger"/>.</para>
/// </summary>
internal static class JsonbValidator
{
    /// <summary>
    /// Returns <paramref name="cell"/> when it is a canonical cell. Otherwise throws
    /// <paramref name="errorCode"/> (or <see cref="CamusDBErrorCodes.JsonNestingTooDeep"/> past the
    /// nesting limit). A client boundary passes <see cref="CamusDBErrorCodes.InvalidJsonText"/>, so a
    /// bad parameter is a 400; a peer boundary passes <see cref="CamusDBErrorCodes.JsonbValueCorrupt"/>.
    /// </summary>
    public static byte[] ValidateCell(byte[]? cell, string errorCode)
    {
        if (cell is null || cell.Length < 2)
            throw Fail(errorCode, "empty cell");

        if (cell[0] != JsonbLimits.FormatVersion)
            throw Fail(errorCode, $"unknown format version {cell[0]}");

        ValidateItem(cell.AsSpan(1), errorCode);
        return cell;
    }

    private struct Frame
    {
        /// <summary>Items still to read: elements of an array, or keys and values of an object.</summary>
        public long Remaining;

        public bool IsObject;

        /// <summary>The offset and length of the previous key of an object, or -1 before the first key.</summary>
        public int PreviousKeyStart;

        public int PreviousKeyLength;
    }

    /// <summary>Checks one CBOR item (a cell without its version byte). See the class summary for the rules.</summary>
    public static void ValidateItem(ReadOnlySpan<byte> item, string errorCode)
    {
        Frame[]? stack = null;
        int depth = 0;
        int pos = 0;
        bool rootRead = false;

        while (true)
        {
            bool isKey = false;
            if (depth == 0)
            {
                if (rootRead)
                    break;
                rootRead = true;
            }
            else
            {
                ref Frame frame = ref stack![depth - 1];
                if (frame.Remaining == 0)
                {
                    depth--;
                    continue;
                }

                isKey = frame.IsObject && frame.Remaining % 2 == 0;
                frame.Remaining--;
            }

            if (isKey)
            {
                ref Frame frame = ref stack![depth - 1];
                ReadHeader(item, ref pos, out int major, out ulong argument, errorCode);
                if (major != JsonbCbor.MajorText)
                    throw Fail(errorCode, "an object key is not a string");

                int length = Length(item, pos, argument, errorCode);
                ReadOnlySpan<byte> key = item.Slice(pos, length);
                CheckText(key, errorCode);

                if (frame.PreviousKeyStart >= 0)
                {
                    ReadOnlySpan<byte> previous = item.Slice(frame.PreviousKeyStart, frame.PreviousKeyLength);
                    int order = previous.Length != key.Length
                        ? previous.Length.CompareTo(key.Length)
                        : previous.SequenceCompareTo(key);
                    if (order >= 0)
                        throw Fail(errorCode, order == 0 ? "duplicate object key" : "object keys are not sorted");
                }

                frame.PreviousKeyStart = pos;
                frame.PreviousKeyLength = length;
                pos += length;
                continue;
            }

            ReadHeader(item, ref pos, out int itemMajor, out ulong itemArgument, errorCode);
            switch (itemMajor)
            {
                case JsonbCbor.MajorUnsigned:
                case JsonbCbor.MajorNegative:
                    if (itemArgument > long.MaxValue)
                        throw Fail(errorCode, "an integer outside the int64 range is not a bignum");
                    break;

                case JsonbCbor.MajorText:
                {
                    int length = Length(item, pos, itemArgument, errorCode);
                    CheckText(item.Slice(pos, length), errorCode);
                    pos += length;
                    break;
                }

                case JsonbCbor.MajorArray:
                case JsonbCbor.MajorMap:
                {
                    if (depth >= JsonbLimits.MaxNestingDepth)
                        throw JsonbEncoder.TooDeep();

                    bool isObject = itemMajor == JsonbCbor.MajorMap;
                    int count = Length(item, pos, itemArgument, errorCode);
                    stack ??= new Frame[8];
                    if (depth == stack.Length)
                        Array.Resize(ref stack, Math.Min(stack.Length * 2, JsonbLimits.MaxNestingDepth));

                    stack[depth++] = new Frame
                    {
                        Remaining = isObject ? 2L * count : count,
                        IsObject = isObject,
                        PreviousKeyStart = -1,
                    };
                    break;
                }

                case JsonbCbor.MajorTag:
                    ValidateTaggedNumber(item, ref pos, itemArgument, errorCode);
                    break;

                case JsonbCbor.MajorSimple:
                    if (itemArgument is not (JsonbCbor.SimpleFalse or JsonbCbor.SimpleTrue or JsonbCbor.SimpleNull))
                        throw Fail(errorCode, "a float or an unknown simple value");
                    break;

                default:
                    throw Fail(errorCode, "a byte string outside a bignum");
            }

        }

        if (pos != item.Length)
            throw Fail(errorCode, "trailing bytes after the value");
    }

    /// <summary>A bignum (tags 2 and 3) or a decimal fraction (tag 4), in the encoder's form.</summary>
    private static void ValidateTaggedNumber(ReadOnlySpan<byte> item, ref int pos, ulong tag, string errorCode)
    {
        switch (tag)
        {
            case JsonbCbor.TagPositiveBignum:
            case JsonbCbor.TagNegativeBignum:
                ValidateBignumBody(item, ref pos, scale: 0, errorCode);
                return;

            case JsonbCbor.TagDecimalFraction:
            {
                ReadHeader(item, ref pos, out int arrayMajor, out ulong arrayCount, errorCode);
                if (arrayMajor != JsonbCbor.MajorArray || arrayCount != 2)
                    throw Fail(errorCode, "malformed decimal fraction");

                // The encoder writes -scale with scale >= 1, so the exponent is a negative integer.
                ReadHeader(item, ref pos, out int exponentMajor, out ulong exponentArgument, errorCode);
                if (exponentMajor != JsonbCbor.MajorNegative || exponentArgument >= JsonbLimits.MaxScale)
                    throw Fail(errorCode, "decimal fraction exponent");

                int scale = (int)exponentArgument + 1;

                ReadHeader(item, ref pos, out int mantissaMajor, out ulong mantissaArgument, errorCode);
                switch (mantissaMajor)
                {
                    case JsonbCbor.MajorUnsigned:
                    case JsonbCbor.MajorNegative:
                        if (mantissaArgument > long.MaxValue)
                            throw Fail(errorCode, "an integer outside the int64 range is not a bignum");
                        return;

                    case JsonbCbor.MajorTag when mantissaArgument is JsonbCbor.TagPositiveBignum or JsonbCbor.TagNegativeBignum:
                        ValidateBignumBody(item, ref pos, scale, errorCode);
                        return;

                    default:
                        throw Fail(errorCode, "decimal fraction mantissa");
                }
            }

            default:
                throw Fail(errorCode, $"unsupported tag {tag}");
        }
    }

    /// <summary>
    /// The byte string of a bignum. The encoder writes one only for a value outside the int64 range,
    /// with the shortest big-endian magnitude, so the magnitude is at least 2^63 and has no leading zero.
    /// </summary>
    private static void ValidateBignumBody(ReadOnlySpan<byte> item, ref int pos, int scale, string errorCode)
    {
        ReadHeader(item, ref pos, out int major, out ulong argument, errorCode);
        if (major != JsonbCbor.MajorBytes)
            throw Fail(errorCode, "bignum without a byte string");

        int length = Length(item, pos, argument, errorCode);
        ReadOnlySpan<byte> magnitude = item.Slice(pos, length);
        pos += length;

        if (length < 8 || magnitude[0] == 0 || (length == 8 && magnitude[0] < 0x80))
            throw Fail(errorCode, "a bignum inside the int64 range or with a leading zero");

        CheckIntegerDigits(magnitude, scale, errorCode);
    }

    /// <summary>
    /// Refuses a number with more than <see cref="JsonbLimits.MaxIntegerDigits"/> digits before the
    /// point, the limit <see cref="JsonbNumber.ParseJsonText"/> applies to text. The byte length gives
    /// bounds on the digit count, so the exact check runs only for a magnitude near the limit.
    /// </summary>
    private static void CheckIntegerDigits(ReadOnlySpan<byte> magnitude, int scale, string errorCode)
    {
        const double DigitsPerByte = 2.408239965311849; // 8 * log10(2)
        int limitDigits = JsonbLimits.MaxIntegerDigits + scale;

        // A magnitude of L bytes (no leading zero) is in [256^(L-1), 256^L).
        if (magnitude.Length * DigitsPerByte <= limitDigits)
            return;

        if ((magnitude.Length - 1) * DigitsPerByte > limitDigits + 1)
            throw Fail(errorCode, "value overflows numeric format");

        System.Numerics.BigInteger value = new(magnitude, isUnsigned: true, isBigEndian: true);
        if (value >= System.Numerics.BigInteger.Pow(10, limitDigits))
            throw Fail(errorCode, "value overflows numeric format");
    }

    private static void CheckText(ReadOnlySpan<byte> utf8, string errorCode)
    {
        if (!Utf8.IsValid(utf8))
            throw Fail(errorCode, "a string is not valid UTF-8");

        if (utf8.IndexOf((byte)0) >= 0)
            throw Fail(errorCode, "a string holds U+0000");
    }

    /// <summary>Reads one header and refuses any form but the shortest one.</summary>
    private static void ReadHeader(ReadOnlySpan<byte> data, ref int pos, out int major, out ulong argument, string errorCode)
    {
        if ((uint)pos >= (uint)data.Length)
            throw Fail(errorCode, "truncated item");

        byte initial = data[pos++];
        major = initial >> 5;
        int additional = initial & 0x1F;

        switch (additional)
        {
            case < 24:
                argument = (ulong)additional;
                return;

            case 24:
                Need(data, pos, 1, errorCode);
                argument = data[pos];
                pos += 1;
                if (argument < 24)
                    break;
                return;

            case 25:
                Need(data, pos, 2, errorCode);
                argument = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(pos, 2));
                pos += 2;
                if (argument <= byte.MaxValue)
                    break;
                return;

            case 26:
                Need(data, pos, 4, errorCode);
                argument = BinaryPrimitives.ReadUInt32BigEndian(data.Slice(pos, 4));
                pos += 4;
                if (argument <= ushort.MaxValue)
                    break;
                return;

            case 27:
                Need(data, pos, 8, errorCode);
                argument = BinaryPrimitives.ReadUInt64BigEndian(data.Slice(pos, 8));
                pos += 8;
                if (argument <= uint.MaxValue)
                    break;
                return;

            default:
                throw Fail(errorCode, "indefinite or reserved length");
        }

        // A float uses the 2, 4 and 8 byte forms with any bit pattern. It is refused as a simple value.
        if (major == JsonbCbor.MajorSimple)
        {
            argument = ulong.MaxValue;
            return;
        }

        throw Fail(errorCode, "a header is not in its shortest form");
    }

    private static void Need(ReadOnlySpan<byte> data, int pos, int count, string errorCode)
    {
        if (data.Length - pos < count)
            throw Fail(errorCode, "truncated header");
    }

    private static int Length(ReadOnlySpan<byte> data, int pos, ulong argument, string errorCode)
    {
        if (argument > (ulong)(data.Length - pos))
            throw Fail(errorCode, "length past the end of the value");
        return (int)argument;
    }

    private static CamusDBException Fail(string errorCode, string detail) =>
        new(errorCode, "invalid binary jsonb value: " + detail);
}
