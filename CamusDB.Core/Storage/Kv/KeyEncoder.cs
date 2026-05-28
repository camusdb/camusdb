
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Order-preserving codec that turns a <see cref="CompositeColumnValue"/> into a string key for
/// Kahuna's key/value store. Kahuna keys are compared lexicographically (ordinal, UTF-16 code unit),
/// so the encoding guarantees:
///
///   sign(a.CompareTo(b)) == sign(string.CompareOrdinal(Encode(a), Encode(b)))
///
/// for any two composite values whose columns share the same type per position (the schema
/// guarantees this), with NULLs sorting before any present value.
///
/// Per-field layout:
///   NULL            -> "0"
///   present value   -> "1" + body
/// where the leading marker makes a present value ("1...") always sort after a NULL ("0").
///
/// Bodies:
///   Integer64  : 16-char uppercase hex of (ulong)value with the sign bit flipped (fixed width).
///   Float64    : 16-char uppercase hex of an order-preserving transform of the IEEE-754 bits.
///   Bool       : "0" / "1".
///   String/Id  : the raw string with U+0000 escaped, terminated by the field terminator.
///
/// Fixed-width fields (Integer64/Float64/Bool) need no terminator; the decoder knows their width
/// from the column type. Variable-length fields (String/Id) are terminated so composite keys keep
/// correct prefix ordering ("ab" sorts before "abc").
///
/// Note: <see cref="ColumnValue.CompareTo"/> has a quirk where null.CompareTo(null) returns 1 rather
/// than 0; this codec treats two NULLs as equal (the intended semantics). Property tests therefore
/// validate ordering against CompareTo using non-null operands, and validate NULL ordering separately.
/// </summary>
public static class KeyEncoder
{
    // Terminator and escape use the lowest code units so that a terminated (shorter) field sorts
    // before a field that continues with more content. Terminator = U+0000 U+0001; a real U+0000 in
    // content is escaped as U+0000 U+FFFF, which sorts after the terminator and so preserves order.
    private const char FieldTerminatorLead = (char)0x0000;
    private const char FieldTerminatorTail = (char)0x0001;
    private const char EscapeTail = (char)0xFFFF;

    private const char NullMarker = '0';
    private const char PresentMarker = '1';

    public static string Encode(CompositeColumnValue composite)
    {
        ArgumentNullException.ThrowIfNull(composite);

        StringBuilder builder = new();

        foreach (ColumnValue value in composite.Values)
            EncodeValue(builder, value);

        return builder.ToString();
    }

    public static string EncodeValue(ColumnValue value)
    {
        StringBuilder builder = new();
        EncodeValue(builder, value);
        return builder.ToString();
    }

    private static void EncodeValue(StringBuilder builder, ColumnValue value)
    {
        if (value.Type == ColumnType.Null)
        {
            builder.Append(NullMarker);
            return;
        }

        builder.Append(PresentMarker);

        switch (value.Type)
        {
            case ColumnType.Integer64:
                builder.Append(EncodeInteger64(value.LongValue));
                break;

            case ColumnType.Float64:
                builder.Append(EncodeFloat64(value.FloatValue));
                break;

            case ColumnType.Bool:
                builder.Append(value.BoolValue ? '1' : '0');
                break;

            case ColumnType.String:
            case ColumnType.Id:
                AppendString(builder, value.StrValue ?? "");
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot encode column type: " + value.Type);
        }
    }

    /// <summary>
    /// Maps a signed long to an order-preserving 16-char hex string by flipping the sign bit, so
    /// negatives sort before positives.
    /// </summary>
    private static string EncodeInteger64(long value)
    {
        ulong ordered = (ulong)value ^ 0x8000_0000_0000_0000UL;
        return ordered.ToString("X16");
    }

    /// <summary>
    /// Maps a double to an order-preserving 16-char hex string. For negatives all bits are flipped;
    /// for non-negatives only the sign bit is flipped. This yields ascending order for ascending doubles.
    /// </summary>
    private static string EncodeFloat64(double value)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);

        if (bits < 0)
            bits = ~bits;
        else
            bits ^= long.MinValue;

        return ((ulong)bits).ToString("X16");
    }

    private static void AppendString(StringBuilder builder, string value)
    {
        foreach (char c in value)
        {
            if (c == FieldTerminatorLead)
            {
                builder.Append(FieldTerminatorLead);
                builder.Append(EscapeTail);
            }
            else
            {
                builder.Append(c);
            }
        }

        builder.Append(FieldTerminatorLead);
        builder.Append(FieldTerminatorTail);
    }

    public static CompositeColumnValue Decode(string key, ColumnType[] types)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(types);

        ColumnValue[] values = new ColumnValue[types.Length];
        int pos = 0;

        for (int i = 0; i < types.Length; i++)
        {
            if (pos >= key.Length)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Key too short for {types.Length} fields at field {i}");

            char marker = key[pos++];

            if (marker == NullMarker)
            {
                values[i] = new ColumnValue(ColumnType.Null, false);
                continue;
            }

            if (marker != PresentMarker)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unexpected marker '{marker}' at position {pos - 1}");

            switch (types[i])
            {
                case ColumnType.Integer64:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    long longValue = (long)(stored ^ 0x8000_0000_0000_0000UL);
                    values[i] = new ColumnValue(ColumnType.Integer64, longValue);
                    pos += 16;
                    break;
                }

                case ColumnType.Float64:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    long storedBits = (long)stored;
                    // Reverse the order-preserving transform:
                    //   non-negatives were encoded as bits ^ long.MinValue  (high bit 1)
                    //   negatives were encoded as ~bits                     (high bit 0)
                    long originalBits = storedBits < 0
                        ? storedBits ^ long.MinValue   // was non-negative: undo sign-bit flip
                        : ~storedBits;                 // was negative: undo complement
                    values[i] = new ColumnValue(ColumnType.Float64, BitConverter.Int64BitsToDouble(originalBits));
                    pos += 16;
                    break;
                }

                case ColumnType.Bool:
                    values[i] = new ColumnValue(ColumnType.Bool, key[pos++] == '1');
                    break;

                case ColumnType.String:
                case ColumnType.Id:
                {
                    StringBuilder sb = new();
                    while (true)
                    {
                        if (pos >= key.Length)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unterminated string field at field {i}");

                        char c = key[pos++];

                        if (c != FieldTerminatorLead)
                        {
                            sb.Append(c);
                            continue;
                        }

                        if (pos >= key.Length)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unexpected end of key after U+0000 at field {i}");

                        char next = key[pos++];

                        if (next == FieldTerminatorTail)
                            break;

                        if (next == EscapeTail)
                        {
                            sb.Append(FieldTerminatorLead);
                            continue;
                        }

                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown escape U+0000 U+{(int)next:X4} at field {i}");
                    }

                    values[i] = new ColumnValue(types[i], sb.ToString());
                    break;
                }

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot decode column type: " + types[i]);
            }
        }

        return new CompositeColumnValue(values);
    }
}
