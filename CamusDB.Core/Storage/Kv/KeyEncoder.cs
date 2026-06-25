
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
///   Float32    : 8-char uppercase hex of an order-preserving transform of the single-precision bits
///                (same sign-bit / complement trick as Float64, but on 32-bit).
///   Date       : 16-char uppercase hex of the backing long (UTC ticks, midnight-aligned) via the
///                same sign-bit-flipped integer transform — inherits Integer64 ordering.
///   DateTime   : 16-char uppercase hex of the backing long (UTC ticks) via the integer transform.
///   Bytes      : each byte as 2 uppercase hex chars, terminated by the field terminator
///                (pure ASCII, prefix-correct, bytewise-order-preserving).
///   Bool       : "0" / "1".
///   String/Id  : each UTF-16 code unit as 4 uppercase hex chars (fixed width), terminated by the
///                field terminator. The output is pure ASCII, so the key's UTF-8 byte order (how the
///                RocksDB/SQLite persistence backends order keys) equals its UTF-16-ordinal order (how
///                the in-memory B-tree, range routing, and scan merge order keys). Without this the
///                two diverge for supplementary-plane characters, which would misroute/misorder a
///                key-range-routed String secondary index. Order within the field is the code-unit
///                order (matches <see cref="ColumnValue.CompareTo"/>). String and Id share this
///                encoding so a String literal in a query matches an Id-typed stored key.
///
/// Fixed-width fields (Integer64/Float64/Float32/Bool/Date/DateTime) need no terminator; the decoder
/// knows their width from the column type. Variable-length fields (String/Id/Bytes) are terminated
/// so composite keys keep correct prefix ordering.
///
/// Array is not indexable — <see cref="EncodeValue(StringBuilder,ColumnValue)"/> throws
/// <see cref="CamusDBException"/> <c>InvalidInput</c> if an Array value is passed.
///
/// Note: <see cref="ColumnValue.CompareTo"/> has a quirk where null.CompareTo(null) returns 1 rather
/// than 0; this codec treats two NULLs as equal (the intended semantics). Property tests therefore
/// validate ordering against CompareTo using non-null operands, and validate NULL ordering separately.
/// </summary>
public static class KeyEncoder
{
    // Field terminator = U+0000 U+0001. It uses the lowest code units so a terminated (shorter)
    // field sorts before a field that continues with more content ("ab" before "abc"). String/Id
    // bodies are pure hex (see AppendStringHex), so U+0000 never appears in content and no escaping
    // is needed — the terminator lead is unambiguous.
    private const char FieldTerminatorLead = (char)0x0000;
    private const char FieldTerminatorTail = (char)0x0001;

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

            case ColumnType.Float32:
                builder.Append(EncodeFloat32((float)value.FloatValue));
                break;

            case ColumnType.Date:
            case ColumnType.DateTime:
                builder.Append(EncodeInteger64(value.LongValue));
                break;

            case ColumnType.Bytes:
                AppendBytesHex(builder, value.BytesValue ?? []);
                break;

            case ColumnType.Array:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array columns are not indexable");

            // String and Id share one encoding so a String literal in a query (e.g. WHERE id IN ('…'))
            // produces the same key as the Id-typed value stored in the index — the two must stay
            // interchangeable. Both use the order-preserving ASCII-hex form.
            case ColumnType.String:
            case ColumnType.Id:
                AppendStringHex(builder, value.StrValue ?? "");
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


    /// <summary>
    /// Maps a single-precision float to an order-preserving 8-char hex string using the same
    /// sign-bit / complement trick as <see cref="EncodeFloat64"/>, but on 32-bit IEEE-754 bits.
    /// </summary>
    private static string EncodeFloat32(float value)
    {
        int bits = BitConverter.SingleToInt32Bits(value);

        if (bits < 0)
            bits = ~bits;
        else
            bits ^= int.MinValue;

        return ((uint)bits).ToString("X8");
    }

    private const string HexChars = "0123456789ABCDEF";

    /// <summary>
    /// Order-preserving <b>pure-ASCII</b> encoding for String keys: each UTF-16 code unit becomes 4
    /// uppercase hex chars (fixed width). Because every output char is ASCII ('0'-'9','A'-'F'), the
    /// key's UTF-8 byte order equals its UTF-16-ordinal order, so a String key sorts identically in
    /// the persistence backends (RocksDB bytewise / SQLite BINARY, which order by UTF-8) and the
    /// in-memory path (B-tree / range routing / scan merge, which order by UTF-16 ordinal). Fixed
    /// width per code unit preserves the code-unit order (matching <see cref="ColumnValue.CompareTo"/>),
    /// and the U+0000 U+0001 terminator — which sorts before any hex digit — preserves prefix ordering
    /// across composite fields. A literal U+0000 inside the value is content (encoded as "0000") and is
    /// never confused with the U+0000 terminator char, so no escaping is needed.
    /// </summary>
    private static void AppendStringHex(StringBuilder builder, string value)
    {
        foreach (char c in value)
        {
            builder.Append(HexChars[(c >> 12) & 0xF]);
            builder.Append(HexChars[(c >> 8) & 0xF]);
            builder.Append(HexChars[(c >> 4) & 0xF]);
            builder.Append(HexChars[c & 0xF]);
        }

        builder.Append(FieldTerminatorLead);
        builder.Append(FieldTerminatorTail);
    }

    /// <summary>
    /// Order-preserving pure-ASCII encoding for Bytes keys: each byte becomes 2 uppercase hex chars,
    /// terminated by the field terminator. Bytewise order is preserved because "AB" &lt; "AC" ordinal,
    /// and the terminator (which sorts before any hex digit) preserves prefix ordering.
    /// </summary>
    private static void AppendBytesHex(StringBuilder builder, byte[] value)
    {
        foreach (byte b in value)
        {
            builder.Append(HexChars[(b >> 4) & 0xF]);
            builder.Append(HexChars[b & 0xF]);
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

                case ColumnType.Float32:
                {
                    uint stored = uint.Parse(key.AsSpan(pos, 8), System.Globalization.NumberStyles.HexNumber);
                    int storedBits = (int)stored;
                    int originalBits = storedBits < 0
                        ? storedBits ^ int.MinValue  // was non-negative: undo sign-bit flip
                        : ~storedBits;               // was negative: undo complement
                    float floatValue = BitConverter.Int32BitsToSingle(originalBits);
                    values[i] = new ColumnValue(ColumnType.Float32, (double)floatValue);
                    pos += 8;
                    break;
                }

                case ColumnType.Date:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    long ticks = (long)(stored ^ 0x8000_0000_0000_0000UL);
                    values[i] = new ColumnValue(ColumnType.Date, ticks);
                    pos += 16;
                    break;
                }

                case ColumnType.DateTime:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    long ticks = (long)(stored ^ 0x8000_0000_0000_0000UL);
                    values[i] = new ColumnValue(ColumnType.DateTime, ticks);
                    pos += 16;
                    break;
                }

                case ColumnType.Bytes:
                {
                    List<byte> bytes = new();
                    while (true)
                    {
                        if (pos >= key.Length)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unterminated bytes field at field {i}");

                        if (key[pos] == FieldTerminatorLead)
                        {
                            pos++;
                            if (pos >= key.Length || key[pos] != FieldTerminatorTail)
                                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Malformed bytes terminator at field {i}");
                            pos++;
                            break;
                        }

                        if (pos + 2 > key.Length)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Truncated hex byte at field {i}");

                        bytes.Add(byte.Parse(key.AsSpan(pos, 2), System.Globalization.NumberStyles.HexNumber));
                        pos += 2;
                    }
                    values[i] = new ColumnValue(bytes.ToArray());
                    break;
                }

                case ColumnType.Bool:
                    values[i] = new ColumnValue(ColumnType.Bool, key[pos++] == '1');
                    break;

                case ColumnType.String:
                case ColumnType.Id:
                {
                    // Body is groups of 4 hex chars (one UTF-16 code unit each), ended by the
                    // U+0000 U+0001 terminator. A literal U+0000 (0x00) only appears as the
                    // terminator lead — never inside the hex body — so it is unambiguous.
                    StringBuilder sb = new();
                    while (true)
                    {
                        if (pos >= key.Length)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unterminated string field at field {i}");

                        if (key[pos] == FieldTerminatorLead)
                        {
                            pos++;
                            if (pos >= key.Length || key[pos] != FieldTerminatorTail)
                                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Malformed string terminator at field {i}");
                            pos++;
                            break;
                        }

                        if (pos + 4 > key.Length)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Truncated hex code unit at field {i}");

                        sb.Append((char)ushort.Parse(key.AsSpan(pos, 4), System.Globalization.NumberStyles.HexNumber, System.Globalization.CultureInfo.InvariantCulture));
                        pos += 4;
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
