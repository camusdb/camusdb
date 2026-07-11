
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
///   Uuid       : the 128-bit value as a fixed 19-char, most-significant-first base-125 number over
///                the same '/'-skipping ordered alphabet as String/Id. Fixed width (no terminator);
///                125^19 > 2^128 so 19 digits cover every value, and MSB-first fixed-width base-N of
///                an unsigned integer is order-preserving, matching <see cref="ColumnValue.CompareTo"/>
///                (unsigned big-endian). At 19 code units it is ~half the size of a UUID stored as a
///                String key.
///   Bool       : "0" / "1".
///   String/Id  : prefix-free ordered ASCII code over UTF-16 code units, terminated by the field
///                terminator. Printable ASCII uses one character, controls use two, and all other
///                code units use three. The alphabet excludes '/' so encoded content cannot change
///                Kahuna's last-slash key-space boundary. Pure ASCII makes the key's UTF-8 byte order
///                (RocksDB/SQLite) equal its UTF-16-ordinal order (in-memory B-tree, range routing,
///                and scan merge). Order within the field matches <see cref="ColumnValue.CompareTo"/>.
///                String and Id share this encoding so a String literal in a query matches an
///                Id-typed stored key.
///
/// Fixed-width fields (Integer64/Float64/Float32/Bool/Date/DateTime/Uuid) need no terminator; the
/// decoder knows their width from the column type. Variable-length fields (String/Id/Bytes) are
/// terminated so composite keys keep correct prefix ordering.
///
/// Array is not indexable — encoding throws <see cref="CamusDBException"/> <c>InvalidInput</c>
/// if an Array value is passed.
///
/// Note: <see cref="ColumnValue.CompareTo"/> has a quirk where null.CompareTo(null) returns 1 rather
/// than 0; this codec treats two NULLs as equal (the intended semantics). Property tests therefore
/// validate ordering against CompareTo using non-null operands, and validate NULL ordering separately.
/// </summary>
public static class KeyEncoder
{
    // Field terminator = U+0000 U+0001. It uses the lowest code units so a terminated (shorter)
    // field sorts before a field that continues with more content ("ab" before "abc"). String/Id
    // bodies use only the ordered alphabet U+0002..U+007F (excluding '/'), so U+0000 never appears
    // in content and the terminator lead is unambiguous.
    private const char FieldTerminatorLead = (char)0x0000;
    private const char FieldTerminatorTail = (char)0x0001;

    private const char NullMarker = '0';
    private const char PresentMarker = '1';

    // Ordered String/Id alphabet: ASCII U+0002..U+007F excluding '/' (125 symbols). Excluding '/'
    // is required because Kahuna derives a key space from everything before the last slash.
    private const int StringAlphabetSize = 125;
    private const int StringSlashRank = '/' - 2;
    private const int StringDirectFirstRank = 1;
    private const int StringDirectLastRank = 95;
    private const int StringHighFirstRank = 96;
    private const int StringHighLastRank = 100;
    private const int StringHighFirstCodeUnit = 127;
    private const int StringHighBlockSize = StringAlphabetSize * StringAlphabetSize;

    // A 128-bit Uuid encodes to a fixed-width base-125 number: ceil(128 / log2(125)) = 19 digits.
    // 125^19 > 2^128, so every value fits and the width is constant (no terminator needed).
    private const int UuidKeyDigits = 19;

    public static string Encode(CompositeColumnValue composite)
    {
        ArgumentNullException.ThrowIfNull(composite);

        int length = MeasureComposite(composite);

        // Write straight into the new string's backing buffer: one allocation, no StringBuilder and
        // no per-field ToString("X…") temporaries. The destination span IS the final string, so the
        // large-value case (String/Id/Bytes can expand to millions of chars) is heap-sized exactly —
        // no stackalloc size ceiling applies.
        return string.Create(length, composite, static (span, state) =>
        {
            int pos = 0;
            foreach (ColumnValue value in state.Values)
                WriteValue(span, ref pos, value);
        });
    }

    public static string EncodeValue(ColumnValue value)
    {
        int length = MeasureValue(value);
        return string.Create(length, value, static (span, v) =>
        {
            int pos = 0;
            WriteValue(span, ref pos, v);
        });
    }

    // -- length measurement (must mirror WriteValue's layout exactly) -----------------------------

    private static int MeasureComposite(CompositeColumnValue composite)
    {
        int total = 0;
        foreach (ColumnValue value in composite.Values)
            total += MeasureValue(value);
        return total;
    }

    private static int MeasureValue(ColumnValue value)
    {
        if (value.Type == ColumnType.Null)
            return 1; // NullMarker only

        // 1 for the PresentMarker + the body width.
        return value.Type switch
        {
            ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Date or ColumnType.DateTime => 1 + 16,
            ColumnType.Float32 => 1 + 8,
            ColumnType.Bool => 1 + 1,
            ColumnType.Uuid => 1 + UuidKeyDigits,
            ColumnType.Bytes => 1 + 2 * (value.BytesValue?.Length ?? 0) + 2,            // 2 hex/byte + terminator
            ColumnType.String or ColumnType.Id => 1 + MeasureString(value.StrValue ?? "") + 2,
            ColumnType.Array => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array columns are not indexable"),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot encode column type: " + value.Type),
        };
    }

    /// <summary>Measures the variable-width String/Id body and must mirror <see cref="WriteStringOrderedAscii"/>.</summary>
    private static int MeasureString(string value)
    {
        int length = 0;
        foreach (char c in value)
            length += c < 32 ? 2 : c <= 126 ? 1 : 3;
        return length;
    }

    // -- span writers -----------------------------------------------------------------------------

    private static void WriteValue(Span<char> dest, ref int pos, ColumnValue value)
    {
        if (value.Type == ColumnType.Null)
        {
            dest[pos++] = NullMarker;
            return;
        }

        dest[pos++] = PresentMarker;

        switch (value.Type)
        {
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                WriteHex16(dest, ref pos, (ulong)value.LongValue ^ 0x8000_0000_0000_0000UL);
                break;

            case ColumnType.Float64:
                WriteHex16(dest, ref pos, Float64Ordered(value.FloatValue));
                break;

            case ColumnType.Bool:
                dest[pos++] = value.BoolValue ? '1' : '0';
                break;

            case ColumnType.Float32:
                WriteHex8(dest, ref pos, Float32Ordered((float)value.FloatValue));
                break;

            case ColumnType.Bytes:
                WriteBytesHex(dest, ref pos, value.BytesValue ?? []);
                break;

            case ColumnType.Uuid:
                WriteUuidBase125(dest, ref pos, value.UuidHigh, value.LongValue);
                break;

            case ColumnType.Array:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array columns are not indexable");

            // String and Id share one encoding so a String literal in a query (e.g. WHERE id IN ('…'))
            // produces the same key as the Id-typed value stored in the index — the two must stay
            // interchangeable. Both use the same order-preserving ASCII form.
            case ColumnType.String:
            case ColumnType.Id:
                WriteStringOrderedAscii(dest, ref pos, value.StrValue ?? "");
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot encode column type: " + value.Type);
        }
    }

    /// <summary>
    /// Writes <paramref name="ordered"/> as 16 fixed-width uppercase hex chars straight into
    /// <paramref name="dest"/> (no intermediate string). Callers pass the order-preserving transform
    /// of the value (sign-bit flip for integers/dates, bit complement for floats).
    /// </summary>
    private static void WriteHex16(Span<char> dest, ref int pos, ulong ordered)
    {
        ordered.TryFormat(dest.Slice(pos, 16), out _, "X16");
        pos += 16;
    }

    /// <summary>32-bit counterpart of <see cref="WriteHex16"/> — 8 fixed-width uppercase hex chars.</summary>
    private static void WriteHex8(Span<char> dest, ref int pos, uint ordered)
    {
        ordered.TryFormat(dest.Slice(pos, 8), out _, "X8");
        pos += 8;
    }

    /// <summary>
    /// Order-preserving transform of a double's IEEE-754 bits: for negatives all bits are flipped;
    /// for non-negatives only the sign bit is flipped. This yields ascending order for ascending doubles.
    /// </summary>
    private static ulong Float64Ordered(double value)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);

        if (bits < 0)
            bits = ~bits;
        else
            bits ^= long.MinValue;

        return (ulong)bits;
    }

    /// <summary>
    /// Single-precision counterpart of <see cref="Float64Ordered"/>, on 32-bit IEEE-754 bits.
    /// </summary>
    private static uint Float32Ordered(float value)
    {
        int bits = BitConverter.SingleToInt32Bits(value);

        if (bits < 0)
            bits = ~bits;
        else
            bits ^= int.MinValue;

        return (uint)bits;
    }

    private const string HexChars = "0123456789ABCDEF";

    /// <summary>
    /// Writes a prefix-free, order-preserving ASCII encoding of UTF-16 code units. C0 controls use
    /// alphabet rank 0 plus one digit, printable ASCII maps directly to ranks 1..95, and U+007F+
    /// uses three base-125 digits beginning at ranks 96..100. Those disjoint leading-rank ranges
    /// make concatenated code words preserve ordinal string order without a per-character delimiter.
    /// The alphabet excludes '/' because Kahuna treats the last slash as the key-space boundary.
    /// </summary>
    private static void WriteStringOrderedAscii(Span<char> dest, ref int pos, string value)
    {
        foreach (char c in value)
        {
            if (c < 32)
            {
                dest[pos++] = StringAlphabetChar(0);
                dest[pos++] = StringAlphabetChar(c);
            }
            else if (c <= 126)
            {
                dest[pos++] = StringAlphabetChar(c - 31);
            }
            else
            {
                int ordered = c - StringHighFirstCodeUnit;
                dest[pos++] = StringAlphabetChar(StringHighFirstRank + ordered / StringHighBlockSize);
                dest[pos++] = StringAlphabetChar(ordered / StringAlphabetSize % StringAlphabetSize);
                dest[pos++] = StringAlphabetChar(ordered % StringAlphabetSize);
            }
        }

        dest[pos++] = FieldTerminatorLead;
        dest[pos++] = FieldTerminatorTail;
    }

    /// <summary>
    /// Writes a Uuid's 128 bits as 19 fixed-width, most-significant-first base-125 digits over the
    /// '/'-skipping ordered alphabet. Because the digit count is constant and the rank→char mapping is
    /// monotonic, ordinal string order equals unsigned big-endian order of the value, matching
    /// <see cref="ColumnValue.CompareTo"/> — no field terminator is required.
    /// </summary>
    private static void WriteUuidBase125(Span<char> dest, ref int pos, long high, long low)
    {
        UInt128 value = ((UInt128)(ulong)high << 64) | (ulong)low;

        for (int i = UuidKeyDigits - 1; i >= 0; i--)
        {
            dest[pos + i] = StringAlphabetChar((int)(value % 125));
            value /= 125;
        }

        pos += UuidKeyDigits;
    }

    /// <summary>Maps an ordered base-125 digit onto ASCII while skipping Kahuna's '/' separator.</summary>
    private static char StringAlphabetChar(int rank) =>
        (char)(rank < StringSlashRank ? rank + 2 : rank + 3);

    /// <summary>Reverses <see cref="StringAlphabetChar"/> and rejects terminators, separators, and non-ASCII.</summary>
    private static int StringAlphabetRank(char value)
    {
        if (value is < (char)2 or > (char)127 || value == '/')
            return -1;

        return value < '/' ? value - 2 : value - 3;
    }

    /// <summary>
    /// Order-preserving pure-ASCII encoding for Bytes keys: each byte becomes 2 uppercase hex chars,
    /// terminated by the field terminator. Bytewise order is preserved because "AB" &lt; "AC" ordinal,
    /// and the terminator (which sorts before any hex digit) preserves prefix ordering.
    /// </summary>
    private static void WriteBytesHex(Span<char> dest, ref int pos, byte[] value)
    {
        foreach (byte b in value)
        {
            dest[pos++] = HexChars[(b >> 4) & 0xF];
            dest[pos++] = HexChars[b & 0xF];
        }

        dest[pos++] = FieldTerminatorLead;
        dest[pos++] = FieldTerminatorTail;
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

                case ColumnType.Uuid:
                {
                    if (pos + UuidKeyDigits > key.Length)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Truncated uuid field at field {i}");

                    UInt128 value = UInt128.Zero;
                    for (int d = 0; d < UuidKeyDigits; d++)
                    {
                        int rank = StringAlphabetRank(key[pos++]);
                        if (rank is < 0 or >= StringAlphabetSize)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid uuid digit at field {i}");
                        value = value * 125 + (UInt128)(uint)rank;
                    }

                    long high = (long)(ulong)(value >> 64);
                    long low = (long)(ulong)value;
                    values[i] = new ColumnValue(ColumnType.Uuid, high, low);
                    break;
                }

                case ColumnType.String:
                case ColumnType.Id:
                {
                    // Pre-scan: count code units and validate structure before allocating.
                    int bodyStart = pos;
                    int charCount = 0;
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

                        ReadStringCodeUnit(key, ref pos, i);
                        charCount++;
                    }

                    // Build the decoded string in one allocation, writing directly into its
                    // backing buffer — no StringBuilder intermediate.
                    string decoded = charCount == 0
                        ? ""
                        : string.Create(charCount, (key, bodyStart, field: i), static (span, state) =>
                        {
                            int p = state.bodyStart;
                            for (int ci = 0; ci < span.Length; ci++)
                                span[ci] = ReadStringCodeUnit(state.key, ref p, state.field);
                        });

                    values[i] = new ColumnValue(types[i], decoded);
                    break;
                }

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot decode column type: " + types[i]);
            }
        }

        return new CompositeColumnValue(values);
    }

    /// <summary>
    /// Reads one prefix-free String/Id code word and rejects ranks that are not assigned by the
    /// encoder. Strict validation keeps malformed keys from consuming a following field as payload.
    /// </summary>
    private static char ReadStringCodeUnit(string key, ref int pos, int field)
    {
        if (pos >= key.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Truncated string code unit at field {field}");

        int first = StringAlphabetRank(key[pos++]);
        if (first < 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid string code unit at field {field}");

        if (first == 0)
        {
            int control = ReadStringAlphabetRank(key, ref pos, field);
            if (control >= 32)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid control code unit at field {field}");
            return (char)control;
        }

        if (first is >= StringDirectFirstRank and <= StringDirectLastRank)
            return (char)(first + 31);

        if (first is >= StringHighFirstRank and <= StringHighLastRank)
        {
            int middle = ReadStringAlphabetRank(key, ref pos, field);
            int last = ReadStringAlphabetRank(key, ref pos, field);
            int codeUnit = StringHighFirstCodeUnit
                + (first - StringHighFirstRank) * StringHighBlockSize
                + middle * StringAlphabetSize
                + last;

            if (codeUnit <= char.MaxValue)
                return (char)codeUnit;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid string code unit at field {field}");
    }

    /// <summary>Reads one validated base-125 digit without crossing the end of a malformed key.</summary>
    private static int ReadStringAlphabetRank(string key, ref int pos, int field)
    {
        if (pos >= key.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Truncated string code unit at field {field}");

        int rank = StringAlphabetRank(key[pos++]);
        if (rank < 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid string code unit at field {field}");
        return rank;
    }
}
