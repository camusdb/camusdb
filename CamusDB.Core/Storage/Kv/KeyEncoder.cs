
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

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
///   Id         : the 96-bit ObjectId as a fixed 14-char, most-significant-first base-125 number over
///                the same '/'-skipping ordered alphabet as Uuid. 125^14 > 2^96; fixed width (no
///                terminator). The magnitude is (a,b,c) as unsigned big-endian limbs, so ordinal order
///                matches the 24-hex ordinal order <see cref="ColumnValue.CompareTo"/> uses for Id. A
///                String literal compared to an Id column is coerced to Id at the query layer so it
///                produces the same key (Id no longer shares String's encoding).
///   String     : prefix-free ordered ASCII code over UTF-16 code units, terminated by the field
///                terminator. Printable ASCII uses one character, controls use two, and all other
///                code units use three. The alphabet excludes '/' so encoded content cannot change
///                Kahuna's last-slash key-space boundary. Pure ASCII makes the key's UTF-8 byte order
///                (RocksDB/SQLite) equal its UTF-16-ordinal order (in-memory B-tree, range routing,
///                and scan merge). Order within the field matches <see cref="ColumnValue.CompareTo"/>.
///
/// Fixed-width fields (Integer64/Float64/Float32/Bool/Date/DateTime/Uuid/Id) need no terminator; the
/// decoder knows their width from the column type. Variable-length fields (String/Bytes) are
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

    // A 96-bit Id (ObjectId) encodes to a fixed-width base-125 number: ceil(96 / log2(125)) = 14
    // digits. 125^14 > 2^96, so every value fits, fixed width, no terminator — ~half the 26 code
    // units the shared String encoding used. The 96-bit magnitude is (a,b,c) as unsigned big-endian
    // limbs, matching the 24-hex ordinal (unsigned) order that ColumnValue.CompareTo uses for Id.
    private const int IdKeyDigits = 14;

    // Low 96 bits set — used to complement an Id magnitude for descending order within its width.
    private static readonly UInt128 Id96BitMask = (UInt128.One << 96) - 1;

    /// <summary>
    /// Encodes a composite index key. <paramref name="directions"/> is positionally aligned with
    /// <paramref name="composite"/>'s values; a null array (or an out-of-range/absent position) means
    /// that column is ascending — so an all-ascending index passes null and encodes exactly as before.
    /// A descending column inverts its field's ordinal order (and its NULL marker, so NULLs sort last
    /// under DESC) while keeping the same fixed width, so composite prefix ordering is unaffected.
    /// </summary>
    public static string Encode(CompositeColumnValue composite, OrderType[]? directions = null)
    {
        ArgumentNullException.ThrowIfNull(composite);

        int length = MeasureComposite(composite);

        // Write straight into the new string's backing buffer: one allocation, no StringBuilder and
        // no per-field ToString("X…") temporaries. The destination span IS the final string, so the
        // large-value case (String/Id/Bytes can expand to millions of chars) is heap-sized exactly —
        // no stackalloc size ceiling applies.
        return string.Create(length, (composite, directions), static (span, state) =>
        {
            int pos = 0;
            ColumnValue[] values = state.composite.Values;
            for (int i = 0; i < values.Length; i++)
                WriteValue(span, ref pos, values[i], DirectionAt(state.directions, i));
        });
    }

    public static string EncodeValue(ColumnValue value, OrderType direction = OrderType.Ascending)
    {
        int length = MeasureValue(value);
        return string.Create(length, (value, direction), static (span, state) =>
        {
            int pos = 0;
            WriteValue(span, ref pos, state.value, state.direction);
        });
    }

    /// <summary>
    /// Direction of the column at <paramref name="index"/>, defaulting to
    /// <see cref="OrderType.Ascending"/> when the vector is null or the index is out of range.
    /// </summary>
    private static OrderType DirectionAt(OrderType[]? directions, int index)
        => directions is not null && index >= 0 && index < directions.Length
            ? directions[index]
            : OrderType.Ascending;

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
            ColumnType.Id => 1 + IdKeyDigits,
            ColumnType.Bytes => 1 + 2 * (value.BytesValue?.Length ?? 0) + 2,            // 2 hex/byte + terminator
            ColumnType.String => 1 + MeasureString(value.StrValue ?? "") + 2,
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

    private static void WriteValue(Span<char> dest, ref int pos, ColumnValue value, OrderType direction)
    {
        bool descending = direction == OrderType.Descending;

        if (value.Type == ColumnType.Null)
        {
            // Ascending: NULL sorts before any present value (NullMarker '0' < PresentMarker '1').
            // Descending: NULL sorts last, so the markers swap — a present value's '0' precedes a
            // NULL's '1'.
            dest[pos++] = descending ? PresentMarker : NullMarker;
            return;
        }

        dest[pos++] = descending ? NullMarker : PresentMarker;

        switch (value.Type)
        {
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                WriteHex16(dest, ref pos, Invert(((ulong)value.LongValue) ^ 0x8000_0000_0000_0000UL, descending));
                break;

            case ColumnType.Float64:
                WriteHex16(dest, ref pos, Invert(Float64Ordered(value.FloatValue), descending));
                break;

            case ColumnType.Bool:
                // Ascending false('0') < true('1'); descending inverts so true sorts first.
                dest[pos++] = value.BoolValue ^ descending ? '1' : '0';
                break;

            case ColumnType.Float32:
                WriteHex8(dest, ref pos, Invert(Float32Ordered((float)value.FloatValue), descending));
                break;

            case ColumnType.Bytes:
                RejectDescendingTerminatedType(descending, value.Type);
                WriteBytesHex(dest, ref pos, value.BytesValue ?? []);
                break;

            case ColumnType.Uuid:
            {
                UInt128 uuid = ((UInt128)(ulong)value.UuidHigh << 64) | (ulong)value.LongValue;
                WriteBase125(dest, ref pos, descending ? ~uuid : uuid, UuidKeyDigits);
                break;
            }

            case ColumnType.Id:
            {
                // Id (96-bit ObjectId) encodes to a fixed-width base-125 number, like Uuid but 14
                // digits. A String literal compared to an Id column is coerced to Id first (see
                // PredicateAnalyzer / CompareValues / CoerceToColumnType), so it reaches this arm and
                // produces the same key — the two are no longer encoding-identical.
                UInt128 id = IdMagnitude(value.StrValue);
                WriteBase125(dest, ref pos, descending ? Id96BitMask - id : id, IdKeyDigits);
                break;
            }

            case ColumnType.Array:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array columns are not indexable");

            case ColumnType.String:
                RejectDescendingTerminatedType(descending, value.Type);
                WriteStringOrderedAscii(dest, ref pos, value.StrValue ?? "");
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot encode column type: " + value.Type);
        }
    }

    /// <summary>Bitwise-complements the order-preserving word when the column is descending.</summary>
    private static ulong Invert(ulong ordered, bool descending) => descending ? ~ordered : ordered;

    private static uint Invert(uint ordered, bool descending) => descending ? ~ordered : ordered;

    /// <summary>
    /// Builds the 96-bit Id magnitude from its 24-hex string as unsigned big-endian limbs (a,b,c),
    /// so base-125 ordinal order equals the 24-hex ordinal order used by <see cref="ColumnValue.CompareTo"/>
    /// for Id. Stored Id values are always canonical 24-hex, so parsing is total here.
    /// </summary>
    private static UInt128 IdMagnitude(string? hex)
    {
        ObjectIdValue oid = ObjectId.ToValue((hex ?? "").AsSpan());
        return ((UInt128)(uint)oid.a << 64) | ((UInt128)(uint)oid.b << 32) | (uint)oid.c;
    }

    /// <summary>
    /// String and Bytes have no descending encoding yet — they use a prefix-free, terminator-delimited
    /// body (String uses the ordered-ASCII form; Bytes uses terminated hex) whose terminator sorts
    /// before content, which a naive rank inversion would break. DDL rejects descending on these
    /// types, so reaching here means a caller bypassed that gate. (Id, though it once shared String's
    /// encoding, is now fixed-width base-125 and supports descending — it is not handled here.)
    /// </summary>
    private static void RejectDescendingTerminatedType(bool descending, ColumnType type)
    {
        if (descending)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Descending index encoding is not supported for column type {type}");
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
    /// Writes an unsigned value as <paramref name="digits"/> fixed-width, most-significant-first
    /// base-125 digits over the '/'-skipping ordered alphabet. Because the digit count is constant and
    /// the rank→char mapping is monotonic, ordinal string order equals unsigned order of the value,
    /// matching <see cref="ColumnValue.CompareTo"/> — no field terminator is required. Used by both
    /// Uuid (128 bits → 19 digits) and Id (96 bits → 14 digits).
    /// </summary>
    private static void WriteBase125(Span<char> dest, ref int pos, UInt128 value, int digits)
    {
        for (int i = digits - 1; i >= 0; i--)
        {
            dest[pos + i] = StringAlphabetChar((int)(value % 125));
            value /= 125;
        }

        pos += digits;
    }

    /// <summary>
    /// Reads <paramref name="digits"/> fixed-width base-125 digits back into an unsigned value —
    /// the inverse of <see cref="WriteBase125"/>. Rejects malformed keys (short, or a code unit that
    /// is not a valid ordered-alphabet rank).
    /// </summary>
    private static UInt128 ReadBase125(string key, ref int pos, int digits, int field)
    {
        if (pos + digits > key.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Truncated base-125 field at field {field}");

        UInt128 value = UInt128.Zero;
        for (int d = 0; d < digits; d++)
        {
            int rank = StringAlphabetRank(key[pos++]);
            if (rank is < 0 or >= StringAlphabetSize)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid base-125 digit at field {field}");
            value = value * 125 + (UInt128)(uint)rank;
        }
        return value;
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

    public static CompositeColumnValue Decode(string key, ColumnType[] types, OrderType[]? directions = null)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(types);

        ColumnValue[] values = new ColumnValue[types.Length];
        int pos = 0;

        for (int i = 0; i < types.Length; i++)
        {
            if (pos >= key.Length)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Key too short for {types.Length} fields at field {i}");

            bool descending = DirectionAt(directions, i) == OrderType.Descending;

            char marker = key[pos++];

            if (marker != NullMarker && marker != PresentMarker)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unexpected marker '{marker}' at position {pos - 1}");

            // Descending fields swap the markers (NULLs sort last), so a NULL is written as the
            // PresentMarker char and vice-versa. Interpret the raw char through the direction.
            bool isNull = descending ? marker == PresentMarker : marker == NullMarker;
            if (isNull)
            {
                values[i] = new ColumnValue(ColumnType.Null, false);
                continue;
            }

            switch (types[i])
            {
                case ColumnType.Integer64:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    ulong ordered = descending ? ~stored : stored;
                    long longValue = (long)(ordered ^ 0x8000_0000_0000_0000UL);
                    values[i] = new ColumnValue(ColumnType.Integer64, longValue);
                    pos += 16;
                    break;
                }

                case ColumnType.Float64:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    if (descending) stored = ~stored;
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
                    if (descending) stored = ~stored;
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
                    ulong ordered = descending ? ~stored : stored;
                    long ticks = (long)(ordered ^ 0x8000_0000_0000_0000UL);
                    values[i] = new ColumnValue(ColumnType.Date, ticks);
                    pos += 16;
                    break;
                }

                case ColumnType.DateTime:
                {
                    ulong stored = ulong.Parse(key.AsSpan(pos, 16), System.Globalization.NumberStyles.HexNumber);
                    ulong ordered = descending ? ~stored : stored;
                    long ticks = (long)(ordered ^ 0x8000_0000_0000_0000UL);
                    values[i] = new ColumnValue(ColumnType.DateTime, ticks);
                    pos += 16;
                    break;
                }

                case ColumnType.Bytes:
                {
                    RejectDescendingTerminatedType(descending, types[i]);
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
                    // Ascending: '1'==true. Descending inverts so '0'==true.
                    values[i] = new ColumnValue(ColumnType.Bool, descending ? key[pos++] == '0' : key[pos++] == '1');
                    break;

                case ColumnType.Uuid:
                {
                    UInt128 value = ReadBase125(key, ref pos, UuidKeyDigits, i);
                    if (descending) value = ~value;

                    long high = (long)(ulong)(value >> 64);
                    long low = (long)(ulong)value;
                    values[i] = new ColumnValue(ColumnType.Uuid, high, low);
                    break;
                }

                case ColumnType.Id:
                {
                    UInt128 value = ReadBase125(key, ref pos, IdKeyDigits, i);
                    if (descending) value = Id96BitMask - value;

                    int a = (int)(uint)(value >> 64);
                    int b = (int)(uint)(value >> 32);
                    int c = (int)(uint)value;
                    values[i] = new ColumnValue(ColumnType.Id, ObjectId.ToString(a, b, c));
                    break;
                }

                case ColumnType.String:
                {
                    RejectDescendingTerminatedType(descending, types[i]);

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
