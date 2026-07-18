
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// A compact, allocation-free value cell: the internal counterpart of the public
/// <see cref="ColumnValue"/> class, intended to become the currency of the query pipeline so a
/// decoded row is one <c>ValueSlot[]</c> rather than N heap objects. It is <b>not</b> a replacement
/// for <see cref="ColumnValue"/> — that stays the public boundary type (HTTP/JSON, schema defaults,
/// gRPC contracts). Conversion happens at the boundaries via <see cref="FromColumnValue"/> /
/// <see cref="ToColumnValue"/>.
///
/// <para>
/// <b>Field packing</b> — two integer payloads plus one reference, so every non-reference type
/// constructs with zero heap allocation:
/// <list type="bullet">
/// <item><see cref="ColumnType.Integer64"/> / <see cref="ColumnType.Date"/> /
///   <see cref="ColumnType.DateTime"/> — value in <c>_low</c>.</item>
/// <item><see cref="ColumnType.Bool"/> — 0/1 in <c>_low</c>.</item>
/// <item><see cref="ColumnType.Float64"/> / <see cref="ColumnType.Float32"/> —
///   <see cref="BitConverter.DoubleToInt64Bits"/> in <c>_low</c> (Float32 keeps the widened double,
///   matching how <see cref="ColumnValue"/> stores it, and compares narrowed to float).</item>
/// <item><see cref="ColumnType.Uuid"/> — low 64 bits in <c>_low</c>, high 64 bits in <c>_high</c>.</item>
/// <item><see cref="ColumnType.String"/> / <see cref="ColumnType.Id"/> — the string in <c>_reference</c>.
///   Id is stored as its string form here so the boundary round-trips <b>any</b> Id value exactly
///   (including non-canonical literal text). Packing the 96-bit ObjectId inline is a decode-time
///   optimization deferred to where the value is a guaranteed-valid <see cref="Util.ObjectIds.ObjectIdValue"/>.</item>
/// <item><see cref="ColumnType.Bytes"/> — the <c>byte[]</c> in <c>_reference</c>.</item>
/// <item><see cref="ColumnType.Array"/> — the element <c>ValueSlot[]</c> in <c>_reference</c> and the
///   element <see cref="ColumnType"/> in <c>_high</c>.</item>
/// <item><see cref="ColumnType.Null"/> — type only.</item>
/// </list>
/// </para>
/// </summary>
internal readonly struct ValueSlot
{
    private readonly ColumnType _type;
    private readonly long _low;
    private readonly long _high;
    private readonly object? _reference;

    /// <summary>The value's column type; the discriminator for every accessor and conversion.</summary>
    public ColumnType Type => _type;

    public static readonly ValueSlot Null = new(ColumnType.Null, 0, 0, null);
    public static readonly ValueSlot True = new(ColumnType.Bool, 1, 0, null);
    public static readonly ValueSlot False = new(ColumnType.Bool, 0, 0, null);

    private ValueSlot(ColumnType type, long low, long high, object? reference)
    {
        _type = type;
        _low = low;
        _high = high;
        _reference = reference;
    }

    // ── Factories (allocation-free except for the unavoidable reference payload) ──

    public static ValueSlot FromLong(ColumnType type, long value) => new(type, value, 0, null);
    public static ValueSlot FromBool(bool value) => value ? True : False;
    public static ValueSlot FromDouble(ColumnType type, double value) => new(type, BitConverter.DoubleToInt64Bits(value), 0, null);
    public static ValueSlot FromString(string value) => new(ColumnType.String, 0, 0, value);
    public static ValueSlot FromId(string value) => new(ColumnType.Id, 0, 0, value);
    public static ValueSlot FromBytes(byte[]? value) => new(ColumnType.Bytes, 0, 0, value);
    public static ValueSlot FromUuid(long high, long low) => new(ColumnType.Uuid, low, high, null);
    public static ValueSlot FromArray(ColumnType elementType, ValueSlot[] elements) => new(ColumnType.Array, 0, (long)(int)elementType, elements);

    // ── Typed accessors ──

    public bool IsNull => _type == ColumnType.Null;
    public long AsLong => _low;
    public bool AsBool => _low != 0;
    public double AsDouble => BitConverter.Int64BitsToDouble(_low);
    public string? AsString => (string?)_reference;
    public byte[]? AsBytes => (byte[]?)_reference;
    public long UuidHigh => _high;
    public long UuidLow => _low;
    public ColumnType ArrayElementType => (ColumnType)(int)_high;
    public ValueSlot[] ArrayElements => (ValueSlot[])(_reference ?? System.Array.Empty<ValueSlot>());

    // ── Boundary conversion ──

    /// <summary>
    /// Packs a public <see cref="ColumnValue"/> into a slot. Recurses for arrays. A null argument or a
    /// <see cref="ColumnType.Null"/> value becomes <see cref="Null"/>.
    /// </summary>
    public static ValueSlot FromColumnValue(ColumnValue? value)
    {
        if (value is null || value.Type == ColumnType.Null)
            return Null;

        switch (value.Type)
        {
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                return FromLong(value.Type, value.LongValue);

            case ColumnType.Bool:
                return FromBool(value.BoolValue);

            case ColumnType.Float64:
            case ColumnType.Float32:
                return FromDouble(value.Type, value.FloatValue);

            case ColumnType.String:
                return FromString(value.StrValue!);

            case ColumnType.Id:
                return FromId(value.StrValue!);

            case ColumnType.Uuid:
                return FromUuid(value.UuidHigh, value.LongValue);

            case ColumnType.Bytes:
                return FromBytes(value.BytesValue);

            case ColumnType.Array:
            {
                IReadOnlyList<ColumnValue> src = value.ArrayValues ?? [];
                ValueSlot[] elements = new ValueSlot[src.Count];
                for (int i = 0; i < src.Count; i++)
                    elements[i] = FromColumnValue(src[i]);
                return FromArray(value.ArrayElementType, elements);
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown ValueSlot type " + value.Type);
        }
    }

    /// <summary>
    /// Materializes this slot as a public <see cref="ColumnValue"/> — the only allocating step, paid at
    /// the boundary where a value escapes the pipeline. Value-identical to the <see cref="ColumnValue"/>
    /// that produced it via <see cref="FromColumnValue"/>.
    /// </summary>
    public ColumnValue ToColumnValue()
    {
        switch (_type)
        {
            case ColumnType.Null:      return ColumnValue.Null;
            case ColumnType.Integer64: return new ColumnValue(ColumnType.Integer64, _low);
            case ColumnType.Date:      return new ColumnValue(ColumnType.Date, _low);
            case ColumnType.DateTime:  return new ColumnValue(ColumnType.DateTime, _low);
            case ColumnType.Bool:      return ColumnValue.FromBool(AsBool);
            case ColumnType.Float64:   return new ColumnValue(ColumnType.Float64, AsDouble);
            case ColumnType.Float32:   return new ColumnValue(ColumnType.Float32, AsDouble);
            case ColumnType.String:    return new ColumnValue(ColumnType.String, AsString);
            case ColumnType.Id:        return new ColumnValue(ColumnType.Id, AsString);
            case ColumnType.Uuid:      return new ColumnValue(ColumnType.Uuid, _high, _low);
            case ColumnType.Bytes:     return AsBytes is null ? ColumnValue.Null : new ColumnValue(AsBytes);

            case ColumnType.Array:
            {
                ValueSlot[] elements = ArrayElements;
                ColumnValue[] converted = new ColumnValue[elements.Length];
                for (int i = 0; i < elements.Length; i++)
                    converted[i] = elements[i].ToColumnValue();
                return ColumnValue.FromArray(ArrayElementType, converted);
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown ValueSlot type " + _type);
        }
    }

    // ── Comparison — mirrors ColumnValue.CompareTo exactly, taken by 'in' to avoid the struct copy ──

    /// <summary>
    /// Orders two slots with the same semantics as <see cref="ColumnValue.CompareTo"/>: NULL sorts
    /// before any non-NULL value, string/Id compare ordinally, floats narrow to their stored precision,
    /// UUIDs compare as unsigned big-endian halves, and arrays compare element-wise then by length.
    /// Throws when the non-NULL types differ, matching <see cref="ColumnValue"/>.
    /// </summary>
    public int CompareTo(in ValueSlot other)
    {
        if (other._type == ColumnType.Null)
            return _type == ColumnType.Null ? 0 : 1;
        if (_type == ColumnType.Null)
            return -1;
        if (_type != other._type)
            throw new ArgumentException($"Comparing incompatible ValueSlot: {_type} and {other._type}");

        switch (_type)
        {
            case ColumnType.String:
            case ColumnType.Id:
            {
                string? a = AsString, b = other.AsString;
                if (a is null) return b is null ? 0 : -1;
                if (b is null) return 1;
                return string.CompareOrdinal(a, b);
            }

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                return _low.CompareTo(other._low);

            case ColumnType.Bool:
                return AsBool.CompareTo(other.AsBool);

            case ColumnType.Float64:
                return AsDouble.CompareTo(other.AsDouble);

            case ColumnType.Float32:
                return ((float)AsDouble).CompareTo((float)other.AsDouble);

            case ColumnType.Uuid:
            {
                int high = ((ulong)_high).CompareTo((ulong)other._high);
                return high != 0 ? high : ((ulong)_low).CompareTo((ulong)other._low);
            }

            case ColumnType.Bytes:
            {
                ReadOnlySpan<byte> a = AsBytes ?? [];
                ReadOnlySpan<byte> b = other.AsBytes ?? [];
                return a.SequenceCompareTo(b);
            }

            case ColumnType.Array:
            {
                ValueSlot[] a = ArrayElements;
                ValueSlot[] b = other.ArrayElements;
                int minLen = Math.Min(a.Length, b.Length);
                for (int i = 0; i < minLen; i++)
                {
                    // Both NULL at the same position are equal here (the top-level NULL guard would
                    // otherwise report 1 for a NULL 'other'), matching ColumnValue's array compare.
                    if (a[i]._type == ColumnType.Null && b[i]._type == ColumnType.Null)
                        continue;
                    int c = a[i].CompareTo(b[i]);
                    if (c != 0) return c;
                }
                return a.Length.CompareTo(b.Length);
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown ValueSlot type " + _type);
        }
    }

    // ── Hash — consistent with CompareTo equality (equal values hash equal within a type) ──

    /// <summary>
    /// A hash consistent with <see cref="CompareTo"/> equality for a single type: two slots that compare
    /// equal produce the same hash. Type is folded in so distinct types do not collide by payload alone.
    /// Intended for the hash operators (GROUP BY / DISTINCT / join) that adopt slots in a later step.
    /// </summary>
    public int GetSlotHashCode()
    {
        switch (_type)
        {
            case ColumnType.Null:
                return 0;

            case ColumnType.String:
            case ColumnType.Id:
                return HashCode.Combine(_type, AsString is null ? 0 : string.GetHashCode(AsString, StringComparison.Ordinal));

            case ColumnType.Float32:
                // Hash on the narrowed float so it matches the float-precision CompareTo.
                return HashCode.Combine(_type, (float)AsDouble);

            case ColumnType.Bytes:
            {
                HashCode hc = new();
                hc.Add(_type);
                hc.AddBytes(AsBytes ?? []);
                return hc.ToHashCode();
            }

            case ColumnType.Array:
            {
                HashCode hc = new();
                hc.Add(_type);
                hc.Add(ArrayElementType);
                ValueSlot[] elements = ArrayElements;
                for (int i = 0; i < elements.Length; i++)
                    hc.Add(elements[i].GetSlotHashCode());
                return hc.ToHashCode();
            }

            default:
                // Integer64/Date/DateTime/Bool/Float64/Uuid — payload lives in the integer fields.
                return HashCode.Combine(_type, _low, _high);
        }
    }
}
