
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// PROTOTYPE, benchmark-only — the original spike. The production type now lives in Core as
/// <c>CamusDB.Core.CommandsExecutor.Models.ValueSlot</c> (internal). This copy is retained only so the
/// spike stays reproducible; it differs deliberately by packing <c>Id</c> inline (the decode upper
/// bound), whereas the Core type stores <c>Id</c> as a string for safe boundary round-tripping.
///
/// A compact value representation that packs every scalar column value
/// into inline fields so a decoded row is a single <c>ValueSlot[]</c> allocation with zero per-cell
/// heap objects — the candidate replacement the hot-path allocation spec parks as a gated spike
/// (do not adopt without the end-to-end benchmark clearing). It is deliberately NOT a drop-in swap
/// for the public <see cref="ColumnValue"/> class; it lives here only to measure the win/regression.
///
/// Field packing (two integer payloads + one reference):
///   Integer64 / Date / DateTime — value in <c>_low</c>.
///   Bool                        — 0/1 in <c>_low</c>.
///   Float64 / Float32           — <c>BitConverter.DoubleToInt64Bits</c> in <c>_low</c>.
///   Uuid                        — low 64 bits in <c>_low</c>, high 64 bits in <c>_high</c>.
///   Id                          — the 96-bit ObjectId inline: (a,b) packed in <c>_low</c>, c in
///                                 <c>_high</c>. This is the sleeper win: no 24-char string on decode
///                                 and no re-parse when the value is used to build a key.
///   String                      — <c>_reference</c> holds the string.
///   Bytes                       — <c>_reference</c> holds the byte[].
///   Array                       — <c>_reference</c> holds ValueSlot[]; element type (int) in <c>_high</c>.
///   Null                        — type only.
/// </summary>
public readonly struct ValueSlot
{
    private readonly ColumnType _type;
    private readonly long _low;
    private readonly long _high;
    private readonly object? _reference;

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

    // ── Factories (no per-value heap object except for the unavoidable string/byte[] payload) ──

    public static ValueSlot FromLong(ColumnType type, long value) => new(type, value, 0, null);
    public static ValueSlot FromBool(bool value) => value ? True : False;
    public static ValueSlot FromDouble(ColumnType type, double value) => new(type, BitConverter.DoubleToInt64Bits(value), 0, null);
    public static ValueSlot FromString(string value) => new(ColumnType.String, 0, 0, value);
    public static ValueSlot FromBytes(byte[] value) => new(ColumnType.Bytes, 0, 0, value);
    public static ValueSlot FromUuid(long high, long low) => new(ColumnType.Uuid, low, high, null);

    public static ValueSlot FromId(ObjectIdValue id)
        => new(ColumnType.Id, ((long)id.a << 32) | (uint)id.b, id.c, null);

    public static ValueSlot FromArray(ColumnType elementType, ValueSlot[] elements)
        => new(ColumnType.Array, 0, (long)(int)elementType, elements);

    // ── Typed accessors ──

    public long AsLong => _low;
    public bool AsBool => _low != 0;
    public double AsDouble => BitConverter.Int64BitsToDouble(_low);
    public string AsString => (string)_reference!;
    public byte[] AsBytes => (byte[])_reference!;
    public long UuidHigh => _high;
    public long UuidLow => _low;

    public ObjectIdValue AsObjectId => new((int)(_low >> 32), (int)(_low & 0xFFFFFFFF), (int)_high);

    // ── Comparison — mirrors ColumnValue.CompareTo semantics exactly, taken by 'in' to avoid the copy ──

    public int CompareTo(in ValueSlot other)
    {
        if (other._type == ColumnType.Null) return _type == ColumnType.Null ? 0 : 1;
        if (_type == ColumnType.Null) return -1;
        if (_type != other._type)
            throw new ArgumentException($"Comparing incompatible ValueSlot: {_type} and {other._type}");

        switch (_type)
        {
            case ColumnType.String:
                return string.CompareOrdinal(AsString, other.AsString);

            case ColumnType.Id:
                return AsObjectId.CompareTo(other.AsObjectId);

            case ColumnType.Integer64 or ColumnType.Date or ColumnType.DateTime:
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
                return ((ReadOnlySpan<byte>)AsBytes).SequenceCompareTo(other.AsBytes);

            default:
                throw new Exception("Unsupported ValueSlot compare: " + _type);
        }
    }

    // ── Hash — enough parity to exercise a GROUP BY / DISTINCT probe on the slot directly ──

    public int GetSlotHashCode()
        => _type switch
        {
            ColumnType.Null => 0,
            ColumnType.String => string.GetHashCode(AsString, StringComparison.Ordinal),
            ColumnType.Id => HashCode.Combine(_type, _low, _high),
            ColumnType.Bytes => BytesHash(AsBytes),
            _ => HashCode.Combine(_type, _low, _high),
        };

    private static int BytesHash(byte[] bytes)
    {
        HashCode hc = new();
        hc.AddBytes(bytes);
        return hc.ToHashCode();
    }

    // ── Boundary conversion — the only allocation, paid when a row actually escapes to the public API ──

    public ColumnValue ToColumnValue()
        => _type switch
        {
            ColumnType.Null      => ColumnValue.Null,
            ColumnType.Integer64 => new ColumnValue(ColumnType.Integer64, _low),
            ColumnType.Date      => new ColumnValue(ColumnType.Date, _low),
            ColumnType.DateTime  => new ColumnValue(ColumnType.DateTime, _low),
            ColumnType.Bool      => ColumnValue.FromBool(AsBool),
            ColumnType.Float64   => new ColumnValue(ColumnType.Float64, AsDouble),
            ColumnType.Float32   => new ColumnValue(ColumnType.Float32, AsDouble),
            ColumnType.String    => new ColumnValue(ColumnType.String, AsString),
            ColumnType.Id        => new ColumnValue(ColumnType.Id, AsObjectId.ToString()),
            ColumnType.Uuid      => new ColumnValue(ColumnType.Uuid, _high, _low),
            ColumnType.Bytes     => new ColumnValue(AsBytes),
            _ => throw new Exception("Unsupported ValueSlot->ColumnValue: " + _type),
        };
}
