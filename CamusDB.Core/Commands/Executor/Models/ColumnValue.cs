
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using System.Text.Json.Serialization;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents a value that can be stored or compared with a column's value.
///
/// Backing storage for new types:
///   Float32   — stored in FloatValue (double). The value is narrowed to float on use; precision
///               round-trips through float→double→float are exact because we only store float-range
///               values and double has strictly more mantissa bits.
///   Date      — stored in LongValue as UTC DateTime.Ticks truncated to midnight (whole days).
///   DateTime  — stored in LongValue as UTC DateTime.Ticks.
///   Bytes     — stored in BytesValue (byte[]).
///   Array     — stored in ArrayValues (IReadOnlyList<ColumnValue>) + ArrayElementType (ColumnType).
///               An empty array still carries its element type. Elements must all be ArrayElementType
///               or Null.
/// </summary>
public sealed class ColumnValue : IComparable<ColumnValue>
{
    public static readonly ColumnValue Null = new(ColumnType.Null, false);
    public static readonly ColumnValue True = new(ColumnType.Bool, true);
    public static readonly ColumnValue False = new(ColumnType.Bool, false);

    public static ColumnValue FromBool(bool value) => value ? True : False;

    public ColumnType Type { get; }

    public long LongValue { get; }

    public double FloatValue { get; }

    public bool BoolValue { get; }

    public string? StrValue { get; }

    public byte[]? BytesValue { get; }

    public IReadOnlyList<ColumnValue>? ArrayValues { get; }

    public ColumnType ArrayElementType { get; }

    /// <summary>
    /// ISO-8601 string representation for Date ("yyyy-MM-dd") and DateTime ("o" round-trip format).
    /// Null for all other types. Included in JSON responses; ignored during deserialization.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? IsoValue => Type switch
    {
        ColumnType.Date     => new DateTime(LongValue, DateTimeKind.Utc).ToString("yyyy-MM-dd"),
        ColumnType.DateTime => new DateTime(LongValue, DateTimeKind.Utc).ToString("o"),
        _ => null,
    };

    // -------------------------------------------------------------------------
    // JsonConstructor — must round-trip all backing fields for schema DefaultValue
    // -------------------------------------------------------------------------

    [JsonConstructor]
    public ColumnValue(
        ColumnType type,
        string? strValue,
        long longValue,
        double floatValue,
        bool boolValue,
        byte[]? bytesValue,
        IReadOnlyList<ColumnValue>? arrayValues,
        ColumnType arrayElementType)
    {
        Type = type;

        if (type is ColumnType.String or ColumnType.Id)
        {
            if (strValue is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.String to string value (null)");
            StrValue = strValue;
        }

        if (type == ColumnType.Integer64)
            LongValue = longValue;

        if (type is ColumnType.Float64 or ColumnType.Float32)
            FloatValue = floatValue;

        if (type == ColumnType.Bool)
            BoolValue = boolValue;

        if (type is ColumnType.Date or ColumnType.DateTime)
            LongValue = longValue;

        if (type == ColumnType.Bytes)
            BytesValue = bytesValue;

        if (type == ColumnType.Array)
        {
            ArrayValues = arrayValues ?? [];
            ArrayElementType = arrayElementType;
        }
    }

    // -------------------------------------------------------------------------
    // Typed constructors
    // -------------------------------------------------------------------------

    public ColumnValue(ColumnType type, bool value)
    {
        if (type != ColumnType.Bool && type != ColumnType.Null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.Bool to bool value");

        Type = type;
        BoolValue = value;
    }

    public ColumnValue(ColumnType type, long value)
    {
        if (type != ColumnType.Integer64 && type != ColumnType.Null &&
            type != ColumnType.Date && type != ColumnType.DateTime)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.Integer64/Date/DateTime to long value");

        Type = type;
        LongValue = value;
    }

    public ColumnValue(ColumnType type, double value)
    {
        if (type != ColumnType.Float64 && type != ColumnType.Float32 && type != ColumnType.Null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.Float64/Float32 to double value");

        Type = type;
        FloatValue = value;
    }

    public ColumnValue(ColumnType type, string value)
    {
        if (type != ColumnType.String && type != ColumnType.Id && type != ColumnType.Null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.String to string value");

        if (type != ColumnType.Null && value is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.String to string value (null)");

        Type = type;
        StrValue = value;
    }

    /// <summary>Constructor for Bytes values.</summary>
    public ColumnValue(byte[] value)
    {
        Type = ColumnType.Bytes;
        BytesValue = value;
    }

    /// <summary>Factory for Array values. All elements must match elementType or be Null.</summary>
    public static ColumnValue FromArray(ColumnType elementType, IReadOnlyList<ColumnValue> elements)
    {
        for (int i = 0; i < elements.Count; i++)
        {
            ColumnValue el = elements[i];
            if (el.Type != ColumnType.Null && el.Type != elementType)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Array element at index {i} has type {el.Type} but array element type is {elementType}");
        }
        return new ColumnValue(ColumnType.Array, null, 0, 0, false, null, elements, elementType);
    }

    // -------------------------------------------------------------------------
    // CompareTo
    // -------------------------------------------------------------------------

    public int CompareTo(ColumnValue? other)
    {
        if (other is null)
            throw new ArgumentException("Object is not a ColumnValue");

        if (other.Type == ColumnType.Null)
            return 1;

        if (Type == ColumnType.Null)
            return -1;

        if (Type != other.Type)
            throw new ArgumentException($"Comparing incompatible ColumnValue: {Type} and {other.Type}");

        switch (Type)
        {
            case ColumnType.String or ColumnType.Id when StrValue is null:
                return -1;

            case ColumnType.String or ColumnType.Id when other.StrValue is null:
                return 1;

            case ColumnType.String or ColumnType.Id:
                return string.Compare(StrValue!, other.StrValue, StringComparison.Ordinal);

            case ColumnType.Integer64:
                return LongValue.CompareTo(other.LongValue);

            case ColumnType.Float64:
                return FloatValue.CompareTo(other.FloatValue);

            case ColumnType.Bool:
                return BoolValue.CompareTo(other.BoolValue);

            case ColumnType.Float32:
                // Compare as float (single precision) to be consistent with stored precision.
                return ((float)FloatValue).CompareTo((float)other.FloatValue);

            case ColumnType.Date:
            case ColumnType.DateTime:
                return LongValue.CompareTo(other.LongValue);

            case ColumnType.Bytes:
            {
                ReadOnlySpan<byte> a = BytesValue ?? [];
                ReadOnlySpan<byte> b = other.BytesValue ?? [];
                return a.SequenceCompareTo(b);
            }

            case ColumnType.Array:
            {
                IReadOnlyList<ColumnValue> a = ArrayValues ?? [];
                IReadOnlyList<ColumnValue> b = other.ArrayValues ?? [];
                int minLen = Math.Min(a.Count, b.Count);
                for (int i = 0; i < minLen; i++)
                {
                    // Both Null at the same position are equal (CompareTo's top-level
                    // null guard returns 1 when other is Null, which would be wrong here).
                    if (a[i].Type == ColumnType.Null && b[i].Type == ColumnType.Null)
                        continue;
                    int c = a[i].CompareTo(b[i]);
                    if (c != 0) return c;
                }
                return a.Count.CompareTo(b.Count);
            }

            default:
                throw new Exception("Unknown value: " + Type);
        }
    }

    // -------------------------------------------------------------------------
    // ToString — diagnostics only; never dumps raw bytes
    // -------------------------------------------------------------------------

    public override string ToString()
    {
        return Type switch
        {
            ColumnType.Integer64 => $"ColumnValue({Type}:{LongValue})",
            ColumnType.Float64   => $"ColumnValue({Type}:{FloatValue})",
            ColumnType.Float32   => $"ColumnValue({Type}:{(float)FloatValue})",
            ColumnType.Bool      => $"ColumnValue({Type}:{BoolValue})",
            ColumnType.String    => $"ColumnValue({Type}:{StrValue})",
            ColumnType.Id        => $"ColumnValue({Type}:{StrValue})",
            ColumnType.Date      => $"ColumnValue({Type}:{new DateTime(LongValue, DateTimeKind.Utc):yyyy-MM-dd})",
            ColumnType.DateTime  => $"ColumnValue({Type}:{new DateTime(LongValue, DateTimeKind.Utc):o})",
            ColumnType.Bytes     => $"ColumnValue({Type}:len={BytesValue?.Length ?? 0})",
            ColumnType.Array     => $"ColumnValue({Type}<{ArrayElementType}>:count={ArrayValues?.Count ?? 0})",
            _                    => $"ColumnValue({Type}:{StrValue})",
        };
    }
}
