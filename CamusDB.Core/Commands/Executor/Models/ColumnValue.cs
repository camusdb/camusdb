
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
/// Represents a value that can be stored or compared with a column's value
/// </summary>
public sealed class ColumnValue : IComparable<ColumnValue>
{
    public ColumnType Type { get; }

    public long LongValue { get; }

    public bool BoolValue { get; }

    public string? StrValue { get; }

    [JsonConstructor]
    public ColumnValue(ColumnType type, string? strValue, long longValue, bool boolValue)
    {
        Type = type;

        if (type == ColumnType.String || type == ColumnType.Id)
        {
            if (strValue is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.String to string value (null)");

            StrValue = strValue;
        }

        if (type == ColumnType.Integer64)
            LongValue = longValue;

        if (type == ColumnType.Bool)
            BoolValue = boolValue;
    }

    public ColumnValue(ColumnType type, bool value)
    {
        if (type != ColumnType.Bool && type != ColumnType.Null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.Bool to bool value");

        Type = type;
        BoolValue = value;
    }

    public ColumnValue(ColumnType type, long value)
    {
        if (type != ColumnType.Integer64 && type != ColumnType.Null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Only type ColumnType.Integer64 to long value");

        Type = type;
        LongValue = value;
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

    public int CompareTo(ColumnValue? other)
    {
        if (other is null)
            throw new ArgumentException("Object is not a ColumnValue");

        if (Type != ColumnType.Null && other.Type == ColumnType.Null)
            return 1;

        if (Type == ColumnType.Null && other.Type == ColumnType.Null)
            return 0;

        if (Type == ColumnType.Null && other.Type != ColumnType.Null)
            return -1;

        if (Type != other.Type)
            throw new ArgumentException($"Comparing incompatible ColumnValue: {Type} and {other.Type}");

        if (Type == ColumnType.String || Type == ColumnType.Id)
        {
            if (StrValue is null || other.StrValue is null)
                return -1;

            return StrValue!.CompareTo(other.StrValue);
        }

        if (Type == ColumnType.Integer64)
            return LongValue.CompareTo(other.LongValue);

        if (Type == ColumnType.Bool)
            return BoolValue.CompareTo(other.BoolValue);        

        throw new Exception("Unknown value: " + Type);
    }

    public override string ToString()
    {
        if (Type == ColumnType.Integer64)
            return string.Format("ColumnValue({0}:{1})", Type, LongValue);

        if (Type == ColumnType.Bool)
            return string.Format("ColumnValue({0}:{1})", Type, BoolValue);

        if (Type == ColumnType.String)
            return string.Format("ColumnValue({0}:{1})", Type, StrValue);

        return string.Format("ColumnValue({0}:{1})", Type, StrValue);
    }
}
