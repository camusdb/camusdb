
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class ColumnValue : IComparable<ColumnValue>
{
    public ColumnType Type { get; }

    public string Value { get; }

    public ColumnValue(ColumnType type, string value)
    {
        Type = type;
        Value = value;
    }

    public int CompareTo(ColumnValue? other)
    {
        if (other is null)
            throw new ArgumentException("Object is not a ColumnValue");

        if (Type != other.Type)
            throw new ArgumentException("Comparing incompatible ColumnValue");

        if (Type == ColumnType.Integer64)
        {
            long value1 = long.Parse(Value);
            long value2 = long.Parse(other.Value);
            return value1.CompareTo(value2);
        }

        return Value.CompareTo(other.Value);
    }
}
