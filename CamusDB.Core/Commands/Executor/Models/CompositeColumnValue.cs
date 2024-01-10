
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents a value that can be stored or compared with a column's value
/// </summary>
public sealed class CompositeColumnValue : IComparable<CompositeColumnValue>
{
    public ColumnValue[] Values { get; }

    public CompositeColumnValue(ColumnValue[] values)
    {
        Values = values;
    }

    public int CompareTo(CompositeColumnValue? other)
    {
        if (other is null)
            return 1;

        if (Values.Length != other.Values.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot compare two composite values with different number of values");

        for (int i = 0; i < Values.Length; i++)
        {
            int result = Values[i].CompareTo(other.Values[i]);

            if (result != 0)
                return result;
        }

        return 0;
    }
}