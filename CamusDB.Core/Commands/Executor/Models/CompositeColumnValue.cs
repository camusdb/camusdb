
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents a group of column values acting as a composite value
/// </summary>
public sealed class CompositeColumnValue : IComparable<CompositeColumnValue>, IPrefixComparable<ColumnValue>
{
    public ColumnValue[] Values { get; }

    public CompositeColumnValue(ColumnValue[] values)
    {
        Values = values;
    }

    public CompositeColumnValue(ColumnValue value)
    {
        Values = new[] { value };
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
            {
                //Console.WriteLine(result);
                return result;
            }
        }

        //Console.WriteLine(0);
        return 0;
    }

    public override string ToString()
    {
        string str = "";

        for (int i = 0; i < Values.Length; i++)
            str += i + "=" + Values[i];

        return str;
    }

    public int IsPrefixedBy(ColumnValue? other)
    {
        if (other is null)
            return 1;

        return other.CompareTo(Values[0]);
    }
}