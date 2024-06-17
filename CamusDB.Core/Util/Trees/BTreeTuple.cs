
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Util.Trees;

/// <summary>
/// Represents a tuple of two <see cref="ObjectIdValue"/> values
/// Usually SlotOne is the address of the rowid and SlotTwo is the address of the data
/// </summary>
public sealed class BTreeTuple : IComparable<BTreeTuple>
{
    public ObjectIdValue SlotOne { get; set; }

    public ObjectIdValue SlotTwo { get; set; }

    public BTreeTuple(ObjectIdValue slotOne, ObjectIdValue slotTwo)
    {
        SlotOne = slotOne;
        SlotTwo = slotTwo;
    }

    public bool IsNull()
    {
        return SlotOne.IsNull() && SlotTwo.IsNull();
    }

    public override string ToString()
    {
        return $"BTreeTuple({SlotOne}:{SlotTwo})";
    }

    public int CompareTo(BTreeTuple? other)
    {
        if (other is null)
            return 1;

        if (IsNull() && !other.IsNull())
            return -1;

        if (SlotOne.CompareTo(other.SlotOne) == 0)
            return SlotTwo.CompareTo(other.SlotTwo);

        if (SlotOne.CompareTo(other.SlotOne) > 1)
            return 1;

        return -1;
    }
}
