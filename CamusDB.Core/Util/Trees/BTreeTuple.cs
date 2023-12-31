﻿
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
public sealed class BTreeTuple
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
        return string.Format("BTreeTuple({0}:{1})", SlotOne, SlotTwo);
    }    
}
