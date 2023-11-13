
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeTuple
{
    public ObjectIdValue SlotOne { get; set; }

    public ObjectIdValue SlotTwo { get; set; }

    public BTreeTuple(ObjectIdValue slotOne, ObjectIdValue slotTwo)
    {
        SlotOne = slotOne;
        SlotTwo = slotTwo;
    }
}
