
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeTuple
{
    public int SlotOne { get; set; }

    public int SlotTwo { get; set; }

    public BTreeTuple(int slotOne, int slotTwo)
    {
        SlotOne = slotOne;
        SlotTwo = slotTwo;
    }
}

