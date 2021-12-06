
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.Journal.Models.Writers;

public sealed class JournalInsertSlots
{
    public BTreeTuple RowTuple { get; }

    public uint Sequence { get; }

    public JournalInsertSlots(uint sequence, BTreeTuple rowTuple)
    {
        Sequence = sequence;
        RowTuple = rowTuple;        
    }
}
