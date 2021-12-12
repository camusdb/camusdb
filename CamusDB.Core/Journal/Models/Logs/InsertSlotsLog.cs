
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Attributes;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.InsertSlots)]
public sealed class InsertSlotsLog
{
    [JournalField(0)]
    public uint Sequence { get; }

    [JournalField(1)]
    public BTreeTuple RowTuple { get; }    

    public InsertSlotsLog(uint sequence, BTreeTuple rowTuple)
    {
        Sequence = sequence;
        RowTuple = rowTuple;
    }
}
