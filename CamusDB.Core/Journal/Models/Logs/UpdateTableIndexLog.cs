
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Attributes;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.UpdateTableIndex)]
public sealed class UpdateTableIndexLog : IJournalLog
{
    [JournalField(0)]
    public uint Sequence { get; }

    public UpdateTableIndexLog(uint sequence)
    {
        Sequence = sequence;
    }
}
