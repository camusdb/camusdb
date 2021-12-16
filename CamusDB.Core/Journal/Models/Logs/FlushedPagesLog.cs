
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Attributes;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.FlushedPages)]
public sealed class FlushedPagesLog : IJournalLog
{
    [JournalField(0)]
    public uint Sequence { get; }

    public FlushedPagesLog(uint sequence)
    {
        Sequence = sequence;
    }
}

