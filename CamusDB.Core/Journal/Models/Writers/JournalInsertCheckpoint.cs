
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Models.Writers;

public class JournalInsertCheckpoint
{
    public uint Sequence { get; }

    public JournalInsertCheckpoint(uint sequence)
    {
        Sequence = sequence;
    }
}
