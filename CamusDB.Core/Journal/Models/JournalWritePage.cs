
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Models;

public sealed class JournalWritePage
{
    public uint Sequence { get; }

    public byte[] Data { get; }

    public JournalWritePage(uint sequence, byte[] data)
    {
        Sequence = sequence;
        Data = data;
    }
}

