
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Indexes;

namespace CamusDB.Core.Journal.Models;

public class JournalUpdateUniqueIndex
{
    public uint Sequence { get; }

    public TableIndexSchema Index { get; }

    public JournalUpdateUniqueIndex(uint sequence, TableIndexSchema index)
    {
        Sequence = sequence;
        Index = index;

    }
}

