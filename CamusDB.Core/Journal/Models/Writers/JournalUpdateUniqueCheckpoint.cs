
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Journal.Models.Writers;

public sealed class JournalUpdateUniqueCheckpoint
{
    public uint Sequence { get; }

    public TableIndexSchema Index { get; }

    public JournalUpdateUniqueCheckpoint(uint sequence, TableIndexSchema index)
    {
        Sequence = sequence;
        Index = index;
    }
}

