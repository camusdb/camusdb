
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Journal.Attributes;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.UpdateUniqueIndexCheckpoint)]
public sealed class UpdateUniqueCheckpointLog : IJournalLog
{
    [JournalField(0)]
    public uint Sequence { get; }

    [JournalField(1)]
    public string ColumnIndex { get; }

    public UpdateUniqueCheckpointLog(uint sequence, string columnIndex)
    {
        Sequence = sequence;
        ColumnIndex = columnIndex;
    }
}

