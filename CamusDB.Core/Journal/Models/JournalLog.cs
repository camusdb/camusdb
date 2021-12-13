
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models.Logs;

namespace CamusDB.Core.Journal.Models;

public class JournalLog
{
    public uint Sequence { get; }

    public JournalLogTypes Type { get; }

    public InsertLog? InsertLog { get; }

    public InsertSlotsLog? InsertSlotsLog { get; }

    public InsertCheckpointLog? InsertCheckpointLog { get; }

    public UpdateUniqueCheckpointLog? UpdateUniqueCheckpointLog { get; }

    public WritePageLog? WritePageLog { get; }

    public JournalLog(uint sequence, JournalLogTypes type, InsertLog insertLog)
    {
        Sequence = sequence;
        Type = type;
        InsertLog = insertLog;
    }

    public JournalLog(uint sequence, JournalLogTypes type, InsertSlotsLog insertSlotsLog)
    {
        Sequence = sequence;
        Type = type;
        InsertSlotsLog = insertSlotsLog;
    }

    public JournalLog(uint sequence, JournalLogTypes type, InsertCheckpointLog insertLog)
    {
        Sequence = sequence;
        Type = type;
        InsertCheckpointLog = insertLog;
    }

    public JournalLog(uint sequence, JournalLogTypes type, WritePageLog writePageLog)
    {
        Sequence = sequence;
        Type = type;
        WritePageLog = writePageLog;
    }

    public JournalLog(uint sequence, JournalLogTypes type, UpdateUniqueCheckpointLog checkpointLog)
    {
        Sequence = sequence;
        Type = type;
        UpdateUniqueCheckpointLog = checkpointLog;
    }
}
