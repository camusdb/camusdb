
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

    public JournalLog(uint sequence, JournalLogTypes type, InsertLog insertTicketLog)
    {
        Sequence = sequence;
        Type = type;
        InsertLog = insertTicketLog;
    }

    public JournalLog(uint sequence, JournalLogTypes type, InsertSlotsLog insertTicketLog)
    {
        Sequence = sequence;
        Type = type;
        InsertSlotsLog = insertTicketLog;
    }    
}
