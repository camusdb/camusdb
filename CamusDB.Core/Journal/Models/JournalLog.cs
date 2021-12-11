
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
    public JournalLogTypes Type { get; }

    public InsertLog InsertTicketLog { get; }

    public JournalLog(JournalLogTypes type, InsertLog insertTicketLog)
    {
        Type = type;
        InsertTicketLog = insertTicketLog;
    }
}
