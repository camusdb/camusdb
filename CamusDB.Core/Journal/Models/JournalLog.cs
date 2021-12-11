
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models.Readers;

namespace CamusDB.Core.Journal.Models;

public class JournalLog
{
    public JournalLogTypes Type { get; }

    public InsertTicketLog InsertTicketLog { get; }

    public JournalLog(JournalLogTypes type, InsertTicketLog insertTicketLog)
    {
        Type = type;
        InsertTicketLog = insertTicketLog;
    }
}
