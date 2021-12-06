
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Models;

public sealed class JournalInsert
{
    public InsertTicket InsertTicket { get; }

    public JournalInsert(InsertTicket insertTicket)
    {
        InsertTicket = insertTicket;
    }
}
