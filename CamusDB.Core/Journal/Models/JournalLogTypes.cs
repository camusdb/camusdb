
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models;

public enum JournalLogTypes
{
    InsertTicket = 0,
    InsertSlots = 1,
    WritePage = 2,
    UpdateUniqueIndex = 3,
    UpdateUniqueIndexCheckpoint = 4,
    InsertTicketCheckpoint = 5
}
