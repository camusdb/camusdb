
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models;

public static class JournalLogTypes
{
    public const short InsertTicket = 0;
    public const short InsertSlots = 1;
    public const short WritePage = 2;
    public const short UpdateUniqueIndex = 3;
    public const short UpdateUniqueIndexCheckpoint = 4;
    public const short InsertTicketCheckpoint = 5;
}
