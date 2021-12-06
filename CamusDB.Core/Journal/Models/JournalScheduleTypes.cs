
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models;

public static class JournalScheduleTypes
{
    public const int InsertTicket = 0;
    public const int InsertSlots = 1;
    public const int WritePage = 2;
    public const int UpdateUniqueIndex = 3;
}
