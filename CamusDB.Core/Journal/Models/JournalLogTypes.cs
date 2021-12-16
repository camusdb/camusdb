
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models;

public enum JournalLogTypes
{
    Insert = 100,
    InsertSlots = 101,
    WritePage = 102,
    UpdateUniqueIndex = 103,
    UpdateUniqueIndexCheckpoint = 104,
    InsertCheckpoint = 105,

    FlushedPages = 999
}
