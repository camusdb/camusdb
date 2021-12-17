
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models;

public enum JournalFailureTypes
{
    None = 0,

    PreInsert = 1,
    PostInsert = 2,
    PreInsertSlots = 3,
    PostInsertSlots = 4,
    PreWritePage = 5,
    PostWritePage = 6,
    PreUpdateUniqueIndex = 7,
    PostUpdateUniqueIndex = 8,
    PreUpdateUniqueCheckpoint = 9,
    PostUpdateUniqueCheckpoint = 10,
    PreInsertCheckpoint = 11,
    PostInsertCheckpoint = 12
}

