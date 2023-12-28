
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

/**
 * The elements in this enum are the steps required to insert a row
 * Keeping the order is very important to ensure the proper operation
 */
public enum InsertFluxSteps
{
    NotInitialized = 0,
    CheckUniqueKeys = 1,
    AdquireLocks = 2,
    AllocateInsertTuple = 3,
    UpdateUniqueKeys = 4,
    InsertToPage = 5,
    UpdateTableIndex = 6,
    UpdateMultiIndexes = 7,
    ApplyPageOperations = 8,
    ReleaseLocks = 9
}
