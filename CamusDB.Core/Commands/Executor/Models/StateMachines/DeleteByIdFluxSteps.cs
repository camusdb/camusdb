
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

/**
 * The elements in this enum are the steps required to delete a row by id
 * Keeping the order is very important to ensure the proper operation
 */
public enum DeleteByIdFluxSteps
{
    NotInitialized = 0,
    AdquireLocks = 1,
    LocateTupleToDelete = 2,
    DeleteUniqueIndexes = 3,
    DeleteMultiIndexes = 4,
    DeleteRow = 5,
    ReleaseLocks = 6
}
