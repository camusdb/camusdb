
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

/**
 * The elements in this enum are the steps required to update a row by id
 * Keeping the order is very important to ensure the proper operation
 */
public enum UpdateFluxSteps
{
    NotInitialized = 0,
    AdquireLocks = 1,
    LocateTupleToUpdate = 2,
    UpdateUniqueIndexes = 3,
    UpdateMultiIndexes = 4,
    UpdateRow = 5,
    ApplyPageOperations = 6,
    ReleaseLocks = 7
}
