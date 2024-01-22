
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
public enum UpdateByIdFluxSteps
{
    NotInitialized = 0,
    LocateTupleToUpdate = 1,
    UpdateRow = 2,
    AdquireLocks = 3,
    UpdateTableIndex = 4,
    UpdateUniqueIndexes = 5,
    UpdateMultiIndexes = 6,
    PersistIndexChanges = 7,
    ApplyPageOperations = 8,    
    ReleaseLocks = 9
}
