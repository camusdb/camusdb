
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

/**
 * The elements in this enum are the steps required to alter a column in a table
 * Keeping the order is very important to ensure the proper operation
 */
public enum AlterColumnFluxSteps
{
    NotInitialized = 0,    
    LocateTupleToAlterColumn = 1,
    UpdateUniqueIndexes = 2,
    UpdateMultiIndexes = 3,
    AlterColumnRow = 4,
    ApplyPageOperations = 5
}
