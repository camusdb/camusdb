
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
    AlterSchema = 1,
    LocateTupleToAlterColumn = 2,
    UpdateUniqueIndexes = 3,
    UpdateMultiIndexes = 4,
    AlterColumnRow = 5,
    ApplyPageOperations = 6,    
}
