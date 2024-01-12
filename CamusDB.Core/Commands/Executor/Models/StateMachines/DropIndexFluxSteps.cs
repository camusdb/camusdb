
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

/**
 * The elements in this enum are the steps required to alter an index in a table
 * Keeping the order is very important to ensure the proper operation
 */
public enum DropIndexFluxSteps
{
    NotInitialized = 0,
    LocateIndex = 1,
    DeleteIndexPages = 2,    
    PersistIndexChanges = 3,
    ApplyPageOperations = 4,
    RemoveSystemObject = 5,
}
