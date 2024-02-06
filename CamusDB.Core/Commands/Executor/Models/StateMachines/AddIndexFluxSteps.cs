
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
public enum AddIndexFluxSteps
{
    NotInitialized = 0,
    AllocateNewIndex = 1,
    TryAdquireLocks = 2,
    LocateTuplesToFeedTheIndex = 3,    
    FeedTheIndex = 4,    
    AddSystemObject = 5,
}
