
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public enum InsertFluxSteps
{
    NotInitialized = 0,
    CheckUniqueKeys = 1,
    UpdateUniqueKeys = 2,
    InsertToPage = 3,
    UpdateTableIndex = 4,
    UpdateMultiIndexes = 5,
    CheckpointInsert = 6
}
