﻿
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
    AllocateInsertTuple = 2,    
    InsertToPage = 3,
    AdquireLocks = 4,
    UpdateTableIndex = 5,
    UpdateUniqueIndexes = 6,
    UpdateMultiIndexes = 7,
    PersistIndexChanges = 8,
    ApplyPageOperations = 9,
    ReleaseLocks = 10
}
