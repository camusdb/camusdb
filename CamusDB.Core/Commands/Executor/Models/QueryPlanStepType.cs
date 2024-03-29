﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public enum QueryPlanStepType
{
    FullScanFromIndex,
    FullScanFromTableIndex,
    SortBy,
    Aggregate,
    ReduceToProjections,
    Limit,
    QueryFromIndex
}
