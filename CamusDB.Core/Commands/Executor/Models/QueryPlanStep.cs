
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public record struct QueryPlanStep
{
    public QueryPlanStepType Type { get; }

    public TableIndexSchema? Index { get; } = null;

    public ColumnValue? ColumnValue { get; } = null;

    public QueryPlanStep(QueryPlanStepType type, TableIndexSchema? index = null, ColumnValue? columnValue = null)
    {
        Type = type;
        Index = index;
        ColumnValue = columnValue;
    }
}