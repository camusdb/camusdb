
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryFilterer
{
    internal bool MeetPlanFilter(QueryPlan plan, Dictionary<string, ColumnValue> row)
    {
        NodeAst? filter = plan.ExecutionFilter;

        if (filter is null)
            return true;

        return MeetWhere(filter, row, plan.Ticket);
    }

    internal bool MeetWhere(NodeAst where, Dictionary<string, ColumnValue> row, QueryTicket ticket)
    {
        ColumnValue evaluatedExpr = SqlExecutor.EvalExpr(where, row, ticket.Parameters, ticket.RowNameResolver);

        return evaluatedExpr.Type switch
        {
            ColumnType.Null => false,
            ColumnType.Bool => evaluatedExpr.BoolValue,
            ColumnType.Float64 => evaluatedExpr.LongValue != 0,
            ColumnType.Integer64 => evaluatedExpr.LongValue != 0,
            _ => false,
        };
    }
}
