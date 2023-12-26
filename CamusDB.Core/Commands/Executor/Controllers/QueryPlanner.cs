
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class QueryPlanner
{
	public QueryPlanner()
	{
	}

    public QueryPlan GetPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        QueryPlan plan = new(database, table, ticket);

        if (!string.IsNullOrEmpty(ticket.IndexName))
            plan.AddStep(new QueryPlanStep(QueryPlanStepType.QueryFromIndex));
        else
            plan.AddStep(new QueryPlanStep(QueryPlanStepType.QueryFromTableIndex));

        if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)        
            plan.AddStep(new QueryPlanStep(QueryPlanStepType.SortBy));

        return plan;
    }
}

