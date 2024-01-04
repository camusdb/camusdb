
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

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

        if (ticket.Projection is not null && ticket.Projection.Count > 0)
        {
            if (HasAggregation(ticket.Projection))
                plan.AddStep(new QueryPlanStep(QueryPlanStepType.Aggregate));
            else
            {
                if (!IsFullProjection(ticket.Projection))
                    plan.AddStep(new QueryPlanStep(QueryPlanStepType.ReduceToProjections));
            }
        }

        return plan;
    }

    private bool IsFullProjection(List<NodeAst> projection)
    {
        return projection.Count == 1 && projection[0].nodeType == NodeType.ExprAllFields;            
    }

    private static bool HasAggregation(List<NodeAst> projection)
    {
        foreach (NodeAst nodeAst in projection)
        {
            if (nodeAst.nodeType == NodeType.ExprFuncCall)
            {
                switch (nodeAst.leftAst!.yytext!.ToLowerInvariant())
                {
                    case "count":
                    case "max":
                    case "min":
                    case "sum":
                    case "avg":
                    case "distinct":

                        if (projection.Count > 1)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Aggregations cannot be accompanied by other projections or expressions.");

                        return true;
                }
            }
        }

        return false;
    }
}

