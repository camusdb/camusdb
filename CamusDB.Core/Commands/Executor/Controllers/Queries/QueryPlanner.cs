
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Numerics;
using CamusDB.Core.Catalogs.Models;
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

        plan.AddStep(GetScanOrQueryType(table, ticket));

        if (ticket.OrderBy is not null && ticket.OrderBy.Count > 0)
            plan.AddStep(new QueryPlanStep(QueryPlanStepType.SortBy));

        if (ticket.Limit is not null || ticket.Offset is not null)
            plan.AddStep(new QueryPlanStep(QueryPlanStepType.Limit));

        if (ticket.Projection is not null && ticket.Projection.Count > 0)
        {
            if (HasAggregation(ticket.Projection))
                plan.AddStep(new QueryPlanStep(QueryPlanStepType.Aggregate));

            if (!IsFullProjection(ticket.Projection))
                plan.AddStep(new QueryPlanStep(QueryPlanStepType.ReduceToProjections));
        }

        return plan;
    }

    private static QueryPlanStep GetScanOrQueryType(TableDescriptor table, QueryTicket ticket)
    {
        if (ticket.Filters is not null && ticket.Filters.Count > 0)
        {
            foreach (QueryFilter filter in ticket.Filters)
            {
                if (table.Indexes.TryGetValue(filter.ColumnName, out TableIndexSchema? index))
                {
                    if (filter.Op == "=")
                        return new QueryPlanStep(QueryPlanStepType.QueryFromIndex, index);
                }
            }
        }

        if (ticket.Where is not null)
        {
            LinkedList<NodeAst> equalities = new();

            GetEqualities(ticket.Where, equalities);

            foreach (NodeAst equality in equalities)
            {
                if (equality.leftAst!.nodeType == NodeType.Identifier)
                {
                    foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
                    {
                        if (index.Value.Column == equality.leftAst!.yytext!)
                        {
                            if (TryGetConstant(equality.rightAst!, ticket.Parameters, out ColumnValue? columnValue))
                                return new QueryPlanStep(QueryPlanStepType.QueryFromIndex, index.Value, columnValue);
                        }
                    }
                }
            }            
        }

        if (!string.IsNullOrEmpty(ticket.IndexName))
            return new QueryPlanStep(QueryPlanStepType.FullScanFromIndex);

        return new QueryPlanStep(QueryPlanStepType.FullScanFromTableIndex);
    }

    private static bool TryGetConstant(NodeAst nodeAst, Dictionary<string, ColumnValue>? parameters, out ColumnValue? columnValue)
    {
        try
        {
            columnValue = SqlExecutor.EvalExpr(nodeAst, new(), parameters);
            if (columnValue.Type != ColumnType.Null)
                return true;
        }
        catch (CamusDBException)
        {

        }

        columnValue = null;
        return false;
    }

    private static void GetEqualities(NodeAst where, LinkedList<NodeAst> equalities)
    {
        if (where.nodeType == NodeType.ExprEquals)
        {
            equalities.AddLast(where);
            return;
        }

        if (where.nodeType.IsBinary())
        {
            if (where.leftAst is not null)
                GetEqualities(where.leftAst, equalities);

            if (where.rightAst is not null)
                GetEqualities(where.rightAst, equalities);
        }
    }

    private static bool IsFullProjection(List<NodeAst> projection)
    {
        return projection.Count == 1 && projection[0].nodeType == NodeType.ExprAllFields;
    }

    private static bool HasAggregation(List<NodeAst> projection)
    {
        foreach (NodeAst nodeAst in projection)
        {
            if (nodeAst.nodeType == NodeType.ExprFuncCall)
                return CheckIfSupportedAggregation(nodeAst, projection);

            if (nodeAst.nodeType == NodeType.ExprAlias)
                return CheckIfSupportedAggregation(nodeAst.leftAst!, projection);
        }

        return false;
    }

    private static bool CheckIfSupportedAggregation(NodeAst nodeAst, List<NodeAst> projection)
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

        return false;
    }
}

