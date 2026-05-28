
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
            plan.AddStep(new(QueryPlanStepType.SortBy));

        if (ticket.Limit is not null || ticket.Offset is not null)
            plan.AddStep(new(QueryPlanStepType.Limit));

        if (ticket.Projection is not null && ticket.Projection.Count > 0)
        {
            if (HasAggregation(ticket.Projection))
                plan.AddStep(new(QueryPlanStepType.Aggregate));

            if (!IsFullProjection(ticket.Projection))
                plan.AddStep(new(QueryPlanStepType.ReduceToProjections));
        }

        return plan;
    }

    private static QueryPlanStep GetScanOrQueryType(TableDescriptor table, QueryTicket ticket)
    {
        if (ticket.Filters is not null && ticket.Filters.Count > 0)
        {
            QueryPlanStep? fromFilters = TryBuildStepFromFilters(table, ticket.Filters, ticket.Parameters);
            if (fromFilters is not null)
                return fromFilters.Value;
        }

        if (ticket.Where is not null)
        {
            QueryPlanStep? fromWhere = TryBuildStepFromWhere(table, ticket.Where, ticket.Parameters);
            if (fromWhere is not null)
                return fromWhere.Value;
        }

        if (!string.IsNullOrEmpty(ticket.IndexName))
            return new(QueryPlanStepType.FullScanFromIndex);

        return new(QueryPlanStepType.FullScanFromTableIndex);
    }

    /// <summary>
    /// Tries to build an index-based scan step from the structured <see cref="QueryFilter"/> list.
    /// Equality on an indexed column → <see cref="QueryPlanStepType.QueryFromIndex"/>.
    /// Any range operator(s) on the same indexed column → <see cref="QueryPlanStepType.RangeScanFromIndex"/>.
    /// </summary>
    private static QueryPlanStep? TryBuildStepFromFilters(
        TableDescriptor table,
        List<QueryFilter> filters,
        Dictionary<string, ColumnValue>? parameters)
    {
        // Group by column name so we can combine a lower + upper bound from multiple filters.
        Dictionary<string, List<QueryFilter>> byColumn = new(StringComparer.Ordinal);
        foreach (QueryFilter f in filters)
        {
            if (!byColumn.TryGetValue(f.ColumnName, out List<QueryFilter>? list))
                byColumn[f.ColumnName] = list = new();
            list.Add(f);
        }

        foreach ((string colName, List<QueryFilter> colFilters) in byColumn)
        {
            if (!table.Indexes.TryGetValue(colName, out TableIndexSchema? index))
                continue;

            // Check equality first (highest priority).
            foreach (QueryFilter f in colFilters)
            {
                if (f.Op == "=")
                    return new(QueryPlanStepType.QueryFromIndex, index, f.Value);
            }

            // Collect range bounds.
            CompositeColumnValue? fromBound = null;
            bool fromInclusive = true;
            CompositeColumnValue? toBound = null;
            bool toInclusive = true;
            bool hasRange = false;

            foreach (QueryFilter f in colFilters)
            {
                switch (f.Op)
                {
                    case ">":
                        fromBound = new CompositeColumnValue(new[] { f.Value });
                        fromInclusive = false;
                        hasRange = true;
                        break;
                    case ">=":
                        fromBound = new CompositeColumnValue(new[] { f.Value });
                        fromInclusive = true;
                        hasRange = true;
                        break;
                    case "<":
                        toBound = new CompositeColumnValue(new[] { f.Value });
                        toInclusive = false;
                        hasRange = true;
                        break;
                    case "<=":
                        toBound = new CompositeColumnValue(new[] { f.Value });
                        toInclusive = true;
                        hasRange = true;
                        break;
                }
            }

            if (hasRange)
                return new(QueryPlanStepType.RangeScanFromIndex, index, fromBound, fromInclusive, toBound, toInclusive);
        }

        return null;
    }

    /// <summary>
    /// Tries to build an index-based scan step from a parsed WHERE AST.
    /// Walks AND nodes recursively to combine a lower + upper range bound on the same column.
    /// </summary>
    private static QueryPlanStep? TryBuildStepFromWhere(
        TableDescriptor table,
        NodeAst where,
        Dictionary<string, ColumnValue>? parameters)
    {
        List<NodeAst> predicates = new();
        CollectPredicates(where, predicates);

        // Group by column name (identifier on the left side of a comparison).
        Dictionary<string, List<NodeAst>> byColumn = new(StringComparer.Ordinal);
        foreach (NodeAst pred in predicates)
        {
            if (pred.leftAst?.nodeType != NodeType.Identifier || pred.leftAst.yytext is null)
                continue;
            string col = pred.leftAst.yytext;
            if (!byColumn.TryGetValue(col, out List<NodeAst>? list))
                byColumn[col] = list = new();
            list.Add(pred);
        }

        foreach ((string colName, List<NodeAst> colPreds) in byColumn)
        {
            TableIndexSchema? matchedIndex = null;
            foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
            {
                if (kv.Value.Columns.Length == 1 && kv.Value.Columns[0] == colName)
                {
                    matchedIndex = kv.Value;
                    break;
                }
            }
            if (matchedIndex is null)
                continue;

            // Equality check (highest priority).
            foreach (NodeAst pred in colPreds)
            {
                if (pred.nodeType == NodeType.ExprEquals)
                {
                    if (TryGetConstant(pred.rightAst!, parameters, out ColumnValue? cv))
                        return new(QueryPlanStepType.QueryFromIndex, matchedIndex, cv);
                }
            }

            // Range bounds.
            CompositeColumnValue? fromBound = null;
            bool fromInclusive = true;
            CompositeColumnValue? toBound = null;
            bool toInclusive = true;
            bool hasRange = false;

            foreach (NodeAst pred in colPreds)
            {
                if (pred.rightAst is null)
                    continue;

                if (!TryGetConstant(pred.rightAst, parameters, out ColumnValue? cv) || cv is null)
                    continue;

                switch (pred.nodeType)
                {
                    case NodeType.ExprGreaterThan:
                        fromBound = new CompositeColumnValue(new[] { cv });
                        fromInclusive = false;
                        hasRange = true;
                        break;
                    case NodeType.ExprGreaterEqualsThan:
                        fromBound = new CompositeColumnValue(new[] { cv });
                        fromInclusive = true;
                        hasRange = true;
                        break;
                    case NodeType.ExprLessThan:
                        toBound = new CompositeColumnValue(new[] { cv });
                        toInclusive = false;
                        hasRange = true;
                        break;
                    case NodeType.ExprLessEqualsThan:
                        toBound = new CompositeColumnValue(new[] { cv });
                        toInclusive = true;
                        hasRange = true;
                        break;
                }
            }

            if (hasRange)
                return new(QueryPlanStepType.RangeScanFromIndex, matchedIndex, fromBound, fromInclusive, toBound, toInclusive);
        }

        return null;
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

    /// <summary>
    /// Recursively collects all leaf comparison predicates from a WHERE tree.
    /// Descends into AND nodes so that compound conditions like
    /// <c>col &gt; 5 AND col &lt; 10</c> contribute both bounds.
    /// </summary>
    private static void CollectPredicates(NodeAst where, ICollection<NodeAst> predicates)
    {
        switch (where.nodeType)
        {
            case NodeType.ExprEquals:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprGreaterEqualsThan:
            case NodeType.ExprLessThan:
            case NodeType.ExprLessEqualsThan:
                predicates.Add(where);
                return;

            case NodeType.ExprAnd:
                if (where.leftAst is not null)
                    CollectPredicates(where.leftAst, predicates);
                if (where.rightAst is not null)
                    CollectPredicates(where.rightAst, predicates);
                return;
        }
    }

    private static bool IsFullProjection(List<NodeAst> projection)
    {
        return projection is [{ nodeType: NodeType.ExprAllFields }];
    }

    private static bool HasAggregation(List<NodeAst> projection)
    {
        foreach (NodeAst nodeAst in projection)
        {
            switch (nodeAst.nodeType)
            {
                case NodeType.ExprFuncCall:
                    return CheckIfSupportedAggregation(nodeAst, projection);
                
                case NodeType.ExprAlias:
                    return CheckIfSupportedAggregation(nodeAst.leftAst!, projection);
            }
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

