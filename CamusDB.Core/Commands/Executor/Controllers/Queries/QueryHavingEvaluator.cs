
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

/// <summary>
/// Evaluates <c>HAVING</c> predicates against post-aggregation workspace rows.
/// </summary>
internal static class QueryHavingEvaluator
{
    public static ColumnValue Evaluate(
        NodeAst expression,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        Dictionary<string, ColumnValue>? parameters)
    {
        // Compound aggregate (e.g. COALESCE(SUM(x),0), SUM(x)+1): the workspace pre-computed its
        // value and stored it under the same output name that CollectHiddenExpressions used when
        // adding the hidden projection. Look it up directly rather than re-evaluating the
        // expression (whose aggregate sub-calls have no values available at evaluation time).
        if (QueryExpressionClassifier.IsCompoundAggregateProjection(expression))
            return LookupRowValue(row, ResolveAggregateOutputName(expression, ticket));

        switch (expression.nodeType)
        {
            case NodeType.Identifier:
                return LookupRowValue(row, expression.yytext!);

            case NodeType.ExprAlias:
                return Evaluate(expression.leftAst!, row, ticket, parameters);

            case NodeType.ExprFuncCall:
                if (QueryExpressionClassifier.IsAggregateProjection(expression))
                    return LookupRowValue(row, ResolveAggregateOutputName(expression, ticket));

                return SqlExecutor.EvalExpr(expression, row, parameters, rowNameResolver: null);

            case NodeType.ExprCast:
                return SqlExecutor.EvalExpr(expression, row, parameters, rowNameResolver: null);

            case NodeType.ExprEquals:
                return Compare(row, ticket, parameters, expression, static (left, right) => left.CompareTo(right) == 0);

            case NodeType.ExprNotEquals:
                return Compare(row, ticket, parameters, expression, static (left, right) => left.CompareTo(right) != 0);

            case NodeType.ExprLessThan:
                return Compare(row, ticket, parameters, expression, static (left, right) => left.CompareTo(right) < 0);

            case NodeType.ExprGreaterThan:
                return Compare(row, ticket, parameters, expression, static (left, right) => left.CompareTo(right) > 0);

            case NodeType.ExprLessEqualsThan:
                return Compare(row, ticket, parameters, expression, static (left, right) => left.CompareTo(right) <= 0);

            case NodeType.ExprGreaterEqualsThan:
                return Compare(row, ticket, parameters, expression, static (left, right) => left.CompareTo(right) >= 0);

            case NodeType.ExprBetween:
                return SqlExecutor.EvalExpr(expression, row, parameters, rowNameResolver: null);

            case NodeType.ExprOr:
            {
                ColumnValue leftValue = Evaluate(expression.leftAst!, row, ticket, parameters);
                ColumnValue rightValue = Evaluate(expression.rightAst!, row, ticket, parameters);

                if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"No matching signature for operator OR for argument types: {leftValue.Type}, {rightValue.Type}");
                }

                return ColumnValue.FromBool(leftValue.BoolValue || rightValue.BoolValue);
            }

            case NodeType.ExprAnd:
            {
                ColumnValue leftValue = Evaluate(expression.leftAst!, row, ticket, parameters);
                ColumnValue rightValue = Evaluate(expression.rightAst!, row, ticket, parameters);

                if (leftValue.Type != ColumnType.Bool || rightValue.Type != ColumnType.Bool)
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"No matching signature for operator AND for argument types: {leftValue.Type}, {rightValue.Type}");
                }

                return ColumnValue.FromBool(leftValue.BoolValue && rightValue.BoolValue);
            }

            case NodeType.ExprNot:
                {
                    ColumnValue value = Evaluate(expression.leftAst!, row, ticket, parameters);
                    if (value.Type == ColumnType.Null)
                        return ColumnValue.Null;
                    if (value.Type != ColumnType.Bool)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator NOT for argument type: {value.Type}");
                    return ColumnValue.FromBool(!value.BoolValue);
                }

            case NodeType.ExprIsNull:
                return ColumnValue.FromBool(
                    Evaluate(expression.leftAst!, row, ticket, parameters).Type == ColumnType.Null);

            case NodeType.ExprIsNotNull:
                return ColumnValue.FromBool(
                    Evaluate(expression.leftAst!, row, ticket, parameters).Type != ColumnType.Null);

            case NodeType.ExprIsTrue:
            case NodeType.ExprIsNotTrue:
            case NodeType.ExprIsFalse:
            case NodeType.ExprIsNotFalse:
                return ColumnValue.FromBool(BooleanTruthTest.Evaluate(
                    expression.nodeType,
                    Evaluate(expression.leftAst!, row, ticket, parameters)));

            default:
                return SqlExecutor.EvalExpr(expression, row, parameters, rowNameResolver: null);
        }
    }

    private static ColumnValue Compare(
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        Dictionary<string, ColumnValue>? parameters,
        NodeAst expression,
        Func<ColumnValue, ColumnValue, bool> compare)
    {
        ColumnValue leftValue = Evaluate(expression.leftAst!, row, ticket, parameters);
        ColumnValue rightValue = Evaluate(expression.rightAst!, row, ticket, parameters);
        return ColumnValue.FromBool(compare(leftValue, rightValue));
    }

    private static ColumnValue LookupRowValue(IReadOnlyDictionary<string, ColumnValue> row, string key)
    {
        if (row.TryGetValue(key, out ColumnValue? value))
            return value;

        throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, "Unknown column: " + key);
    }

    private static string ResolveAggregateOutputName(NodeAst aggregateExpression, QueryTicket ticket)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(aggregateExpression);

        if (ticket.Projection is not null)
        {
            for (int i = 0; i < ticket.Projection.Count; i++)
            {
                if (!QueryAstComparer.AreEquivalent(
                    QueryExpressionClassifier.UnwrapAlias(ticket.Projection[i]),
                    target))
                {
                    continue;
                }

                return QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[i], i);
            }
        }

        int syntheticIndex = ticket.Projection?.Count ?? 0;

        if (ticket.OrderBy is not null && ticket.GroupBy is { Count: > 0 })
        {
            HashSet<string> outputNames = new(StringComparer.Ordinal);

            if (ticket.Projection is not null)
            {
                for (int i = 0; i < ticket.Projection.Count; i++)
                    outputNames.Add(QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[i], i));
            }

            foreach (QueryOrderBy orderClause in ticket.OrderBy)
            {
                if (outputNames.Contains(orderClause.ColumnName))
                    continue;

                syntheticIndex++;
                outputNames.Add(orderClause.ColumnName);
            }
        }

        return QueryProjectionResolver.GetOutputNameFromProjectionExpression(target, syntheticIndex);
    }
}
