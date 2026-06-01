
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Collects hidden aggregate-workspace columns required to evaluate HAVING predicates.
/// </summary>
internal static class QueryHavingWorkspace
{
    public static bool NeedsExpandedGlobalAggregate(QueryTicket ticket)
    {
        if (ticket.Having is null || ticket.Projection is null)
            return false;

        return HasHiddenExpressions(ticket.Having, ticket);
    }

    public static bool HasHiddenExpressions(NodeAst havingExpr, QueryTicket ticket)
    {
        HashSet<string> outputNames = new(StringComparer.Ordinal);

        if (ticket.Projection is not null)
        {
            for (int i = 0; i < ticket.Projection.Count; i++)
                outputNames.Add(QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[i], i));
        }

        return ContainsHiddenExpression(havingExpr, ticket, outputNames, insideAggregate: false);
    }

    public static void AddHiddenProjections(
        NodeAst havingExpr,
        QueryTicket ticket,
        List<QueryAggregator.AnalyzedProjection> projections,
        HashSet<string> outputNames)
    {
        CollectHiddenExpressions(havingExpr, ticket, projections, outputNames, insideAggregate: false);
    }

    private static bool ContainsHiddenExpression(
        NodeAst expression,
        QueryTicket ticket,
        HashSet<string> outputNames,
        bool insideAggregate)
    {
        switch (expression.nodeType)
        {
            case NodeType.Identifier:
                if (insideAggregate)
                    return false;

                if (outputNames.Contains(expression.yytext!))
                    return false;

                return TryResolveHiddenExpression(expression, ticket, outputNames, out _);

            case NodeType.ExprAlias:
                return ContainsHiddenExpression(expression.leftAst!, ticket, outputNames, insideAggregate);

            case NodeType.ExprFuncCall:
                if (QueryExpressionClassifier.IsAggregateProjection(expression))
                {
                    if (insideAggregate)
                        return false;

                    if (TryFindMatchingProjection(expression, ticket.Projection, out _))
                        return false;

                    return true;
                }

                return expression.rightAst is not null
                    && ContainsHiddenExpression(expression.rightAst, ticket, outputNames, insideAggregate);

            case NodeType.ExprCast:
                return expression.leftAst is not null
                    && ContainsHiddenExpression(expression.leftAst, ticket, outputNames, insideAggregate);

            case NodeType.ExprArgumentList:
                return (expression.leftAst is not null
                        && ContainsHiddenExpression(expression.leftAst, ticket, outputNames, insideAggregate))
                    || (expression.rightAst is not null
                        && ContainsHiddenExpression(expression.rightAst, ticket, outputNames, insideAggregate));

            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprLessThan:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprGreaterEqualsThan:
            case NodeType.ExprOr:
            case NodeType.ExprAnd:
            case NodeType.ExprAdd:
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprLike:
            case NodeType.ExprILike:
                return (expression.leftAst is not null
                        && ContainsHiddenExpression(expression.leftAst, ticket, outputNames, insideAggregate))
                    || (expression.rightAst is not null
                        && ContainsHiddenExpression(expression.rightAst, ticket, outputNames, insideAggregate));

            case NodeType.ExprBetween:
                return (expression.leftAst is not null
                        && ContainsHiddenExpression(expression.leftAst, ticket, outputNames, insideAggregate))
                    || (expression.extendedOne is not null
                        && ContainsHiddenExpression(expression.extendedOne, ticket, outputNames, insideAggregate))
                    || (expression.extendedTwo is not null
                        && ContainsHiddenExpression(expression.extendedTwo, ticket, outputNames, insideAggregate));

            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
                return expression.leftAst is not null
                    && ContainsHiddenExpression(expression.leftAst, ticket, outputNames, insideAggregate);

            default:
                return false;
        }
    }

    private static void CollectHiddenExpressions(
        NodeAst expression,
        QueryTicket ticket,
        List<QueryAggregator.AnalyzedProjection> projections,
        HashSet<string> outputNames,
        bool insideAggregate)
    {
        switch (expression.nodeType)
        {
            case NodeType.Identifier:
                if (insideAggregate || outputNames.Contains(expression.yytext!))
                    return;

                if (!TryResolveHiddenExpression(expression, ticket, outputNames, out QueryAggregator.AnalyzedProjection hidden))
                    return;

                projections.Add(hidden);
                outputNames.Add(hidden.OutputName);
                return;

            case NodeType.ExprAlias:
                CollectHiddenExpressions(expression.leftAst!, ticket, projections, outputNames, insideAggregate);
                return;

            case NodeType.ExprFuncCall:
                if (QueryExpressionClassifier.IsAggregateProjection(expression))
                {
                    if (insideAggregate)
                        return;

                    if (TryFindMatchingProjection(expression, ticket.Projection, out _))
                        return;

                    string outputName = QueryProjectionResolver.GetOutputNameFromProjectionExpression(
                        expression,
                        projections.Count);

                    if (outputNames.Contains(outputName))
                        return;

                    projections.Add(new QueryAggregator.AnalyzedProjection(
                        expression,
                        outputName,
                        IsAggregate: true,
                        FuncCall: QueryAggregator.GetAggregateFuncCall(expression)));

                    outputNames.Add(outputName);
                    return;
                }

                if (expression.rightAst is not null)
                    CollectHiddenExpressions(expression.rightAst, ticket, projections, outputNames, insideAggregate);

                return;

            case NodeType.ExprCast:
                if (expression.leftAst is not null)
                    CollectHiddenExpressions(expression.leftAst, ticket, projections, outputNames, insideAggregate);

                return;

            case NodeType.ExprArgumentList:
                if (expression.leftAst is not null)
                    CollectHiddenExpressions(expression.leftAst, ticket, projections, outputNames, insideAggregate);

                if (expression.rightAst is not null)
                    CollectHiddenExpressions(expression.rightAst, ticket, projections, outputNames, insideAggregate);

                return;

            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprLessThan:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprGreaterEqualsThan:
            case NodeType.ExprOr:
            case NodeType.ExprAnd:
            case NodeType.ExprAdd:
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprLike:
            case NodeType.ExprILike:
                if (expression.leftAst is not null)
                    CollectHiddenExpressions(expression.leftAst, ticket, projections, outputNames, insideAggregate);

                if (expression.rightAst is not null)
                    CollectHiddenExpressions(expression.rightAst, ticket, projections, outputNames, insideAggregate);

                return;

            case NodeType.ExprBetween:
                if (expression.leftAst is not null)
                    CollectHiddenExpressions(expression.leftAst, ticket, projections, outputNames, insideAggregate);

                if (expression.extendedOne is not null)
                    CollectHiddenExpressions(expression.extendedOne, ticket, projections, outputNames, insideAggregate);

                if (expression.extendedTwo is not null)
                    CollectHiddenExpressions(expression.extendedTwo, ticket, projections, outputNames, insideAggregate);

                return;

            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
                if (expression.leftAst is not null)
                    CollectHiddenExpressions(expression.leftAst, ticket, projections, outputNames, insideAggregate);

                return;
        }
    }

    private static bool TryResolveHiddenExpression(
        NodeAst expression,
        QueryTicket ticket,
        HashSet<string> outputNames,
        out QueryAggregator.AnalyzedProjection hiddenProjection)
    {
        hiddenProjection = default;

        if (ticket.Projection is null)
            return false;

        List<ProjectionItem> projectionItems = ticket.Projection
            .Select((nodeAst, index) => new ProjectionItem(nodeAst, TryGetOutputName(nodeAst)))
            .ToList();

        if (!QueryProjectionResolver.TryResolvePostAggregateOrderColumn(
            expression,
            projectionItems,
            ticket.GroupBy,
            out string columnName))
        {
            return false;
        }

        if (outputNames.Contains(columnName))
            return false;

        NodeAst resolvedExpression = ResolveHiddenExpressionAst(expression, ticket, columnName);
        bool isAggregate = QueryExpressionClassifier.IsAggregateProjection(resolvedExpression);

        hiddenProjection = new QueryAggregator.AnalyzedProjection(
            resolvedExpression,
            columnName,
            isAggregate,
            isAggregate ? QueryAggregator.GetAggregateFuncCall(resolvedExpression) : null);

        return true;
    }

    private static NodeAst ResolveHiddenExpressionAst(
        NodeAst expression,
        QueryTicket ticket,
        string columnName)
    {
        if (ticket.GroupBy is { Count: > 0 })
        {
            for (int i = 0; i < ticket.GroupBy.Count; i++)
            {
                if (string.Equals(
                    QueryProjectionResolver.GetGroupByOutputName(ticket.GroupBy[i], i),
                    columnName,
                    StringComparison.Ordinal))
                {
                    return ticket.GroupBy[i];
                }
            }
        }

        if (ticket.Projection is not null)
        {
            for (int i = 0; i < ticket.Projection.Count; i++)
            {
                if (!string.Equals(
                    QueryProjectionResolver.GetOutputNameFromProjectionExpression(ticket.Projection[i], i),
                    columnName,
                    StringComparison.Ordinal))
                {
                    continue;
                }

                return QueryExpressionClassifier.UnwrapAlias(ticket.Projection[i]);
            }
        }

        return expression;
    }

    private static bool TryFindMatchingProjection(
        NodeAst expression,
        List<NodeAst>? projections,
        out NodeAst matchedProjection)
    {
        matchedProjection = expression;

        if (projections is null)
            return false;

        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

        foreach (NodeAst projection in projections)
        {
            if (!QueryAstComparer.AreEquivalent(QueryExpressionClassifier.UnwrapAlias(projection), target))
                continue;

            matchedProjection = projection;
            return true;
        }

        return false;
    }

    private static string? TryGetOutputName(NodeAst ast)
    {
        return ast.nodeType switch
        {
            NodeType.ExprAlias => ast.rightAst?.yytext,
            NodeType.Identifier => ast.yytext,
            _ => null,
        };
    }
}
