
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Validates and resolves identifiers in post-aggregate scopes (ORDER BY, HAVING).
/// </summary>
internal static class QueryPostAggregateScopeValidator
{
    public static void ValidateHaving(SelectQuery query, QueryRowNameResolver rowNames)
    {
        if (query.Having is null)
            return;

        bool hasGroupBy = query.GroupBy is { Count: > 0 };
        bool hasAggregateProjection = false;

        foreach (ProjectionItem projection in query.Projections)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(projection.Expression)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(projection.Expression))
            {
                hasAggregateProjection = true;
                break;
            }
        }

        if (!hasGroupBy && !hasAggregateProjection)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "HAVING requires GROUP BY or an aggregate projection");
        }

        ValidatePostAggregateExpression(query.Having.Expression, query, rowNames, insideAggregate: false);
    }

    public static void ValidatePostAggregateExpression(
        NodeAst expression,
        SelectQuery query,
        QueryRowNameResolver rowNames,
        bool insideAggregate)
    {
        switch (expression.nodeType)
        {
            case NodeType.Identifier:
                if (insideAggregate)
                {
                    rowNames.ValidateColumnReference(expression.yytext!);
                    return;
                }

                if (TryResolvePostAggregateColumn(expression, query, out _))
                    return;

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "HAVING must reference a SELECT projection, GROUP BY expression, or aggregate");
            case NodeType.ExprAlias:
                ValidatePostAggregateExpression(expression.leftAst!, query, rowNames, insideAggregate);
                return;

            case NodeType.ExprFuncCall:
                if (QueryExpressionClassifier.IsAggregateProjection(expression))
                {
                    if (expression.rightAst is not null)
                        ValidatePostAggregateExpression(expression.rightAst, query, rowNames, insideAggregate: true);

                    return;
                }

                if (expression.rightAst is not null)
                    ValidatePostAggregateExpression(expression.rightAst, query, rowNames, insideAggregate);

                return;

            case NodeType.ExprCast:
                if (expression.leftAst is not null)
                    ValidatePostAggregateExpression(expression.leftAst, query, rowNames, insideAggregate);

                return;

            case NodeType.ExprArgumentList:
                if (expression.leftAst is not null)
                    ValidatePostAggregateExpression(expression.leftAst, query, rowNames, insideAggregate);

                if (expression.rightAst is not null)
                    ValidatePostAggregateExpression(expression.rightAst, query, rowNames, insideAggregate);

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
            case NodeType.ExprDiv:
            case NodeType.ExprLike:
            case NodeType.ExprILike:
                if (expression.leftAst is not null)
                    ValidatePostAggregateExpression(expression.leftAst, query, rowNames, insideAggregate);

                if (expression.rightAst is not null)
                    ValidatePostAggregateExpression(expression.rightAst, query, rowNames, insideAggregate);

                return;

            case NodeType.ExprBetween:
                if (expression.leftAst is not null)
                    ValidatePostAggregateExpression(expression.leftAst, query, rowNames, insideAggregate);

                if (expression.extendedOne is not null)
                    ValidatePostAggregateExpression(expression.extendedOne, query, rowNames, insideAggregate);

                if (expression.extendedTwo is not null)
                    ValidatePostAggregateExpression(expression.extendedTwo, query, rowNames, insideAggregate);

                return;

            case NodeType.ExprNot:
            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
                if (expression.leftAst is not null)
                    ValidatePostAggregateExpression(expression.leftAst, query, rowNames, insideAggregate);

                return;

            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
            case NodeType.ExprAllFields:
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidAstStmt,
                    $"Unsupported HAVING expression node: {expression.nodeType}");
        }
    }

    public static bool TryResolvePostAggregateColumn(
        NodeAst expression,
        SelectQuery query,
        out string columnName)
    {
        return QueryProjectionResolver.TryResolvePostAggregateOrderColumn(
            expression,
            query.Projections,
            query.GroupBy,
            out columnName);
    }

    public static bool TryResolvePostAggregateExpression(
        NodeAst expression,
        SelectQuery query,
        out string columnName,
        out NodeAst resolvedExpression)
    {
        if (TryResolvePostAggregateColumn(expression, query, out columnName))
        {
            resolvedExpression = ResolvePostAggregateExpressionAst(expression, query, columnName);
            return true;
        }

        columnName = "";
        resolvedExpression = expression;
        return false;
    }

    private static NodeAst ResolvePostAggregateExpressionAst(
        NodeAst expression,
        SelectQuery query,
        string columnName)
    {
        if (query.GroupBy is { Count: > 0 })
        {
            for (int i = 0; i < query.GroupBy.Count; i++)
            {
                if (!string.Equals(
                    QueryProjectionResolver.GetGroupByOutputName(query.GroupBy[i], i),
                    columnName,
                    StringComparison.Ordinal))
                {
                    continue;
                }

                return query.GroupBy[i];
            }
        }

        for (int i = 0; i < query.Projections.Count; i++)
        {
            ProjectionItem projection = query.Projections[i];

            if (!string.Equals(
                QueryProjectionResolver.GetOutputName(projection, i),
                columnName,
                StringComparison.Ordinal))
            {
                continue;
            }

            return QueryExpressionClassifier.UnwrapAlias(projection.Expression);
        }

        return expression;
    }
}
