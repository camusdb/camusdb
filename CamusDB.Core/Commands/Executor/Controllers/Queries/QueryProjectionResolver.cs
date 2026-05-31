
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
/// Resolves SELECT projection output names and post-aggregate ORDER BY targets.
/// </summary>
internal static class QueryProjectionResolver
{
    public static string GetOutputName(ProjectionItem projection, int index)
    {
        if (!string.IsNullOrEmpty(projection.OutputName))
            return projection.OutputName;

        return GetOutputNameFromProjectionExpression(projection.Expression, index);
    }

    public static string GetOutputNameFromProjectionExpression(NodeAst expression, int index)
    {
        return expression.nodeType switch
        {
            NodeType.ExprAlias => expression.rightAst!.yytext!,
            NodeType.Identifier => GetBareColumnName(expression.yytext!),
            _ => index.ToString(),
        };
    }

    private static string GetBareColumnName(string identifier)
    {
        int dotIndex = identifier.LastIndexOf('.');
        return dotIndex >= 0 ? identifier[(dotIndex + 1)..] : identifier;
    }

    public static string GetGroupByOutputName(NodeAst groupExpression, int index)
    {
        return groupExpression.nodeType switch
        {
            NodeType.Identifier => groupExpression.yytext!,
            _ => $"$groupby{index}",
        };
    }

    public static bool TryResolvePostAggregateOrderColumn(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        IReadOnlyList<NodeAst>? groupBy,
        out string columnName)
    {
        if (TryResolveProjectionOrderColumn(orderExpression, projections, out columnName))
            return true;

        if (groupBy is not { Count: > 0 })
        {
            columnName = "";
            return false;
        }

        NodeAst orderExpr = QueryExpressionClassifier.UnwrapAlias(orderExpression);

        for (int i = 0; i < groupBy.Count; i++)
        {
            if (!QueryAstComparer.AreEquivalent(groupBy[i], orderExpr))
                continue;

            columnName = GetGroupByOutputName(groupBy[i], i);
            return true;
        }

        columnName = "";
        return false;
    }

    private static bool TryResolveProjectionOrderColumn(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        out string columnName)
    {
        if (orderExpression.nodeType == NodeType.Identifier)
        {
            string name = orderExpression.yytext!;

            for (int i = 0; i < projections.Count; i++)
            {
                ProjectionItem projection = projections[i];

                if (string.Equals(projection.OutputName, name, StringComparison.Ordinal))
                {
                    columnName = GetOutputName(projection, i);
                    return true;
                }
            }
        }

        NodeAst orderExpr = QueryExpressionClassifier.UnwrapAlias(orderExpression);

        for (int i = 0; i < projections.Count; i++)
        {
            ProjectionItem projection = projections[i];

            if (QueryAstComparer.AreEquivalent(
                QueryExpressionClassifier.UnwrapAlias(projection.Expression),
                orderExpr))
            {
                columnName = GetOutputName(projection, i);
                return true;
            }
        }

        columnName = "";
        return false;
    }
}
