
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Classifies parsed projection and filter expressions for query validation.
/// </summary>
internal static class QueryExpressionClassifier
{
    public static NodeAst UnwrapAlias(NodeAst expression)
    {
        return expression.nodeType == NodeType.ExprAlias
            ? expression.leftAst!
            : expression;
    }

    public static bool IsAggregateProjection(NodeAst expression)
    {
        NodeAst target = UnwrapAlias(expression);

        if (target.nodeType != NodeType.ExprFuncCall || target.leftAst?.yytext is null)
            return false;

        return target.leftAst.yytext.ToLowerInvariant() switch
        {
            "count" or "max" or "min" or "sum" or "avg" or "distinct" => true,
            _ => false,
        };
    }
}
