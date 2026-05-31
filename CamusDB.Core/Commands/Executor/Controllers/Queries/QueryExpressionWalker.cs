
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Walks parsed expression trees to collect column references for binding.
/// </summary>
internal static class QueryExpressionWalker
{
    public static void CollectColumnReferences(NodeAst expr, ICollection<string> identifiers)
    {
        switch (expr.nodeType)
        {
            case NodeType.Identifier:
                identifiers.Add(expr.yytext!);
                return;

            case NodeType.ExprAlias:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

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
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                return;

            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                return;

            case NodeType.ExprFuncCall:
                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                return;

            case NodeType.ExprArgumentList:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                return;

            case NodeType.ExprAllFields:
            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidAstStmt,
                    $"Unsupported expression node during binding: {expr.nodeType}");
        }
    }
}
