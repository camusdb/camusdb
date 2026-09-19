
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
            case NodeType.ExprDiv:
            case NodeType.ExprSubscript:
            case NodeType.ArrayLiteral:
            case NodeType.ExprLike:
            case NodeType.ExprILike:
            case NodeType.ExprRegexMatch:
            case NodeType.ExprRegexMatchCi:
            case NodeType.ExprRegexNotMatch:
            case NodeType.ExprRegexNotMatchCi:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                return;

            case NodeType.ExprNot:

            case NodeType.ExprNegate:
            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
            case NodeType.ExprIsTrue:
            case NodeType.ExprIsNotTrue:
            case NodeType.ExprIsFalse:
            case NodeType.ExprIsNotFalse:
            case NodeType.ExprInMembership:
            case NodeType.ExprNotInMembership:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                return;

            case NodeType.ExprInSubquery:
            case NodeType.ExprNotInSubquery:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                return;

            case NodeType.ExprBetween:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                if (expr.extendedOne is not null)
                    CollectColumnReferences(expr.extendedOne, identifiers);

                if (expr.extendedTwo is not null)
                    CollectColumnReferences(expr.extendedTwo, identifiers);

                return;

            // A nested SELECT is its own scope: the identifiers inside it bind against its own
            // sources, so none of them is a column reference of the query being walked. The
            // subquery is resolved (rewritten, prepared, or lifted) by its own stage before this
            // query's names are bound, exactly as an IN subquery's right-hand SELECT is above.
            case NodeType.ExprScalarSubquery:
            case NodeType.ExprExistsSubquery:
            case NodeType.ExprExistsCorrelated:
                return;

            case NodeType.ExprList:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                return;

            case NodeType.ExprFuncCall:
                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                return;

            case NodeType.ExprCast:
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                return;

            case NodeType.ExprCase:
                // Operand (simple CASE), the WHEN/THEN chain, and the ELSE result.
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

                if (expr.rightAst is not null)
                    CollectColumnReferences(expr.rightAst, identifiers);

                if (expr.extendedOne is not null)
                    CollectColumnReferences(expr.extendedOne, identifiers);

                return;

            case NodeType.ExprCaseWhen:
            case NodeType.ExprCaseWhenList:
                // Chain/clause nodes: leftAst and rightAst together cover every condition and result.
                if (expr.leftAst is not null)
                    CollectColumnReferences(expr.leftAst, identifiers);

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
