
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Classifies parsed projection and filter expressions for query validation.
///
/// Three classification levels:
/// <list type="bullet">
///   <item><see cref="IsAggregateProjection"/> — the top-level call (after alias unwrap) is itself
///     one of count/max/min/sum/avg. Existing fast-path accumulator code depends on this exact shape.</item>
///   <item><see cref="ContainsAggregate"/> — any node anywhere in the expression tree is an aggregate call.
///     Used to detect compound forms like COALESCE(SUM(x),0) or SUM(a)+1 where the aggregate is buried.</item>
///   <item><see cref="IsCompoundAggregateProjection"/> — contains an aggregate but is NOT the bare top-level
///     form. These require the post-aggregation scalar evaluator path added in later work.</item>
/// </list>
///
/// Known limitation: <see cref="ContainsAggregate"/> and <see cref="ValidateNoNestedAggregate"/> recurse
/// into <c>ExprInMembership</c>/<c>ExprNotInMembership</c> via <c>leftAst</c> (the tested expression) only,
/// not into the right-hand candidate list. An aggregate inside an <c>IN (...)</c> value list is therefore
/// not detected. This is a degenerate case with no practical query, so it is left as-is.
/// </summary>
internal static class QueryExpressionClassifier
{
    public static NodeAst UnwrapAlias(NodeAst expression)
    {
        return expression.nodeType == NodeType.ExprAlias
            ? expression.leftAst!
            : expression;
    }

    /// <summary>
    /// Returns true when the expression (after alias unwrap) is a bare top-level aggregate call:
    /// <c>count(…)</c>, <c>max(…)</c>, <c>min(…)</c>, <c>sum(…)</c>, or <c>avg(…)</c>.
    /// Existing accumulator fast-paths depend on this exact shape — do not broaden it.
    /// </summary>
    public static bool IsAggregateProjection(NodeAst expression)
    {
        NodeAst target = UnwrapAlias(expression);

        if (target.nodeType != NodeType.ExprFuncCall || target.leftAst?.yytext is null)
            return false;

        return target.leftAst.yytext.ToLowerInvariant() switch
        {
            "count" or "max" or "min" or "sum" or "avg" => true,
            _ => false,
        };
    }

    /// <summary>
    /// Returns true when <paramref name="funcName"/> is the name of a SQL aggregate function.
    /// </summary>
    internal static bool IsAggregateName(string funcName) =>
        funcName.ToLowerInvariant() switch
        {
            "count" or "max" or "min" or "sum" or "avg" => true,
            _ => false,
        };

    /// <summary>
    /// Returns true when any node in the expression tree is an aggregate function call
    /// (count/max/min/sum/avg), regardless of its depth. Does not recurse into subqueries.
    /// </summary>
    public static bool ContainsAggregate(NodeAst expression)
    {
        return Walk(expression);

        static bool Walk(NodeAst node)
        {
            switch (node.nodeType)
            {
                case NodeType.ExprAlias:
                    return node.leftAst is not null && Walk(node.leftAst);

                case NodeType.ExprFuncCall:
                    if (node.leftAst?.yytext is not null && IsAggregateName(node.leftAst.yytext))
                        return true;
                    return node.rightAst is not null && Walk(node.rightAst);

                case NodeType.ExprArgumentList:
                    return (node.leftAst is not null && Walk(node.leftAst))
                        || (node.rightAst is not null && Walk(node.rightAst));

                case NodeType.ExprCast:
                    return node.leftAst is not null && Walk(node.leftAst);

                case NodeType.ExprAdd:
                case NodeType.ExprSub:
                case NodeType.ExprMult:
                case NodeType.ExprDiv:
                case NodeType.ExprEquals:
                case NodeType.ExprNotEquals:
                case NodeType.ExprLessThan:
                case NodeType.ExprGreaterThan:
                case NodeType.ExprLessEqualsThan:
                case NodeType.ExprGreaterEqualsThan:
                case NodeType.ExprOr:
                case NodeType.ExprAnd:
                case NodeType.ExprLike:
                case NodeType.ExprILike:
                case NodeType.ExprRegexMatch:
                case NodeType.ExprRegexMatchCi:
                case NodeType.ExprRegexNotMatch:
                case NodeType.ExprRegexNotMatchCi:
                case NodeType.ExprList:
                    return (node.leftAst is not null && Walk(node.leftAst))
                        || (node.rightAst is not null && Walk(node.rightAst));

                case NodeType.ExprNot:
                case NodeType.ExprIsNull:
                case NodeType.ExprIsNotNull:
                case NodeType.ExprInMembership:
                case NodeType.ExprNotInMembership:
                    return node.leftAst is not null && Walk(node.leftAst);

                case NodeType.ExprBetween:
                    return (node.leftAst is not null && Walk(node.leftAst))
                        || (node.extendedOne is not null && Walk(node.extendedOne))
                        || (node.extendedTwo is not null && Walk(node.extendedTwo));

                case NodeType.ExprCase:
                    return (node.leftAst is not null && Walk(node.leftAst))
                        || (node.rightAst is not null && Walk(node.rightAst))
                        || (node.extendedOne is not null && Walk(node.extendedOne));

                case NodeType.ExprCaseWhen:
                case NodeType.ExprCaseWhenList:
                    return (node.leftAst is not null && Walk(node.leftAst))
                        || (node.rightAst is not null && Walk(node.rightAst));

                // Subqueries are opaque — do not descend.
                case NodeType.ExprScalarSubquery:
                case NodeType.ExprInSubquery:
                case NodeType.ExprNotInSubquery:
                case NodeType.ExprExistsSubquery:
                case NodeType.ExprExistsCorrelated:
                // Leaf nodes carry no sub-expressions.
                case NodeType.Identifier:
                case NodeType.Integer:
                case NodeType.Float:
                case NodeType.String:
                case NodeType.Bool:
                case NodeType.Null:
                case NodeType.ObjectIdLiteral:
                case NodeType.Placeholder:
                case NodeType.ExprAllFields:
                default:
                    return false;
            }
        }
    }

    /// <summary>
    /// Returns true when the expression contains an aggregate call anywhere inside it but is NOT
    /// itself a bare top-level aggregate (i.e. <see cref="IsAggregateProjection"/> is false).
    /// Examples: <c>COALESCE(SUM(x),0)</c>, <c>SUM(a)+1</c>, <c>SUM(a)/SUM(b)</c>.
    /// </summary>
    public static bool IsCompoundAggregateProjection(NodeAst expression) =>
        !IsAggregateProjection(expression) && ContainsAggregate(expression);

    /// <summary>
    /// Collects all <see cref="NodeType.Identifier"/> nodes in <paramref name="expression"/>
    /// that are NOT inside any aggregate function call. These are the "free" column references
    /// the expression depends on beyond what the aggregate accumulators provide — for example,
    /// <c>bucket</c> in <c>bucket + SUM(amount)</c>.
    ///
    /// Stops recursing into an aggregate call's argument list because those column refs are
    /// consumed by the accumulator, not by the outer expression evaluator.
    /// </summary>
    public static void CollectNonAggregateColumnRefs(NodeAst expression, List<NodeAst> collected)
    {
        Walk(expression);

        void Walk(NodeAst node)
        {
            switch (node.nodeType)
            {
                case NodeType.ExprAlias:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    return;

                case NodeType.Identifier:
                    collected.Add(node);
                    return;

                case NodeType.ExprFuncCall:
                    // Stop at aggregate boundaries: column refs inside an aggregate call
                    // are consumed by the accumulator and are not free variables of the
                    // outer expression.
                    if (node.leftAst?.yytext is not null && IsAggregateName(node.leftAst.yytext))
                        return;
                    if (node.rightAst is not null) Walk(node.rightAst);
                    return;

                case NodeType.ExprArgumentList:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    if (node.rightAst is not null) Walk(node.rightAst);
                    return;

                case NodeType.ExprCast:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    return;

                case NodeType.ExprAdd:
                case NodeType.ExprSub:
                case NodeType.ExprMult:
                case NodeType.ExprDiv:
                case NodeType.ExprEquals:
                case NodeType.ExprNotEquals:
                case NodeType.ExprLessThan:
                case NodeType.ExprGreaterThan:
                case NodeType.ExprLessEqualsThan:
                case NodeType.ExprGreaterEqualsThan:
                case NodeType.ExprOr:
                case NodeType.ExprAnd:
                case NodeType.ExprLike:
                case NodeType.ExprILike:
                case NodeType.ExprRegexMatch:
                case NodeType.ExprRegexMatchCi:
                case NodeType.ExprRegexNotMatch:
                case NodeType.ExprRegexNotMatchCi:
                case NodeType.ExprList:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    if (node.rightAst is not null) Walk(node.rightAst);
                    return;

                case NodeType.ExprNot:
                case NodeType.ExprIsNull:
                case NodeType.ExprIsNotNull:
                case NodeType.ExprInMembership:
                case NodeType.ExprNotInMembership:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    return;

                case NodeType.ExprBetween:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    if (node.extendedOne is not null) Walk(node.extendedOne);
                    if (node.extendedTwo is not null) Walk(node.extendedTwo);
                    return;

                case NodeType.ExprCase:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    if (node.rightAst is not null) Walk(node.rightAst);
                    if (node.extendedOne is not null) Walk(node.extendedOne);
                    return;

                case NodeType.ExprCaseWhen:
                case NodeType.ExprCaseWhenList:
                    if (node.leftAst is not null) Walk(node.leftAst);
                    if (node.rightAst is not null) Walk(node.rightAst);
                    return;

                // Subqueries, literals, and other leaf nodes carry no free column refs.
                default:
                    return;
            }
        }
    }

    /// <summary>
    /// Validates that no aggregate argument itself contains another aggregate call
    /// (<c>SUM(SUM(x))</c>, <c>SUM(COUNT(*))</c> — not supported).
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// if a nested aggregate is found. Call from the binder before query execution begins.
    /// </summary>
    public static void ValidateNoNestedAggregate(NodeAst expression)
    {
        ValidateNode(expression, insideAggregate: false);

        static void ValidateNode(NodeAst node, bool insideAggregate)
        {
            switch (node.nodeType)
            {
                case NodeType.ExprAlias:
                    if (node.leftAst is not null)
                        ValidateNode(node.leftAst, insideAggregate);
                    return;

                case NodeType.ExprFuncCall:
                    bool isAgg = node.leftAst?.yytext is not null && IsAggregateName(node.leftAst.yytext);
                    if (isAgg && insideAggregate)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            $"Aggregate function '{node.leftAst!.yytext}' cannot be nested inside another aggregate");

                    if (node.rightAst is not null)
                        ValidateNode(node.rightAst, insideAggregate || isAgg);
                    return;

                case NodeType.ExprArgumentList:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    if (node.rightAst is not null) ValidateNode(node.rightAst, insideAggregate);
                    return;

                case NodeType.ExprCast:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    return;

                case NodeType.ExprAdd:
                case NodeType.ExprSub:
                case NodeType.ExprMult:
                case NodeType.ExprDiv:
                case NodeType.ExprEquals:
                case NodeType.ExprNotEquals:
                case NodeType.ExprLessThan:
                case NodeType.ExprGreaterThan:
                case NodeType.ExprLessEqualsThan:
                case NodeType.ExprGreaterEqualsThan:
                case NodeType.ExprOr:
                case NodeType.ExprAnd:
                case NodeType.ExprLike:
                case NodeType.ExprILike:
                case NodeType.ExprRegexMatch:
                case NodeType.ExprRegexMatchCi:
                case NodeType.ExprRegexNotMatch:
                case NodeType.ExprRegexNotMatchCi:
                case NodeType.ExprList:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    if (node.rightAst is not null) ValidateNode(node.rightAst, insideAggregate);
                    return;

                case NodeType.ExprNot:
                case NodeType.ExprIsNull:
                case NodeType.ExprIsNotNull:
                case NodeType.ExprInMembership:
                case NodeType.ExprNotInMembership:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    return;

                case NodeType.ExprBetween:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    if (node.extendedOne is not null) ValidateNode(node.extendedOne, insideAggregate);
                    if (node.extendedTwo is not null) ValidateNode(node.extendedTwo, insideAggregate);
                    return;

                case NodeType.ExprCase:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    if (node.rightAst is not null) ValidateNode(node.rightAst, insideAggregate);
                    if (node.extendedOne is not null) ValidateNode(node.extendedOne, insideAggregate);
                    return;

                case NodeType.ExprCaseWhen:
                case NodeType.ExprCaseWhenList:
                    if (node.leftAst is not null) ValidateNode(node.leftAst, insideAggregate);
                    if (node.rightAst is not null) ValidateNode(node.rightAst, insideAggregate);
                    return;

                // Subqueries, leaf nodes, and unrecognised nodes: nothing to validate.
                default:
                    return;
            }
        }
    }
}
