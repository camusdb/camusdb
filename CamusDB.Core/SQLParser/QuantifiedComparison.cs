/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Builds the node for a comparison, and turns the PostgreSQL quantified comparisons
/// <c>x = ANY (…)</c>, <c>x = SOME (…)</c> and <c>x &lt;&gt; ALL (…)</c> into the membership test
/// each one is. No new node or evaluator exists for them: each form becomes a node that
/// <c>IN</c>, <c>NOT IN</c> or <c>array_contains</c> already builds, so the NULL rules cannot
/// differ between the spellings.
/// <list type="table">
///   <item><term>an <c>ARRAY[…]</c> literal whose elements are all constants or parameters</term>
///     <description><c>x IN (e1, …)</c> / <c>x NOT IN (e1, …)</c> — the literal list keeps the
///     index path of <c>IN</c> with no planner change.</description></item>
///   <item><term>a subquery</term>
///     <description><c>x IN (SELECT …)</c> / <c>x NOT IN (SELECT …)</c>.</description></item>
///   <item><term>any other expression: a column, a parameter, a call, a cast, an empty
///     <c>ARRAY[]</c>, or an <c>ARRAY[…]</c> with a non-constant element</term>
///     <description><c>array_contains(a, x)</c> / <c>NOT array_contains(a, x)</c>.</description></item>
/// </list>
/// <para>Two traps decide the second and third rows. An <c>IN</c> list item is evaluated with no
/// row, so an element that reads a column (<c>ARRAY[a, b]</c>) cannot move into an <c>IN</c> list.
/// And the renderer cannot write an empty <c>IN</c> list, so an empty <c>ARRAY[]</c> must stay an
/// <c>array_contains</c> call, or a CHECK or view body that holds it would not re-parse. The item
/// kinds accepted here are exactly the non-identifier forms the grammar allows in an <c>IN</c>
/// list, so the rendered <c>IN</c> text always re-parses.</para>
/// <para>ANY, SOME and ALL are identifiers, not tokens, so the quantifier reaches here as an
/// <see cref="NodeType.ExprFuncCall"/> named <c>any</c>, <c>some</c> or <c>all</c>. The other
/// operators with a quantifier (<c>&lt; ANY</c>, <c>= ALL</c>, <c>&lt;&gt; ANY</c>, …) are rejected
/// here with <see cref="CamusDBErrorCodes.FeatureNotSupported"/>: they need an ordered fold over the
/// elements, which does not exist yet. A quantifier call that this rewrite does not see, for
/// example <c>SELECT any(tags)</c>, reaches the stub functions in <c>QuantifierScalarFunctions</c>,
/// which reject it at evaluation.</para>
/// <para>A rewritten comparison renders as its target, so <c>SHOW CREATE VIEW</c>, CHECK text and
/// EXPLAIN show <c>x IN (…)</c> or <c>array_contains(a, x)</c> where the user wrote <c>= ANY</c>.</para>
/// </summary>
internal static class QuantifiedComparison
{
    private const string OnlySupportedForms = "only = ANY, = SOME and <> ALL are supported";

    /// <summary>
    /// Builds <paramref name="op"/> over the two operands, or its rewrite when the right operand is
    /// a quantifier. Called from every comparison action of the grammar, so that an unsupported
    /// operator with a quantifier fails here with a clear message and does not fall through to
    /// the stub function.
    /// </summary>
    public static NodeAst Build(NodeType op, NodeAst left, NodeAst right)
    {
        if (!TryGetQuantifier(right, out string quantifier))
            return new(op, left, right, null, null, null, null, null, null);

        bool isAny = !string.Equals(quantifier, "all", StringComparison.OrdinalIgnoreCase);
        bool negated;

        if (op == NodeType.ExprEquals && isAny)
            negated = false;
        else if (op == NodeType.ExprNotEquals && !isAny)
            negated = true;
        else
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"The quantified comparison {OperatorText(op)} {quantifier.ToUpperInvariant()} is not supported: {OnlySupportedForms}");

        NodeAst operand = SingleArgument(right, quantifier);

        if (operand.nodeType == NodeType.ExprScalarSubquery)
            return new(negated ? NodeType.ExprNotInSubquery : NodeType.ExprInSubquery,
                left, operand.leftAst, null, null, null, null, null, null);

        if (operand.nodeType == NodeType.ArrayLiteral && operand.leftAst is not null && AllItemsListSafe(operand.leftAst))
            return new(negated ? NodeType.ExprNotInMembership : NodeType.ExprInMembership,
                left, operand.leftAst, null, null, null, null, null, null);

        NodeAst contains = new(
            NodeType.ExprFuncCall,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "array_contains"),
            new NodeAst(NodeType.ExprArgumentList, operand, left, null, null, null, null, null, null),
            null, null, null, null, null, null);

        return negated ? new(NodeType.ExprNot, contains, null, null, null, null, null, null, null) : contains;
    }

    /// <summary>
    /// True when <paramref name="name"/> is a quantifier word. Also used by the stub functions,
    /// so the two lists cannot drift apart.
    /// </summary>
    public static bool IsQuantifierName(string? name) =>
        string.Equals(name, "any", StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, "some", StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, "all", StringComparison.OrdinalIgnoreCase);

    private static bool TryGetQuantifier(NodeAst right, out string quantifier)
    {
        quantifier = "";

        if (right.nodeType != NodeType.ExprFuncCall || right.leftAst?.nodeType != NodeType.Identifier)
            return false;

        string? name = right.leftAst.yytext;
        if (!IsQuantifierName(name))
            return false;

        quantifier = name!;
        return true;
    }

    private static NodeAst SingleArgument(NodeAst call, string quantifier)
    {
        NodeAst? argument = call.rightAst;

        if (argument is null || argument.nodeType is NodeType.ExprArgumentList or NodeType.ExprAllFields)
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"{quantifier.ToUpperInvariant()} takes exactly one argument: an array or a subquery");

        return argument;
    }

    /// <summary>
    /// True when every element of an <c>ARRAY[…]</c> element list is a kind the grammar accepts as
    /// an <c>IN</c> list item and that evaluates without a row: a literal or a parameter. Walks the
    /// left-deep <see cref="NodeType.ExprList"/> chain with a loop, so a long literal cannot
    /// overflow the stack.
    /// </summary>
    private static bool AllItemsListSafe(NodeAst list)
    {
        NodeAst? node = list;

        while (node is not null)
        {
            if (node.nodeType == NodeType.ExprList)
            {
                if (node.rightAst is null || !IsListSafeItem(node.rightAst))
                    return false;

                node = node.leftAst;
                continue;
            }

            return IsListSafeItem(node);
        }

        return false;
    }

    private static bool IsListSafeItem(NodeAst item) => item.nodeType is
        NodeType.Integer or NodeType.Float or NodeType.String or NodeType.Bool or NodeType.Null
        or NodeType.BytesLiteral or NodeType.Placeholder;

    private static string OperatorText(NodeType op) => op switch
    {
        NodeType.ExprEquals => "=",
        NodeType.ExprNotEquals => "<>",
        NodeType.ExprLessThan => "<",
        NodeType.ExprGreaterThan => ">",
        NodeType.ExprLessEqualsThan => "<=",
        NodeType.ExprGreaterEqualsThan => ">=",
        _ => op.ToString(),
    };
}
