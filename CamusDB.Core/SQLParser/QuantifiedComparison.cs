/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Builds the node for a comparison, and gives the PostgreSQL quantified comparisons
/// <c>x &lt;op&gt; ANY (…)</c>, <c>x &lt;op&gt; SOME (…)</c> and <c>x &lt;op&gt; ALL (…)</c> the shape
/// each one needs. There are two shapes, and which one a form takes is not a style choice.
/// <list type="bullet">
///   <item><b>The three membership forms</b> — <c>= ANY</c>, <c>= SOME</c> and <c>&lt;&gt; ALL</c> —
///     become the <c>IN</c>, <c>NOT IN</c> or <c>array_contains</c> node they are equal to. No new
///     node and no new evaluator exist for them, so the NULL rules cannot differ between the
///     spellings, and an index path stays reachable.</item>
///   <item><b>Every other operator and quantifier pair</b> — <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>,
///     <c>&gt;=</c> with any quantifier, plus <c>= ALL</c> and <c>&lt;&gt; ANY</c> — becomes a
///     <see cref="NodeType.ExprQuantifiedComparison"/>. They are ordered folds over the elements,
///     which no existing node computes.</item>
/// </list>
///
/// <para>A membership form picks its target by the shape of the right operand:</para>
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
///
/// <para>Two traps decide the second and third rows. An <c>IN</c> list item is evaluated with no
/// row, so an element that reads a column (<c>ARRAY[a, b]</c>) cannot move into an <c>IN</c> list.
/// And the renderer cannot write an empty <c>IN</c> list, so an empty <c>ARRAY[]</c> must stay an
/// <c>array_contains</c> call, or a CHECK or view body that holds it would not re-parse. The item
/// kinds accepted here are exactly the non-identifier forms the grammar allows in an <c>IN</c>
/// list, so the rendered <c>IN</c> text always re-parses.</para>
///
/// <para>An ordered form keeps its right operand exactly as the grammar handed it over, a
/// subquery included: <see cref="NodeType.ExprQuantifiedComparison"/> carries an
/// <see cref="NodeType.ExprScalarSubquery"/> until the subquery rewrite replaces it with the
/// materialized <c>ARRAY[…]</c>. Keeping the subquery in that slot is what makes every existing
/// "does this tree hold a subquery" check find it without being taught the new node.</para>
///
/// <para>ANY, SOME and ALL are identifiers, not tokens, so the quantifier reaches here as an
/// <see cref="NodeType.ExprFuncCall"/> named <c>any</c>, <c>some</c> or <c>all</c>. A quantifier
/// call that this rewrite does not see, for example <c>SELECT any(tags)</c>, reaches the stub
/// functions in <c>QuantifierScalarFunctions</c>, which reject it at evaluation.</para>
///
/// <para>A rewritten membership form renders as its target, so <c>SHOW CREATE VIEW</c>, CHECK text
/// and EXPLAIN show <c>x IN (…)</c> or <c>array_contains(a, x)</c> where the user wrote
/// <c>= ANY</c>. An ordered form renders as itself.</para>
/// </summary>
internal static class QuantifiedComparison
{
    /// <summary>
    /// The operator markers a <see cref="NodeType.ExprQuantifiedComparison"/> carries in
    /// <c>extendedOne</c>. Each one is a childless node whose own type is the comparison operator,
    /// so the evaluator reads the operator from a node type and never parses text. They are shared
    /// across every parsed tree, which a <see cref="NodeAst"/> allows because it is immutable once
    /// parsed.
    ///
    /// <para>A visitor that walks <c>extendedOne</c> generically meets a comparison node with two
    /// null operands. Every such visitor in the engine guards its children, and none of them
    /// evaluates <c>extendedOne</c>; a new one must do the same.</para>
    /// </summary>
    private static readonly NodeAst EqualsMarker = Marker(NodeType.ExprEquals);
    private static readonly NodeAst NotEqualsMarker = Marker(NodeType.ExprNotEquals);
    private static readonly NodeAst LessThanMarker = Marker(NodeType.ExprLessThan);
    private static readonly NodeAst GreaterThanMarker = Marker(NodeType.ExprGreaterThan);
    private static readonly NodeAst LessEqualsMarker = Marker(NodeType.ExprLessEqualsThan);
    private static readonly NodeAst GreaterEqualsMarker = Marker(NodeType.ExprGreaterEqualsThan);

    /// <summary>
    /// Builds <paramref name="op"/> over the two operands, or its quantified form when the right
    /// operand is <c>ANY (…)</c>, <c>SOME (…)</c> or <c>ALL (…)</c>. Called from every comparison
    /// action of the grammar, so a quantifier can never fall through to the stub function.
    /// </summary>
    public static NodeAst Build(NodeType op, NodeAst left, NodeAst right)
    {
        if (!TryGetQuantifier(right, out string quantifier))
            return new(op, left, right, null, null, null, null, null, null);

        bool isAll = IsAllQuantifier(quantifier);
        NodeAst operand = SingleArgument(right, quantifier);

        // The two membership forms. Both are `x equals one element`, so both reuse the IN family.
        if (op == NodeType.ExprEquals && !isAll)
            return BuildMembership(left, operand, negated: false);

        if (op == NodeType.ExprNotEquals && isAll)
            return BuildMembership(left, operand, negated: true);

        NodeAst marker = op switch
        {
            NodeType.ExprEquals => EqualsMarker,
            NodeType.ExprNotEquals => NotEqualsMarker,
            NodeType.ExprLessThan => LessThanMarker,
            NodeType.ExprGreaterThan => GreaterThanMarker,
            NodeType.ExprLessEqualsThan => LessEqualsMarker,
            NodeType.ExprGreaterEqualsThan => GreaterEqualsMarker,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Not a comparison operator: {op}"),
        };

        return new NodeAst(
            NodeType.ExprQuantifiedComparison,
            left,
            operand,
            extendedOne: marker,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: quantifier.ToUpperInvariant());
    }

    /// <summary>
    /// True when <paramref name="name"/> is a quantifier word. Also used by the stub functions,
    /// so the two lists cannot drift apart.
    /// </summary>
    public static bool IsQuantifierName(string? name) =>
        string.Equals(name, "any", StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, "some", StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, "all", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True for the <c>ALL</c> quantifier. <c>ANY</c> and <c>SOME</c> are the same quantifier under
    /// two names, so everything else is the existential one.
    /// </summary>
    public static bool IsAllQuantifier(string quantifier) =>
        string.Equals(quantifier, "all", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The comparison operator a <see cref="NodeType.ExprQuantifiedComparison"/> applies to each
    /// element. Reading it here keeps the slot that holds it in one place; no caller spells out
    /// <c>extendedOne</c>. The check is a switch rather than a table lookup because this runs once
    /// per row the node is evaluated against.
    /// </summary>
    public static NodeType OperatorOf(NodeAst expr)
    {
        NodeType? marker = expr.extendedOne?.nodeType;

        return marker switch
        {
            NodeType.ExprEquals or NodeType.ExprNotEquals
                or NodeType.ExprLessThan or NodeType.ExprGreaterThan
                or NodeType.ExprLessEqualsThan or NodeType.ExprGreaterEqualsThan => marker.Value,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A quantified comparison carries no comparison operator"),
        };
    }

    /// <summary>The quantifier word of a <see cref="NodeType.ExprQuantifiedComparison"/>.</summary>
    public static string QuantifierOf(NodeAst expr) => expr.yytext ?? "ANY";

    /// <summary>True when a <see cref="NodeType.ExprQuantifiedComparison"/> is an <c>ALL</c> fold.</summary>
    public static bool IsAll(NodeAst expr) => IsAllQuantifier(QuantifierOf(expr));

    /// <summary>The SQL spelling of a comparison operator, for rendering and error messages.</summary>
    public static string OperatorText(NodeType op) => op switch
    {
        NodeType.ExprEquals => "=",
        NodeType.ExprNotEquals => "<>",
        NodeType.ExprLessThan => "<",
        NodeType.ExprGreaterThan => ">",
        NodeType.ExprLessEqualsThan => "<=",
        NodeType.ExprGreaterEqualsThan => ">=",
        _ => op.ToString(),
    };

    private static NodeAst Marker(NodeType op) =>
        new(op, null, null, null, null, null, null, null, null);

    private static NodeAst BuildMembership(NodeAst left, NodeAst operand, bool negated)
    {
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
}
