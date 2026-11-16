/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// The one owner of the word that marks a <see cref="NodeType.DerivedTableReference"/> as
/// <c>LATERAL</c>. The mark sits in the node's <c>yytext</c>, which no other form of the node uses.
///
/// <para>A <c>LATERAL</c> derived table can read the columns of the <c>FROM</c> items to its left, so
/// it runs once for each row of those items, as in PostgreSQL. Every code path that builds a derived
/// table node again (view expansion, stored view bodies, the CTE expansion) must carry the
/// <c>yytext</c> of the node it copies, or the copy loses the mark and its outer names no longer
/// resolve.</para>
/// </summary>
public static class LateralAst
{
    private const string Word = "lateral";

    /// <summary>Returns a copy of the derived table node <paramref name="derived"/> with the mark set.</summary>
    public static NodeAst Mark(NodeAst derived)
    {
        if (derived.nodeType != NodeType.DerivedTableReference)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Only a derived table can be marked LATERAL");

        return new NodeAst(
            NodeType.DerivedTableReference,
            derived.leftAst,
            derived.rightAst,
            derived.extendedOne,
            derived.extendedTwo,
            derived.extendedThree,
            derived.extendedFour,
            derived.extendedFive,
            Word,
            derived.extendedSix,
            derived.extendedSeven);
    }

    /// <summary>True when <paramref name="node"/> is a derived table node with the <c>LATERAL</c> mark.</summary>
    public static bool IsLateral(NodeAst node) =>
        node.nodeType == NodeType.DerivedTableReference
        && string.Equals(node.yytext, Word, StringComparison.Ordinal);

    /// <summary>
    /// The form of a function in <c>FROM</c> (<see cref="NodeType.TableFunctionReference"/>). A function whose
    /// arguments name a column reads the <c>FROM</c> items to its left, as in PostgreSQL, where every function
    /// in <c>FROM</c> is <c>LATERAL</c> with or without the keyword. Such a call becomes the <c>LATERAL</c>
    /// derived table <c>LATERAL (SELECT * FROM fn(args) AS t) AS t</c>, so it runs once for each left row with
    /// the same code as any <c>LATERAL</c> derived table. The inner call carries the mark too: it tells query
    /// creation that its arguments may name columns. A call whose arguments name no column stays as it is and
    /// runs once.
    /// </summary>
    public static NodeAst FunctionInFrom(NodeAst function)
    {
        if (function.nodeType != NodeType.TableFunctionReference || !ArgumentsNameAColumn(function.rightAst))
            return function;

        NodeAst inner = new(
            NodeType.TableFunctionReference,
            function.leftAst,
            function.rightAst,
            function.extendedOne,
            function.extendedTwo,
            function.extendedThree,
            function.extendedFour,
            function.extendedFive,
            Word,
            function.extendedSix,
            function.extendedSeven);

        string relationName = function.extendedOne?.yytext ?? function.leftAst!.yytext!;

        NodeAst select = new(NodeType.Select, NodeAst.ExprAllFields, inner, null, null, null, null, null, null);

        return new NodeAst(
            NodeType.DerivedTableReference,
            select,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, relationName),
            null, null, null, null, null,
            Word);
    }

    /// <summary>
    /// True when <paramref name="function"/> is a function in <c>FROM</c> whose arguments may name columns of
    /// the items to the left of the <c>LATERAL</c> derived table around it, see <see cref="FunctionInFrom"/>.
    /// </summary>
    public static bool IsLateralFunction(NodeAst function) =>
        function.nodeType == NodeType.TableFunctionReference
        && string.Equals(function.yytext, Word, StringComparison.Ordinal);

    /// <summary>
    /// The function call that <see cref="FunctionInFrom"/> wrapped in <paramref name="derived"/>, or null when
    /// <paramref name="derived"/> is not such a wrap. A view body renders the call, not the wrap, so its text
    /// parses again to the same tree.
    /// </summary>
    public static NodeAst? WrappedFunction(NodeAst derived)
    {
        if (!IsLateral(derived) || derived.extendedThree is not null)
            return null;

        NodeAst? select = derived.leftAst;

        bool isWrap = select is { nodeType: NodeType.Select, rightAst: { } from }
            && ReferenceEquals(select.leftAst, NodeAst.ExprAllFields)
            && IsLateralFunction(from)
            && select.extendedOne is null && select.extendedTwo is null && select.extendedThree is null
            && select.extendedFour is null && select.extendedFive is null && select.extendedSix is null
            && select.extendedSeven is null && select.yytext is null;

        return isWrap ? select!.rightAst : null;
    }

    /// <summary>
    /// True when an argument in <paramref name="arguments"/> names a column. A function call keeps its own
    /// name in <c>leftAst</c> and a cast keeps its target type in <c>rightAst</c>, and neither is a column. A
    /// subquery is not entered: query creation refuses a subquery argument with its own error.
    /// </summary>
    private static bool ArgumentsNameAColumn(NodeAst? arguments)
    {
        if (arguments is null)
            return false;

        Stack<NodeAst> pending = new();
        pending.Push(arguments);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.Identifier:
                    return true;

                case NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery or NodeType.ExprInSubquery
                    or NodeType.ExprNotInSubquery or NodeType.Select:
                    continue;

                case NodeType.ExprFuncCall:
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;

                case NodeType.ExprCast:
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;
            }

            foreach (NodeAst? child in (ReadOnlySpan<NodeAst?>)[
                node.leftAst, node.rightAst,
                node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour,
                node.extendedFive, node.extendedSix, node.extendedSeven])
            {
                if (child is not null)
                    pending.Push(child);
            }
        }

        return false;
    }
}
