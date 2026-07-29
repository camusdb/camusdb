
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Collects the distinct placeholder names of a parsed statement.
///
/// <para><b>The returned order is a wire contract.</b> A prepared statement publishes these names
/// once, and every later execution binds its value at index <c>i</c> to the name at index
/// <c>i</c>. Changing the traversal order would silently rebind every already-published statement,
/// so the walk is fixed: depth-first, and within a node its children are visited in declaration
/// order (<see cref="NodeAst.leftAst"/>, <see cref="NodeAst.rightAst"/>, then
/// <c>extendedOne</c> … <c>extendedSeven</c>). A name that appears more than once collapses to its
/// first occurrence and gets exactly one slot; every occurrence then resolves to that one value.</para>
///
/// <para>Names are returned <b>verbatim, including the leading <c>@</c></b>. The lexer keeps the
/// sigil in the token text and the executor resolves a placeholder by looking that exact text up in
/// the parameter dictionary, so stripping or normalizing it here would produce a dictionary the
/// engine cannot bind against.</para>
///
/// <para>The walk covers the whole tree, so placeholders inside subqueries are included. That is
/// what makes name-based rehydration safe for DML: subquery rewriting happens later, inside the
/// executor, against the same named dictionary this order was derived from.</para>
/// </summary>
public static class PlaceholderCollector
{
    /// <summary>
    /// Returns the statement's distinct placeholder names in first-occurrence order, or an empty
    /// array when it has none. Iterative rather than recursive so a deeply nested expression tree
    /// cannot overflow the stack of the thread serving a request.
    /// </summary>
    public static string[] Collect(NodeAst? ast)
    {
        if (ast is null)
            return [];

        List<string>? names = null;
        HashSet<string>? seen = null;

        // Explicit stack; children are pushed in reverse so they pop in declaration order and the
        // published ordering matches the order a reader sees in the SQL text.
        Stack<NodeAst> pending = new();
        pending.Push(ast);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.Placeholder && node.yytext is { } name)
            {
                seen ??= new(StringComparer.Ordinal);
                if (seen.Add(name))
                {
                    names ??= new();
                    names.Add(name);
                }
            }

            Push(pending, node.extendedSeven);
            Push(pending, node.extendedSix);
            Push(pending, node.extendedFive);
            Push(pending, node.extendedFour);
            Push(pending, node.extendedThree);
            Push(pending, node.extendedTwo);
            Push(pending, node.extendedOne);
            Push(pending, node.rightAst);
            Push(pending, node.leftAst);
        }

        return names is null ? [] : names.ToArray();
    }

    private static void Push(Stack<NodeAst> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(child);
    }
}
