/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Text;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The literal-free fingerprint statement statistics group executions by, computed from the parsed
/// statement rather than from a plan.
///
/// <para><b>Why the parse tree.</b> The plan cache's shape id comes from a bound <c>SELECT</c>, is only
/// computed while the plan cache is on, and does not exist for <c>INSERT</c>, <c>UPDATE</c> or
/// <c>DELETE</c>. Every recorded statement has a parse tree, on every configuration, before anything
/// else happens to it, so the fingerprint is taken there.</para>
///
/// <para><b>What is stripped.</b> Every literal — numbers, strings, booleans, NULL, bytes, object ids —
/// and every placeholder renders as <c>?</c>, so <c>WHERE id = 1</c> and <c>WHERE id = $1</c> share a
/// shape. A list made only of constants — an <c>IN (…)</c> list, an <c>ARRAY[…]</c>, or the rows of a
/// multi-row <c>VALUES</c> — renders as one <c>?*</c>, so a batch of three rows and a batch of three
/// hundred are one shape instead of three hundred. Identifiers are kept, lower-cased, because the
/// engine resolves them case-insensitively. The digest is the 16-hex XxHash64 the plan cache uses, so
/// it is the same on every node and across restarts for the same build.</para>
///
/// <para><b>Computed once per parse tree.</b> The result is kept in a weak table keyed by the root
/// node. The parser cache hands every execution of the same statement text the same tree, so an
/// application that sends parameterized statements pays the walk once per distinct text. The tree is
/// never modified: <see cref="NodeAst"/> is shared across concurrent executions and must stay
/// immutable.</para>
///
/// <para>The walk uses an explicit stack because a long <c>IN</c> list is a deep chain of list nodes,
/// and a recursive walk over one would overflow the thread's stack.</para>
/// </summary>
public static class StatementShape
{
    private static readonly ConditionalWeakTable<NodeAst, string> Cache = new();

    /// <summary>
    /// The statement kind statement statistics record a root node under, or null for a statement they
    /// do not record. Only <c>SELECT</c>, <c>INSERT</c>, <c>UPDATE</c> and <c>DELETE</c> are recorded:
    /// they are the workload. Schema changes, transaction control and introspection — including the
    /// statement that reads the statistics — are not, which is also why reading them never changes them.
    /// </summary>
    public static string? KindOf(NodeType nodeType) => nodeType switch
    {
        NodeType.Select or NodeType.SelectForUpdate => "select",
        NodeType.Insert or NodeType.InsertSelect => "insert",
        NodeType.Update => "update",
        NodeType.Delete => "delete",
        _ => null,
    };

    /// <summary>Returns the shape id of a parsed statement, computing it on first use of the tree.</summary>
    public static string Of(NodeAst root) => Cache.GetValue(root, static r => Compute(r));

    private static string Compute(NodeAst root)
    {
        HashSet<NodeAst> constant = ConstantSubtrees(root);
        StringBuilder text = new(256);

        // A frame is either a node to render or, when Node is null, a literal piece of text.
        Stack<(NodeAst? Node, string? Text)> stack = new();
        stack.Push((root, null));

        while (stack.Count > 0)
        {
            (NodeAst? node, string? piece) = stack.Pop();

            if (node is null)
            {
                text.Append(piece);
                continue;
            }

            if (IsLiteral(node.nodeType))
            {
                text.Append('?');
                continue;
            }

            if (IsList(node.nodeType) && constant.Contains(node))
            {
                text.Append("?*");
                continue;
            }

            text.Append('(').Append(node.nodeType.ToString());

            if (node.yytext is { Length: > 0 } nodeText)
                text.Append(' ').Append(nodeText.ToLowerInvariant());

            Span<NodeAst?> children = [
                node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo, node.extendedThree,
                node.extendedFour, node.extendedFive, node.extendedSix, node.extendedSeven];

            int last = children.Length - 1;
            while (last >= 0 && children[last] is null)
                last--;

            stack.Push((null, ")"));

            for (int i = last; i >= 0; i--)
            {
                if (children[i] is { } child)
                    stack.Push((child, null));
                else
                    stack.Push((null, " _"));

                stack.Push((null, " "));
            }
        }

        return Convert.ToHexStringLower(XxHash64.Hash(Encoding.UTF8.GetBytes(text.ToString())));
    }

    /// <summary>
    /// The nodes whose whole subtree holds nothing but literals and lists of literals, found in one
    /// post-order pass so a deep list chain is examined once rather than once per level.
    /// </summary>
    private static HashSet<NodeAst> ConstantSubtrees(NodeAst root)
    {
        HashSet<NodeAst> constant = new(ReferenceEqualityComparer.Instance);
        Stack<(NodeAst Node, bool Expanded)> stack = new();
        stack.Push((root, false));

        while (stack.Count > 0)
        {
            (NodeAst node, bool expanded) = stack.Pop();

            if (!expanded)
            {
                stack.Push((node, true));
                foreach (NodeAst? child in Children(node))
                {
                    if (child is not null)
                        stack.Push((child, false));
                }

                continue;
            }

            if (IsLiteral(node.nodeType))
            {
                constant.Add(node);
                continue;
            }

            if (!IsList(node.nodeType))
                continue;

            bool allConstant = true;
            foreach (NodeAst? child in Children(node))
            {
                if (child is not null && !constant.Contains(child))
                {
                    allConstant = false;
                    break;
                }
            }

            if (allConstant)
                constant.Add(node);
        }

        return constant;
    }

    private static IEnumerable<NodeAst?> Children(NodeAst node)
    {
        yield return node.leftAst;
        yield return node.rightAst;
        yield return node.extendedOne;
        yield return node.extendedTwo;
        yield return node.extendedThree;
        yield return node.extendedFour;
        yield return node.extendedFive;
        yield return node.extendedSix;
        yield return node.extendedSeven;
    }

    private static bool IsLiteral(NodeType nodeType) => nodeType is
        NodeType.Integer or NodeType.Float or NodeType.String or NodeType.Bool or NodeType.Null or
        NodeType.BytesLiteral or NodeType.ObjectIdLiteral or NodeType.Placeholder;

    private static bool IsList(NodeType nodeType) => nodeType is
        NodeType.ExprList or NodeType.InsertBatchList or NodeType.ArrayLiteral;
}
