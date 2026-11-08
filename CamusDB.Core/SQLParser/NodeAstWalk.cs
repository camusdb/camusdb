/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Whole-tree traversal of a parsed statement that does not consume the call stack.
///
/// <para><b>Why this exists.</b> Several list-shaped grammar rules are left-recursive, so the tree
/// they build is a spine one node deep per list element rather than a balanced tree. The VALUES
/// list of a multi-row INSERT is the worst case: its depth is the number of rows, and a statement
/// large enough to overflow a thread's stack is well inside what the server admits. A recursive
/// predicate over such a tree does not throw — a stack overflow ends the process, so no
/// <c>catch</c>, no retry and no error response can save the node. Anything that walks a whole
/// statement tree must therefore walk it iteratively.</para>
///
/// <para><see cref="PlaceholderCollector"/> solves the same problem for its own ordered walk and
/// keeps its own loop, because its published order is a wire contract that must stay visible at
/// its call site.</para>
/// </summary>
internal static class NodeAstWalk
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="predicate"/> holds for
    /// <paramref name="ast"/> or for any node below it, and stops at the first match.
    ///
    /// <para>Every child slot is visited, so a caller never has to know which slots a given node
    /// type populates. Visit order is unspecified and callers must not depend on it: the answer is
    /// a yes or no about the whole tree, so order cannot change it.</para>
    ///
    /// <para>Pass a <c>static</c> lambda or a static method group. The delegate is then cached by
    /// the runtime and the walk allocates nothing per call beyond its stack.</para>
    /// </summary>
    public static bool Any(NodeAst? ast, Func<NodeAst, bool> predicate)
    {
        if (ast is null)
            return false;

        Stack<NodeAst> pending = new();
        pending.Push(ast);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (predicate(node))
                return true;

            Push(pending, node.leftAst);
            Push(pending, node.rightAst);
            Push(pending, node.extendedOne);
            Push(pending, node.extendedTwo);
            Push(pending, node.extendedThree);
            Push(pending, node.extendedFour);
            Push(pending, node.extendedFive);
            Push(pending, node.extendedSix);
            Push(pending, node.extendedSeven);
        }

        return false;
    }

    /// <summary>
    /// Returns a copy of <paramref name="root"/> in which each node that is a key of
    /// <paramref name="replacements"/> (by reference) is replaced by its value. A subtree that holds no
    /// such node is shared with <paramref name="root"/>, not copied, and <paramref name="root"/> is not
    /// changed: a parsed tree can be shared through the parser cache. Returns <paramref name="root"/>
    /// itself when no node matched.
    ///
    /// <para>A node can occur in more than one place, because query creation shares one node between the
    /// select list and a GROUP BY or ORDER BY position that names it. Each occurrence is replaced.</para>
    /// </summary>
    public static NodeAst Replace(NodeAst root, IReadOnlyDictionary<NodeAst, NodeAst> replacements)
    {
        if (replacements.Count == 0)
            return root;

        // A post-order walk with an explicit stack: a node is rebuilt after its children, from the
        // results of those children. Each frame is visited twice, first to push the children and then
        // to rebuild.
        Dictionary<NodeAst, NodeAst> rebuilt = new(ReferenceEqualityComparer.Instance);
        Stack<(NodeAst Node, bool ChildrenDone)> pending = new();
        pending.Push((root, false));

        while (pending.TryPop(out (NodeAst Node, bool ChildrenDone) frame))
        {
            NodeAst node = frame.Node;

            if (rebuilt.ContainsKey(node))
                continue;

            if (replacements.TryGetValue(node, out NodeAst? replacement))
            {
                rebuilt[node] = replacement;
                continue;
            }

            if (!frame.ChildrenDone)
            {
                pending.Push((node, true));
                PushFrame(pending, node.leftAst);
                PushFrame(pending, node.rightAst);
                PushFrame(pending, node.extendedOne);
                PushFrame(pending, node.extendedTwo);
                PushFrame(pending, node.extendedThree);
                PushFrame(pending, node.extendedFour);
                PushFrame(pending, node.extendedFive);
                PushFrame(pending, node.extendedSix);
                PushFrame(pending, node.extendedSeven);
                continue;
            }

            NodeAst? left = Rebuilt(rebuilt, node.leftAst);
            NodeAst? right = Rebuilt(rebuilt, node.rightAst);
            NodeAst? one = Rebuilt(rebuilt, node.extendedOne);
            NodeAst? two = Rebuilt(rebuilt, node.extendedTwo);
            NodeAst? three = Rebuilt(rebuilt, node.extendedThree);
            NodeAst? four = Rebuilt(rebuilt, node.extendedFour);
            NodeAst? five = Rebuilt(rebuilt, node.extendedFive);
            NodeAst? six = Rebuilt(rebuilt, node.extendedSix);
            NodeAst? seven = Rebuilt(rebuilt, node.extendedSeven);

            bool same = ReferenceEquals(left, node.leftAst)
                && ReferenceEquals(right, node.rightAst)
                && ReferenceEquals(one, node.extendedOne)
                && ReferenceEquals(two, node.extendedTwo)
                && ReferenceEquals(three, node.extendedThree)
                && ReferenceEquals(four, node.extendedFour)
                && ReferenceEquals(five, node.extendedFive)
                && ReferenceEquals(six, node.extendedSix)
                && ReferenceEquals(seven, node.extendedSeven);

            rebuilt[node] = same
                ? node
                : new NodeAst(node.nodeType, left, right, one, two, three, four, five, node.yytext, six, seven);
        }

        return rebuilt[root];
    }

    private static void PushFrame(Stack<(NodeAst, bool)> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push((child, false));
    }

    private static NodeAst? Rebuilt(Dictionary<NodeAst, NodeAst> rebuilt, NodeAst? child) =>
        child is null ? null : rebuilt[child];

    private static void Push(Stack<NodeAst> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(child);
    }
}
