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

    private static void Push(Stack<NodeAst> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(child);
    }
}
