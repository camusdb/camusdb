/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>The subquery node kinds a <see cref="SubqueryNodeScan"/> looks for.</summary>
[Flags]
internal enum SubqueryNodeKinds
{
    None = 0,

    /// <summary><c>(SELECT …)</c> used as a value.</summary>
    Scalar = 1,

    /// <summary><c>x IN (SELECT …)</c>.</summary>
    In = 2,

    /// <summary><c>x NOT IN (SELECT …)</c>.</summary>
    NotIn = 4,

    /// <summary><c>EXISTS (SELECT …)</c>, before the preparer has classified it.</summary>
    Exists = 8,

    /// <summary>The marker the preparer leaves for a correlated <c>EXISTS</c> the filter evaluates per row.</summary>
    ExistsCorrelated = 16,

    /// <summary>The kinds <see cref="SubqueryRewriter"/> folds in the WHERE of a SELECT, where <c>EXISTS</c> belongs to the preparer.</summary>
    Rewritable = Scalar | In | NotIn,

    /// <summary>
    /// The nodes a prepared clause can still hold. The rewriter folds every uncorrelated subquery, so each
    /// one left is correlated, and the preparer registered it: see <see cref="CorrelatedSubqueryValues"/>.
    /// </summary>
    Correlated = Scalar | In | NotIn | ExistsCorrelated,

    All = Scalar | In | NotIn | Exists | ExistsCorrelated,
}

/// <summary>
/// Answers "does this expression hold a subquery node of these kinds" without recursion. The one
/// tree whose depth the parser does not bound is the row spine of a multi-row INSERT
/// (<see cref="StatementDepthGuard"/> admits ten thousand rows by design), and a recursive scan of
/// it ended the process with a stack overflow. Every scan over a tree that can hold that spine, or
/// that runs once per statement on an arbitrary expression, goes through here.
///
/// <para>A nested <see cref="NodeType.Select"/> is not entered: it is its own scope, and its
/// subqueries are resolved by its own pass through the bind pipeline. A subquery node itself is a
/// match, so the scan never needs what is below it.</para>
/// </summary>
internal static class SubqueryNodeScan
{
    /// <summary>True when <paramref name="root"/> or any descendant is a subquery node of one of <paramref name="kinds"/>.</summary>
    public static bool Contains(NodeAst? root, SubqueryNodeKinds kinds)
    {
        if (root is null)
            return false;

        if (Matches(root.nodeType, kinds))
            return true;

        // A leaf answers from its own type; the explicit stack is allocated only when the root has
        // children, so a bare column or literal costs nothing.
        Stack<NodeAst>? pending = null;
        PushChildren(root, ref pending);

        if (pending is null)
            return false;

        return Contains(pending, kinds);
    }

    /// <summary>
    /// The same scan over a caller-owned <paramref name="scratch"/> stack, for a loop that scans many
    /// trees (the rows of a multi-row INSERT) and does not want one allocation per tree. The stack is
    /// left empty.
    /// </summary>
    public static bool Contains(NodeAst? root, SubqueryNodeKinds kinds, Stack<NodeAst> scratch)
    {
        if (root is null)
            return false;

        if (Matches(root.nodeType, kinds))
            return true;

        scratch.Clear();
        Stack<NodeAst>? pending = scratch;
        PushChildren(root, ref pending);

        bool found = Contains(scratch, kinds);
        scratch.Clear();
        return found;
    }

    private static bool Contains(Stack<NodeAst> pending, SubqueryNodeKinds kinds)
    {
        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (Matches(node.nodeType, kinds))
                return true;

            if (node.nodeType == NodeType.Select)
                continue;

            Stack<NodeAst>? stack = pending;
            PushChildren(node, ref stack);
        }

        return false;
    }

    private static bool Matches(NodeType nodeType, SubqueryNodeKinds kinds) => nodeType switch
    {
        NodeType.ExprScalarSubquery => (kinds & SubqueryNodeKinds.Scalar) != 0,
        NodeType.ExprInSubquery => (kinds & SubqueryNodeKinds.In) != 0,
        NodeType.ExprNotInSubquery => (kinds & SubqueryNodeKinds.NotIn) != 0,
        NodeType.ExprExistsSubquery => (kinds & SubqueryNodeKinds.Exists) != 0,
        NodeType.ExprExistsCorrelated => (kinds & SubqueryNodeKinds.ExistsCorrelated) != 0,
        _ => false,
    };

    /// <summary>
    /// Pushes the children of <paramref name="node"/>. The left child goes first, so a left-deep list
    /// (an IN list, an AND chain, the INSERT row spine) pops its right-hand leaves before it descends,
    /// and the stack stays a few entries long instead of one entry per list element.
    /// </summary>
    private static void PushChildren(NodeAst node, ref Stack<NodeAst>? pending)
    {
        Push(node.leftAst, ref pending);
        Push(node.rightAst, ref pending);
        Push(node.extendedOne, ref pending);
        Push(node.extendedTwo, ref pending);
        Push(node.extendedThree, ref pending);
        Push(node.extendedFour, ref pending);
        Push(node.extendedFive, ref pending);
        Push(node.extendedSix, ref pending);
        Push(node.extendedSeven, ref pending);
    }

    private static void Push(NodeAst? child, ref Stack<NodeAst>? pending)
    {
        if (child is null)
            return;

        pending ??= new Stack<NodeAst>();
        pending.Push(child);
    }
}
