/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Refuses a parsed statement whose tree is deeper than the engine can walk safely, with a
/// <see cref="CamusDBErrorCodes.StatementTooDeeplyNested"/> error instead of a dead process.
///
/// <para><b>Why a limit and not only iterative walkers.</b> More than sixty engine walkers recurse
/// over a statement tree — the per-row evaluators, the planner analyzers, the binders, the renderers —
/// and a stack overflow in any of them cannot be caught in .NET: the server process ends, with no
/// error response, no rollback and, in a cluster, one node fewer. Making each walker iterative would
/// leave the next walker nobody has found, and the one added tomorrow, exposed. One check, run once
/// per parse and before any walker, covers all of them.</para>
///
/// <para><b>What counts.</b> Depth is the number of nodes on the longest root-to-leaf path. Two
/// shapes are deliberately cheap: <see cref="ExpressionChains.Normalize"/> runs first and rebalances
/// long OR and AND chains, IN lists and ARRAY literals, so they cost <c>log2</c> of their length; and
/// the row spine of a multi-row INSERT (<see cref="NodeType.InsertBatchList"/>) costs nothing, because
/// every walker on that path is iterative and a statement of ten thousand rows is admitted by
/// design.</para>
///
/// <para><b>Why a constant and not a <c>CamusDBOptions</c> setting.</b> The safe depth is a
/// property of the code — the frame size of the deepest walker and the thread stack size — not of a
/// workload, so there is no value an operator could tune toward without risking a crash. The limit
/// sits well below the measured failure point of the weakest walker; see
/// <see cref="MaxDepth"/>.</para>
///
/// <para>The walk is iterative, or it would be the crash it exists to prevent.</para>
/// </summary>
internal static class StatementDepthGuard
{
    /// <summary>
    /// The deepest parse tree a statement may have.
    ///
    /// <para>Chosen from a child-process probe (<c>CamusDB.MicroBenchmarks</c>,
    /// <c>probe exprscan</c>) that raises each nesting shape until the process dies, on a thread-pool
    /// thread with the default 1.5 MB stack. With the evaluator frame kept small, the async walkers
    /// moving to a fresh stack, and function calls checking headroom, the weakest shape measured
    /// survives about 5,300 levels (a deep expression beside an IN subquery in an UPDATE) and most
    /// shapes survive 6,500 to 9,500. The limit keeps a five-fold margin under that, so a 1 MB stack
    /// (the Windows default) and walkers the probe does not reach still have room. It is also above
    /// any realistic statement: one that names every column of a table at the default column limit
    /// is about half as deep.</para>
    /// </summary>
    internal const int MaxDepth = 1_000;

    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.StatementTooDeeplyNested"/> when the tree under
    /// <paramref name="root"/> is deeper than <see cref="MaxDepth"/>. Stops at the first node past
    /// the limit.
    /// </summary>
    public static void Enforce(NodeAst root)
    {
        Stack<(NodeAst Node, int Depth)> pending = new();
        pending.Push((root, 1));

        while (pending.Count > 0)
        {
            (NodeAst node, int depth) = pending.Pop();

            if (depth > MaxDepth)
                throw TooDeep();

            int childDepth = depth + 1;

            // The next row of a multi-row INSERT is a sibling of this one, not a level below it.
            Push(pending, node.leftAst, node.nodeType == NodeType.InsertBatchList ? depth : childDepth);
            Push(pending, node.rightAst, childDepth);
            Push(pending, node.extendedOne, childDepth);
            Push(pending, node.extendedTwo, childDepth);
            Push(pending, node.extendedThree, childDepth);
            Push(pending, node.extendedFour, childDepth);
            Push(pending, node.extendedFive, childDepth);
            Push(pending, node.extendedSix, childDepth);
            Push(pending, node.extendedSeven, childDepth);
        }
    }

    /// <summary>
    /// True while the current thread still has comfortable stack headroom.
    ///
    /// <para>An asynchronous walker checks this once per level and, when it is false, continues the
    /// walk on a fresh thread-pool stack instead of recursing further. An async frame is several
    /// times the size of a synchronous one — a correlated <c>EXISTS</c> under a chain of <c>NOT</c>s
    /// overflowed at about 780 levels, below <see cref="MaxDepth"/> — and moving the work is
    /// cheaper and safer than lowering the limit for every statement.</para>
    /// </summary>
    internal static bool HasStackHeadroom() => RuntimeHelpers.TryEnsureSufficientExecutionStack();

    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.StatementTooDeeplyNested"/> when the current thread is
    /// close to the end of its stack. For a synchronous walker whose per-level cost is high enough
    /// that <see cref="MaxDepth"/> alone leaves too small a margin; the check is not free, so keep it
    /// off paths that run once per node per row unless the node is already expensive.
    /// </summary>
    internal static void EnsureStackHeadroom()
    {
        if (!RuntimeHelpers.TryEnsureSufficientExecutionStack())
        {
            throw new CamusDBException(
                CamusDBErrorCodes.StatementTooDeeplyNested,
                "Statement is nested too deeply to evaluate. Reduce the nesting of expressions or " +
                "function calls, or split the statement.");
        }
    }

    private static CamusDBException TooDeep() => new(
        CamusDBErrorCodes.StatementTooDeeplyNested,
        $"Statement is nested too deeply: its parse tree exceeds {MaxDepth} levels. " +
        "Reduce the nesting of expressions, function calls, CASE branches or subqueries, " +
        "or split the statement.");

    private static void Push(Stack<(NodeAst Node, int Depth)> pending, NodeAst? child, int depth)
    {
        if (child is not null)
            pending.Push((child, depth));
    }
}
