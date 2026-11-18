/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// The argument array and lower-case name of a function call node, built one time per node by
/// <see cref="NodeShapes.CallOf"/>.
/// </summary>
internal sealed class FunctionCallShape
{
    /// <summary>The function name in lower case, the form the function registry and the error texts use.</summary>
    public readonly string Name;

    /// <summary>The argument nodes in source order. Callers must not write to the array: other rows and other statements read it.</summary>
    public readonly NodeAst[] Arguments;

    public FunctionCallShape(string name, NodeAst[] arguments)
    {
        Name = name;
        Arguments = arguments;
    }
}

/// <summary>
/// Gives the row evaluator flat views of a function call and of a CASE, and keeps each view on its
/// node (<see cref="NodeAst.shape"/>) so the evaluator builds it one time instead of one time per row.
///
/// <para>The parser builds an argument list and a WHEN list as chains of nodes. A walk of such a chain
/// allocates a stack and a list, and before this cache it ran for each call of each row. A call node or
/// a CASE node never changes after the parser returns it, so the flat array is the same on each
/// evaluation.</para>
///
/// <para>Concurrent readers are safe without a lock. A cached plan shares one tree between statements, so
/// two threads can build the same view at the same time. Each builds a complete, equal array before it
/// publishes it with <see cref="Volatile.Write{T}(ref T, T)"/>, so a reader sees either no view or a
/// complete one. All walks here are iterative, because the length of a chain is not bounded.</para>
/// </summary>
internal static class NodeShapes
{
    /// <summary>The cached name and argument nodes of an <see cref="NodeType.ExprFuncCall"/> node.</summary>
    public static FunctionCallShape CallOf(NodeAst call)
    {
        if (Volatile.Read(ref call.shape) is FunctionCallShape cached)
            return cached;

        FunctionCallShape built = new(call.leftAst!.yytext!.ToLowerInvariant(), BuildArguments(call.rightAst));
        Volatile.Write(ref call.shape, built);
        return built;
    }

    /// <summary>
    /// The cached WHEN clauses of an <see cref="NodeType.ExprCase"/> node, as
    /// <see cref="NodeType.ExprCaseWhen"/> nodes in source order. Callers must not write to the array.
    /// </summary>
    public static NodeAst[] WhenClausesOf(NodeAst caseNode)
    {
        if (Volatile.Read(ref caseNode.shape) is NodeAst[] cached)
            return cached;

        NodeAst[] built = BuildWhenClauses(caseNode.rightAst!);
        Volatile.Write(ref caseNode.shape, built);
        return built;
    }

    /// <summary>
    /// The argument nodes of the argument list <paramref name="argumentAst"/> in source order, built
    /// each time it is called. A null list has no arguments, and a node of another type is the one
    /// argument.
    /// </summary>
    public static NodeAst[] BuildArguments(NodeAst? argumentAst)
    {
        if (argumentAst is null)
            return [];

        if (argumentAst.nodeType != NodeType.ExprArgumentList)
            return [argumentAst];

        List<NodeAst> arguments = [];
        ExpressionChains.Flatten(argumentAst, NodeType.ExprArgumentList, arguments);
        return arguments.ToArray();
    }

    /// <summary>
    /// The WHEN clauses of a CASE WHEN list in source order, built each time it is called. The grammar
    /// builds the list left-associatively, <c>((c1 c2) c3)</c>, so the first clause is the deepest node
    /// and the walk goes down the left spine first. A CASE with one clause has no list node: the grammar
    /// gives the bare <see cref="NodeType.ExprCaseWhen"/>. This is the one place that defines the WHEN
    /// order. The list cannot be rebalanced (see <see cref="ExpressionChains"/>), so its depth is bounded
    /// by <see cref="StatementDepthGuard"/> instead.
    /// </summary>
    public static NodeAst[] BuildWhenClauses(NodeAst whenList)
    {
        if (whenList.nodeType != NodeType.ExprCaseWhenList)
            return [whenList];

        int depth = 0;
        NodeAst node = whenList;

        while (node.nodeType == NodeType.ExprCaseWhenList)
        {
            depth++;
            node = node.leftAst!;
        }

        // depth list nodes hold depth + 1 clauses: the deepest left child, then each right child from
        // the bottom up. Fill the array from the end while the walk goes down again.
        NodeAst[] clauses = new NodeAst[depth + 1];
        clauses[0] = node;

        node = whenList;
        for (int i = depth; i > 0; i--)
        {
            clauses[i] = node.rightAst!;
            node = node.leftAst!;
        }

        return clauses;
    }
}
