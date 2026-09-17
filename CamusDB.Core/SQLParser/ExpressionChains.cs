/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Reads and builds chains of one associative operator — <c>OR</c>, <c>AND</c>, and the
/// <see cref="NodeType.ExprList"/> of an <c>IN (…)</c> list or an <c>ARRAY[…]</c> literal — without
/// recursion, and rebuilds a long chain as a balanced tree.
///
/// <para><b>Why this exists.</b> The grammar declares <c>%left TOR</c> and <c>%left TAND</c> and
/// builds every list left-recursively, so <c>a OR b OR c …</c> and <c>IN (1, 2, 3 …)</c> parse into a
/// spine one node deep per term. More than sixty engine walkers recurse over expressions — the per-row
/// evaluators, the planner analyzers, the renderers — so a chain of a few thousand terms overflowed the
/// stack, and a stack overflow ends the process: no <c>catch</c>, no error response, no rollback. A
/// generated filter of that size is ordinary SQL, well inside the transport limits.</para>
///
/// <para><b>Why rebalancing is safe.</b> OR and AND are associative under three-valued logic, and an
/// IN or ARRAY list is a sequence. A balanced tree keeps the terms in their source order when read left
/// to right, so every walker that visits <c>leftAst</c> before <c>rightAst</c> sees the same terms in
/// the same order: evaluation results, short-circuit points, placeholder order and element order do not
/// change. Only the depth changes, from <c>n</c> to <c>ceil(log2 n)</c>, which removes the hazard for
/// every walker at once, including walkers nobody has enumerated. The one observable difference is
/// on a statement that holds two distinct errors: which of them is reported can change.</para>
///
/// <para><b>Trap.</b> A list whose consumer walks only the left spine cannot be rebalanced. The
/// <c>CASE</c> WHEN list is such a list (see <c>SQLExecutorBaseCreator.EnumerateWhenClauses</c>), and
/// so it is left alone and bounded by <see cref="StatementDepthGuard"/> instead.</para>
/// </summary>
internal static class ExpressionChains
{
    /// <summary>
    /// A chain with at most this many terms keeps the shape the parser gave it. Short chains are the
    /// overwhelming majority, and keeping them untouched keeps their rendered text and their error
    /// messages exactly as before. Above it, depth — not shape — is what matters.
    /// </summary>
    internal const int BalanceThreshold = 32;

    /// <summary>
    /// Appends the operands of the maximal <paramref name="chainType"/> chain rooted at
    /// <paramref name="root"/> to <paramref name="operands"/>, in source order. A root of another type
    /// is its own single operand. Nested groups of the same operator, such as <c>(a OR b) OR c</c>, are
    /// flattened too, which associativity permits.
    /// </summary>
    public static void Flatten(NodeAst root, NodeType chainType, List<NodeAst> operands)
    {
        if (root.nodeType != chainType)
        {
            operands.Add(root);
            return;
        }

        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType != chainType)
            {
                operands.Add(node);
                continue;
            }

            // Right first, so the left side pops first and source order is preserved.
            if (node.rightAst is not null)
                pending.Push(node.rightAst);

            if (node.leftAst is not null)
                pending.Push(node.leftAst);
        }
    }

    /// <summary>
    /// Builds a balanced <paramref name="chainType"/> tree over <paramref name="operands"/>, whose
    /// in-order sequence is exactly <paramref name="operands"/>. Returns the single operand when there
    /// is one. Depth is <c>ceil(log2 n)</c>.
    /// </summary>
    public static NodeAst Balance(NodeType chainType, IReadOnlyList<NodeAst> operands)
    {
        if (operands.Count == 0)
            throw new ArgumentException("A chain needs at least one operand.", nameof(operands));

        int count = operands.Count;
        NodeAst[] level = new NodeAst[count];

        for (int i = 0; i < count; i++)
            level[i] = operands[i];

        // Pair neighbours level by level, writing each level into the front of the same array. The
        // write index never passes the read index, so no unread entry is overwritten.
        while (count > 1)
        {
            int write = 0;
            int read = 0;

            for (; read + 1 < count; read += 2)
                level[write++] = new NodeAst(chainType, level[read], level[read + 1], null, null, null, null, null, null);

            if (read < count)
                level[write++] = level[read];

            count = write;
        }

        return level[0];
    }

    /// <summary>
    /// Builds a chain over <paramref name="operands"/> in the shape the parser would give a short
    /// chain and a balanced shape for a long one. Use this wherever engine code composes a new chain,
    /// such as a residual conjunction, so a rebuilt predicate does not reintroduce the depth that
    /// <see cref="Normalize"/> removed. Returns null for an empty list.
    /// </summary>
    public static NodeAst? Combine(NodeType chainType, IReadOnlyList<NodeAst> operands)
    {
        if (operands.Count == 0)
            return null;

        if (operands.Count > BalanceThreshold)
            return Balance(chainType, operands);

        NodeAst combined = operands[0];

        for (int i = 1; i < operands.Count; i++)
            combined = new NodeAst(chainType, combined, operands[i], null, null, null, null, null, null);

        return combined;
    }

    /// <summary>
    /// Rebalances, in place, every OR chain, AND chain, IN value list and ARRAY element list longer
    /// than <see cref="BalanceThreshold"/> in a freshly parsed tree. The walk is iterative.
    ///
    /// <para>Must run inside the parser, before the tree is returned: it assigns child slots of nodes
    /// the parser has just created, which the <see cref="NodeAst"/> immutability invariant forbids once
    /// the tree is visible to anyone else, such as the parser cache.</para>
    /// </summary>
    public static void Normalize(NodeAst root)
    {
        Stack<NodeAst> pending = new();
        List<NodeAst> operands = [];

        pending.Push(root);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            switch (node.nodeType)
            {
                case NodeType.ExprOr:
                case NodeType.ExprAnd:
                    operands.Clear();
                    Flatten(node, node.nodeType, operands);

                    if (operands.Count > BalanceThreshold)
                    {
                        NodeAst balanced = Balance(node.nodeType, operands);
                        node.leftAst = balanced.leftAst;
                        node.rightAst = balanced.rightAst;
                    }

                    // The chain's own nodes hold nothing but their two children, so only the
                    // operands need visiting.
                    PushAll(pending, operands);
                    continue;

                case NodeType.ExprInMembership:
                case NodeType.ExprNotInMembership:
                    node.rightAst = NormalizeList(node.rightAst, pending, operands);
                    PushChildren(pending, node, skipRight: true);
                    continue;

                case NodeType.ArrayLiteral:
                    node.leftAst = NormalizeList(node.leftAst, pending, operands);
                    PushChildren(pending, node, skipLeft: true);
                    continue;

                default:
                    PushChildren(pending, node);
                    continue;
            }
        }
    }

    /// <summary>
    /// Returns <paramref name="list"/> rebalanced when it is a long <see cref="NodeType.ExprList"/>,
    /// and queues its items for the rest of the walk.
    /// </summary>
    private static NodeAst? NormalizeList(NodeAst? list, Stack<NodeAst> pending, List<NodeAst> operands)
    {
        if (list is null)
            return null;

        operands.Clear();
        Flatten(list, NodeType.ExprList, operands);

        NodeAst result = operands.Count > BalanceThreshold ? Balance(NodeType.ExprList, operands) : list;

        PushAll(pending, operands);
        return result;
    }

    private static void PushAll(Stack<NodeAst> pending, List<NodeAst> operands)
    {
        for (int i = 0; i < operands.Count; i++)
            pending.Push(operands[i]);
    }

    private static void PushChildren(Stack<NodeAst> pending, NodeAst node, bool skipLeft = false, bool skipRight = false)
    {
        if (!skipLeft) Push(pending, node.leftAst);
        if (!skipRight) Push(pending, node.rightAst);
        Push(pending, node.extendedOne);
        Push(pending, node.extendedTwo);
        Push(pending, node.extendedThree);
        Push(pending, node.extendedFour);
        Push(pending, node.extendedFive);
        Push(pending, node.extendedSix);
        Push(pending, node.extendedSeven);
    }

    private static void Push(Stack<NodeAst> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(child);
    }
}
