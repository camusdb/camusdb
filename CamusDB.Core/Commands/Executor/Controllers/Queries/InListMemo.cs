/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a <see cref="PreparedInSet"/> once for an <c>ExprInMembership</c> /
/// <c>ExprNotInMembership</c> node and keeps it for every later evaluation of the same node, so the
/// per-row evaluator (<see cref="SubqueryValueListAst.EvaluateMembership"/>) does a hash lookup instead
/// of a scan of the whole list.
///
/// <para><b>Why it exists.</b> The planner builds a prepared set only for an IN conjunct at the top of
/// a WHERE clause. An IN list anywhere else — in a CASE, a function argument, a projection, under an
/// OR or a NOT that the analyzer does not split — was scanned for each row, and each item was evaluated
/// again for each row. A list of two million literals over a table of M rows then cost 2M × M
/// evaluations, inside one request and under every size cap.</para>
///
/// <para><b>Which lists qualify.</b> A list of more than <see cref="MinItems"/> items whose items are
/// all constants: literals, <c>@parameters</c>, and casts or negations of those. Such an item has the
/// same value for every row of one statement, so a set built from the first evaluation is correct for
/// all of them. A list with any other item — a column, a function call, which can be volatile — keeps
/// the per-row scan, as does a short list, where a scan is as fast as a hash.</para>
///
/// <para><b>Keys.</b> Parse trees are cached and shared by every statement with the same text, so a
/// set keyed by the node alone is correct only when no item reads a parameter. A list with parameters
/// is keyed by the statement's parameter dictionary and then by the node; every transport builds a
/// new dictionary for each statement, so two statements never share a set. Both tables are
/// <see cref="ConditionalWeakTable{TKey,TValue}"/>, so an entry lives only as long as its node (and
/// dictionary), and a subquery list, which is a new node for each execution, leaves nothing behind.</para>
///
/// <para><b>Same result as the scan.</b> <see cref="PreparedInSet"/> uses the equality of the scan
/// (<see cref="MixedNumericComparison.EqualsForMembership"/>) and the same NULL rules. One difference
/// is not allowed: the scan stops at the first match and never evaluates the items after it, so an
/// item that throws (a bad cast) fails only some rows. A build that throws therefore marks the node
/// ineligible, and the node keeps the scan with its exact behavior.</para>
///
/// <para>Thread safety: a parallel scan evaluates one node on many threads.
/// <see cref="ConditionalWeakTable{TKey,TValue}.GetValue"/> can run the factory more than once under a
/// race; the factory has no side effect, and one result is kept. A set is safe to share after it is
/// built.</para>
/// </summary>
internal static class InListMemo
{
    /// <summary>Lists of this many items or fewer keep the scan; see <see cref="PreparedInSet"/>.</summary>
    private const int MinItems = 8;

    /// <summary>The row an item is evaluated against. A constant item reads no column.</summary>
    private static readonly Dictionary<string, ColumnValue> EmptyRow = new();

    /// <summary>What a node is: not a candidate, a list with parameters, or a list with its set.</summary>
    private sealed class Entry(bool eligible, bool readsParameters, PreparedInSet? set)
    {
        public bool Eligible { get; } = eligible;

        public bool ReadsParameters { get; } = readsParameters;

        /// <summary>The set of a list without parameters; null otherwise.</summary>
        public PreparedInSet? Set { get; } = set;
    }

    private static readonly Entry Ineligible = new(eligible: false, readsParameters: false, set: null);

    private static readonly ConditionalWeakTable<NodeAst, Entry> ByNode = new();

    private static readonly ConditionalWeakTable<Dictionary<string, ColumnValue>, ConditionalWeakTable<NodeAst, Entry>> ByParameters = new();

    /// <summary>
    /// The prepared set for <paramref name="expr"/> with <paramref name="parameters"/>, or null when the
    /// caller must scan the list. Call it only for a list that is not empty.
    /// </summary>
    internal static PreparedInSet? TryGet(NodeAst expr, Dictionary<string, ColumnValue>? parameters)
    {
        Entry entry = ByNode.GetValue(expr, static node => Classify(node, parameters: null));

        if (!entry.Eligible)
            return null;

        if (!entry.ReadsParameters)
            return entry.Set;

        // No dictionary means an unbound placeholder; the scan raises the error it always did.
        if (parameters is null)
            return null;

        ConditionalWeakTable<NodeAst, Entry> forStatement =
            ByParameters.GetValue(parameters, static _ => new ConditionalWeakTable<NodeAst, Entry>());

        // The hit is the per-row path, so it must not allocate: the factory that captures the
        // dictionary lives in its own method, which only the first evaluation calls.
        if (forStatement.TryGetValue(expr, out Entry? built))
            return built.Set;

        return BuildForStatement(forStatement, expr, parameters);
    }

    private static PreparedInSet? BuildForStatement(
        ConditionalWeakTable<NodeAst, Entry> forStatement, NodeAst expr, Dictionary<string, ColumnValue> parameters)
        => forStatement.GetValue(expr, node => Classify(node, parameters)).Set;

    /// <summary>
    /// Decides whether <paramref name="node"/> qualifies and, when it does and the list reads no
    /// parameter (or <paramref name="parameters"/> is given), builds its set.
    /// </summary>
    private static Entry Classify(NodeAst node, Dictionary<string, ColumnValue>? parameters)
    {
        List<NodeAst> items = [];
        bool readsParameters = false;

        foreach (NodeAst item in Items(node.rightAst))
        {
            if (!IsConstant(item, ref readsParameters))
                return Ineligible;

            items.Add(item);
        }

        if (items.Count <= MinItems)
            return Ineligible;

        if (readsParameters && parameters is null)
            return new Entry(eligible: true, readsParameters: true, set: null);

        ColumnValue[] values = new ColumnValue[items.Count];

        try
        {
            for (int i = 0; i < items.Count; i++)
                values[i] = SQLExecutorBaseCreator.EvalExpr(items[i], EmptyRow, parameters);
        }
        catch (Exception)
        {
            // The scan evaluates items lazily and may never reach this one; keep its behavior.
            return Ineligible;
        }

        PreparedInSet set = new(values, containsNull: SubqueryValueListAst.ListContainsRemovedNull(node));
        return new Entry(eligible: true, readsParameters, set);
    }

    /// <summary>
    /// True when <paramref name="item"/> has one value for the whole statement: a literal, a
    /// parameter, or a cast or negation of one. Iterative over the chain of casts and negations.
    /// </summary>
    private static bool IsConstant(NodeAst item, ref bool readsParameters)
    {
        NodeAst? node = item;

        while (node is not null)
        {
            switch (node.nodeType)
            {
                case NodeType.Integer:
                case NodeType.Float:
                case NodeType.String:
                case NodeType.BytesLiteral:
                case NodeType.Bool:
                case NodeType.Null:
                case NodeType.ObjectIdLiteral:
                    return true;

                case NodeType.Placeholder:
                    readsParameters = true;
                    return true;

                case NodeType.ExprNegate:
                case NodeType.ExprCast:
                    node = node.leftAst;
                    continue;

                default:
                    return false;
            }
        }

        return false;
    }

    /// <summary>The items of an <c>ExprList</c> tree in source order, without recursion.</summary>
    private static IEnumerable<NodeAst> Items(NodeAst? list)
    {
        if (list is null)
            yield break;

        Stack<NodeAst> pending = new();
        pending.Push(list);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.ExprList)
            {
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);

                if (node.leftAst is not null)
                    pending.Push(node.leftAst);

                continue;
            }

            yield return node;
        }
    }
}
