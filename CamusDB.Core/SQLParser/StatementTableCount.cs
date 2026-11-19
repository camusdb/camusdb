/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Counts the relations a statement reads and refuses a statement with more than
/// <see cref="CamusDBOptions.MaxTablesPerStatement"/> of them, with
/// <see cref="CamusDBErrorCodes.TooManyTablesInStatement"/>, before the statement is bound or planned.
///
/// <para><b>Purpose.</b> Planning time and memory grow about with the square of the
/// relations in one join block: 64 comma-joined tables plan in a few milliseconds, 300 take about
/// 100 ms and 30 MB. <see cref="StatementDepthGuard"/> bounds one block at about 300 relations,
/// because a comma list or a JOIN chain adds one level per relation, but it does not bound the
/// statement: many derived tables, UNION branches or subqueries of 300 relations each stay shallow,
/// and one statement of a few megabytes could keep a node planning for minutes. The planner's own
/// caps (<c>JoinGraph.MaxLeaves</c>, the enumeration budgets of <c>JoinEnumerator</c>) fall back to a
/// cheaper plan instead of refusing, so they bound the exponential search but not this growth.</para>
///
/// <para><b>What counts.</b> Each table, materialized view, function in <c>FROM</c> and recursive work
/// table counts as one, each time the statement names it, in every FROM list and JOIN, in
/// <c>UPDATE … FROM</c> and <c>DELETE … USING</c>, and inside subqueries. The table that an
/// <c>UPDATE</c> or <c>DELETE</c> changes counts as one, because the statement scans it. A derived
/// table, a view and a CTE reference count as the relations of their query, and as one when their
/// query reads none (a VALUES list, a SELECT with no FROM). A CTE that the statement computes once
/// (<see cref="NodeType.CteMaterialization"/>) counts the relations of its query at its first
/// reference only; each further reference counts as one. So every join leaf adds at least one, and a
/// count within the limit also bounds the leaves of every block.</para>
///
/// <para><b>Where it runs.</b> Before binding, on the tree that is planned. The read path counts after
/// view expansion, because a view's body is part of the work. The source query builder counts the
/// query of an <c>INSERT … SELECT</c>, a joined <c>UPDATE</c> or <c>DELETE</c>,
/// <c>CREATE TABLE … AS</c> and a view or materialized-view body after it expands their views. The
/// write path counts the parsed statement too, which covers the subqueries of a plain
/// <c>INSERT</c>, <c>UPDATE</c> or <c>DELETE</c>, and counts a write through a view again after it is
/// rewritten onto the view's table, because the rewrite copies a computed column's expression, with
/// any subquery in it, to each place the statement names the column. CTEs are already written out as
/// derived tables by the parser.</para>
///
/// <para>The walk is iterative, like every whole-tree walk (see <see cref="NodeAstWalk"/>): a list
/// rule of the grammar makes a tree as deep as its list is long. It runs on every statement, so it
/// reuses one frame stack per thread instead of allocating one; the walk never awaits, so no other
/// statement can use the stack while it runs.</para>
/// </summary>
internal static class StatementTableCount
{
    /// <summary>One pending node, or the end of a derived table whose count started at <see cref="CountAtStart"/>.</summary>
    private readonly record struct Frame(NodeAst Node, bool End, int CountAtStart);

    /// <summary>A frame stack that grew past this many frames is dropped after the walk, so a thread does not keep a large array alive.</summary>
    private const int MaxRetainedFrames = 1024;

    [ThreadStatic]
    private static Stack<Frame>? cachedFrames;

    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.TooManyTablesInStatement"/> when <paramref name="ast"/>
    /// reads more than <paramref name="limit"/> relations. A limit of zero or less disables the check.
    /// The walk stops at the first relation past the limit.
    /// </summary>
    public static void Enforce(NodeAst ast, int limit)
    {
        if (limit <= 0)
            return;

        int count = Count(ast, limit);

        if (count > limit)
            throw new CamusDBException(
                CamusDBErrorCodes.TooManyTablesInStatement,
                $"Statement reads more than {limit} tables (max_tables_per_statement). The count includes " +
                "the tables of every join, subquery, derived table, CTE and view body. Split the statement, " +
                "or raise the setting.");
    }

    /// <summary>
    /// The number of relations <paramref name="ast"/> reads, as the class summary defines it. The walk
    /// stops as soon as the count passes <paramref name="stopAbove"/>, so the result is then only
    /// known to be larger than it.
    /// </summary>
    internal static int Count(NodeAst ast, int stopAbove = int.MaxValue)
    {
        Stack<Frame> pending = cachedFrames ?? new Stack<Frame>();
        cachedFrames = null;

        int count = 0;
        int mostFrames = 0;

        // The markers of the computed-once CTEs whose query was already counted. Almost no statement
        // has one, so the set is made on first use.
        HashSet<NodeAst>? countedOnce = null;

        try
        {
            pending.Push(new Frame(ast, End: false, CountAtStart: 0));

            while (pending.Count > 0)
            {
                if (pending.Count > mostFrames)
                    mostFrames = pending.Count;

                Frame frame = pending.Pop();

                if (frame.End)
                {
                    // A derived table whose query read no relation is still one leaf of its join.
                    if (count == frame.CountAtStart)
                        count++;

                    if (count > stopAbove)
                        return count;

                    continue;
                }

                NodeAst node = frame.Node;

                switch (node.nodeType)
                {
                    case NodeType.TableReference:
                    case NodeType.TableFunctionReference:
                    case NodeType.WorkTableReference:
                        count++;
                        break;

                    case NodeType.DerivedTableReference:
                        if (node.extendedFour is { nodeType: NodeType.CteMaterialization } marker)
                        {
                            countedOnce ??= new HashSet<NodeAst>(ReferenceEqualityComparer.Instance);

                            // A further reference reads the rows the first one keeps, so its copy of
                            // the query is not walked.
                            if (!countedOnce.Add(marker))
                            {
                                count++;

                                if (count > stopAbove)
                                    return count;

                                continue;
                            }
                        }

                        // Pushed below the children, so it pops after the whole query is counted.
                        pending.Push(new Frame(node, End: true, CountAtStart: count));
                        PushChildren(pending, node);
                        continue;

                    case NodeType.Update:
                    case NodeType.Delete:
                        // The target is a bare name, and the statement scans it.
                        count++;
                        break;

                    case NodeType.Select:
                        if (node.rightAst is { nodeType: NodeType.Identifier })
                            count++;
                        break;

                    case NodeType.Join:
                    case NodeType.CommaJoin:
                    case NodeType.CommaJoinTableList:
                        if (node.leftAst is { nodeType: NodeType.Identifier })
                            count++;
                        if (node.rightAst is { nodeType: NodeType.Identifier })
                            count++;
                        break;
                }

                if (count > stopAbove)
                    return count;

                PushChildren(pending, node);
            }

            return count;
        }
        finally
        {
            pending.Clear();

            if (mostFrames <= MaxRetainedFrames)
                cachedFrames = pending;
        }
    }

    private static void PushChildren(Stack<Frame> pending, NodeAst node)
    {
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

    private static void Push(Stack<Frame> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(new Frame(child, End: false, CountAtStart: 0));
    }
}
