
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Nested-loop join: execute <see cref="PhysicalPlanNode.Input"/> (left), scan
/// <see cref="RightSource"/> for each left row, evaluate <see cref="OnPredicate"/> on merged rows.
/// For <see cref="JoinKind.LeftOuter"/> a left row whose right scan produced no accepted pair is
/// emitted once, padded with NULL right columns.
/// </summary>
public sealed class NestedLoopJoinNode : PhysicalPlanNode
{
    /// <summary>Inner or left outer; the left input is the preserved side of an outer join.</summary>
    public JoinKind Kind { get; init; } = JoinKind.Inner;

    public BoundJoinRightSource RightSource { get; }

    public NodeAst OnPredicate { get; }

    /// <summary>Single-table predicate pushed into the right-side scan.</summary>
    public NodeAst? RightExecutionFilter { get; init; }

    /// <summary>
    /// The physical input that reads the right table, when the planner gave it its own access path
    /// (an index range or IN-list scan driven by the pushed filter); null for a full scan with
    /// <see cref="RightExecutionFilter"/>. The spool reads the right side through it.
    /// </summary>
    public PhysicalPlanNode? RightPhysicalNode { get; internal set; }

    /// <summary>
    /// The index range of the right table to seek for each left row, when the <c>ON</c> predicate bounds
    /// the leading column of a right-side index with left values; null for the plain nested loop. With
    /// a seek the right side is not spooled and <see cref="RightPhysicalNode"/> is not used: each left
    /// row reads only its own range, and the <c>ON</c> predicate still decides each pair.
    /// </summary>
    public JoinRangeSeek? RangeSeek { get; init; }

    public NestedLoopJoinNode(PhysicalPlanNode left, BoundJoinRightSource rightSource, NodeAst onPredicate)
    {
        Input = left;
        RightSource = rightSource;
        OnPredicate = onPredicate;
    }
}
