
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

    public NestedLoopJoinNode(PhysicalPlanNode left, BoundJoinRightSource rightSource, NodeAst onPredicate)
    {
        Input = left;
        RightSource = rightSource;
        OnPredicate = onPredicate;
    }
}
