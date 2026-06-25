
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
/// Nested-loop inner join: execute <see cref="PhysicalPlanNode.Input"/> (left), scan
/// <see cref="RightSource"/> for each left row, evaluate <see cref="OnPredicate"/> on merged rows.
/// </summary>
public sealed class NestedLoopJoinNode : PhysicalPlanNode
{
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
