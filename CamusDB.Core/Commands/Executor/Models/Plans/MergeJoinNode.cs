
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
/// Sort-merge inner join: advance two pointers in lockstep over equi-join key(s), skip on
/// mismatch, emit the cross-product of equal-key groups via <c>QueryRowMerger</c>.
///
/// NULL join-key values are excluded from both sides (consistent with inner-join semantics
/// where NULL = NULL is unknown).
///
/// When both <see cref="LeftIsOrdered"/> and <see cref="RightIsOrdered"/> are true the
/// executor uses a streaming two-pointer path that buffers only the current equal-key run
/// on the right side — O(max right run size) memory. When either side is unordered the
/// executor materialises that side and sorts it in O(n log n) before merging.
///
/// <see cref="CanDecomposeToLocalPlusMerge"/> is false.
/// <see cref="PhysicalPlanNode.OutputOrdering"/> is set to ascending on
/// <see cref="LeftKeyColumns"/> so downstream ORDER BY on the join key can be elided.
/// </summary>
public sealed class MergeJoinNode : PhysicalPlanNode
{
    /// <summary>Right (build) source of the join.</summary>
    public BoundJoinRightSource RightSource { get; }

    /// <summary>Full ON predicate including residual non-equi conjuncts.</summary>
    public NodeAst OnPredicate { get; }

    /// <summary>
    /// Qualified left-side key column names, one per equi-key conjunct, e.g. <c>o.id</c>.
    /// Parallel to <see cref="RightKeyColumns"/>.
    /// </summary>
    public IReadOnlyList<string> LeftKeyColumns { get; }

    /// <summary>
    /// Unqualified column names on the right side, e.g. <c>order_id</c>.
    /// Parallel to <see cref="LeftKeyColumns"/>.
    /// </summary>
    public IReadOnlyList<string> RightKeyColumns { get; }

    /// <summary>Single-table predicate pushed into the right-side scan.</summary>
    public NodeAst? RightExecutionFilter { get; init; }

    /// <summary>
    /// Physical plan node for the right side. When non-null the executor iterates this node
    /// (which may be a <see cref="SortNode"/> wrapping a scan, or a <c>ForcedIndex</c>
    /// <see cref="TableScanNode"/> for free ordering) instead of scanning
    /// <see cref="RightSource"/> directly.
    /// </summary>
    public PhysicalPlanNode? RightPhysicalNode { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the executor streams the left input via
    /// <c>ExecuteJoinTree(Input)</c> without an internal in-memory sort. The left side
    /// either already has <see cref="PhysicalPlanNode.OutputOrdering"/> covering the join
    /// key (free ordering from an index) or carries a <see cref="SortNode"/> that the
    /// executor resolves. When <see langword="false"/> the executor materialises and sorts
    /// the left side internally.
    /// </summary>
    public bool LeftIsOrdered { get; init; }

    /// <summary>
    /// When <see langword="true"/> and <see cref="RightPhysicalNode"/> is non-null, the
    /// executor streams the right side via <c>ExecuteJoinTree(RightPhysicalNode)</c> without
    /// an internal sort. When <see langword="false"/> the executor materialises and sorts
    /// <see cref="RightSource"/> internally.
    /// </summary>
    public bool RightIsOrdered { get; init; }

    public MergeJoinNode(
        PhysicalPlanNode left,
        BoundJoinRightSource rightSource,
        NodeAst onPredicate,
        IReadOnlyList<string> leftKeyColumns,
        IReadOnlyList<string> rightKeyColumns)
    {
        Input = left;
        RightSource = rightSource;
        OnPredicate = onPredicate;
        LeftKeyColumns = leftKeyColumns;
        RightKeyColumns = rightKeyColumns;
    }

    public override bool CanDecomposeToLocalPlusMerge => false;
}
