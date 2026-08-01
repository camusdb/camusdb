
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
/// Which physical side of the join is materialised into the hash table.
/// <see cref="Right"/> is the default (build = declared right source).
/// <see cref="Left"/> is chosen when the left input has a smaller
/// estimated cardinality — it streams the right source as the probe side and
/// materialises the left subtree into the hash table.
/// </summary>
public enum HashJoinBuildSide { Right, Left }

/// <summary>
/// Hash inner join: materialise the build side into an in-memory hash table keyed on the
/// equi-join columns, then stream the probe side (<see cref="PhysicalPlanNode.Input"/>) and
/// look up each row.
///
/// NULL join-key values are excluded from both the build table and the probe stream —
/// consistent with SQL inner-join semantics where NULL = NULL is unknown.
///
/// When <c>CamusDBOptions.SpillEnabled</c> is true and the build side exceeds
/// <c>CamusDBOptions.SpillEffectiveThreshold</c>, the executor routes to the Grace/hybrid hash
/// join: both sides are partitioned to spill files by hash of the join key, then each partition
/// pair is joined in memory. Skewed partitions that still exceed the threshold are recursively
/// repartitioned; beyond the recursion depth limit a nested-loop join over the partition files
/// is used as a bounded-memory backstop.
/// When spill is disabled and the build exceeds <c>CamusDBOptions.HashJoinMaxBuildRows</c>,
/// the executor falls back to nested-loop (full-scan fallback, no spill).
///
/// <see cref="CanDecomposeToLocalPlusMerge"/> is false and
/// <see cref="PhysicalPlanNode.OutputOrdering"/> is null: hash join does not preserve order.
/// </summary>
public sealed class HashJoinNode : PhysicalPlanNode
{
    /// <summary>
    /// The build (materialised) side of the join.
    ///
    /// Today <c>BuildSource</c> is always the declared right side of the join and
    /// <see cref="PhysicalPlanNode.Input"/> is always the probe (left) side.
    /// The smaller-side-as-build optimisation will need to swap these when the left input is smaller.
    /// Swapping changes which side's alias qualifies output columns in <c>MergeRows</c> — the
    /// executor must pass the build alias, not a hardcoded "right alias", after any swap.
    /// </summary>
    public BoundJoinRightSource BuildSource { get; }

    /// <summary>Full ON predicate including residual non-equi conjuncts.</summary>
    public NodeAst? OnPredicate { get; }

    /// <summary>
    /// Qualified left-side (probe) lookup column names, one per equi-key conjunct, e.g. <c>o.id</c>.
    /// Parallel to <see cref="BuildKeyColumns"/>.
    /// </summary>
    public IReadOnlyList<string> ProbeKeyColumns { get; }

    /// <summary>
    /// Unqualified column names on the build (right) side, e.g. <c>order_id</c>.
    /// Parallel to <see cref="ProbeKeyColumns"/>.
    /// </summary>
    public IReadOnlyList<string> BuildKeyColumns { get; }

    /// <summary>Single-table predicate pushed into the build-side scan.</summary>
    public NodeAst? BuildExecutionFilter { get; init; }

    /// <summary>
    /// Which physical side is materialised. Set by the planner's build-side selection.
    /// Default is <see cref="HashJoinBuildSide.Right"/> (declared right = build).
    /// When <see cref="HashJoinBuildSide.Left"/>, the executor builds from
    /// <see cref="PhysicalPlanNode.Input"/> and probes <see cref="BuildSource"/> instead.
    /// </summary>
    public HashJoinBuildSide BuildSide { get; init; } = HashJoinBuildSide.Right;

    public HashJoinNode(
        PhysicalPlanNode probe,
        BoundJoinRightSource buildSource,
        NodeAst? onPredicate,
        IReadOnlyList<string> probeKeyColumns,
        IReadOnlyList<string> buildKeyColumns)
    {
        Input = probe;
        BuildSource = buildSource;
        OnPredicate = onPredicate;
        ProbeKeyColumns = probeKeyColumns;
        BuildKeyColumns = buildKeyColumns;
    }

    public override bool CanDecomposeToLocalPlusMerge => false;
}
