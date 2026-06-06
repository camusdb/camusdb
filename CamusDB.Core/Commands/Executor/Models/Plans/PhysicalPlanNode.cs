
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Base type for physical plan nodes produced by <see cref="Controllers.Queries.QueryPlanner"/>.
/// Each node wraps zero or one input; leaf nodes read from storage.
/// </summary>
public abstract class PhysicalPlanNode
{
    public PhysicalPlanNode? Input { get; init; }

    /// <summary>Columns that must be present in rows produced by this subtree (QP6.1). Null means all table columns.</summary>
    public IReadOnlySet<string>? RequiredColumns { get; set; }

    // ── R4: Distributed-ready plan properties ──────────────────────────────

    /// <summary>
    /// The ordering this node guarantees on its output rows (R4).
    /// Set by the planner when an index scan satisfies ORDER BY (sort elision) or when a
    /// SortNode is added. Null means the output order is undefined.
    /// </summary>
    public IReadOnlyList<QueryOrderBy>? OutputOrdering { get; internal set; }

    /// <summary>Estimated output row count; populated by the cost model (R9). Null until then.</summary>
    public long? EstimatedCardinality { get; internal set; }

    /// <summary>
    /// Partition affinity hint for distributed execution; always null in the current
    /// single-partition deployment. Reserved for future sharding (R4 placeholder).
    /// </summary>
    public string? PartitionLocality { get; internal set; }

    /// <summary>
    /// True when this node's work can be split into per-partition local execution plus a
    /// coordinator-side merge step (e.g. table scan, filter, project, decomposable aggregate).
    /// Defaults to false; leaf and pipeline nodes that are decomposable override to true (R4).
    /// </summary>
    public virtual bool CanDecomposeToLocalPlusMerge => false;

    // ── R5: EXPLAIN ANALYZE runtime counters ───────────────────────────────

    /// <summary>
    /// Cost estimate assigned by the R9 cost model after plan construction.
    /// Null during normal query execution (only populated when the planner runs with stats).
    /// </summary>
    public PlanCost? Cost { get; internal set; }

    /// <summary>
    /// Runtime counters populated when the query is executed under EXPLAIN ANALYZE (R5).
    /// Null during normal query execution and plain EXPLAIN.
    /// </summary>
    public PlanNodeStats? Stats { get; internal set; }
}
