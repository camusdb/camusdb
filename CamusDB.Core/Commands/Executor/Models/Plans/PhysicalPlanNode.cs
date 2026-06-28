
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

    /// <summary>Columns that must be present in rows produced by this subtree. Null means all table columns.</summary>
    public IReadOnlySet<string>? RequiredColumns { get; set; }

    // ── R4: Distributed-ready plan properties ──────────────────────────────

    /// <summary>
    /// The ordering this node guarantees on its output rows.
    /// Set by the planner when an index scan satisfies ORDER BY (sort elision) or when a
    /// SortNode is added. Null means the output order is undefined.
    /// </summary>
    public IReadOnlyList<QueryOrderBy>? OutputOrdering { get; internal set; }

    /// <summary>Estimated output row count; populated by the cost model. Null until then.</summary>
    public long? EstimatedCardinality { get; internal set; }

    /// <summary>
    /// Distribution of this node's output data across the cluster.
    /// Populated on scan leaves by <see cref="Controllers.Queries.QueryPlanner"/> /
    /// <see cref="Controllers.Queries.JoinQueryPlanner"/> from the table/index sharding scheme.
    /// Null on all non-leaf nodes (filter, sort, join, …).
    ///
    /// Staleness contract: reflects <see cref="CamusDB.Core.CamusDBConfig.KeyRangeShardingEnabled"/>
    /// at plan-build time; leadership transitions are not tracked (approximate cost-estimation snapshot).
    /// </summary>
    public DataDistribution? Distribution { get; internal set; }

    /// <summary>
    /// True when this node's work can be split into per-partition local execution plus a
    /// coordinator-side merge step (e.g. table scan, filter, project, decomposable aggregate).
    /// Defaults to false; leaf and pipeline nodes that are decomposable override to true.
    /// </summary>
    public virtual bool CanDecomposeToLocalPlusMerge => false;

    // ── R5: EXPLAIN ANALYZE runtime counters ───────────────────────────────

    /// <summary>
    /// Cost estimate assigned by the cost model after plan construction.
    /// Null during normal query execution (only populated when the planner runs with stats).
    /// </summary>
    public PlanCost? Cost { get; internal set; }

    /// <summary>
    /// Runtime counters populated when the query is executed under EXPLAIN ANALYZE.
    /// Null during normal query execution and plain EXPLAIN.
    /// </summary>
    public PlanNodeStats? Stats { get; internal set; }
}
