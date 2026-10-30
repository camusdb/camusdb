
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
/// Materialized scan over a bound derived table source.
/// </summary>
public sealed class DerivedTableScanNode : PhysicalPlanNode
{
    public BoundDerivedTableSource BoundSource { get; init; } = null!;

    /// <summary>Single-source predicate pushed into derived materialization.</summary>
    public NodeAst? ExecutionFilter { get; init; }

    /// <summary>
    /// The row count of each operand when the derived table is a set operation and EXPLAIN (ANALYZE)
    /// collects runtime statistics; null otherwise. The explain path sets it before the plan runs.
    /// </summary>
    internal Controllers.Queries.SetOperationStats? SetOperationStats { get; set; }

    /// <summary>
    /// The window operator's counters when the derived table is a windowed SELECT's and EXPLAIN
    /// (ANALYZE) collects runtime statistics; null otherwise. The explain path sets it before the plan
    /// runs, and the render after the run prints it.
    /// </summary>
    internal Controllers.Queries.WindowStats? WindowStats { get; set; }

    /// <summary>
    /// The operand plans of a set operation, left then right, when EXPLAIN built them; null otherwise.
    /// See <see cref="SetOperationOperandNode"/>.
    /// </summary>
    public IReadOnlyList<SetOperationOperandNode>? SetOperationOperands { get; internal set; }
}
