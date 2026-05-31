
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
/// Materialized scan over a bound derived table source (QP5.5).
/// </summary>
public sealed class DerivedTableScanNode : PhysicalPlanNode
{
    public BoundDerivedTableSource BoundSource { get; init; } = null!;

    /// <summary>Single-source predicate pushed into derived materialization (QP4.4).</summary>
    public NodeAst? ExecutionFilter { get; init; }
}
