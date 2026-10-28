/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// One operand of a set operation in an EXPLAIN plan. It is not an operator: it labels the side and
/// holds the operand's own plan, whose root is <see cref="PhysicalPlanNode.Input"/>.
///
/// <para><b>EXPLAIN only.</b> A set operation plans its operands when it runs, so the plan of the
/// statement holds no operand plans. EXPLAIN builds them in advance and hangs them under the
/// <see cref="DerivedTableScanNode"/> of the set operation (see
/// <see cref="DerivedTableScanNode.SetOperationOperands"/>), so the operand nodes render under it.
/// EXPLAIN (ANALYZE) then runs these same plans, so their counters are the real ones.</para>
/// </summary>
public sealed class SetOperationOperandNode : PhysicalPlanNode
{
    /// <summary>"left" or "right".</summary>
    public string Side { get; init; } = "";

    /// <summary>The operand's own plan. Its nodes render with it, because a node's detail can read its plan.</summary>
    public QueryPlan Plan { get; init; } = null!;
}
