
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Residual row filter applied after a scan. Emitted when predicate analysis leaves
/// conjuncts that are not satisfied by the chosen index scan.
/// </summary>
public sealed class FilterNode : PhysicalPlanNode
{
    public NodeAst Predicate { get; }

    public override bool CanDecomposeToLocalPlusMerge => true;

    public FilterNode(NodeAst predicate, PhysicalPlanNode input)
    {
        Predicate = predicate;
        Input = input;
    }
}
