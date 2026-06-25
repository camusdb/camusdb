
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Post-aggregate row filter for <c>HAVING</c> predicates.
/// </summary>
public sealed class HavingFilterNode : PhysicalPlanNode
{
    public NodeAst Predicate { get; }

    public HavingFilterNode(NodeAst predicate, PhysicalPlanNode input)
    {
        Predicate = predicate;
        Input = input;
    }
}
