
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Computes aggregate projections over its input rows.</summary>
public sealed class AggregateNode : PhysicalPlanNode
{
    public AggregateNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
