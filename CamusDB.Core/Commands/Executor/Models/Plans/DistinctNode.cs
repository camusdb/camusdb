
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Eliminates duplicate projected output rows (QP3.6).</summary>
public sealed class DistinctNode : PhysicalPlanNode
{
    public DistinctNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
