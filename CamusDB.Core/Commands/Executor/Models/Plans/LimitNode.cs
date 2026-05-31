
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Applies LIMIT and OFFSET over its input rows.</summary>
public sealed class LimitNode : PhysicalPlanNode
{
    public LimitNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
