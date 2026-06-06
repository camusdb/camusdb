
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
    /// <summary>Evaluated LIMIT value captured at plan time; null when the expression is non-constant.</summary>
    public long? LimitValue { get; init; }

    /// <summary>Evaluated OFFSET value captured at plan time; null when absent or non-constant.</summary>
    public long? OffsetValue { get; init; }

    public LimitNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
