
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
    /// <summary>
    /// True when the input scan guarantees index-ordered output covering all DISTINCT key columns,
    /// enabling O(1)-memory streaming deduplication instead of the default hash-set approach.
    /// </summary>
    public bool IsStreaming { get; init; }

    public DistinctNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
