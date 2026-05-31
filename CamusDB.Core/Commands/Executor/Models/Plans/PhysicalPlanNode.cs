
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Base type for physical plan nodes produced by <see cref="Controllers.Queries.QueryPlanner"/>.
/// Each node wraps zero or one input; leaf nodes read from storage.
/// </summary>
public abstract class PhysicalPlanNode
{
    public PhysicalPlanNode? Input { get; init; }
}
