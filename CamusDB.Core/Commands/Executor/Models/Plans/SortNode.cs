
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Sorts input rows according to the query ORDER BY clause.</summary>
public sealed class SortNode : PhysicalPlanNode
{
    /// <summary>Sort keys captured at plan time.</summary>
    public IReadOnlyList<QueryOrderBy>? OrderBy { get; init; }

    public SortNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
