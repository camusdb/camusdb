
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

    /// <summary>
    /// Number of rows this sort may retain when a LIMIT above it makes the rest unreachable, or null
    /// to sort the whole input.
    ///
    /// <para>It is <c>offset + limit</c>, not <c>limit</c>: the rows OFFSET skips still have to be
    /// ranked before they can be skipped. Set only when the limit is a known finite value and the
    /// bound is small enough to hold in memory; the full external sort remains the fallback for
    /// everything else.</para>
    ///
    /// <para>The single-table executor runs this plan tree directly, so this one field decides both
    /// what EXPLAIN shows and what actually executes. A separate execution-side decision could drift
    /// from the plan and report an operator the query never used.</para>
    /// </summary>
    public long? BoundedLimit { get; internal set; }

    public SortNode(PhysicalPlanNode input)
    {
        Input = input;
    }
}
