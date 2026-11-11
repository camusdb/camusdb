/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The k-th count that the bounded sort above a <c>GROUP BY ... ORDER BY COUNT(...) DESC LIMIT n</c>
/// currently holds, passed down to the spill aggregation below it so that it can skip a spill
/// partition in which no group can reach that count.
///
/// <para><b>One writer, one reader, one flow.</b> The bounded sort (<c>QuerySorter</c>) writes it each
/// time its heap of k rows changes once full. The spill aggregation (<c>QueryAggregator</c>) reads it
/// before it reads back each partition. Both run in the same asynchronous flow: the sort pulls each
/// aggregated row and puts it in its heap before it asks for the next one, so when the aggregator
/// finishes one partition, the value already reflects every row of it. No lock is needed.</para>
///
/// <para><b>Why a skipped group changes nothing.</b> A value exists only once the heap holds k rows,
/// each with a count at or above it, and the k-th count of a heap only grows as rows arrive. A
/// partition is skipped when its upper bound is at or below the value read. Each skipped group then
/// has a count at or below the worst row of the full heap, and the heap refuses such a row: it takes
/// only a row that is strictly better, so the first of two tied rows stays. The output is therefore
/// the same as the output of the same partitions read in full, in the same order. When the sort
/// hands over to its spilling selection the value stops changing, which only skips less.</para>
///
/// <para>A new instance is made for each execution of the plan by <c>QueryExecutor</c>, never kept on
/// a plan node, so a second run of a plan does not start with an old value.</para>
/// </summary>
internal sealed class AggregateTopKThreshold
{
    private long? kthCount;

    /// <summary>The k-th count of the full heap, or null until the heap holds k rows.</summary>
    public long? KthCount => kthCount;

    /// <summary>Records the value of the worst row of the full heap. A count is always an
    /// Integer64, so any other type is ignored and leaves the value unset.</summary>
    public void Publish(ColumnValue value)
    {
        if (value.Type == ColumnType.Integer64)
            kthCount = value.LongValue;
    }

    /// <summary>True when no group whose count is at most <paramref name="upperBound"/> can enter the
    /// full heap: the bound is at or below its k-th count.</summary>
    public bool Prunes(long upperBound) => kthCount is { } kth && upperBound <= kth;
}
