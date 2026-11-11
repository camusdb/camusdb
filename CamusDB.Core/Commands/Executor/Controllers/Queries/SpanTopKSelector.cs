/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Keeps the partial rows of one span whose ranked value is in the first k, for a grouped aggregate
/// fragment that the coordinator asked for its local top k (<c>QueryFragmentRequest.TopK</c>).
///
/// <para><b>Ties are kept.</b> Every row whose value ties with the k-th value is kept, so the result
/// can hold more than k rows. With ties kept, each group of the global top k reaches the coordinator
/// with its exact value, whatever order the span saw the tied groups in, which is the simple form of
/// the proof in <c>AggregateTopK.SpanLocalTopKIsExact</c>. A selector that kept exactly k rows would
/// drop some tied groups at random; the answer would still be valid, because a kept tied group has a
/// value at least as large, but that proof is longer and depends on the merge order.</para>
///
/// <para><b>Order.</b> Values are ranked with <see cref="ColumnValue.CompareTo"/>, the same comparison
/// the coordinator's sort and the MAX and MIN accumulators use, NULL as the smallest value. The rows
/// come back in their input order; the coordinator sorts after the merge.</para>
///
/// <para>Cost: one pass with a heap of k values, then one pass to keep the rows, so O(n log k) time
/// and O(k) extra memory over the n rows the aggregation already holds.</para>
/// </summary>
internal static class SpanTopKSelector
{
    /// <summary>
    /// Returns the rows of <paramref name="rows"/> whose <paramref name="outputName"/> value ranks in
    /// the first <paramref name="k"/> in the given direction, ties with the k-th value included. A row
    /// that does not carry the value counts as NULL.
    /// </summary>
    public static List<QueryResultRow> Select(List<QueryResultRow> rows, string outputName, bool descending, int k)
    {
        if (k <= 0)
            return [];

        if (rows.Count <= k)
            return rows;

        // "Better" sorts first. The heap holds the k best values seen so far with the worst of them at
        // the top, so a better value replaces the top in O(log k).
        Comparison<ColumnValue> rank = descending
            ? static (left, right) => right.CompareTo(left)
            : static (left, right) => left.CompareTo(right);

        PriorityQueue<ColumnValue, ColumnValue> best = new(k, Comparer<ColumnValue>.Create((left, right) => rank(right, left)));

        for (int i = 0; i < rows.Count; i++)
        {
            ColumnValue value = ValueOf(rows[i], outputName);

            if (best.Count < k)
            {
                best.Enqueue(value, value);
                continue;
            }

            if (rank(value, best.Peek()) < 0)
                best.DequeueEnqueue(value, value);
        }

        ColumnValue threshold = best.Peek();
        List<QueryResultRow> kept = new(k);

        for (int i = 0; i < rows.Count; i++)
        {
            if (rank(ValueOf(rows[i], outputName), threshold) <= 0)
                kept.Add(rows[i]);
        }

        return kept;
    }

    private static ColumnValue ValueOf(QueryResultRow row, string outputName) =>
        row.Row.TryGetValue(outputName, out ColumnValue? value) && value is not null ? value : ColumnValue.Null;
}
