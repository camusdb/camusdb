/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// The aggregate and direction of a grouped top-k query, the one ORDER BY key that a top-k
/// aggregation can use to send or keep fewer groups.
/// </summary>
public enum AggregateTopKOrder
{
    /// <summary>
    /// <c>COUNT(*)</c> or <c>COUNT(col)</c>, descending. Each row adds 0 or 1 to its group, so a
    /// partial count is a lower bound of the total, and the sum of partial counts is an upper bound
    /// of any one group in them.
    /// </summary>
    CountDescending,

    /// <summary>
    /// <c>MAX(x)</c>, descending. MAX skips NULL, and NULL is the smallest value of
    /// <see cref="ColumnValue.CompareTo"/>, so the merged MAX of a group is the largest of its span
    /// values in the order the sort uses. That is what makes a span-local top k exact.
    /// </summary>
    MaxDescending,

    /// <summary>
    /// <c>MIN(x)</c>, ascending, only on a NOT NULL column. MIN skips NULL but the sort puts NULL
    /// first, so with NULL values the merged MIN is not the smallest span value in sort order, and a
    /// span-local top k could lose a group.
    /// </summary>
    MinAscending,
}

/// <summary>
/// The shape of <c>GROUP BY ... ORDER BY aggregate LIMIT n [OFFSET m]</c>, recorded by the planner on
/// the <see cref="AggregateNode"/> so that an executor can send or keep fewer than all groups.
///
/// <para>The planner records it only when the single ORDER BY key names the alias of one projected
/// aggregate of an <see cref="AggregateTopKOrder"/> kind, and the query has no HAVING. Recording the
/// shape changes no result by itself: each executor that reads it must prove its own pruning safe for
/// the <see cref="Order"/> it supports, and must keep its full path for the others.</para>
///
/// <para><b>Ties.</b> Without a second ORDER BY key, any choice among the groups that tie with the
/// k-th value is a valid answer, so a pruned result must have the same values at each position as an
/// unpruned one, but not always the same groups. A consumer that prunes keeps every group that ties
/// with its own k-th value, so that each group of the true top k reaches the merge.</para>
/// </summary>
/// <param name="OutputName">The alias of the ordered aggregate, as it appears in the output row.</param>
/// <param name="Order">Which aggregate, and in which direction.</param>
/// <param name="K"><c>offset + limit</c>: the number of leading groups the query can return.</param>
/// <param name="OtherAggregatesProjected">True when the projection holds anything besides bare columns
/// and the ordered aggregate: another aggregate, or a computed expression. A span that sends only its
/// local top k by the ordered aggregate gives exact values for that aggregate only, so this case needs
/// values from every span.</param>
public sealed record AggregateTopK(string OutputName, AggregateTopKOrder Order, int K, bool OtherAggregatesProjected)
{
    /// <summary>True when the ordered aggregate sorts its largest value first.</summary>
    public bool Descending => Order != AggregateTopKOrder.MinAscending;

    /// <summary>
    /// True when each span may send only its local top k (ties included) and the merge still
    /// returns the exact answer. Let group g be in the global top k, and let s be the span that
    /// holds its merged value v. A group that is above v in s is above v globally too, and fewer
    /// than k groups are, so g is in the local top k of s with its exact value. A group outside the
    /// global top k can merge to a value that is too low, but such a group cannot displace a true
    /// top-k group. This holds for MAX DESC and MIN ASC on a NOT NULL column. It does not hold for a
    /// COUNT, which adds up over the spans, and not when another aggregate must be complete.
    /// </summary>
    public bool SpanLocalTopKIsExact =>
        Order is AggregateTopKOrder.MaxDescending or AggregateTopKOrder.MinAscending
        && !OtherAggregatesProjected;
}
