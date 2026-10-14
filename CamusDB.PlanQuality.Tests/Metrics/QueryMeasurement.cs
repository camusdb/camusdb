/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.PlanQuality.Tests.Metrics;

/// <summary>
/// The counters of one plan node, as one <c>EXPLAIN (ANALYZE)</c> row prints them.
/// <see cref="EstimatedRows"/> is the estimate that q-error uses: the join-order search's own
/// estimate (<c>join-order-est-rows=</c>) on a join it costed, and the plan annotation's
/// <c>estimated_rows</c> elsewhere. The annotation estimates an index-nested-loop join as one row
/// for each outer row, which is a different quantity from the rows the node produces.
/// </summary>
internal sealed record PlanNodeMeasure(
    string Node,
    string Detail,
    long? EstimatedRows,
    long? ActualRows,
    long RowsRead,
    long KvLookups,
    long KvScanEntries);

/// <summary>
/// Everything the harness measures for one query. The fields fall in three groups:
/// <list type="bullet">
///   <item><b>Repeats between runs, gated:</b> <see cref="ResultHash"/>, <see cref="Work"/>,
///   <see cref="StatementRowsRead"/>, <see cref="JoinStepsCosted"/>.</item>
///   <item><b>Repeats between runs, reported:</b> <see cref="Plan"/> (the fingerprint),
///   <see cref="JoinOrder"/>, <see cref="QErrorMedian"/>, <see cref="QErrorMax"/>,
///   <see cref="ResultRows"/>, <see cref="Nodes"/>.</item>
///   <item><b>Changes between runs, reported only:</b> <see cref="PlanningMs"/> and
///   <see cref="ExecutionMs"/>. They never go into a baseline.</item>
/// </list>
/// <see cref="Work"/> is the sum of the storage counters of the plan nodes. <see cref="StatementRowsRead"/>
/// is the rows the whole statement read, from the statement probe of the slow query log. It also
/// counts the reads of the subqueries that run outside the plan nodes, such as a correlated
/// <c>EXISTS</c> that runs once for each outer row, so it sees a regression there that
/// <see cref="Work"/> cannot see. When the query failed, <see cref="Error"/> is set and the numbers are
/// zero.
/// </summary>
internal sealed record QueryMeasurement(
    string Id,
    IReadOnlyList<string> Tags,
    long ResultRows,
    string ResultHash,
    long Work,
    long StatementRowsRead,
    double QErrorMedian,
    double QErrorMax,
    string? JoinOrder,
    long JoinStepsCosted,
    IReadOnlyList<string> Plan,
    double PlanningMs,
    double ExecutionMs,
    IReadOnlyList<PlanNodeMeasure> Nodes,
    string? Error)
{
    public static QueryMeasurement Failed(string id, IReadOnlyList<string> tags, string error) =>
        new(id, tags, 0, "", 0, 0, 1, 1, null, 0, [], 0, 0, [], error);
}
