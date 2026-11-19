/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The wrappers that EXPLAIN (ANALYZE) puts on the row cursor of a plan node to fill its
/// <see cref="PlanNodeStats"/>. A plain query never gets one, so it pays for no counter and no timer.
///
/// <para>The time of a node is inclusive: the timer runs from each request for a row until the row or
/// the end arrives, and a request runs the inputs of the node. The time between two requests is the
/// parent's work on the row it got, so it is left out. A parent therefore takes at least the time of
/// each of its inputs, because it asks for their rows inside its own requests.</para>
///
/// <para>The counters are plain fields, written on the one thread that consumes the cursor (see
/// <see cref="PlanNodeStats"/>). Each row costs two reads of <see cref="Stopwatch.GetTimestamp"/>.</para>
/// </summary>
internal static class PlanNodeCursor
{
    /// <summary>
    /// Streams <paramref name="source"/>, counts its rows in <see cref="PlanNodeStats.RowsEmitted"/> and
    /// adds the time of each request to <see cref="PlanNodeStats.CursorTicks"/>. A node that runs more
    /// than one time (a fallback reads its input again) adds up over the runs. A consumer that stops early
    /// adds nothing for the request it did not make.
    /// </summary>
    public static async IAsyncEnumerable<QueryResultRow> CountAndTime(
        IAsyncEnumerable<QueryResultRow> source,
        PlanNodeStats stats)
    {
        stats.CursorTimed = true;

        // Zero while the parent holds the last row: a stop at that point has no open request to add.
        long requested = Stopwatch.GetTimestamp();

        try
        {
            await foreach (QueryResultRow row in source.ConfigureAwait(false))
            {
                stats.CursorTicks += Stopwatch.GetTimestamp() - requested;
                stats.RowsEmitted++;
                requested = 0;

                yield return row;

                requested = Stopwatch.GetTimestamp();
            }
        }
        finally
        {
            if (requested != 0)
                stats.CursorTicks += Stopwatch.GetTimestamp() - requested;
        }
    }

    /// <summary>
    /// Streams the rows of one run of a <c>LATERAL</c> body for a nested-loop join and adds the time of
    /// each request to <see cref="PlanNodeStats.LateralTicks"/>. The caller counts the run.
    /// </summary>
    public static async IAsyncEnumerable<QueryResultRow> TimeLateralBody(
        IAsyncEnumerable<QueryResultRow> body,
        PlanNodeStats stats)
    {
        long requested = Stopwatch.GetTimestamp();

        try
        {
            await foreach (QueryResultRow row in body.ConfigureAwait(false))
            {
                stats.LateralTicks += Stopwatch.GetTimestamp() - requested;
                requested = 0;

                yield return row;

                requested = Stopwatch.GetTimestamp();
            }
        }
        finally
        {
            if (requested != 0)
                stats.LateralTicks += Stopwatch.GetTimestamp() - requested;
        }
    }
}
