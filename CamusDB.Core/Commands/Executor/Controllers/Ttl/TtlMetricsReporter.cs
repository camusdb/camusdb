
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// Shapes the row-level TTL counters a <see cref="TtlScheduler"/> maintains into the rows
/// <c>SHOW ENGINE STATS</c> reports. Separate from the scheduler because the scheduler's job is to
/// run sweeps, not to know the presentation format of a diagnostic statement — and separate from
/// the statement because these counters are maintained by the scheduler rather than by a meter, so
/// they cannot come from the metrics collector the rest of the statement reads.
/// </summary>
internal static class TtlMetricsReporter
{
    /// <summary>
    /// Builds the TTL rows for <c>SHOW ENGINE STATS</c>, or null when no scheduler is running on this
    /// node — the statement's existing all-or-nothing rule, where a disabled subsystem reports nothing
    /// rather than a table of zeros that reads as "TTL ran and found no work".
    ///
    /// <para><c>ttl.rows_skipped_recheck</c> is the one worth watching: it counts rows that looked
    /// expired during a scan but had their expiry extended before the delete. A steady trickle is
    /// normal on a session table; a number close to <c>ttl.rows_expired</c> means the sweep is mostly
    /// racing live traffic and its cadence or batch sizes want revisiting.</para>
    /// </summary>
    internal static IReadOnlyList<EngineMetricRow>? Build(TtlScheduler? scheduler)
    {
        if (scheduler is null)
            return null;

        List<EngineMetricRow> rows =
        [
            Counter("ttl.rows_expired", Interlocked.Read(ref scheduler.RowsExpired)),
            Counter("ttl.rows_skipped_recheck", Interlocked.Read(ref scheduler.RowsSkippedByRecheck)),
            Counter("ttl.rows_failed", Interlocked.Read(ref scheduler.RowsFailed)),
            Counter("ttl.spans_completed", Interlocked.Read(ref scheduler.SpansCompleted)),
            Counter("ttl.spans_reclaimed", Interlocked.Read(ref scheduler.SpansReclaimed)),
            Counter("ttl.runs_planned", Interlocked.Read(ref scheduler.RunsPlanned)),
            Counter("ttl.runs_completed", Interlocked.Read(ref scheduler.RunsCompleted)),
            Counter("ttl.sweep_duration_ms", Interlocked.Read(ref scheduler.SweepMillis)),
        ];

        // Per-table rows, tagged by database and table. Totals say whether TTL works at all; they cannot
        // say which table stopped, because one healthy table's numbers mask another's silence.
        foreach (KeyValuePair<string, TtlTableStatus> entry in scheduler.TableStatus)
        {
            TtlTableStatus status = entry.Value;
            string tags = $"db={status.DatabaseName},table={status.TableName}";

            rows.Add(Gauge("ttl.table.state", tags, (long)status.State));
            rows.Add(Gauge("ttl.table.spans_done", tags, status.SpansDone));
            rows.Add(Gauge("ttl.table.span_count", tags, status.SpanCount));
            rows.Add(Gauge("ttl.table.rows_deleted", tags, status.RowsDeleted));
            rows.Add(Gauge("ttl.table.rows_failed", tags, status.RowsFailed));

            // The horizon is what makes a stalled run diagnosable: it says which instant the run is
            // still judging rows against, and therefore how far behind the table has fallen.
            rows.Add(Gauge("ttl.table.horizon_ms", tags, status.HorizonPhysical));
            rows.Add(Gauge("ttl.table.last_observed_ms", tags, status.LastObservedPhysical));
        }

        return rows;
    }

    private static EngineMetricRow Counter(string metric, long value)
        => new("camusdb", metric, "", EngineMetricKind.Counter, value, value, null, null, null);

    private static EngineMetricRow Gauge(string metric, string tags, long value)
        => new("camusdb", metric, tags, EngineMetricKind.Gauge, 1, null, null, null, value);
}
