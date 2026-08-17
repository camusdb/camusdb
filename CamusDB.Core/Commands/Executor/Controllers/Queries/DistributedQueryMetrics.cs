
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Process-lifetime counters for distributed query execution, reported by
/// <c>SHOW ENGINE STATS</c> as <c>distributed.*</c> rows. Maintained directly (Interlocked on
/// public fields, the same pattern as the TTL scheduler's totals) rather than through a meter,
/// because the statement's meter collector observes only the embedded Kahuna/Kommander meters
/// and the diagnostics meter is opt-in — an operator asking "is distribution actually doing
/// anything" must get an answer regardless of tracing configuration.
///
/// <para>One instance per engine, owned by <see cref="QueryExecutor"/>. The coordinator-side
/// counters (dispatched, fallbacks, shipped-in, partial-aggregate merges) and the serving-side
/// counters (served, shipped-out) live together deliberately: every node is both roles, and
/// comparing the two sides across a fleet is how an operator spots asymmetry (one node serving
/// everyone, or fallbacks concentrating on one peer).</para>
/// </summary>
internal sealed class DistributedQueryMetrics
{
    /// <summary>Remote fragment executions this coordinator started (row and aggregate fragments).</summary>
    internal long FragmentsDispatched;

    /// <summary>Remote fragments that failed and were resumed or recomputed locally.</summary>
    internal long FragmentFallbacks;

    /// <summary>Frames received from peers: survivor rows plus partial-aggregate group rows.</summary>
    internal long RowsShippedIn;

    /// <summary>Peer fragments this node executed (the serving side of the channel).</summary>
    internal long FragmentsServed;

    /// <summary>Frames this node returned to peers (survivor rows plus partial-aggregate group rows).</summary>
    internal long RowsShippedOut;

    /// <summary>Aggregations executed as per-span partials with a coordinator-side merge.</summary>
    internal long PartialAggregateGathers;
}

/// <summary>
/// Shapes <see cref="DistributedQueryMetrics"/> into the rows <c>SHOW ENGINE STATS</c> merges
/// alongside the meter-backed metrics. Separate from the holder for the same reason the TTL
/// reporter is separate from its scheduler: recording sites should not know the presentation
/// format of a diagnostic statement. Callers gate on <c>DistributedQueryExecutionEnabled</c> —
/// with the feature off the statement reports no <c>distributed.*</c> rows at all, following
/// the statement's all-or-nothing rule (a table of zeros would read as "distribution ran and
/// found no work" rather than "distribution is off").
/// </summary>
internal static class DistributedQueryMetricsReporter
{
    internal static IReadOnlyList<EngineMetricRow> Build(DistributedQueryMetrics metrics) =>
    [
        Counter("distributed.fragments_dispatched", Interlocked.Read(ref metrics.FragmentsDispatched)),
        Counter("distributed.fragment_fallbacks", Interlocked.Read(ref metrics.FragmentFallbacks)),
        Counter("distributed.fragments_served", Interlocked.Read(ref metrics.FragmentsServed)),
        Counter("distributed.partial_aggregate_gathers", Interlocked.Read(ref metrics.PartialAggregateGathers)),
        Counter("distributed.rows_shipped_in", Interlocked.Read(ref metrics.RowsShippedIn)),
        Counter("distributed.rows_shipped_out", Interlocked.Read(ref metrics.RowsShippedOut)),
    ];

    private static EngineMetricRow Counter(string metric, long value)
        => new("camusdb", metric, "", EngineMetricKind.Counter, value, value, null, null, null);
}
