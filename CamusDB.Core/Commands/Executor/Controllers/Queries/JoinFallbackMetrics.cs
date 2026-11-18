/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The count of hash joins that ran as a nested-loop join since the engine started, reported by
/// <c>SHOW ENGINE STATS</c> as <c>join.nested_loop_fallbacks</c>, and the rate limit of the warning
/// that each fallback writes.
///
/// <para>With <see cref="CamusDBOptions.SpillEnabled"/> off, a hash-join build larger than
/// <see cref="CamusDBOptions.HashJoinMaxBuildRows"/> rows or
/// <see cref="CamusDBOptions.HashJoinMaxBuildBytes"/> estimated bytes runs as a nested-loop join,
/// which compares each row of one side with every row of the other. The plan still shows a hash join, and only
/// <c>EXPLAIN ANALYZE</c> shows what ran, so the count and the warning are the signs an operator
/// sees. The count is a field changed with Interlocked, as in <see cref="DistributedQueryMetrics"/>,
/// because the diagnostics meter records only when diagnostics are on.</para>
///
/// <para>The count is per run of the fallback. A statement that runs one join more than once, for
/// example on the right side of a nested loop, can add more than one.</para>
///
/// <para>The warning is written at most once each <see cref="LogIntervalMs"/>, so a workload that
/// falls back on every statement does not fill the log. The next warning gives the number of
/// fallbacks that wrote no line. There is one instance per engine, owned by
/// <see cref="QueryExecutor"/>, and statements on many threads record into it at the same
/// time.</para>
/// </summary>
internal sealed class JoinFallbackMetrics
{
    /// <summary>The shortest time between two fallback warnings of one engine: one minute.</summary>
    internal const long LogIntervalMs = 60_000;

    private readonly Func<long> clockMs;

    private long nestedLoopFallbacks;

    private long nextLogAtMs = long.MinValue;

    private long fallbacksNotLogged;

    /// <param name="clockMs">
    /// A monotonic clock in milliseconds. The engine uses <see cref="Environment.TickCount64"/>;
    /// tests pass a clock they move.
    /// </param>
    internal JoinFallbackMetrics(Func<long>? clockMs = null)
    {
        this.clockMs = clockMs ?? (static () => Environment.TickCount64);
    }

    /// <summary>Hash joins that ran as a nested-loop join since the engine started.</summary>
    internal long NestedLoopFallbacks => Interlocked.Read(ref nestedLoopFallbacks);

    /// <summary>
    /// Counts one fallback and returns true when the caller must write the warning for it. Then
    /// <paramref name="notLogged"/> is the number of fallbacks since the previous warning that wrote
    /// no line. When two threads reach a new interval at the same time, one wins the
    /// compare-exchange and writes the warning, and the other counts as not logged.
    /// </summary>
    internal bool NoteNestedLoopFallback(out long notLogged)
    {
        Interlocked.Increment(ref nestedLoopFallbacks);

        long now = clockMs();
        long next = Interlocked.Read(ref nextLogAtMs);

        if (now >= next && Interlocked.CompareExchange(ref nextLogAtMs, now + LogIntervalMs, next) == next)
        {
            notLogged = Interlocked.Exchange(ref fallbacksNotLogged, 0);
            return true;
        }

        Interlocked.Increment(ref fallbacksNotLogged);
        notLogged = 0;
        return false;
    }
}

/// <summary>
/// Shapes <see cref="JoinFallbackMetrics"/> into the rows <c>SHOW ENGINE STATS</c> merges with the
/// meter rows. Separate from the holder for the same reason as
/// <see cref="DistributedQueryMetricsReporter"/>: the code that records a fallback does not need to
/// know the format of a diagnostic statement.
/// </summary>
internal static class JoinFallbackMetricsReporter
{
    /// <summary>
    /// The <c>join.nested_loop_fallbacks</c> row. With spill on the fallback cannot run, so when the
    /// count is also zero the statement reports no row, as it does for a meter counter that never
    /// fired. With spill off the row is there from the start, and zero means the fallback did not run.
    /// </summary>
    internal static IReadOnlyList<EngineMetricRow>? Build(JoinFallbackMetrics metrics, bool spillEnabled)
    {
        long fallbacks = metrics.NestedLoopFallbacks;

        if (spillEnabled && fallbacks == 0)
            return null;

        return [new EngineMetricRow("camusdb", "join.nested_loop_fallbacks", "", EngineMetricKind.Counter, fallbacks, fallbacks, null, null, null)];
    }
}
