/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

namespace CamusDB.Workload.Metrics;

/// <summary>One second of the measured run: cumulative-delta counters plus per-kind latency percentiles for that second.</summary>
public readonly record struct IntervalRow(
    int Second,
    long Offered,
    long Started,
    long Completed,
    long Failed,
    long InFlight,
    long ScheduleDrops,
    double ReadP50, double ReadP95, double ReadP99,
    double WriteP50, double WriteP95, double WriteP99);

/// <summary>
/// Samples <see cref="RunMetrics"/> once per second on a monotonic clock and records the per-second
/// deltas the report needs, without pausing the workload. Percentiles for each second come from the
/// difference of two histogram snapshots, so an interval reflects only that second's operations. The
/// recorder is started when measurement begins and stopped when it ends; totals across all rows
/// reconcile with the final summary.
/// </summary>
public sealed class IntervalRecorder
{
    private readonly RunMetrics _metrics;
    private readonly List<IntervalRow> _rows = new();
    private CancellationTokenSource? _cts;
    private Task? _loop;

    public IntervalRecorder(RunMetrics metrics)
    {
        _metrics = metrics;
    }

    public IReadOnlyList<IntervalRow> Rows => _rows;

    public void Start()
    {
        // Anchor the measured window to wall-clock so intervals.csv (second 0 == now) can be aligned
        // with external wall-clock event streams (e.g. a chaos harness's fault timeline).
        _metrics.MeasureStartUtc = DateTime.UtcNow;
        _cts = new CancellationTokenSource();
        _loop = Task.Run(() => SampleLoopAsync(_cts.Token));
    }

    public async Task StopAsync()
    {
        if (_cts is null || _loop is null)
            return;
        _cts.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // expected on stop
        }
        _cts.Dispose();
    }

    private async Task SampleLoopAsync(CancellationToken ct)
    {
        int second = 0;
        long prevOffered = 0, prevStarted = 0, prevCompleted = 0, prevFailed = 0, prevDrops = 0;
        LatencyHistogram prevRead = _metrics.ReadLatency.Snapshot();
        LatencyHistogram prevWrite = _metrics.WriteLatency.Snapshot();
        long nextTick = Stopwatch.GetTimestamp() + Stopwatch.Frequency;

        while (!ct.IsCancellationRequested)
        {
            long now = Stopwatch.GetTimestamp();
            if (now < nextTick)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(20), ct).ConfigureAwait(false);
                continue;
            }
            nextTick += Stopwatch.Frequency;

            long offered = _metrics.Offered;
            long started = _metrics.Started;
            long completed = _metrics.Completed;
            long failed = _metrics.Failed;
            long drops = _metrics.ScheduleDrops;

            LatencyHistogram readSnap = _metrics.ReadLatency.Snapshot();
            LatencyHistogram writeSnap = _metrics.WriteLatency.Snapshot();
            LatencyHistogram readDelta = readSnap.Since(prevRead);
            LatencyHistogram writeDelta = writeSnap.Since(prevWrite);

            _rows.Add(new IntervalRow(
                second++,
                offered - prevOffered,
                started - prevStarted,
                completed - prevCompleted,
                failed - prevFailed,
                _metrics.InFlight,
                drops - prevDrops,
                readDelta.Percentile(50), readDelta.Percentile(95), readDelta.Percentile(99),
                writeDelta.Percentile(50), writeDelta.Percentile(95), writeDelta.Percentile(99)));

            prevOffered = offered;
            prevStarted = started;
            prevCompleted = completed;
            prevFailed = failed;
            prevDrops = drops;
            prevRead = readSnap;
            prevWrite = writeSnap;
        }
    }
}
