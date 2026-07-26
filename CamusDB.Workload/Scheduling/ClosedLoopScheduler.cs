/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Workload.Metrics;

namespace CamusDB.Workload.Scheduling;

/// <summary>
/// Closed-loop (saturation) load: a fixed set of workers, each submitting its next operation only
/// after the previous one completes. This discovers maximum completed ops/s and the latency/concurrency
/// knee, but its percentiles are inherently closed-loop and must be labeled as such — they are not
/// comparable to open-loop percentiles because a slow server slows the offered rate too. Warm-up runs
/// the same loop without recording; measurement records; because each worker awaits its own operation,
/// stopping the loop after the measurement window drains naturally (no operation is left dangling).
/// </summary>
public sealed class ClosedLoopScheduler
{
    private readonly IOperationExecutor _dispatcher;
    private readonly int _writesPerTransaction;

    public ClosedLoopScheduler(IOperationExecutor dispatcher, int writesPerTransaction)
    {
        _dispatcher = dispatcher;
        _writesPerTransaction = writesPerTransaction;
    }

    public async Task<(RunMetrics Metrics, IReadOnlyList<IntervalRow> Intervals)> RunAsync(
        WorkerState[] workers, TimeSpan warmup, TimeSpan measure, CancellationToken ct)
    {
        long freq = Stopwatch.Frequency;
        long warmupEnd = Stopwatch.GetTimestamp() + (long)(warmup.TotalSeconds * freq);

        // Warm-up: same loop, no recording.
        await Task.WhenAll(workers.Select(w => WorkerLoopAsync(w, null, warmupEnd, ct))).ConfigureAwait(false);

        RunMetrics metrics = new(_writesPerTransaction);
        IntervalRecorder recorder = new(metrics);
        long measureEnd = Stopwatch.GetTimestamp() + (long)(measure.TotalSeconds * freq);

        recorder.Start();
        await Task.WhenAll(workers.Select(w => WorkerLoopAsync(w, metrics, measureEnd, ct))).ConfigureAwait(false);
        await recorder.StopAsync().ConfigureAwait(false);

        return (metrics, recorder.Rows);
    }

    private async Task WorkerLoopAsync(WorkerState worker, RunMetrics? metrics, long endTs, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && Stopwatch.GetTimestamp() < endTs)
        {
            (Workload.OperationKind kind, long rowIndex) = worker.Selector.Next();

            if (metrics is not null)
            {
                metrics.MarkOffered();
                metrics.MarkStarted();
                metrics.IncInFlight();
            }

            var (result, totalMs) = await _dispatcher
                .ExecuteAsync(kind, rowIndex, worker.Shard, ct).ConfigureAwait(false);

            if (metrics is not null)
            {
                metrics.RecordResult(result, totalMs);
                metrics.DecInFlight();
            }
        }
    }
}
