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

    /// <summary>
    /// Runs warm-up and then the measured window. <paramref name="stopAfterCompleted"/> ends the window
    /// early once that many operations completed successfully (0 = never); the metrics then carry the
    /// seconds actually measured, so the rates stay honest for a run defined by a count.
    /// </summary>
    public async Task<(RunMetrics Metrics, IReadOnlyList<IntervalRow> Intervals)> RunAsync(
        WorkerState[] workers, TimeSpan warmup, TimeSpan measure, CancellationToken ct, long stopAfterCompleted = 0)
    {
        long freq = Stopwatch.Frequency;
        long warmupEnd = Stopwatch.GetTimestamp() + (long)(warmup.TotalSeconds * freq);

        // Warm-up: same loop, no recording.
        await Task.WhenAll(workers.Select(w => WorkerLoopAsync(w, null, warmupEnd, 0, ct))).ConfigureAwait(false);

        RunMetrics metrics = new(_writesPerTransaction);
        IntervalRecorder recorder = new(metrics);
        long measureStart = Stopwatch.GetTimestamp();
        long measureEnd = measureStart + (long)(measure.TotalSeconds * freq);

        recorder.Start();
        await Task.WhenAll(workers.Select(w => WorkerLoopAsync(w, metrics, measureEnd, stopAfterCompleted, ct))).ConfigureAwait(false);
        metrics.MeasuredSeconds = Math.Min(measure.TotalSeconds, Stopwatch.GetElapsedTime(measureStart).TotalSeconds);
        await recorder.StopAsync().ConfigureAwait(false);

        return (metrics, recorder.Rows);
    }

    private async Task WorkerLoopAsync(WorkerState worker, RunMetrics? metrics, long endTs, long stopAfterCompleted, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && Stopwatch.GetTimestamp() < endTs
               && (stopAfterCompleted <= 0 || metrics is null || metrics.Completed < stopAfterCompleted))
        {
            (Workload.OperationKind kind, long rowIndex) = worker.Selector.Next();

            if (metrics is not null)
            {
                metrics.MarkOffered();
                metrics.MarkStarted();
                metrics.IncInFlight();
            }

            // Guard the dispatch like the open-loop scheduler does: an operation that lets an exception
            // escape (e.g. a request-level TaskCanceledException from a client timeout under a fault)
            // must be recorded as a failed op and classified, never allowed to unwind the worker loop
            // and crash the whole run. Without this a single stray transport cancellation during a fault
            // window aborts an otherwise-recoverable closed-loop run.
            try
            {
                var (result, totalMs) = await _dispatcher
                    .ExecuteAsync(kind, rowIndex, worker.Shard, ct).ConfigureAwait(false);

                if (metrics is not null)
                    metrics.RecordResult(result, totalMs);
            }
            catch (Exception ex)
            {
                if (metrics is not null)
                {
                    (Operations.OperationStatus status, string code) = Operations.ErrorClassifier.Classify(ex);
                    metrics.RecordResult(Operations.OperationResult.Failure(kind, status, code, Operations.ErrorClassifier.MessageOf(ex)), 0);
                }
            }
            finally
            {
                if (metrics is not null)
                    metrics.DecInFlight();
            }
        }
    }
}
