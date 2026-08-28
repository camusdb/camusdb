/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Util;

namespace CamusDB.Workload.Scheduling;

/// <summary>
/// Open-loop load: operations are generated at a fixed target rate on a monotonic clock and dispatched
/// without waiting for prior operations to finish, so an overloaded server cannot slow the offered
/// rate (the defense against coordinated omission). Each arrival records its intended time; scheduling
/// delay (intended → actual dispatch) is reported separately from response latency. A bounded in-flight
/// cap prevents an unbounded queue from hiding overload — when the cap is hit the arrival is counted as
/// a schedule drop instead of being silently deferred. Selection happens on the pacing thread (the
/// per-worker PRNG is not thread-safe); dispatch is fire-and-forget and drained after the window.
/// </summary>
public sealed class OpenLoopScheduler
{
    private readonly IOperationExecutor _dispatcher;
    private readonly int _writesPerTransaction;
    private readonly ulong _seed;

    private int _liveInFlight;

    public OpenLoopScheduler(IOperationExecutor dispatcher, int writesPerTransaction, ulong seed)
    {
        _dispatcher = dispatcher;
        _writesPerTransaction = writesPerTransaction;
        _seed = seed;
    }

    public async Task<(RunMetrics Metrics, IReadOnlyList<IntervalRow> Intervals)> RunAsync(
        WorkerState[] workers, int targetOps, int maxInFlight,
        TimeSpan warmup, TimeSpan measure, TimeSpan drain, CancellationToken ct)
    {
        RunMetrics metrics = new(_writesPerTransaction);
        IntervalRecorder recorder = new(metrics);

        long freq = Stopwatch.Frequency;
        double perWorkerInterval = (double)workers.Length / Math.Max(1, targetOps); // seconds between a worker's arrivals
        long intervalTicks = Math.Max(1, (long)(perWorkerInterval * freq));

        long start = Stopwatch.GetTimestamp();
        long measureStart = start + (long)(warmup.TotalSeconds * freq);
        long measureEnd = measureStart + (long)(measure.TotalSeconds * freq);

        // Start recording exactly at the measurement window.
        Task recorderStart = Task.Run(async () =>
        {
            await DelayUntilAsync(measureStart, ct).ConfigureAwait(false);
            recorder.Start();
        }, ct);

        Task[] pacers = new Task[workers.Length];
        for (int w = 0; w < workers.Length; w++)
            pacers[w] = PaceWorkerAsync(workers[w], w, intervalTicks, maxInFlight, measureStart, measureEnd, metrics, ct);

        await Task.WhenAll(pacers).ConfigureAwait(false);

        // Drain in-flight operations up to the drain budget.
        long drainDeadline = Stopwatch.GetTimestamp() + (long)(drain.TotalSeconds * freq);
        while (Volatile.Read(ref _liveInFlight) > 0 && Stopwatch.GetTimestamp() < drainDeadline)
            await Task.Delay(20, CancellationToken.None).ConfigureAwait(false);

        // Whatever is still running when the budget expires can still commit, including after the
        // post-run verification scan reads its rows. Record it so reconciliation can tell a run whose
        // durable state was settled from one whose was not.
        metrics.UnfinishedAtDrain = Volatile.Read(ref _liveInFlight);

        try { await recorderStart.ConfigureAwait(false); } catch (OperationCanceledException) { }
        await recorder.StopAsync().ConfigureAwait(false);

        return (metrics, recorder.Rows);
    }

    private async Task PaceWorkerAsync(
        WorkerState worker, int workerIndex, long intervalTicks, int maxInFlight,
        long measureStart, long measureEnd, RunMetrics metrics, CancellationToken ct)
    {
        DeterministicRandom jitter = DeterministicRandom.ForWorker(_seed ^ 0xA5A5A5A5A5A5A5A5UL, workerIndex);
        // Stagger worker starts across one interval so they do not all fire on the same tick.
        long intended = Stopwatch.GetTimestamp() + (long)(jitter.NextDouble() * intervalTicks);

        while (!ct.IsCancellationRequested && intended < measureEnd)
        {
            if (Stopwatch.GetTimestamp() < intended)
                await DelayUntilAsync(intended, ct).ConfigureAwait(false);

            bool measured = intended >= measureStart;
            if (measured)
                metrics.MarkOffered();

            (Workload.OperationKind kind, long rowIndex) = worker.Selector.Next();

            if (Volatile.Read(ref _liveInFlight) >= maxInFlight)
            {
                if (measured)
                    metrics.MarkScheduleDrop();
            }
            else
            {
                Interlocked.Increment(ref _liveInFlight);
                if (measured)
                {
                    metrics.MarkStarted();
                    metrics.IncInFlight();
                }
                long intendedCopy = intended;
                _ = DispatchAsync(worker, kind, rowIndex, intendedCopy, measured, metrics, ct);
            }

            // Next arrival: interval ± 25% seeded jitter, never moving backwards.
            long step = intervalTicks + (long)((jitter.NextDouble() - 0.5) * 0.5 * intervalTicks);
            intended += Math.Max(intervalTicks / 2, step);
        }
    }

    private async Task DispatchAsync(
        WorkerState worker, Workload.OperationKind kind, long rowIndex,
        long intendedTs, bool measured, RunMetrics metrics, CancellationToken ct)
    {
        try
        {
            long dispatchTs = Stopwatch.GetTimestamp();
            if (measured)
                metrics.RecordScheduleDelay(Stopwatch.GetElapsedTime(intendedTs, dispatchTs).TotalMilliseconds);

            var (result, totalMs) = await _dispatcher
                .ExecuteAsync(kind, rowIndex, worker.Shard, ct).ConfigureAwait(false);

            if (measured)
                metrics.RecordResult(result, totalMs);
        }
        catch (Exception ex)
        {
            if (measured)
            {
                (Operations.OperationStatus status, string code) = Operations.ErrorClassifier.Classify(ex);
                metrics.RecordResult(
                    Operations.OperationResult.Failure(kind, status, code), 0);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _liveInFlight);
            if (measured)
                metrics.DecInFlight();
        }
    }

    private static async Task DelayUntilAsync(long targetTs, CancellationToken ct)
    {
        long freq = Stopwatch.Frequency;
        while (!ct.IsCancellationRequested)
        {
            long now = Stopwatch.GetTimestamp();
            if (now >= targetTs)
                return;
            double ms = (targetTs - now) * 1000.0 / freq;
            if (ms > 4)
                await Task.Delay((int)(ms - 2), ct).ConfigureAwait(false);
            else
                await Task.Yield();
        }
    }
}
