/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Scheduling;
using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Timing-sensitive scheduler behavior, driven by a fake executor so no server is needed. Marked
/// non-parallelizable because it leans on real wall-clock windows.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class SchedulerTests
{
    /// <summary>Mirrors the real dispatcher's contract: it never throws — cancellation returns a classified failure.</summary>
    private sealed class FakeExecutor : IOperationExecutor
    {
        public int DelayMs;
        private long _calls;
        public long Calls => Interlocked.Read(ref _calls);

        public async Task<(OperationResult Result, double TotalMs)> ExecuteAsync(
            OperationKind kind, long rowIndex, WorkerShard shard, CancellationToken ct)
        {
            Interlocked.Increment(ref _calls);
            try
            {
                if (DelayMs > 0)
                    await Task.Delay(DelayMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return (OperationResult.Failure(kind, OperationStatus.Transient, "CANCELED"), DelayMs);
            }

            OperationResult ok = kind == OperationKind.Read
                ? OperationResult.ReadOk()
                : new OperationResult(OperationKind.Write, OperationStatus.Ok, null, 0.1, 0.2, 0.3, 1.0);
            return (ok, DelayMs);
        }
    }

    [Test]
    public async Task OpenLoopReportsDropsInsteadOfUnboundedQueueUnderOverload()
    {
        var exec = new FakeExecutor { DelayMs = 50 };
        var scheduler = new OpenLoopScheduler(exec, writesPerTransaction: 1, seed: 7);
        WorkerState[] workers = WorkerState.Build(seed: 7, workers: 4, readPercent: 60, rows: 10_000);

        // Offer 1000 ops/s but cap in-flight at 8 with 50ms latency (~160 ops/s capacity) -> forced drops.
        var (metrics, intervals) = await scheduler.RunAsync(
            workers, targetOps: 1000, maxInFlight: 8,
            warmup: TimeSpan.Zero, measure: TimeSpan.FromSeconds(1.5), drain: TimeSpan.FromSeconds(1),
            CancellationToken.None);

        Assert.That(metrics.ScheduleDrops, Is.GreaterThan(0), "overload must surface as schedule drops");
        Assert.That(metrics.Offered, Is.EqualTo(metrics.Started + metrics.ScheduleDrops),
            "every offered op is either started or dropped — no lost/hidden operations");
        Assert.That(intervals.Count, Is.GreaterThan(0));
    }

    [Test]
    public async Task ClosedLoopAccountsForEveryOperationOnCancellation()
    {
        var exec = new FakeExecutor { DelayMs = 20 };
        var scheduler = new ClosedLoopScheduler(exec, writesPerTransaction: 1);
        WorkerState[] workers = WorkerState.Build(seed: 3, workers: 8, readPercent: 50, rows: 10_000);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(600));
        var (metrics, _) = await scheduler.RunAsync(
            workers, warmup: TimeSpan.Zero, measure: TimeSpan.FromSeconds(30), cts.Token);

        Assert.That(metrics.InFlight, Is.EqualTo(0), "no operation left in flight after drain");
        Assert.That(metrics.Started, Is.EqualTo(metrics.Completed + metrics.Failed),
            "every started operation is accounted for (completed or failed)");
    }

    [Test]
    public async Task IntervalTotalsReconcileWithFinalSummary()
    {
        var exec = new FakeExecutor { DelayMs = 2 };
        var scheduler = new ClosedLoopScheduler(exec, writesPerTransaction: 1);
        WorkerState[] workers = WorkerState.Build(seed: 11, workers: 8, readPercent: 60, rows: 10_000);

        var (metrics, intervals) = await scheduler.RunAsync(
            workers, warmup: TimeSpan.Zero, measure: TimeSpan.FromSeconds(3), CancellationToken.None);

        long summedCompleted = intervals.Sum(r => r.Completed);
        long maxSecond = intervals.Count == 0 ? 0 : intervals.Max(r => r.Completed);

        // Per-second deltas should sum to the total minus at most the final partial second.
        Assert.That(summedCompleted, Is.LessThanOrEqualTo(metrics.Completed));
        Assert.That(metrics.Completed - summedCompleted, Is.LessThanOrEqualTo(maxSecond + 1),
            "interval completed totals must reconcile with the final summary");
    }
}
