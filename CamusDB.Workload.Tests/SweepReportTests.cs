/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Results;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Covers how a concurrency sweep is read. The knee matters more than the maximum: throughput that
/// rises while the tail rises far faster is a queue forming, and calling that point "capacity" is the
/// mistake the sweep exists to prevent.
/// </summary>
[TestFixture]
public sealed class SweepReportTests
{
    private static LatencySummary Latency(double p99) => new(P50: p99 / 4, P90: p99 / 2, P95: p99 / 1.5, P99: p99, P999: p99 * 2, Max: p99 * 3);

    private static SweepPoint Point(int workers, double opsPerSec, double p99Ms, bool valid = true, bool reconciled = true)
    {
        RunSummary summary = new(
            MeasuredSeconds: 60, Mode: "closed", Offered: 0, Started: 0, Completed: 0, CompletedRead: 0,
            CompletedWrite: 0, Failed: 0, Conflicts: 0, Transient: 0, Indeterminate: 0, DomainErrors: 0,
            InternalErrors: 0, ScheduleDrops: 0, AchievedOpsPerSec: opsPerSec, ReadOpsPerSec: opsPerSec * 0.6,
            WriteTxnsPerSec: opsPerSec * 0.4, RowsPerSec: opsPerSec, ReadPercentActual: 60, WritePercentActual: 40,
            ReadLatency: Latency(p99Ms), WriteLatency: Latency(p99Ms), WriteBegin: Latency(1), WriteRead: Latency(1),
            WriteUpdate: Latency(1), WriteCommit: Latency(p99Ms), ScheduleDelay: Latency(0),
            ExpectFaults: false, Valid: valid, ValidityWarnings: Array.Empty<string>(),
            RetryAttempts: 0, RetriedTxns: 0, MaxAttemptsUsed: 0, RetriesPerWriteTxn: 0);

        return new SweepPoint(workers, summary, reconciled, $"runs/workers-{workers}");
    }

    [Test]
    public void FindsTheKneeWhereLatencyOutgrowsThroughput()
    {
        // 64 -> 128 buys 2% more throughput for 4x the tail. That is the queue, not capacity.
        List<SweepPoint> points = new()
        {
            Point(16, 400, 20),
            Point(32, 780, 30),
            Point(64, 1000, 60),
            Point(128, 1020, 240),
        };

        Assert.That(SweepReport.FindKnee(points)!.Workers, Is.EqualTo(128));
    }

    [Test]
    public void FindsNoKneeWhileThroughputStillScales()
    {
        List<SweepPoint> points = new()
        {
            Point(16, 400, 20),
            Point(32, 800, 24),
            Point(64, 1600, 30),
        };

        Assert.That(SweepReport.FindKnee(points), Is.Null);
    }

    [Test]
    public void DoesNotCallAPureLatencyRiseAKnee()
    {
        // Latency tripled but throughput also doubled: the system is using the concurrency.
        List<SweepPoint> points = new() { Point(16, 400, 20), Point(32, 800, 60) };

        Assert.That(SweepReport.FindKnee(points), Is.Null);
    }

    [Test]
    public void NamesTheHighestValidPoint()
    {
        string markdown = SweepReport.RenderMarkdown(new List<SweepPoint>
        {
            Point(16, 400, 20),
            Point(32, 900, 30),
        });

        Assert.That(markdown, Does.Contain("900.00 ops/s at 32 worker(s)"));
    }

    [Test]
    public void RefusesToRankAnUnreconciledPoint()
    {
        // The fastest row failed reconciliation, so it is not a capacity result at all.
        string markdown = SweepReport.RenderMarkdown(new List<SweepPoint>
        {
            Point(16, 400, 20),
            Point(32, 5000, 30, reconciled: false),
        });

        Assert.That(markdown, Does.Contain("400.00 ops/s at 16 worker(s)"));
        Assert.That(markdown, Does.Not.Contain("5000.00 ops/s at"));
    }

    [Test]
    public void SaysWhenNoPointEstablishesCapacity()
    {
        string markdown = SweepReport.RenderMarkdown(new List<SweepPoint> { Point(16, 400, 20, valid: false) });

        Assert.That(markdown, Does.Contain("establishes no capacity"));
    }

    [Test]
    public void CsvHasOneRowPerPointOrderedByWorkers()
    {
        string csv = SweepReport.RenderCsv(new List<SweepPoint> { Point(64, 1000, 60), Point(16, 400, 20) });
        string[] lines = csv.Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.That(lines[0], Does.StartWith("workers,"));
        Assert.That(lines[1], Does.StartWith("16,"));
        Assert.That(lines[2], Does.StartWith("64,"));
    }
}
