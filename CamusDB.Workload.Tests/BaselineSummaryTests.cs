/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Cluster;
using CamusDB.Workload.Results;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Covers what a set of repeated runs establishes. The spread is the point: a single number is a
/// result, but only a number with a known spread can tell a real improvement from two runs of an
/// unchanged system.
/// </summary>
[TestFixture]
public sealed class BaselineSummaryTests
{
    private static LatencySummary Latency(double p99) => new(p99 / 4, p99 / 2, p99 / 1.5, p99, p99 * 2, p99 * 3);

    private static RunSummary Summary(double opsPerSec, bool valid = true, double p99 = 20)
        => new(
            MeasuredSeconds: 600, Mode: "closed", Offered: 0, Started: 0, Completed: 0, CompletedRead: 0,
            CompletedWrite: 0, Failed: 0, Conflicts: 0, Transient: 0, Indeterminate: 0, DomainErrors: 0,
            InternalErrors: 0, ScheduleDrops: 0, AchievedOpsPerSec: opsPerSec, ReadOpsPerSec: opsPerSec / 2,
            WriteTxnsPerSec: opsPerSec / 2, RowsPerSec: opsPerSec, ReadPercentActual: 50, WritePercentActual: 50,
            ReadLatency: Latency(p99), WriteLatency: Latency(p99), WriteBegin: Latency(1), WriteRead: Latency(1),
            WriteUpdate: Latency(1), WriteCommit: Latency(p99), ScheduleDelay: Latency(0),
            ExpectFaults: false, Valid: valid, ValidityWarnings: Array.Empty<string>(),
            RetryAttempts: 0, RetriedTxns: 0, MaxAttemptsUsed: 0, RetriesPerWriteTxn: 0);

    private static RunManifest Manifest(long rows = 2000) => new(
        ToolVersion: "1.0.0", GitCommit: "abc", Endpoint: "e", Database: "caraxes", Protocol: "grpc",
        Mode: "closed", Seed: 1847, Rows: rows, PayloadBytes: 256, Tables: 1, WorkloadKind: "bank",
        Workers: 32, Connections: 16, TargetOps: 0, ReadPercent: 50, WritePercent: 50,
        WritesPerTransaction: 1, Locking: "Optimistic", Isolation: "ReadCommitted", NoAutoPrepare: false,
        RequestTimeoutSeconds: null, ExpectFaults: true, SchemaFingerprint: "fp",
        StartedAtUtc: "2026-08-29T00:00:00Z", Runtime: "10.0", Os: "test", ProcessorCount: 6,
        ClientPackageVersion: "CamusDB.Client 0.10.0");

    private static RunBundle Bundle(double opsPerSec, bool valid = true, long rows = 2000, string dir = "run", double p99 = 20)
        => new(dir, Manifest(rows), Summary(opsPerSec, valid, p99), null);

    [Test]
    public void EstablishesABaselineFromThreeRepeatableRuns()
    {
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(200, dir: "r1"), Bundle(208, dir: "r2"), Bundle(205, dir: "r3"),
        });

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.UsableCount, Is.EqualTo(3));
        Assert.That(result.Median, Is.EqualTo(205));
        Assert.That(result.Spread, Is.LessThan(0.10));
        Assert.That(result.Established, Is.True);
    }

    [Test]
    public void RefusesToCallOneRunABaseline()
    {
        BaselineResult result = BaselineSummary.Build(new[] { Bundle(208) });

        Assert.That(result.Established, Is.False);
        Assert.That(string.Join(" ", result.Warnings), Does.Contain("fewer than three usable runs"));
    }

    [Test]
    public void FlagsVariationTooWideToAttributeAChangeTo()
    {
        // 150 to 260 around a median of 208: a later "20% improvement" would be indistinguishable
        // from running the unchanged system again.
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(150, dir: "r1"), Bundle(208, dir: "r2"), Bundle(260, dir: "r3"),
        });

        Assert.That(result.CoefficientOfVariation, Is.GreaterThan(0.10));
        Assert.That(result.Established, Is.False);
        Assert.That(string.Join(" ", result.Warnings), Does.Contain("above the 10% bar"));
    }

    [Test]
    public void JudgesRepeatabilityOnVariationRatherThanOnMaxMinusMin()
    {
        // Five evenly spaced runs: 12.5% spread but only 4.9% variation. Gating on spread would fail
        // this set, and would fail it harder the more runs were added — which is the opposite of what
        // repetitions are for. The median of five is the more trustworthy number, not the less.
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(300, dir: "r1"), Bundle(310, dir: "r2"), Bundle(320, dir: "r3"),
            Bundle(330, dir: "r4"), Bundle(340, dir: "r5"),
        });

        Assert.That(result.Spread, Is.GreaterThan(0.10));
        Assert.That(result.CoefficientOfVariation, Is.LessThan(0.10));
        Assert.That(result.Established, Is.True);
    }

    [Test]
    public void ExcludesAnInvalidRunFromTheStatistics()
    {
        // An invalid run is not a slow result; it is not a result. Including it would drag the median.
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(200, dir: "r1"), Bundle(205, dir: "r2"), Bundle(208, dir: "r3"), Bundle(20, valid: false, dir: "r4"),
        });

        Assert.That(result.UsableCount, Is.EqualTo(3));
        Assert.That(result.Median, Is.EqualTo(205));
        Assert.That(result.Runs.Single(r => r.Directory == "r4").Usable, Is.False);
        Assert.That(string.Join(" ", result.Warnings), Does.Contain("excluded as invalid"));
    }

    [Test]
    public void RefusesRunsThatAreNotOneExperiment()
    {
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(200, rows: 2000, dir: "r1"), Bundle(900, rows: 200, dir: "r2"), Bundle(205, rows: 2000, dir: "r3"),
        });

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Incomparable.Single(), Does.Contain("rows"));
    }

    [Test]
    public void RenderedRefusalReportsNoMedian()
    {
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(200, rows: 2000, dir: "r1"), Bundle(900, rows: 200, dir: "r2"),
        });

        string text = BaselineSummary.Render(result);

        Assert.That(text, Does.Contain("REFUSED"));
        Assert.That(text, Does.Not.Contain("## Median"));
    }

    [Test]
    public void NamesTheLatencyBudgetToFreezeWhenTheBaselineHolds()
    {
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(200, dir: "r1", p99: 44), Bundle(205, dir: "r2", p99: 46), Bundle(208, dir: "r3", p99: 45),
        });

        string text = BaselineSummary.Render(result);

        Assert.That(text, Does.Contain("Baseline established"));
        Assert.That(text, Does.Contain("--p99-budget-ms 45"));
    }

    [Test]
    public void TakesTheMedianNotTheMeanSoOneOutlierDoesNotMoveIt()
    {
        BaselineResult result = BaselineSummary.Build(new[]
        {
            Bundle(200, dir: "r1"), Bundle(205, dir: "r2"), Bundle(500, dir: "r3"),
        });

        Assert.That(result.Median, Is.EqualTo(205));
        Assert.That(result.Max, Is.EqualTo(500));
    }
}
