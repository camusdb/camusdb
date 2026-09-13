/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The in-window scan probe's verdict: a short count always fails, a failed read fails unless faults
/// are expected, a probe that never ran or died early has no verdict rather than a clean one, and the
/// verdict folds into reconciliation's pass/fail like any other invariant.
/// </summary>
[TestFixture]
public sealed class ScanProbeTests
{
    private static readonly DateTime WindowStart = new(2026, 9, 12, 12, 0, 0, DateTimeKind.Utc);
    private const double WindowSeconds = 600;

    private static ScanProbeSample Ok(int secondsIntoRun, string endpoint = "http://g1", long rows = 2000, long expected = 2000) =>
        new(WindowStart.AddSeconds(secondsIntoRun - 30), endpoint, "workload_accounts", expected, rows, 150, rows == expected ? "ok" : "short", "");

    private static ScanProbeSample Error(int secondsIntoRun, string code, string endpoint = "http://g1") =>
        new(WindowStart.AddSeconds(secondsIntoRun - 30), endpoint, "workload_accounts", 2000, null, 5700, "error", code);

    private static ScanProbeResult Summarize(IReadOnlyList<ScanProbeSample> samples, bool expectFaults = false, string? loopFailure = null) =>
        ScanProbe.Summarize(samples, WindowStart, WindowSeconds, gateways: 3, TimeSpan.FromSeconds(5), expectFaults, loopFailure);

    [Test]
    public void AllExact_Passes_AndCountsTheWindowSeparately()
    {
        // Two probes during warm-up (before the window), three inside it.
        ScanProbeResult r = Summarize([Ok(0), Ok(10), Ok(60), Ok(300), Ok(600)]);

        Assert.That(r.Passed, Is.True);
        Assert.That(r.Probes, Is.EqualTo(5));
        Assert.That(r.Exact, Is.EqualTo(5));
        Assert.That(r.InWindowProbes, Is.EqualTo(3));
        Assert.That(r.Short, Is.EqualTo(0));
        Assert.That(r.Errors, Is.EqualTo(0));
        Assert.That(r.MinRows, Is.EqualTo(2000));
        Assert.That(r.Failures, Is.Empty);
    }

    [Test]
    public void OneShortCount_FailsWithTheRowsSeenAndWhere()
    {
        ScanProbeResult r = Summarize([Ok(60), Ok(65, "http://g2", rows: 1_962), Ok(70), Ok(75, rows: 1_996)]);

        Assert.That(r.Passed, Is.False);
        Assert.That(r.Short, Is.EqualTo(2));
        Assert.That(r.InWindowShort, Is.EqualTo(2));
        Assert.That(r.MinRows, Is.EqualTo(1_962));
        Assert.That(r.Failures, Has.Count.EqualTo(1));
        Assert.That(r.Failures[0], Does.Contain("2 of 4"));
        Assert.That(r.Failures[0], Does.Contain("http://g2"));
        Assert.That(r.Failures[0], Does.Contain("1962 of 2000"));
        Assert.That(r.Failures[0], Does.Contain("skipped committed rows"));
    }

    [Test]
    public void ShortCount_IsNeverWaivedByExpectFaults()
    {
        ScanProbeResult r = Summarize([Ok(60), Ok(65, rows: 1_999)], expectFaults: true);

        Assert.That(r.Passed, Is.False, "a scan that misses a committed row is wrong under any fault");
        Assert.That(r.Failures[0], Does.Not.Contain("waived"));
    }

    [Test]
    public void FailedRead_FailsOnAFaultFreeRun_AndIsWaivedUnderExpectFaults()
    {
        ScanProbeSample[] samples = [Ok(60), Error(65, "CADB0504"), Error(70, "CADB0504", "http://g3"), Ok(75)];

        ScanProbeResult strict = Summarize(samples);
        Assert.That(strict.Passed, Is.False);
        Assert.That(strict.Errors, Is.EqualTo(2));
        Assert.That(strict.Failures[0], Does.Contain("CADB0504×2"));
        Assert.That(strict.Failures[0], Does.Contain("could not be answered"));

        ScanProbeResult waived = Summarize(samples, expectFaults: true);
        Assert.That(waived.Passed, Is.True, "a killed node is expected to refuse reads for a while");
        Assert.That(waived.ErrorsWaived, Is.True);
        Assert.That(waived.Failures[0], Does.Contain("waived by --expect-faults"));
    }

    [Test]
    public void NoSamples_OrADeadLoop_IsNotAPass()
    {
        ScanProbeResult empty = Summarize([]);
        Assert.That(empty.Passed, Is.False);
        Assert.That(empty.Failures[0], Does.Contain("no probe was taken"));

        ScanProbeResult died = Summarize([Ok(60), Ok(65)], loopFailure: "InvalidOperationException: boom");
        Assert.That(died.Passed, Is.False);
        Assert.That(died.ProbeFailure, Is.EqualTo("InvalidOperationException: boom"));
        Assert.That(died.Failures[0], Does.Contain("stopped early"));
    }

    [Test]
    public void ElapsedStatistics_ComeFromAnsweredProbesOnly()
    {
        ScanProbeSample slow = Ok(60) with { ElapsedMs = 3_000 };
        ScanProbeResult r = Summarize([slow, Ok(65), Error(70, "CANCELED")]);

        Assert.That(r.MaxElapsedMs, Is.EqualTo(3_000));
        Assert.That(r.MeanElapsedMs, Is.EqualTo((3_000 + 150) / 2.0));
    }

    [Test]
    public void ReconciliationResult_FailsWhenTheProbeFailed_EvenWithEveryAggregateRight()
    {
        ScanProbeResult failed = Summarize([Ok(60, rows: 1_980)]);
        ScanProbeResult passed = Summarize([Ok(60)]);

        ReconciliationResult clean = new(
            ExpectedMin: 10, ExpectedMax: 10, Observed: 10, IndeterminateTxns: 0,
            VersionsMatch: true, RowCount: 2000, RowCountMatches: true, AccountingBalances: true,
            NoConflicts: true, ConflictsWaived: false, Failures: []);

        Assert.That(clean.Passed, Is.True);
        Assert.That((clean with { ScanProbe = passed }).Passed, Is.True);
        Assert.That((clean with { ScanProbe = failed }).Passed, Is.False,
            "the post-run aggregates count rows on a quiet cluster; the in-window probe is the only witness for this class");
    }

    [Test]
    public void Inconclusive_KeepsTheProbeVerdict()
    {
        ScanProbeResult failed = Summarize([Ok(60, rows: 1_980)]);

        ReconciliationResult r = Reconciliation.Inconclusive("aggregate timed out", 0, 0, 0, scanProbe: failed);

        Assert.That(r.Passed, Is.False);
        Assert.That(r.ScanProbe, Is.SameAs(failed));
        Assert.That(r.Failures[0], Does.StartWith("reconciliation could not complete: aggregate timed out"));
        Assert.That(r.Failures[1], Does.Contain("scan probe"));
    }

    [Test]
    public void EndpointPool_SplitsIntoDistinctGateways()
    {
        Assert.That(ScanProbe.EndpointsOf("http://a:1, http://b:2,http://a:1,"), Is.EqualTo(new[] { "http://a:1", "http://b:2" }));
    }

    [Test]
    public void GiveUpMessage_NamesTheCause()
    {
        TimeSpan deadline = TimeSpan.FromSeconds(11);

        string timedOut = Reconciliation.DescribeGiveUp(
            "SELECT COUNT(*) FROM t", new CamusException("", "Status(StatusCode=\"DeadlineExceeded\", Detail=\"Deadline Exceeded\")"),
            attempts: 40, waited: TimeSpan.FromSeconds(600), lastAttempt: deadline, retryable: true, budgetSpent: true);
        Assert.That(timedOut, Does.Contain("retry budget spent"));
        Assert.That(timedOut, Does.Contain("exceeded the client's per-request deadline after 11.0s"));
        Assert.That(timedOut, Does.Not.Contain("unavailable"));

        string refused = Reconciliation.DescribeGiveUp(
            "SELECT COUNT(*) FROM t", new CamusException("", "Error connecting to subchannel: Connection refused"),
            attempts: 3, waited: TimeSpan.FromSeconds(2), lastAttempt: TimeSpan.FromMilliseconds(5), retryable: true, budgetSpent: true);
        Assert.That(refused, Does.Contain("could not be reached"));

        string verdict = Reconciliation.DescribeGiveUp(
            "SELECT COUNT(*) FROM t", new CamusException("CADB0300", "unknown table t"),
            attempts: 1, waited: TimeSpan.FromMilliseconds(20), lastAttempt: TimeSpan.FromMilliseconds(20), retryable: false, budgetSpent: false);
        Assert.That(verdict, Does.Contain("not retryable"));
        Assert.That(verdict, Does.Contain("the server answered CADB0300"));

        string canceled = ErrorClassifier.DescribeReadFailure(new TaskCanceledException("A task was canceled."), deadline);
        Assert.That(canceled, Does.Contain("per-request deadline"));
    }
}
