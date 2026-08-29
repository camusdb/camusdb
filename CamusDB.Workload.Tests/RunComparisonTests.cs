/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Cluster;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Workload;
using CamusDB.Workload.Results;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Covers the A/B refusal. The refusal is the point of the tool: a ratio computed across two runs
/// that offered different work, or ran different code, measures the change in the experiment rather
/// than the change in the system — and that mistake is invisible in a directory of JSON files.
/// </summary>
[TestFixture]
public sealed class RunComparisonTests
{
    private static RunSummary SummaryWith(int reads, double readLatencyMs = 1.0, bool conflict = false)
    {
        RunMetrics m = new(writesPerTransaction: 1);
        for (int i = 0; i < reads; i++)
        {
            m.MarkOffered();
            m.MarkStarted();
            m.RecordResult(OperationResult.ReadOk(), readLatencyMs);
        }
        if (conflict)
        {
            m.MarkOffered();
            m.MarkStarted();
            m.RecordResult(OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, "CADB0502"), 3.0);
        }
        return RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 10);
    }

    private static RunManifest ManifestWith(
        long rows = 100_000, int workers = 64, string workload = "accounts", string mode = "closed",
        int targetOps = 0, string client = "CamusDB.Client 0.10.0")
        => new(
            ToolVersion: "1.0.0", GitCommit: "abc", Endpoint: "http://camus1:5096", Database: "wl", Protocol: "grpc",
            Mode: mode, Seed: 1847, Rows: rows, PayloadBytes: 256, Tables: 1, WorkloadKind: workload,
            Workers: workers, Connections: 8, TargetOps: targetOps, ReadPercent: 60, WritePercent: 40,
            WritesPerTransaction: 1, Locking: "Optimistic", Isolation: "ReadCommitted", NoAutoPrepare: false,
            RequestTimeoutSeconds: null, ExpectFaults: false, SchemaFingerprint: "fp-1",
            StartedAtUtc: "2026-08-28T12:00:00Z", Runtime: "10.0", Os: "test", ProcessorCount: 8,
            ClientPackageVersion: client);

    private static ClusterFacts FactsWith(
        string kahunaVersion = "1.4.14", string walSync = "True", string linger = "0", string walPath = "/data/wal")
    {
        NodeFacts node = new(
            Node: "camus1", BaseUrl: "http://camus1:5095", Server: "0.11.1", Runtime: "10.0.10",
            Components: new[] { new NodeComponent("Kahuna.Core", kahunaVersion) },
            Ready: true,
            Variables: new Dictionary<string, string> { ["kahuna.wal_sync_writes"] = walSync },
            EngineSettings: new Dictionary<string, string>
            {
                ["RaftWalGroupCommitLingerMs"] = linger,
                ["WalSyncWrites"] = walSync,
                ["WalPath"] = walPath,
            },
            Errors: Array.Empty<string>());

        return new ClusterFacts(
            "2026-08-28T12:00:00Z", new[] { node }, Array.Empty<IReadOnlyDictionary<string, string>>(),
            Array.Empty<string>(), ClusterProbe.Fingerprint(new[] { node }));
    }

    private static RunBundle Bundle(RunManifest m, RunSummary s, ClusterFacts? f = null, string dir = "run")
        => new(dir, m, s, f);

    [Test]
    public void RefusesRunsWithDifferentDataShapes()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(rows: 100_000), SummaryWith(100)),
            Bundle(ManifestWith(rows: 10_000), SummaryWith(1000)));

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Blockers.Select(b => b.Name), Does.Contain("rows"));
        Assert.That(result.Ratio, Is.Null, "an incomparable pair must not report a speedup");
    }

    [Test]
    public void RefusesRunsOnDifferentBuilds()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(kahunaVersion: "1.4.14")),
            Bundle(ManifestWith(), SummaryWith(1000), FactsWith(kahunaVersion: "1.5.0")));

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Blockers.Select(b => b.Name), Does.Contain("build"));
    }

    [Test]
    public void RefusesRunsWithDifferentDurabilitySettings()
    {
        // Same code, but one node was running with a different WAL setting. The plan's whole premise
        // is that a durability change must never be allowed to look like an optimization.
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(walSync: "True")),
            Bundle(ManifestWith(), SummaryWith(1000), FactsWith(walSync: "False")));

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Blockers.Select(b => b.Name), Does.Contain("engine.WalSyncWrites"));
    }

    [Test]
    public void RefusesRunsWhoseResolvedEngineSettingsDiffer()
    {
        // The setting nobody wrote in a config file is the one most likely to differ unnoticed, and a
        // group-commit linger change alters exactly what a throughput comparison is measuring.
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(linger: "0")),
            Bundle(ManifestWith(), SummaryWith(1000), FactsWith(linger: "2")));

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Blockers.Select(b => b.Name), Does.Contain("engine.RaftWalGroupCommitLingerMs"));
    }

    [Test]
    public void WaivesOneNamedEngineSettingWhileKeepingTheRestPinned()
    {
        // The A/B case: the experiment varies the linger on purpose, and must still refuse a run whose
        // fsync setting moved with it.
        ComparisonResult allowed = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(linger: "0")),
            Bundle(ManifestWith(), SummaryWith(200), FactsWith(linger: "2")),
            waive: new[] { "engine.RaftWalGroupCommitLingerMs" });

        Assert.That(allowed.Comparable, Is.True);
        Assert.That(allowed.Waived.Single(), Does.Contain("RaftWalGroupCommitLingerMs"));
        Assert.That(allowed.Ratio, Is.EqualTo(2.0).Within(0.001));

        ComparisonResult stillRefused = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(linger: "0", walSync: "True")),
            Bundle(ManifestWith(), SummaryWith(200), FactsWith(linger: "2", walSync: "False")),
            waive: new[] { "engine.RaftWalGroupCommitLingerMs" });

        Assert.That(stillRefused.Comparable, Is.False);
        Assert.That(stillRefused.Blockers.Select(b => b.Name), Does.Contain("engine.WalSyncWrites"));
    }

    [Test]
    public void WaivingOneSettingDoesNotWaiveTheBuild()
    {
        // The reason the fingerprint was split: waiving a knob must never excuse a dependency bump.
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(kahunaVersion: "1.4.14", linger: "0")),
            Bundle(ManifestWith(), SummaryWith(200), FactsWith(kahunaVersion: "1.5.0", linger: "2")),
            waive: new[] { "engine.RaftWalGroupCommitLingerMs" });

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Blockers.Select(b => b.Name), Does.Contain("build"));
    }

    [Test]
    public void SaysWhenOneSideCapturedNoEngineSettings()
    {
        // A run predating the capture must not read as "no durability differences found".
        NodeFacts bare = new(
            Node: "camus1", BaseUrl: "u", Server: "0.11.1", Runtime: "10.0",
            Components: new[] { new NodeComponent("Kahuna.Core", "1.4.14") }, Ready: true,
            Variables: new Dictionary<string, string>(),
            EngineSettings: new Dictionary<string, string>(),
            Errors: Array.Empty<string>());
        ClusterFacts oldFacts = new(
            "2026-08-29T00:00:00Z", new[] { bare }, Array.Empty<IReadOnlyDictionary<string, string>>(),
            Array.Empty<string>(), ClusterProbe.Fingerprint(new[] { bare }));

        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), oldFacts),
            Bundle(ManifestWith(), SummaryWith(200), FactsWith()));

        Assert.That(string.Join(" ", result.Warnings), Does.Contain("engine settings were NOT compared"));
    }

    [Test]
    public void IgnoresAPathThatDiffersBetweenRunsOfOneConfiguration()
    {
        // Each run gets its own data directory. Folding paths into the fingerprint would make every
        // run incomparable with every other, which is the same as having no check at all.
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith(walPath: "/run/a/wal")),
            Bundle(ManifestWith(), SummaryWith(110), FactsWith(walPath: "/run/b/wal")),
            requireRatio: null);

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.BuildVerified, Is.True);
    }

    [Test]
    public void RefusesRunsOnDifferentClientVersions()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(client: "CamusDB.Client 0.10.0"), SummaryWith(100)),
            Bundle(ManifestWith(client: "CamusDB.Client 0.11.0"), SummaryWith(1000)));

        Assert.That(result.Comparable, Is.False);
        Assert.That(result.Blockers.Select(b => b.Name), Does.Contain("client-package"));
    }

    [Test]
    public void TreatsWorkerCountAsNotableRatherThanBlocking()
    {
        // A concurrency sweep varies workers on purpose; refusing it would make the sweep unusable.
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(workers: 32), SummaryWith(100), FactsWith()),
            Bundle(ManifestWith(workers: 64), SummaryWith(200), FactsWith()));

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.Differences.Select(d => d.Name), Does.Contain("workers"));
        Assert.That(result.Ratio, Is.EqualTo(2.0).Within(0.001));
    }

    [Test]
    public void BlocksTargetOpsOnlyInOpenLoop()
    {
        ComparisonResult closed = RunComparison.Compare(
            Bundle(ManifestWith(mode: "closed", targetOps: 800), SummaryWith(100)),
            Bundle(ManifestWith(mode: "closed", targetOps: 1600), SummaryWith(100)));
        Assert.That(closed.Comparable, Is.True);

        ComparisonResult open = RunComparison.Compare(
            Bundle(ManifestWith(mode: "open", targetOps: 800), SummaryWith(100)),
            Bundle(ManifestWith(mode: "open", targetOps: 1600), SummaryWith(100)));
        Assert.That(open.Comparable, Is.False);
        Assert.That(open.Blockers.Select(b => b.Name), Does.Contain("target-ops"));
    }

    [Test]
    public void AllowsAnExplicitWaiver()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(rows: 100_000), SummaryWith(100)),
            Bundle(ManifestWith(rows: 200_000), SummaryWith(200)),
            waive: new[] { "rows" });

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.Waived.Single(), Does.Contain("rows"));
    }

    [Test]
    public void WarnsWhenTheBuildWasNeverVerified()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100)),
            Bundle(ManifestWith(), SummaryWith(1000)));

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.BuildVerified, Is.False);
        Assert.That(result.Warnings.Single(), Does.Contain("NOT verified"));
    }

    [Test]
    public void FailsTheRatioGateWhenTheSpeedupIsShort()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith()),
            Bundle(ManifestWith(), SummaryWith(500), FactsWith()),
            requireRatio: 10.0);

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.GatesPassed, Is.False);
        Assert.That(result.GateFailures.Single(), Does.Contain("below the required 10.00x"));
    }

    [Test]
    public void FailsTheAbsoluteFloorGate()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(10), FactsWith()),
            Bundle(ManifestWith(), SummaryWith(200), FactsWith()),
            requireOpsPerSec: 2100);

        Assert.That(result.GateFailures.Single(), Does.Contain("below the required floor"));
    }

    [Test]
    public void FailsTheFrozenLatencyBudget()
    {
        // The candidate is faster on average and worse at the tail. The plan refuses to call that a win.
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100, readLatencyMs: 5), FactsWith()),
            Bundle(ManifestWith(), SummaryWith(1000, readLatencyMs: 900), FactsWith()),
            p99BudgetMs: 100);

        Assert.That(result.Comparable, Is.True);
        Assert.That(result.GatesPassed, Is.False);
        Assert.That(result.GateFailures.Single(), Does.Contain("exceeds the frozen budget"));
    }

    [Test]
    public void FailsWhenEitherRunIsInvalid()
    {
        ComparisonResult result = RunComparison.Compare(
            Bundle(ManifestWith(), SummaryWith(100), FactsWith()),
            Bundle(ManifestWith(), SummaryWith(1000, conflict: true), FactsWith()));

        Assert.That(result.GateFailures.Single(), Does.Contain("candidate run is INVALID"));
    }

    [Test]
    public void RenderedRefusalNamesTheOffendingFieldAndReportsNoSpeedup()
    {
        RunBundle baseline = Bundle(ManifestWith(rows: 100_000), SummaryWith(100), dir: "runs/a");
        RunBundle candidate = Bundle(ManifestWith(rows: 10_000), SummaryWith(5000), dir: "runs/b");

        string text = RunComparison.Render(baseline, candidate, RunComparison.Compare(baseline, candidate));

        Assert.That(text, Does.Contain("REFUSED"));
        Assert.That(text, Does.Contain("rows"));
        Assert.That(text, Does.Not.Contain("Speedup"));
    }
}
