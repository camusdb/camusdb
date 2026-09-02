/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Cluster;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Reporting;
using CamusDB.Workload.Results;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Covers the judgements the report makes from a collected multi-node series. The verdicts are the
/// point: a comma-separated endpoint list in a connection string is a configuration, not a
/// measurement, and a run whose backlog was still climbing at the end did not measure a steady state.
/// </summary>
[TestFixture]
public sealed class BottleneckReportTests
{
    private static readonly DateTime Base = new(2026, 8, 28, 12, 0, 0, DateTimeKind.Utc);

    private static long At(int second) => new DateTimeOffset(Base.AddSeconds(second)).ToUnixTimeMilliseconds();

    private static RunSummary Summary()
    {
        RunMetrics m = new(writesPerTransaction: 1);
        m.MarkOffered();
        m.MarkStarted();
        m.RecordResult(OperationResult.ReadOk(), 1.0);
        return RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 60);
    }

    private static RunManifest Manifest(string endpoint) => new(
        ToolVersion: "1.0.0", GitCommit: "abc123", Endpoint: endpoint, Database: "wl", Protocol: "grpc",
        Mode: "closed", Seed: 1, Rows: 1000, PayloadBytes: 256, Tables: 1, WorkloadKind: "accounts",
        Workers: 64, Connections: 8, TargetOps: 0, ReadPercent: 60, WritePercent: 40, WritesPerTransaction: 1,
        Locking: "Optimistic", Isolation: "ReadCommitted", NoAutoPrepare: false, RequestTimeoutSeconds: null,
        ExpectFaults: false, SchemaFingerprint: "fp", StartedAtUtc: Base.ToString("O"), Runtime: "10.0",
        Os: "test", ProcessorCount: 8, ClientPackageVersion: "CamusDB.Client 0.10.0");

    private static MetricPoint Point(int second, string node, string metric, double value, string labels = "")
        => new(At(second), node, metric, labels, value);

    [Test]
    public void ReportsAPoolThatDidNotDistribute()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "camus_request_count_total", 0),
            Point(60, "camus1", "camus_request_count_total", 50_000),
            Point(0, "camus2", "camus_request_count_total", 0),
            Point(60, "camus2", "camus_request_count_total", 0),
            Point(0, "camus3", "camus_request_count_total", 0),
            Point(60, "camus3", "camus_request_count_total", 0),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096,http://camus2:5096,http://camus3:5096"), Summary(), scrape: null,
            series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("**FAIL**"));
        Assert.That(report, Does.Contain("not distributing"));
    }

    [Test]
    public void ReportsAPoolThatDidDistribute()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "camus_request_count_total", 0),
            Point(60, "camus1", "camus_request_count_total", 17_000),
            Point(0, "camus2", "camus_request_count_total", 0),
            Point(60, "camus2", "camus_request_count_total", 16_500),
            Point(0, "camus3", "camus_request_count_total", 0),
            Point(60, "camus3", "camus_request_count_total", 16_500),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096,http://camus2:5096,http://camus3:5096"), Summary(), scrape: null,
            series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("**Distributed**"));
        Assert.That(report, Does.Not.Contain("**FAIL**"));
    }

    [Test]
    public void SaysWhenASingleGatewayWasConfigured()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "camus_request_count_total", 0),
            Point(60, "camus1", "camus_request_count_total", 50_000),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("Single gateway as configured"));
    }

    [Test]
    public void SaysWhenNoNodeReportedAnyRequest()
    {
        // Diagnostics off, or the wrong nodes scraped. Either way the distribution is unverified, and
        // the report must say so rather than print a clean-looking 0/0 table.
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "workload_scrape_ok", 1),
            Point(60, "camus1", "workload_scrape_ok", 1),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("Cannot verify distribution"));
    }

    [Test]
    public void ExcludesWorkOutsideTheMeasuredWindow()
    {
        // Seeding put 40,000 requests on camus1 before the window opened. Only the 10,000 inside it
        // may be attributed to the run.
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "camus_request_count_total", 40_000),
            Point(30, "camus1", "camus_request_count_total", 45_000),
            Point(90, "camus1", "camus_request_count_total", 50_000),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null,
            series, MetricsWindow.Measured(Base.AddSeconds(30), 60));

        Assert.That(report, Does.Contain("5,000"));
        Assert.That(report, Does.Not.Contain("50,000"));
    }

    [Test]
    public void ShowsPerNodeBatchDensity()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "kahuna_kv_write_entries_total", 0),
            Point(60, "camus1", "kahuna_kv_write_entries_total", 6_000),
            Point(0, "camus1", "kahuna_kv_write_batches_total", 0),
            Point(60, "camus1", "kahuna_kv_write_batches_total", 100),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("Per-node storage and WAL"));
        Assert.That(report, Does.Contain("60.00"));
    }

    [Test]
    public void FlagsABacklogStillRisingAtTheEndOfTheWindow()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "kahuna_durable_tx_resident_prepared_intents", 5),
            Point(30, "camus1", "kahuna_durable_tx_resident_prepared_intents", 900),
            Point(60, "camus1", "kahuna_durable_tx_resident_prepared_intents", 1_800),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("Backlog and queue growth"));
        Assert.That(report, Does.Contain("**still rising**"));
    }

    [Test]
    public void NamesTheTwoPhaseFallbackAsTheExpectedClusterPath()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "kahuna_durable_tx_one_phase_commits_total", 0),
            Point(60, "camus1", "kahuna_durable_tx_one_phase_commits_total", 3),
            Point(0, "camus1", "kahuna_durable_tx_one_phase_fallbacks_total", 0),
            Point(60, "camus1", "kahuna_durable_tx_one_phase_fallbacks_total", 20_000),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("Commit path"));
        Assert.That(report, Does.Contain("expected safe fallback"));
    }

    [Test]
    public void StillBuildsWithoutAnySeries()
    {
        string report = BottleneckReport.Build(Manifest("http://camus1:5096"), Summary(), scrape: null);

        Assert.That(report, Does.Contain("# Bottleneck report"));
        Assert.That(report, Does.Contain("No per-node time series was collected"));
    }

    // ── Concentration: the four shares ────────────────────────────────────────

    private static MetricPoint Executor(int second, string node, string partitionId, string operationClass, double value)
        => new(At(second), node, "raft_executor_operations_total",
               $"operation_class={operationClass};partition_id={partitionId}", value);

    private static ClusterFacts Facts(params (int Partition, string? Leader, string? Table)[] partitions)
        => new(
            CapturedAtUtc: Base.ToString("O"),
            Nodes: [],
            Ranges: partitions
                .Where(p => p.Table is not null)
                .Select(p => (IReadOnlyDictionary<string, string>)new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["relation"] = p.Table!,
                    ["partition_id"] = p.Partition.ToString(),
                })
                .ToList(),
            Errors: [],
            DurabilityFingerprint: "fp")
        {
            Partitions = partitions
                .Select(p => new PartitionFacts(p.Partition, "Active", 1, 3, p.Leader, []))
                .ToList(),
        };

    /// <summary>
    /// The Phase 2 hypothesis in one assertion: three nodes can share the requests evenly while one
    /// partition takes every operation, because hash routing places a table's key space on a single
    /// partition. A gateway share alone would call this run distributed.
    /// </summary>
    [Test]
    public void ReportsASinglePartitionCarryingEverythingDespiteAnEvenGatewaySplit()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Point(0, "camus1", "camus_request_count_total", 0),
            Point(60, "camus1", "camus_request_count_total", 10_000),
            Point(0, "camus2", "camus_request_count_total", 0),
            Point(60, "camus2", "camus_request_count_total", 10_000),
            Point(0, "camus3", "camus_request_count_total", 0),
            Point(60, "camus3", "camus_request_count_total", 10_000),

            Executor(0, "camus1", "1", "Client", 0),
            Executor(60, "camus1", "1", "Client", 30_000),
            Executor(0, "camus1", "2", "Client", 0),
            Executor(60, "camus1", "2", "Client", 0),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096,http://camus2:5096,http://camus3:5096"), Summary(), scrape: null,
            series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("Concentration"));
        Assert.That(report, Does.Contain("**Single partition**"));
        Assert.That(report, Does.Contain("| Key/value partition | 100.0% | partition 1 |"));
    }

    /// <summary>
    /// Heartbeat and timer traffic accrues on every partition at a fixed rate whether or not it carries
    /// load, so counting it would flatten every share toward evenness and hide the hotspot.
    /// </summary>
    [Test]
    public void PartitionWorkExcludesControlAndMaintenanceTraffic()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Executor(0, "camus1", "1", "Client", 0),
            Executor(60, "camus1", "1", "Client", 1_000),
            Executor(0, "camus1", "2", "Control", 0),
            Executor(60, "camus1", "2", "Control", 8_000),
            Executor(0, "camus1", "2", "Maintenance", 0),
            Executor(60, "camus1", "2", "Maintenance", 500),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("| Key/value partition | 100.0% | partition 1 |"));
        Assert.That(report, Does.Contain("| 2 | 0 | 0 | 0 | 0.0% |"));
    }

    [Test]
    public void AttributesPartitionWorkToTheNodeThePlacementSaysLeadsIt()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Executor(0, "camus1", "1", "Client", 0),
            Executor(60, "camus1", "1", "Client", 7_500),
            Executor(0, "camus1", "2", "Client", 0),
            Executor(60, "camus1", "2", "Client", 2_500),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60),
            clientResources: null,
            facts: Facts((1, "camus2", "accounts_a"), (2, "camus3", "accounts_b")));

        Assert.That(report, Does.Contain("| Raft leader | 75.0% | camus2 |"));
        Assert.That(report, Does.Contain("| 1 | 7,500 | 0 | 7,500 | 75.0% | camus2 | accounts_a |"));
        Assert.That(report, Does.Contain("| 2 | 2,500 | 0 | 2,500 | 25.0% | camus3 | accounts_b |"));
    }

    /// <summary>
    /// A leader is read from the placement capture rather than inferred from replication volume, so an
    /// unattributed partition has to say so instead of guessing.
    /// </summary>
    [Test]
    public void SaysSoWhenNoPlacementWasCaptured()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            Executor(0, "camus1", "1", "Client", 0),
            Executor(60, "camus1", "1", "Client", 1_000),
        });

        string report = BottleneckReport.Build(
            Manifest("http://camus1:5096"), Summary(), scrape: null, series, MetricsWindow.Measured(Base, 60));

        Assert.That(report, Does.Contain("| Raft leader | n/a | n/a | n/a |"));
        Assert.That(report, Does.Contain("No `cluster-facts.json` was available"));
    }
}
