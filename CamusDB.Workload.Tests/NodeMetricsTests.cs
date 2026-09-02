/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Reporting;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Covers the per-node metric collection the cluster harness depends on: target parsing, the CSV
/// round trip, and the windowed queries the report is built from. The queries carry the load-bearing
/// behaviour — a counter delta that survives a node restart, and a per-node split that a single
/// end-of-run scrape cannot produce.
/// </summary>
[TestFixture]
public sealed class NodeMetricsTests
{
    private static readonly DateTime Base = new(2026, 8, 28, 12, 0, 0, DateTimeKind.Utc);

    private static long At(int second) => new DateTimeOffset(Base.AddSeconds(second)).ToUnixTimeMilliseconds();

    // ── Target parsing ────────────────────────────────────────────────────────

    [Test]
    public void TryParse_AcceptsNameAndUrl()
    {
        Assert.That(NodeTarget.TryParse("camus1=http://camus1:5095/metrics", out NodeTarget? t, out string? err), Is.True, err);
        Assert.That(t!.Name, Is.EqualTo("camus1"));
        Assert.That(t.MetricsUrl.ToString(), Is.EqualTo("http://camus1:5095/metrics"));
    }

    [Test]
    public void TryParse_DefaultsTheMetricsPath()
    {
        Assert.That(NodeTarget.TryParse("camus2=http://camus2:5095", out NodeTarget? t, out _), Is.True);
        Assert.That(t!.MetricsUrl.AbsolutePath, Is.EqualTo("/metrics"));
    }

    [Test]
    public void TryParse_RejectsAMissingName()
    {
        Assert.That(NodeTarget.TryParse("http://camus1:5095/metrics", out _, out string? err), Is.False);
        Assert.That(err, Does.Contain("name=url"));
    }

    [Test]
    public void TryParse_RejectsANonHttpUrl()
    {
        Assert.That(NodeTarget.TryParse("camus1=ftp://camus1/metrics", out _, out string? err), Is.False);
        Assert.That(err, Does.Contain("http"));
    }

    [Test]
    public void TryParseAll_SplitsACommaSeparatedList()
    {
        Assert.That(
            NodeTarget.TryParseAll(new[] { "a=http://a:1/metrics,b=http://b:1/metrics" }, out List<NodeTarget> targets, out string? err),
            Is.True, err);
        Assert.That(targets.Select(t => t.Name), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void TryParseAll_RejectsADuplicateNodeName()
    {
        Assert.That(
            NodeTarget.TryParseAll(new[] { "a=http://a:1/metrics", "a=http://b:1/metrics" }, out _, out string? err),
            Is.False);
        Assert.That(err, Does.Contain("duplicate"));
    }

    // ── CSV round trip ────────────────────────────────────────────────────────

    [Test]
    public void Csv_RoundTripsALabelValueContainingACommaAndAQuote()
    {
        MetricPoint point = new(At(0), "camus1", "camus_request_count", "operation=query;note=a,b\"c", 42);
        string row = MetricsCsv.RenderRow(point);

        IReadOnlyList<MetricPoint> parsed = MetricsCsv.Parse(MetricsCsv.Header + "\n" + row);

        Assert.That(parsed, Has.Count.EqualTo(1));
        Assert.That(parsed[0], Is.EqualTo(point));
    }

    [Test]
    public void Csv_CanonicalLabelsAreOrderedByKey()
    {
        Dictionary<string, string> labels = new() { ["z"] = "1", ["a"] = "2" };
        Assert.That(MetricsCsv.CanonicalLabels(labels), Is.EqualTo("a=2;z=1"));
    }

    [Test]
    public void Csv_RoundTripsAValueExactly()
    {
        MetricPoint point = new(At(3), "n", "m", "", 0.1 + 0.2);
        IReadOnlyList<MetricPoint> parsed = MetricsCsv.Parse(MetricsCsv.RenderRow(point));
        Assert.That(parsed[0].Value, Is.EqualTo(point.Value));
    }

    // ── Windowed queries ──────────────────────────────────────────────────────

    private static NodeMetricsSeries Series(params MetricPoint[] points) => NodeMetricsSeries.From(points);

    [Test]
    public void Delta_MeasuresTheIncreaseInsideTheWindowOnly()
    {
        // The counter was already at 1000 when the measured window opened: a report that read the
        // cumulative value would attribute seeding and warm-up to the run.
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "camus_request_count_total", "", 1000),
            new MetricPoint(At(10), "camus1", "camus_request_count_total", "", 1500),
            new MetricPoint(At(20), "camus1", "camus_request_count_total", "", 2100),
            new MetricPoint(At(30), "camus1", "camus_request_count_total", "", 2600));

        MetricsWindow window = MetricsWindow.Measured(Base.AddSeconds(10), 10);

        Assert.That(series.Delta("camus_request_count", window, "camus1"), Is.EqualTo(600));
    }

    [Test]
    public void Delta_SurvivesACounterResetFromANodeRestart()
    {
        // A killed and restarted node resets to zero. Last minus first would report -400 for the
        // busiest node in a fault run; the positive steps are 300 before the restart and 700 after.
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "camus_request_count_total", "", 200),
            new MetricPoint(At(5), "camus1", "camus_request_count_total", "", 500),
            new MetricPoint(At(10), "camus1", "camus_request_count_total", "", 0),
            new MetricPoint(At(15), "camus1", "camus_request_count_total", "", 700));

        Assert.That(series.Delta("camus_request_count", MetricsWindow.All, "camus1"), Is.EqualTo(1000));
    }

    [Test]
    public void Delta_SumsLabelSetsAtEachInstant()
    {
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "camus_request_count_total", "outcome=ok", 10),
            new MetricPoint(At(0), "camus1", "camus_request_count_total", "outcome=domain_error", 1),
            new MetricPoint(At(10), "camus1", "camus_request_count_total", "outcome=ok", 40),
            new MetricPoint(At(10), "camus1", "camus_request_count_total", "outcome=domain_error", 3));

        Assert.That(series.Delta("camus_request_count", MetricsWindow.All, "camus1"), Is.EqualTo(32));
    }

    [Test]
    public void Delta_FiltersOnALabel()
    {
        // Commits are counted by the request counter's operation tag, which is how the report reads
        // one node's committed transactions out of its total request volume.
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "camus_request_count_total", "operation=commit;outcome=ok", 5),
            new MetricPoint(At(0), "camus1", "camus_request_count_total", "operation=begin;outcome=ok", 5),
            new MetricPoint(At(10), "camus1", "camus_request_count_total", "operation=commit;outcome=ok", 25),
            new MetricPoint(At(10), "camus1", "camus_request_count_total", "operation=begin;outcome=ok", 45));

        Assert.That(series.Delta("camus_request_count", MetricsWindow.All, "camus1", "operation=commit"), Is.EqualTo(20));
    }

    [Test]
    public void PerNodeDelta_ShowsOneLeaderCarryingTheWrites()
    {
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "kahuna_kv_write_entries_total", "", 0),
            new MetricPoint(At(0), "camus2", "kahuna_kv_write_entries_total", "", 0),
            new MetricPoint(At(10), "camus1", "kahuna_kv_write_entries_total", "", 9000),
            new MetricPoint(At(10), "camus2", "kahuna_kv_write_entries_total", "", 100));

        IReadOnlyList<(string Node, double Delta)> perNode = series.PerNodeDelta("kahuna_kv_write_entries", MetricsWindow.All);

        Assert.That(perNode, Has.Count.EqualTo(2));
        Assert.That(perNode.Single(p => p.Node == "camus1").Delta, Is.EqualTo(9000));
        Assert.That(perNode.Single(p => p.Node == "camus2").Delta, Is.EqualTo(100));
    }

    [Test]
    public void PerNodeDelta_OmitsANodeThatNeverReportedTheMetric()
    {
        // "Did not report" and "reported zero" are different findings, so the absent node is absent.
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "kahuna_kv_write_entries_total", "", 0),
            new MetricPoint(At(10), "camus1", "kahuna_kv_write_entries_total", "", 10),
            new MetricPoint(At(0), "camus2", "workload_scrape_ok", "", 0));

        Assert.That(series.PerNodeDelta("kahuna_kv_write_entries", MetricsWindow.All).Select(p => p.Node),
            Is.EqualTo(new[] { "camus1" }));
    }

    [Test]
    public void Gauge_ReturnsFirstLastAndMaxInsideTheWindow()
    {
        NodeMetricsSeries series = Series(
            new MetricPoint(At(0), "camus1", "kahuna_durable_tx_outstanding", "", 4),
            new MetricPoint(At(10), "camus1", "kahuna_durable_tx_outstanding", "", 90),
            new MetricPoint(At(20), "camus1", "kahuna_durable_tx_outstanding", "", 70));

        Assert.That(series.Gauge("kahuna_durable_tx_outstanding", MetricsWindow.All, GaugeAggregate.First, "camus1"), Is.EqualTo(4));
        Assert.That(series.Gauge("kahuna_durable_tx_outstanding", MetricsWindow.All, GaugeAggregate.Last, "camus1"), Is.EqualTo(70));
        Assert.That(series.Gauge("kahuna_durable_tx_outstanding", MetricsWindow.All, GaugeAggregate.Max, "camus1"), Is.EqualTo(90));
    }

    [Test]
    public void Resolve_FindsTheExporterSuffixedName()
    {
        NodeMetricsSeries series = Series(new MetricPoint(At(0), "n", "raft_wal_operations_total", "", 1));

        Assert.That(series.Resolve("raft_wal_operations"), Is.EqualTo("raft_wal_operations_total"));
        Assert.That(series.Resolve("raft_wal_batches"), Is.Null);
    }

    [Test]
    public void Delta_ReturnsNullWhenTheMetricWasNeverCollected()
    {
        NodeMetricsSeries series = Series(new MetricPoint(At(0), "n", "raft_wal_operations_total", "", 1));

        Assert.That(series.Delta("kahuna_durable_tx_outstanding", MetricsWindow.All, "n"), Is.Null);
    }

    // ── Growth classification ─────────────────────────────────────────────────

    [Test]
    public void GrowthVerdict_FlagsASeriesStillRisingAtTheEnd()
        => Assert.That(BottleneckReport.GrowthVerdict(first: 10, last: 900, max: 950), Does.Contain("still rising"));

    [Test]
    public void GrowthVerdict_AcceptsAQueueThatSpikedAndDrained()
        => Assert.That(BottleneckReport.GrowthVerdict(first: 10, last: 12, max: 900), Is.EqualTo("bounded").Or.EqualTo("grew slightly"));

    [Test]
    public void GrowthVerdict_CallsAnIdleSeriesIdle()
        => Assert.That(BottleneckReport.GrowthVerdict(first: 0, last: 0, max: 0), Is.EqualTo("idle"));

    // ── Label matching and per-label queries ──────────────────────────────────

    [Test]
    public void LabelValue_ReadsOneLabelExactly()
    {
        const string labels = "operation_class=Client;otel_scope_name=Kommander;partition_id=10";

        Assert.That(MetricsCsv.LabelValue(labels, "partition_id"), Is.EqualTo("10"));
        Assert.That(MetricsCsv.LabelValue(labels, "operation_class"), Is.EqualTo("Client"));
        Assert.That(MetricsCsv.LabelValue(labels, "absent"), Is.Null);
    }

    /// <summary>
    /// The reason exact matching exists at all: a substring search for <c>partition_id=1</c> hits
    /// <c>partition_id=10</c>, which would fold a busy partition's samples into a quiet one's and
    /// report a hotspot that is an artifact of string matching.
    /// </summary>
    [Test]
    public void HasLabel_DoesNotMatchALongerValueByPrefix()
    {
        Assert.That(MetricsCsv.HasLabel("partition_id=10", "partition_id", "1"), Is.False);
        Assert.That(MetricsCsv.HasLabel("partition_id=10", "partition_id", "10"), Is.True);
    }

    [Test]
    public void HasLabel_DoesNotMatchALongerKeyByPrefix()
    {
        Assert.That(MetricsCsv.HasLabel("sub_partition_id=1", "partition_id", "1"), Is.False);
    }

    [Test]
    public void LabelValues_DiscoversThePartitionsSeenAndOrdersThemNumerically()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            new MetricPoint(At(0), "camus1", "raft_executor_operations_total", "partition_id=10", 1),
            new MetricPoint(At(0), "camus1", "raft_executor_operations_total", "partition_id=2", 1),
            new MetricPoint(At(0), "camus1", "raft_executor_operations_total", "partition_id=1", 1),
        });

        Assert.That(series.LabelValues("raft_executor_operations", "partition_id", MetricsWindow.All),
                    Is.EqualTo(new[] { "1", "2", "10" }));
    }

    [Test]
    public void DeltaWhere_CountsOnlyTheMatchingLabelSet()
    {
        NodeMetricsSeries series = NodeMetricsSeries.From(new[]
        {
            new MetricPoint(At(0), "camus1", "raft_executor_operations_total", "operation_class=Client;partition_id=1", 100),
            new MetricPoint(At(60), "camus1", "raft_executor_operations_total", "operation_class=Client;partition_id=1", 400),
            new MetricPoint(At(0), "camus1", "raft_executor_operations_total", "operation_class=Control;partition_id=1", 0),
            new MetricPoint(At(60), "camus1", "raft_executor_operations_total", "operation_class=Control;partition_id=1", 9_000),
            new MetricPoint(At(0), "camus1", "raft_executor_operations_total", "operation_class=Client;partition_id=10", 0),
            new MetricPoint(At(60), "camus1", "raft_executor_operations_total", "operation_class=Client;partition_id=10", 5_000),
        });

        double? delta = series.DeltaWhere("raft_executor_operations", MetricsWindow.All, "camus1",
            ("partition_id", "1"), ("operation_class", "Client"));

        Assert.That(delta, Is.EqualTo(300));
    }
}
