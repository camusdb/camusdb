/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Drives <see cref="EngineMetricsCollector"/> through a private meter named <c>"Kommander"</c>, which
/// is indistinguishable to a <see cref="MeterListener"/> from the real embedded one.
///
/// <para>Every instrument here carries a per-test unique name. The collector observes a process-wide
/// meter name, so a real embedded engine running in a neighbouring test publishes into the same
/// listener; filtering by a unique instrument name is what keeps these assertions exact instead of
/// merely approximate.</para>
/// </summary>
[TestFixture]
internal sealed class TestEngineMetricsCollector
{
    private Meter meter = null!;
    private EngineMetricsCollector collector = null!;
    private string prefix = null!;

    [SetUp]
    public void SetUp()
    {
        prefix = "test." + Guid.NewGuid().ToString("n") + ".";
        meter = new Meter(EngineMetricsCollector.KommanderMeterName, "1.0");
        collector = new EngineMetricsCollector();
    }

    [TearDown]
    public void TearDown()
    {
        collector.Dispose();
        meter.Dispose();
    }

    private List<EngineMetricRow> Rows()
        => collector.Snapshot().Where(r => r.Metric.StartsWith(prefix, StringComparison.Ordinal)).ToList();

    private EngineMetricRow Single(string name)
    {
        List<EngineMetricRow> rows = Rows().Where(r => r.Metric == prefix + name).ToList();
        Assert.AreEqual(1, rows.Count, $"expected exactly one row for '{name}', got {rows.Count}");
        return rows[0];
    }

    [Test]
    public void Counter_AccumulatesTotal_AndLeavesDistributionColumnsNull()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "ops");

        counter.Add(3);
        counter.Add(4);

        EngineMetricRow row = Single("ops");

        Assert.AreEqual("kommander", row.Source);
        Assert.AreEqual(EngineMetricKind.Counter, row.Kind);
        Assert.AreEqual("", row.Tags);
        Assert.AreEqual(7, row.Count);
        Assert.AreEqual(7d, row.Total);

        // A counter has no distribution, so these must be SQL NULL rather than a misleading zero.
        Assert.IsNull(row.Min);
        Assert.IsNull(row.Max);
        Assert.IsNull(row.Last);
    }

    [Test]
    public void Histogram_ReportsCountSumMinMaxAndLast()
    {
        Histogram<double> histogram = meter.CreateHistogram<double>(prefix + "duration");

        histogram.Record(10);
        histogram.Record(2);
        histogram.Record(6);

        EngineMetricRow row = Single("duration");

        Assert.AreEqual(EngineMetricKind.Histogram, row.Kind);
        Assert.AreEqual(3, row.Count);
        Assert.AreEqual(18d, row.Total);
        Assert.AreEqual(2d, row.Min);
        Assert.AreEqual(10d, row.Max);
        Assert.AreEqual(6d, row.Last);
    }

    /// <summary>
    /// Kommander publishes <c>Histogram&lt;int&gt;</c> (raft.wal.batch_size) and Kahuna publishes
    /// <c>Histogram&lt;long&gt;</c> (kahuna.kv.write.batch_bytes). A collector that registered only the
    /// double callback would drop both silently, so each width is exercised explicitly.
    /// </summary>
    [Test]
    public void Histogram_ObservesEveryNumericWidth()
    {
        meter.CreateHistogram<int>(prefix + "ints").Record(5);
        meter.CreateHistogram<long>(prefix + "longs").Record(9);
        meter.CreateHistogram<float>(prefix + "floats").Record(1.5f);
        meter.CreateCounter<int>(prefix + "intcount").Add(2);

        Assert.AreEqual(5d, Single("ints").Max);
        Assert.AreEqual(9d, Single("longs").Max);
        Assert.AreEqual(1.5d, Single("floats").Max);
        Assert.AreEqual(2, Single("intcount").Count);
    }

    [Test]
    public void Tags_AreKeySorted_SoCallSiteOrderDoesNotSplitTheRow()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "tagged");

        counter.Add(1, new KeyValuePair<string, object?>("zone", "b"), new KeyValuePair<string, object?>("api", "read"));
        counter.Add(1, new KeyValuePair<string, object?>("api", "read"), new KeyValuePair<string, object?>("zone", "b"));

        EngineMetricRow row = Single("tagged");

        Assert.AreEqual("api=read,zone=b", row.Tags);
        Assert.AreEqual(2, row.Count);
    }

    [Test]
    public void Tags_DistinctSets_BecomeDistinctRows()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "split");

        counter.Add(1, new KeyValuePair<string, object?>("api", "read"));
        counter.Add(5, new KeyValuePair<string, object?>("api", "write"));

        List<EngineMetricRow> rows = Rows().Where(r => r.Metric == prefix + "split").OrderBy(r => r.Tags).ToList();

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual("api=read", rows[0].Tags);
        Assert.AreEqual(1, rows[0].Count);
        Assert.AreEqual("api=write", rows[1].Tags);
        Assert.AreEqual(5, rows[1].Count);
    }

    /// <summary>
    /// Numeric tag values format through the invariant culture. The rendering is part of the aggregate's
    /// dictionary key, so a locale that writes <c>1,5</c> instead of <c>1.5</c> would otherwise split one
    /// metric into different rows depending on where the server runs.
    /// </summary>
    [Test]
    public void Tags_NumericValues_FormatInvariantly()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "numeric");

        counter.Add(1, new KeyValuePair<string, object?>("partition_id", 7), new KeyValuePair<string, object?>("ratio", 1.5));

        Assert.AreEqual("partition_id=7,ratio=1.5", Single("numeric").Tags);
    }

    [Test]
    public void ObservableInstrument_IsSampledAtSnapshotTime()
    {
        int value = 41;
        meter.CreateObservableGauge(prefix + "queue", () => value);

        EngineMetricRow first = Single("queue");
        Assert.AreEqual(EngineMetricKind.Gauge, first.Kind);
        Assert.AreEqual(41d, first.Last);

        // A gauge holds no history: the second snapshot must show the new reading, not an aggregate
        // of both, and must not report a sum.
        value = 12;
        EngineMetricRow second = Single("queue");
        Assert.AreEqual(12d, second.Last);
        Assert.IsNull(second.Total);
        Assert.IsNull(second.Min);
    }

    [Test]
    public void Counters_AreMonotonicAcrossSnapshots()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "monotonic");

        counter.Add(2);
        long first = Single("monotonic").Count;

        counter.Add(3);
        long second = Single("monotonic").Count;

        Assert.AreEqual(2, first);
        Assert.AreEqual(5, second);
        Assert.GreaterOrEqual(second, first);
    }

    [Test]
    public void UnrelatedMeter_IsNotObserved()
    {
        using Meter other = new("SomeOtherLibrary", "1.0");
        other.CreateCounter<long>(prefix + "foreign").Add(1);

        Assert.IsEmpty(Rows().Where(r => r.Metric == prefix + "foreign"));
    }

    [Test]
    public void Dispose_StopsObserving()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "stopped");

        counter.Add(1);
        Assert.AreEqual(1, Single("stopped").Count);

        collector.Dispose();
        counter.Add(100);

        // Snapshot still answers after disposal (it must never throw at shutdown), but the reading is
        // frozen at the moment the listener stopped.
        EngineMetricRow row = collector.Snapshot().Single(r => r.Metric == prefix + "stopped");
        Assert.AreEqual(1, row.Count);
    }

    /// <summary>
    /// Guards the property the hot path depends on: the canonical tag string is built once per tag-set,
    /// not once per measurement. Kommander records into these instruments from the Raft executor, so a
    /// per-measurement string would be a steady allocation on a latency-sensitive path. String tag
    /// values are used deliberately — a numeric tag is boxed by the metrics API at the call site, which
    /// would be measured here without belonging to the collector.
    /// </summary>
    [Test]
    public void RepeatMeasurements_DoNotAllocateAfterFirstSight()
    {
        Counter<long> counter = meter.CreateCounter<long>(prefix + "alloc");
        KeyValuePair<string, object?> zone = new("zone", "eu-west");
        KeyValuePair<string, object?> api = new("api", "read");

        for (int i = 0; i < 2_000; i++)
            counter.Add(1, zone, api);

        long before = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < 20_000; i++)
            counter.Add(1, zone, api);

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // Building the canonical string every time would cost tens of bytes per measurement; a few
        // hundred bytes total leaves room for JIT noise while still failing that regression loudly.
        Assert.Less(allocated, 4_000, $"20,000 repeat measurements allocated {allocated} bytes");
        Assert.AreEqual(22_000, Single("alloc").Count);
    }
}
