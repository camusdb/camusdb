/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Reporting;

/// <summary>The half-open instant range a query covers, in UTC.</summary>
public readonly record struct MetricsWindow(DateTime StartUtc, DateTime EndUtc)
{
    public static MetricsWindow All { get; } = new(DateTime.MinValue, DateTime.MaxValue);

    /// <summary>The measured window of a run: <c>measureStartUtc</c> plus the measured duration.</summary>
    public static MetricsWindow Measured(DateTime measureStartUtc, double measuredSeconds)
        => new(measureStartUtc, measureStartUtc.AddSeconds(measuredSeconds));

    public bool Contains(long unixMs)
    {
        DateTime at = DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime;
        return at >= StartUtc && at <= EndUtc;
    }
}

/// <summary>How a gauge's samples inside a window are reduced to one number.</summary>
public enum GaugeAggregate
{
    First,
    Last,
    Max,
}

/// <summary>
/// The queryable form of a collected multi-node time series.
///
/// <para>Two properties make it different from the single end-of-run scrape the report used before,
/// and both matter for a cluster measurement:</para>
///
/// <list type="number">
/// <item><description><b>It is per node.</b> One scrape from one gateway cannot show that a single
/// leader carried the write load, which is the exact hypothesis a hot-partition investigation has to
/// test.</description></item>
/// <item><description><b>It is windowed.</b> A cumulative counter read once at the end includes
/// seeding, warm-up, and reconciliation. A delta over the measured window is the only number
/// comparable between two runs.</description></item>
/// </list>
///
/// <para>A counter delta is the sum of the positive steps between consecutive samples, not
/// last minus first. A node that is killed and restarted mid-run — the normal case in a fault run —
/// resets its counters to zero, and last minus first would then report a negative or absurdly small
/// increase for the busiest node in the run.</para>
///
/// <para>Samples that share a node, a metric and an instant but differ in labels are summed. That is
/// correct for a counter partitioned by an outcome or a partition id, and for a queue-depth gauge
/// reported per partition. A query that needs one label set passes <c>labelContains</c>.</para>
/// </summary>
public sealed class NodeMetricsSeries
{
    private readonly IReadOnlyList<MetricPoint> _points;
    private readonly HashSet<string> _metrics;

    private NodeMetricsSeries(IReadOnlyList<MetricPoint> points)
    {
        _points = points;
        Nodes = points.Select(p => p.Node).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
        _metrics = new HashSet<string>(points.Select(p => p.Metric), StringComparer.Ordinal);
    }

    public static NodeMetricsSeries From(IEnumerable<MetricPoint> points) => new(points.ToList());

    public static NodeMetricsSeries Load(string csvPath) => new(MetricsCsv.Parse(File.ReadAllText(csvPath)));

    public IReadOnlyList<string> Nodes { get; }

    public int PointCount => _points.Count;

    public bool IsEmpty => _points.Count == 0;

    /// <summary>
    /// The name this series actually carries for a wanted metric, or null when it collected none.
    /// The Prometheus exporter appends <c>_total</c> to a counter and a unit suffix to a histogram, and
    /// which suffixes appear depends on the exporter version in the running image — so a report asks
    /// for the base name and lets the collected data decide.
    /// </summary>
    public string? Resolve(string metric)
    {
        if (_metrics.Contains(metric))
            return metric;

        foreach (string suffix in Suffixes)
        {
            if (_metrics.Contains(metric + suffix))
                return metric + suffix;
        }
        return null;
    }

    private static readonly string[] Suffixes = { "_total", "_milliseconds", "_bytes", "_seconds", "_count" };

    /// <summary>
    /// The increase of a counter over the window, summing positive steps so a node restart inside the
    /// window does not turn its contribution negative. Null when the series holds no sample for that
    /// node and metric inside the window.
    /// </summary>
    public double? Delta(string metric, MetricsWindow window, string? node = null, string? labelContains = null)
    {
        List<(long At, double Value)> series = Reduce(metric, window, node, labelContains);
        if (series.Count == 0)
            return null;
        if (series.Count == 1)
            return 0;

        double total = 0;
        for (int i = 1; i < series.Count; i++)
        {
            double step = series[i].Value - series[i - 1].Value;
            if (step > 0)
                total += step;
        }
        return total;
    }

    /// <summary>The sum of every node's counter increase over the window.</summary>
    public double? DeltaAcrossNodes(string metric, MetricsWindow window, string? labelContains = null)
    {
        double total = 0;
        bool any = false;
        foreach (string node in Nodes)
        {
            if (Delta(metric, window, node, labelContains) is double d)
            {
                total += d;
                any = true;
            }
        }
        return any ? total : null;
    }

    /// <summary>One reduction of a gauge's samples inside the window.</summary>
    public double? Gauge(string metric, MetricsWindow window, GaugeAggregate aggregate, string? node = null, string? labelContains = null)
    {
        List<(long At, double Value)> series = Reduce(metric, window, node, labelContains);
        if (series.Count == 0)
            return null;

        return aggregate switch
        {
            GaugeAggregate.First => series[0].Value,
            GaugeAggregate.Last => series[^1].Value,
            _ => series.Max(s => s.Value),
        };
    }

    /// <summary>
    /// Per-node counter increases over the window, ordered by node name. A node the series never saw
    /// is absent rather than zero — "did not report" and "reported nothing" are different findings.
    /// </summary>
    public IReadOnlyList<(string Node, double Delta)> PerNodeDelta(string metric, MetricsWindow window, string? labelContains = null)
    {
        List<(string, double)> result = new();
        foreach (string node in Nodes)
        {
            if (Delta(metric, window, node, labelContains) is double d)
                result.Add((node, d));
        }
        return result;
    }

    /// <summary>
    /// Collapses the matching samples to one value per instant, ordered by instant. Values that share
    /// an instant are summed across label sets.
    /// </summary>
    private List<(long At, double Value)> Reduce(string metric, MetricsWindow window, string? node, string? labelContains)
    {
        string? resolved = Resolve(metric);
        if (resolved is null)
            return new List<(long, double)>();

        SortedDictionary<long, double> byInstant = new();
        foreach (MetricPoint p in _points)
        {
            if (!string.Equals(p.Metric, resolved, StringComparison.Ordinal))
                continue;
            if (node is not null && !string.Equals(p.Node, node, StringComparison.Ordinal))
                continue;
            if (labelContains is not null && !p.Labels.Contains(labelContains, StringComparison.Ordinal))
                continue;
            if (!window.Contains(p.UnixMs))
                continue;

            byInstant[p.UnixMs] = byInstant.TryGetValue(p.UnixMs, out double existing) ? existing + p.Value : p.Value;
        }

        return byInstant.Select(kv => (kv.Key, kv.Value)).ToList();
    }
}
