/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Reporting;

/// <summary>
/// One collected sample: the value of one labelled series on one node at one instant.
///
/// <para>The timestamp is absolute (Unix milliseconds), not an offset from the collector's start.
/// A run's other artifacts are anchored on <c>run-meta.json</c>'s <c>measureStartUtc</c>, and the
/// collector starts before the measured window opens; an absolute stamp lets any consumer cut the
/// window it wants without knowing when the collector happened to start.</para>
/// </summary>
public readonly record struct MetricPoint(long UnixMs, string Node, string Metric, string Labels, double Value);

/// <summary>
/// Reads and writes <c>node-metrics.csv</c>, the per-node metric time series a cluster run collects.
///
/// <para>The file is plain CSV (<c>unix_ms,node,metric,labels,value</c>) so it loads in any tool.
/// A label set is flattened to a canonical <c>k=v;k=v</c> string sorted by key, which makes two
/// samples of the same series compare equal regardless of the order the exporter emitted the labels
/// in. Fields are quoted only when they must be, because a Prometheus label value may legally
/// contain a comma or a quote.</para>
/// </summary>
public static class MetricsCsv
{
    public const string Header = "unix_ms,node,metric,labels,value";

    /// <summary>Canonical flattened form of a label set: <c>k=v;k=v</c>, ordered by key.</summary>
    public static string CanonicalLabels(IReadOnlyDictionary<string, string> labels)
    {
        if (labels.Count == 0)
            return "";

        return string.Join(';', labels.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                                      .Select(kv => $"{kv.Key}={kv.Value}"));
    }

    /// <summary>
    /// The value of one label in a canonical label string, or null when the label is absent.
    ///
    /// <para>Exact on the key, which a substring search is not: <c>partition_id=1</c> occurs inside
    /// <c>partition_id=10</c>, so a filter written the obvious way would fold a busy partition's
    /// samples into a quiet one's and report a hotspot that is an artifact of string matching.</para>
    /// </summary>
    public static string? LabelValue(string labels, string key)
    {
        if (labels.Length == 0 || key.Length == 0)
            return null;

        int from = 0;
        while (from <= labels.Length)
        {
            int end = labels.IndexOf(';', from);
            if (end < 0)
                end = labels.Length;

            int equals = labels.IndexOf('=', from);
            if (equals > from && equals < end &&
                string.CompareOrdinal(labels, from, key, 0, key.Length) == 0 && equals - from == key.Length)
            {
                return labels[(equals + 1)..end];
            }

            from = end + 1;
        }

        return null;
    }

    /// <summary>Whether a canonical label string carries exactly <c>key=value</c>.</summary>
    public static bool HasLabel(string labels, string key, string value)
        => string.Equals(LabelValue(labels, key), value, StringComparison.Ordinal);

    public static string RenderRow(in MetricPoint p)
    {
        StringBuilder sb = new();
        sb.Append(p.UnixMs).Append(',');
        AppendField(sb, p.Node).Append(',');
        AppendField(sb, p.Metric).Append(',');
        AppendField(sb, p.Labels).Append(',');
        // "R" round-trips the double exactly, so a reloaded series is the series that was collected.
        sb.Append(p.Value.ToString("R", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    public static IReadOnlyList<MetricPoint> Parse(string text)
    {
        List<MetricPoint> points = new();
        bool first = true;

        foreach (string rawLine in text.Split('\n'))
        {
            string line = rawLine.TrimEnd('\r');
            if (line.Length == 0)
                continue;
            if (first)
            {
                first = false;
                if (line.StartsWith("unix_ms", StringComparison.Ordinal))
                    continue;
            }

            string[] fields = SplitRow(line);
            if (fields.Length != 5)
                continue;
            if (!long.TryParse(fields[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out long unixMs))
                continue;
            if (!double.TryParse(fields[4], NumberStyles.Float, CultureInfo.InvariantCulture, out double value))
                continue;

            points.Add(new MetricPoint(unixMs, fields[1], fields[2], fields[3], value));
        }

        return points;
    }

    private static StringBuilder AppendField(StringBuilder sb, string value)
    {
        if (value.IndexOfAny(NeedsQuote) < 0)
            return sb.Append(value);

        sb.Append('"');
        foreach (char c in value)
        {
            if (c == '"')
                sb.Append('"');
            sb.Append(c);
        }
        return sb.Append('"');
    }

    private static readonly char[] NeedsQuote = { ',', '"', '\n', '\r' };

    /// <summary>Splits one CSV row, honouring quoted fields and doubled quotes inside them.</summary>
    private static string[] SplitRow(string line)
    {
        List<string> fields = new();
        StringBuilder current = new();
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];
            if (inQuotes)
            {
                if (c != '"')
                {
                    current.Append(c);
                }
                else if (i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = false;
                }
            }
            else if (c == '"')
            {
                inQuotes = true;
            }
            else if (c == ',')
            {
                fields.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }

        fields.Add(current.ToString());
        return fields.ToArray();
    }
}
