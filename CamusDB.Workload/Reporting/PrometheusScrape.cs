/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

namespace CamusDB.Workload.Reporting;

/// <summary>One parsed Prometheus sample: a metric name, its label set, and value.</summary>
public readonly record struct PromSample(string Name, IReadOnlyDictionary<string, string> Labels, double Value);

/// <summary>
/// A minimal Prometheus text-exposition parser — just enough to turn a scraped <c>/metrics</c> snapshot
/// into queryable samples for the bottleneck report. It is deliberately tolerant: unknown lines and
/// comments are skipped, so a scrape from any OpenTelemetry Prometheus exporter version still parses.
/// For histograms it exposes the derived <c>_sum</c>/<c>_count</c> (mean) and the cumulative
/// <c>_bucket</c> series (approximate quantiles), since the exporter emits no precomputed percentiles.
/// </summary>
public sealed class PrometheusScrape
{
    private readonly List<PromSample> _samples;

    private PrometheusScrape(List<PromSample> samples) => _samples = samples;

    /// <summary>Every sample in the scrape, in the order the exporter emitted them.</summary>
    public IReadOnlyList<PromSample> Samples => _samples;

    public static PrometheusScrape Parse(string text) => new(ParseSamples(text));

    /// <summary>
    /// Parses the exposition text into flat samples. Exposed separately from <see cref="Parse"/> so the
    /// multi-node collector can stream samples into its time series without building a query object it
    /// would immediately discard.
    /// </summary>
    public static List<PromSample> ParseSamples(string text)
    {
        List<PromSample> samples = new();
        foreach (string rawLine in text.Split('\n'))
        {
            string line = rawLine.Trim();
            if (line.Length == 0 || line[0] == '#')
                continue;

            // Format: name{label="v",...} value [timestamp]
            int braceOpen = line.IndexOf('{');
            string name;
            Dictionary<string, string> labels = new();
            string remainder;

            if (braceOpen >= 0)
            {
                name = line[..braceOpen];
                int braceClose = line.IndexOf('}', braceOpen);
                if (braceClose < 0)
                    continue;
                string labelBlock = line[(braceOpen + 1)..braceClose];
                ParseLabels(labelBlock, labels);
                remainder = line[(braceClose + 1)..].Trim();
            }
            else
            {
                int firstSpace = line.IndexOf(' ');
                if (firstSpace < 0)
                    continue;
                name = line[..firstSpace];
                remainder = line[(firstSpace + 1)..].Trim();
            }

            string[] parts = remainder.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0)
                continue;
            if (!double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out double value))
                continue;

            samples.Add(new PromSample(name, labels, value));
        }
        return samples;
    }

    private static void ParseLabels(string block, Dictionary<string, string> labels)
    {
        int i = 0;
        while (i < block.Length)
        {
            int eq = block.IndexOf('=', i);
            if (eq < 0)
                break;
            string key = block[i..eq].Trim();
            int quoteStart = block.IndexOf('"', eq);
            if (quoteStart < 0)
                break;
            int quoteEnd = block.IndexOf('"', quoteStart + 1);
            if (quoteEnd < 0)
                break;
            string val = block[(quoteStart + 1)..quoteEnd];
            labels[key] = val;
            i = quoteEnd + 1;
            while (i < block.Length && (block[i] == ',' || block[i] == ' '))
                i++;
        }
    }

    /// <summary>Sum of all samples for a metric name whose labels match every entry in <paramref name="match"/>.</summary>
    public double Sum(string name, params (string Key, string Value)[] match)
    {
        double total = 0;
        foreach (PromSample s in _samples)
        {
            if (s.Name != name)
                continue;
            if (Matches(s, match))
                total += s.Value;
        }
        return total;
    }

    /// <summary>Latest gauge value for a metric name (max across matching series), or null when absent.</summary>
    public double? Gauge(string name, params (string Key, string Value)[] match)
    {
        double? best = null;
        foreach (PromSample s in _samples)
        {
            if (s.Name != name || !Matches(s, match))
                continue;
            best = best is null ? s.Value : Math.Max(best.Value, s.Value);
        }
        return best;
    }

    /// <summary>Mean of a histogram = <c>_sum / _count</c> over matching series, or null when count is zero/absent.</summary>
    public double? HistogramMean(string baseName, params (string Key, string Value)[] match)
    {
        double sum = Sum(baseName + "_sum", match);
        double count = Sum(baseName + "_count", match);
        return count > 0 ? sum / count : null;
    }

    public double HistogramCount(string baseName, params (string Key, string Value)[] match)
        => Sum(baseName + "_count", match);

    /// <summary>
    /// Approximate quantile from a histogram's cumulative <c>_bucket</c> series (linear pick of the
    /// bucket whose cumulative count first reaches the target rank). Coarse — bounded by bucket width —
    /// but enough to rank stages. Null when the histogram is empty.
    /// </summary>
    public double? HistogramQuantile(string baseName, double quantile, params (string Key, string Value)[] match)
    {
        // Collect (le, cumulativeCount) summed across matching series.
        SortedDictionary<double, double> buckets = new();
        foreach (PromSample s in _samples)
        {
            if (s.Name != baseName + "_bucket" || !Matches(s, match))
                continue;
            if (!s.Labels.TryGetValue("le", out string? leStr))
                continue;
            double le = leStr == "+Inf" ? double.PositiveInfinity
                : double.Parse(leStr, CultureInfo.InvariantCulture);
            buckets[le] = buckets.TryGetValue(le, out double c) ? c + s.Value : s.Value;
        }
        if (buckets.Count == 0)
            return null;

        double total = buckets.Values.Max();
        if (total <= 0)
            return null;
        double target = quantile * total;
        foreach ((double le, double cumulative) in buckets)
        {
            if (cumulative >= target)
                return double.IsPositiveInfinity(le) ? buckets.Keys.Where(k => !double.IsPositiveInfinity(k)).DefaultIfEmpty(0).Max() : le;
        }
        return buckets.Keys.Max();
    }

    private static bool Matches(PromSample s, (string Key, string Value)[] match)
    {
        foreach ((string key, string value) in match)
        {
            if (!s.Labels.TryGetValue(key, out string? v) || v != value)
                return false;
        }
        return true;
    }
}
