/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Reporting;

/// <summary>
/// One node the run scrapes metrics from: a short stable name plus its Prometheus URL.
///
/// <para>The name is what every collected sample is keyed by, and it is supplied by the operator
/// rather than read from the node. A Prometheus scrape carries its resource attributes only in
/// <c>target_info</c>, so samples themselves say nothing about which node produced them; the
/// collector is the only component that knows, because it chose the URL. Naming the node here keeps
/// the whole time series attributable after the fact.</para>
/// </summary>
public sealed record NodeTarget(string Name, Uri MetricsUrl)
{
    /// <summary>
    /// Parses a <c>name=url</c> pair. When <paramref name="defaultPath"/> is given and the URL carries
    /// no path, it is appended — a metrics target written as a bare host:port then still points at the
    /// exporter's default path instead of the site root.
    /// </summary>
    public static bool TryParse(string value, out NodeTarget? target, out string? error, string? defaultPath = "/metrics")
    {
        target = null;
        error = null;

        string text = value.Trim();
        if (text.Length == 0)
        {
            error = "an empty --metrics-endpoint value is not a node target.";
            return false;
        }

        int eq = text.IndexOf('=');
        if (eq <= 0 || eq == text.Length - 1)
        {
            error = $"'{value}' is not a node target; use name=url (e.g. camus1=http://camus1:5095/metrics).";
            return false;
        }

        string name = text[..eq].Trim();
        string url = text[(eq + 1)..].Trim();
        if (name.Length == 0 || url.Length == 0)
        {
            error = $"'{value}' is not a node target; use name=url (e.g. camus1=http://camus1:5095/metrics).";
            return false;
        }
        if (name.Contains(',') || name.Contains('"'))
        {
            error = $"node name '{name}' may not contain a comma or a quote; it is a column value in node-metrics.csv.";
            return false;
        }
        if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            error = $"'{url}' is not an absolute http(s) URL.";
            return false;
        }

        if (defaultPath is not null && uri.AbsolutePath is "" or "/")
            uri = new Uri(uri, defaultPath);

        target = new NodeTarget(name, uri);
        return true;
    }

    /// <summary>
    /// Parses every <c>name=url</c> pair, rejecting a duplicate name. Two targets under one name would
    /// silently merge into one series and understate the per-node spread the run is trying to measure.
    /// </summary>
    public static bool TryParseAll(
        IEnumerable<string> values, out List<NodeTarget> targets, out string? error, string? defaultPath = "/metrics")
    {
        targets = new List<NodeTarget>();
        error = null;

        foreach (string value in values)
        {
            foreach (string part in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (!TryParse(part, out NodeTarget? target, out error, defaultPath))
                    return false;

                if (targets.Any(t => string.Equals(t.Name, target!.Name, StringComparison.Ordinal)))
                {
                    error = $"duplicate node name '{target!.Name}' in --metrics-endpoint.";
                    return false;
                }
                targets.Add(target!);
            }
        }
        return true;
    }
}
