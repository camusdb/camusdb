/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Estimates;

/// <summary>
/// One plan node with both an estimate and an actual row count: where it came from (query,
/// template, tier, scale), what <c>EXPLAIN (ANALYZE)</c> printed for it, and the trace of its
/// estimate. <see cref="Rule"/> is <c>unknown</c> for a node the planner did not trace; the share of
/// those is reported and the target is none. <see cref="Template"/> groups the variants of one Join
/// Order Benchmark query (<c>1a</c>, <c>1b</c>, …).
/// </summary>
internal sealed record EstimateRecord(
    string Query,
    string Template,
    string Tier,
    string Scale,
    int NodeIndex,
    string Node,
    string Rule,
    string Formula,
    double Estimate,
    long EstimatedRows,
    long ActualRows,
    string? Trace)
{
    public double QError => PlanMeasures.QError(EstimatedRows, ActualRows);

    /// <summary>The template of a Join Order Benchmark id (<c>17f</c> is template <c>17</c>); any other id is its own template.</summary>
    public static string TemplateOf(string queryId)
    {
        Match m = Regex.Match(queryId, @"^(\d+)[a-z]$");
        return m.Success ? m.Groups[1].Value : queryId;
    }
}

/// <summary>
/// Builds, writes and reads the estimate records of a run, and summarizes their errors. The file
/// is JSON in the harness output directory, next to the run and the report, never in the
/// repository: it is a measurement of one tree on one data set, not a baseline.
/// </summary>
internal static class TrainingSet
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = false };

    /// <summary>One record for each plan node of the run that has both an estimate and an actual row count.</summary>
    public static List<EstimateRecord> FromRun(IReadOnlyList<QueryMeasurement> run, string tier, string scale)
    {
        List<EstimateRecord> records = [];
        foreach (QueryMeasurement query in run)
        {
            if (query.Error is not null)
                continue;

            for (int i = 0; i < query.Nodes.Count; i++)
            {
                PlanNodeMeasure node = query.Nodes[i];
                if (node.EstimatedRows is not { } estimated || node.ActualRows is not { } actual)
                    continue;

                string rule = "unknown";
                string formula = "unknown";
                double estimate = estimated;
                if (node.Trace is { } json)
                {
                    TraceNode trace = TraceNode.Parse(json);
                    rule = trace.Rule;
                    formula = trace.Formula;
                    estimate = trace.Estimate;
                }

                records.Add(new EstimateRecord(query.Id, EstimateRecord.TemplateOf(query.Id), tier, scale, i, node.Node, rule, formula, estimate, estimated, actual, node.Trace));
            }
        }
        return records;
    }

    public static void Save(IReadOnlyList<EstimateRecord> records, string path) =>
        File.WriteAllText(path, JsonSerializer.Serialize(records, Json));

    public static List<EstimateRecord> Load(string path) =>
        JsonSerializer.Deserialize<List<EstimateRecord>>(File.ReadAllText(path), Json) ?? [];

    /// <summary>
    /// The estimate-error summary of a set of records: the nodes, how many have a q-error over 100,
    /// the median and the 95th percentile of the q-error, the mean squared log q-error, and the
    /// share of nodes with no rule.
    /// </summary>
    public sealed record Summary(int Nodes, int Over100, double Median, double P95, double MeanSquaredLog, double UnknownShare)
    {
        public string Row(string label) => FormattableString.Invariant(
            $"| {label} | {Nodes} | {Over100} | {Median:0.00} | {P95:0.0} | {MeanSquaredLog:0.000} | {UnknownShare:P1} |");

        public const string Header = "| set | nodes | q-error > 100 | median q-error | p95 q-error | mean sq log q-error | unknown rule |\n|---|---:|---:|---:|---:|---:|---:|";
    }

    public static Summary Summarize(IReadOnlyList<EstimateRecord> records)
    {
        if (records.Count == 0)
            return new Summary(0, 0, 1, 1, 0, 0);

        List<double> qErrors = new(records.Count);
        double sumSquaredLog = 0;
        int unknown = 0;
        foreach (EstimateRecord record in records)
        {
            double q = record.QError;
            qErrors.Add(q);
            double logQ = Math.Log(Math.Max(1, record.EstimatedRows)) - Math.Log(Math.Max(1, record.ActualRows));
            sumSquaredLog += logQ * logQ;
            if (record.Rule == "unknown")
                unknown++;
        }

        qErrors.Sort();
        return new Summary(
            records.Count,
            qErrors.Count(q => q > 100),
            Percentile(qErrors, 0.5),
            Percentile(qErrors, 0.95),
            sumSquaredLog / records.Count,
            (double)unknown / records.Count);
    }

    private static double Percentile(List<double> sorted, double p)
    {
        if (sorted.Count == 0)
            return 1;
        double rank = p * (sorted.Count - 1);
        int lo = (int)Math.Floor(rank);
        int hi = Math.Min(sorted.Count - 1, lo + 1);
        return sorted[lo] + (sorted[hi] - sorted[lo]) * (rank - lo);
    }

    /// <summary>A Markdown table of the records by rule: count, nodes over 100, median and p95 q-error.</summary>
    public static string RulesTable(IReadOnlyList<EstimateRecord> records)
    {
        StringBuilder md = new();
        md.Append("| rule | nodes | q-error > 100 | median q-error | p95 q-error |\n|---|---:|---:|---:|---:|\n");
        foreach (IGrouping<string, EstimateRecord> group in records.GroupBy(r => r.Rule).OrderByDescending(g => g.Count()).ThenBy(g => g.Key, StringComparer.Ordinal))
        {
            Summary s = Summarize(group.ToList());
            md.Append(CultureInfo.InvariantCulture, $"| `{group.Key}` | {s.Nodes} | {s.Over100} | {s.Median:0.00} | {s.P95:0.0} |\n");
        }
        return md.ToString();
    }
}
