/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Corpus;

namespace CamusDB.PlanQuality.Tests.Metrics;

/// <summary>
/// Turns the rows of <c>EXPLAIN (ANALYZE)</c> and the result rows of a query into one
/// <see cref="QueryMeasurement"/>. Pure functions, so the unit tests can check every rule without an
/// engine.
///
/// <para><b>Work</b> is the sum of <c>rows_read</c>, <c>kv_lookups</c> and <c>kv_scan_entries</c>
/// over the plan nodes. A join row already includes the reads of a right input that has no row of
/// its own, so nothing is counted twice. One known gap: the reads of a subquery that the bind stage
/// runs to fold it into a literal are not on any node, so they are not in the work.</para>
///
/// <para><b>The fingerprint</b> is one line for each plan node: its name and its detail without the
/// join-order estimate. It has no costs, estimates, timings or ids, so it changes only when the plan
/// changes. The trailing <c>plan-info</c>, <c>cache</c> and span rows are not plan nodes and are left
/// out.</para>
/// </summary>
internal static partial class PlanMeasures
{
    private static readonly HashSet<string> NonPlanRows = new(StringComparer.Ordinal)
    {
        "plan-info", "cache", "gather-span", "broadcast-span",
    };

    [GeneratedRegex(@", join-order-est-rows=\d+")]
    private static partial Regex JoinOrderEstimate();

    [GeneratedRegex(@"join-order-est-rows=(\d+)")]
    private static partial Regex JoinOrderEstimateValue();

    [GeneratedRegex(@"planning-ms=([0-9.]+)")]
    private static partial Regex PlanningMs();

    [GeneratedRegex(@"join-order=([a-z-]+)")]
    private static partial Regex JoinOrder();

    [GeneratedRegex(@"join-steps-costed=(\d+)")]
    private static partial Regex JoinStepsCosted();

    public static QueryMeasurement Build(
        CorpusQuery query,
        IReadOnlyList<QueryResultRow> analyzeRows,
        IReadOnlyList<QueryResultRow> resultRows,
        long statementRowsRead)
    {
        List<PlanNodeMeasure> nodes = new();
        string? planInfo = null;
        double executionMs = 0;

        foreach (QueryResultRow row in analyzeRows)
        {
            string node = Text(row, "node");

            if (node == "plan-info")
                planInfo = Text(row, "detail");

            if (NonPlanRows.Contains(node))
                continue;

            string detail = Text(row, "detail");
            Match search = JoinOrderEstimateValue().Match(detail);
            long? estimate = search.Success
                ? long.Parse(search.Groups[1].Value, CultureInfo.InvariantCulture)
                : NullableLong(row, "estimated_rows");

            if (row.Row.TryGetValue("actual_time_ms", out ColumnValue? time) && time.Type == ColumnType.Float64)
                executionMs = Math.Max(executionMs, time.FloatValue);

            nodes.Add(new PlanNodeMeasure(
                node,
                detail,
                estimate,
                NullableLong(row, "actual_rows"),
                NullableLong(row, "rows_read") ?? 0,
                NullableLong(row, "kv_lookups") ?? 0,
                NullableLong(row, "kv_scan_entries") ?? 0));
        }

        long work = 0;
        List<double> qErrors = new();
        foreach (PlanNodeMeasure node in nodes)
        {
            work += node.RowsRead + node.KvLookups + node.KvScanEntries;

            if (node.EstimatedRows is { } estimated && node.ActualRows is { } actual)
                qErrors.Add(QError(estimated, actual));
        }

        (string hash, long count) = ResultHash(resultRows);

        return new QueryMeasurement(
            query.Id,
            query.Tags,
            count,
            hash,
            work,
            statementRowsRead,
            Median(qErrors),
            qErrors.Count == 0 ? 1 : qErrors.Max(),
            planInfo is null ? null : MatchOrNull(JoinOrder(), planInfo),
            planInfo is null ? 0 : long.Parse(MatchOrNull(JoinStepsCosted(), planInfo) ?? "0", CultureInfo.InvariantCulture),
            Fingerprint(nodes),
            planInfo is null ? 0 : double.Parse(MatchOrNull(PlanningMs(), planInfo) ?? "0", CultureInfo.InvariantCulture),
            executionMs,
            nodes,
            Error: null);
    }

    /// <summary>
    /// max(estimated / actual, actual / estimated), with both values floored at 1. A value of 1 is a
    /// perfect estimate. The floor keeps an empty result finite: estimating 5 rows for a node that
    /// produced none is a q-error of 5.
    /// </summary>
    public static double QError(long estimated, long actual)
    {
        double e = Math.Max(1, estimated);
        double a = Math.Max(1, actual);
        return Math.Max(e / a, a / e);
    }

    /// <summary>One line for each plan node, as described in the class summary.</summary>
    public static IReadOnlyList<string> Fingerprint(IReadOnlyList<PlanNodeMeasure> nodes)
    {
        List<string> lines = new(nodes.Count);
        foreach (PlanNodeMeasure node in nodes)
        {
            string detail = JoinOrderEstimate().Replace(node.Detail, "");
            if (detail.StartsWith("join-order-est-rows=", StringComparison.Ordinal))
                detail = "";

            lines.Add(detail.Length == 0 ? node.Node : $"{node.Node}({detail})");
        }
        return lines;
    }

    /// <summary>
    /// A hash of the result that does not depend on row order or column order. Each row is its
    /// <c>column=value</c> cells sorted by column name, and the rows are sorted, so two plans that
    /// return the same rows in a different order have the same hash. An <c>ORDER BY</c> is checked
    /// only through the rows a <c>LIMIT</c> keeps, which is why the corpus breaks every tie of an
    /// ordered limit with a unique column. A float is printed with 6 decimals, so a sum that adds
    /// the same values in another order still matches.
    /// </summary>
    public static (string Hash, long Rows) ResultHash(IReadOnlyList<QueryResultRow> rows)
    {
        List<string> rendered = new(rows.Count);
        foreach (QueryResultRow row in rows)
        {
            StringBuilder line = new();
            foreach (KeyValuePair<string, ColumnValue> cell in row.Row.OrderBy(c => c.Key, StringComparer.Ordinal))
                line.Append(cell.Key).Append('=').Append(Render(cell.Value)).Append('\u001f');
            rendered.Add(line.ToString());
        }

        rendered.Sort(StringComparer.Ordinal);

        StringBuilder all = new();
        foreach (string line in rendered)
            all.Append(line).Append('\u001e');

        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(all.ToString()));
        return (Convert.ToHexString(digest, 0, 8).ToLowerInvariant(), rows.Count);
    }

    /// <summary>The text form of a value that the result hash uses.</summary>
    public static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 or ColumnType.Float32 => value.FloatValue.ToString("0.######", CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        ColumnType.String => "'" + value.StrValue + "'",
        _ => value.ToString(),
    };

    private static double Median(List<double> values)
    {
        if (values.Count == 0)
            return 1;

        values.Sort();
        int middle = values.Count / 2;
        return values.Count % 2 == 1 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
    }

    private static string Text(QueryResultRow row, string column) =>
        row.Row.TryGetValue(column, out ColumnValue? value) ? value.StrValue ?? "" : "";

    private static long? NullableLong(QueryResultRow row, string column) =>
        row.Row.TryGetValue(column, out ColumnValue? value) && value.Type == ColumnType.Integer64 ? value.LongValue : null;

    private static string? MatchOrNull(Regex regex, string text)
    {
        Match match = regex.Match(text);
        return match.Success ? match.Groups[1].Value : null;
    }
}
