/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;

namespace CamusDB.Workload.Tpch;

/// <summary>The measured-window figures of one query.</summary>
public sealed record TpchQueryStats(
    string Query,
    int ReferenceRows,
    string ReferenceDigest,
    double ReferenceMs,
    long Executions,
    long Verified,
    long Errors,
    long Mismatches,
    double MeanMs,
    double P50Ms,
    double P95Ms,
    double P99Ms,
    double MaxMs);

/// <summary>The content of <c>tpch.json</c>.</summary>
public sealed record TpchRunReport(
    string Scale,
    string Seed,
    double MeasuredSeconds,
    long Executions,
    long Verified,
    double QueriesPerHour,
    IReadOnlyList<TpchQueryStats> Queries);

/// <summary>
/// The TPC-H verdict that joins reconciliation. <see cref="Mismatches"/> counts the whole run, warm-up
/// and drain included, because a wrong answer is wrong whenever it was given; the other counts are of
/// the measured window. It fails on any mismatch and on any enabled query that has no verified
/// execution inside the window, since a run says nothing about a query it never finished.
/// </summary>
public sealed record TpchResult(
    bool Passed,
    bool LoadVerified,
    int QueriesEnabled,
    int QueriesChecked,
    long Executions,
    long Verified,
    long Errors,
    long Mismatches,
    IReadOnlyList<string> Failures);

public static class TpchReport
{
    public static (TpchRunReport Report, TpchResult Result) Build(
        string scale, string seed, IReadOnlyList<TpchReference> references, IReadOnlyCollection<TpchExecution> executions,
        long mismatchesWholeRun, DateTime measureStartUtc, double measuredSeconds)
    {
        DateTime from = measureStartUtc;
        DateTime until = measureStartUtc.AddSeconds(measuredSeconds);

        Dictionary<int, List<TpchExecution>> byQuery = new();
        foreach (TpchExecution e in executions)
        {
            // An execution belongs to the window when it finished inside it, the way the scheduler counts
            // a completed operation.
            DateTime finished = e.StartedUtc.AddMilliseconds(e.Milliseconds);
            if (finished < from || finished > until)
                continue;
            if (!byQuery.TryGetValue(e.Query, out List<TpchExecution>? list))
                byQuery[e.Query] = list = new List<TpchExecution>();
            list.Add(e);
        }

        List<TpchQueryStats> stats = new(references.Count);
        List<string> failures = new();
        long totalExecutions = 0, totalVerified = 0, totalErrors = 0;
        int queriesChecked = 0;

        foreach (TpchReference reference in references)
        {
            List<TpchExecution> list = byQuery.GetValueOrDefault(reference.Query.Number) ?? [];
            LatencyHistogram histogram = new();
            long verified = 0, errors = 0, mismatches = 0;
            double sumMs = 0;
            foreach (TpchExecution e in list)
            {
                switch (e.Outcome)
                {
                    case TpchOutcome.Verified:
                        verified++;
                        sumMs += e.Milliseconds;
                        histogram.Record(e.Milliseconds);
                        break;
                    case TpchOutcome.Mismatch:
                        mismatches++;
                        break;
                    default:
                        errors++;
                        break;
                }
            }

            stats.Add(new TpchQueryStats(
                reference.Query.Name, reference.Result.Rows.Count, reference.Result.Digest, Math.Round(reference.Milliseconds, 3),
                list.Count, verified, errors, mismatches,
                verified > 0 ? Math.Round(sumMs / verified, 3) : 0,
                Math.Round(histogram.Percentile(50), 3), Math.Round(histogram.Percentile(95), 3),
                Math.Round(histogram.Percentile(99), 3), Math.Round(histogram.MaxMilliseconds, 3)));

            totalExecutions += list.Count;
            totalVerified += verified;
            totalErrors += errors;
            if (verified > 0)
                queriesChecked++;
            else
                failures.Add($"{reference.Query.Name} has no verified execution in the measured window " +
                             $"({list.Count} attempt(s), {errors} error(s)); the run says nothing about it");
        }

        if (mismatchesWholeRun > 0)
            failures.Insert(0, $"{mismatchesWholeRun:N0} query execution(s) returned rows that differ from the reference " +
                               "(see tpch-mismatches.jsonl)");

        TpchRunReport report = new(
            scale, seed, measuredSeconds, totalExecutions, totalVerified,
            measuredSeconds > 0 ? Math.Round(totalVerified * 3600.0 / measuredSeconds, 1) : 0, stats);

        TpchResult result = new(
            Passed: failures.Count == 0,
            LoadVerified: true,
            QueriesEnabled: references.Count,
            QueriesChecked: queriesChecked,
            Executions: totalExecutions,
            Verified: totalVerified,
            Errors: totalErrors,
            Mismatches: mismatchesWholeRun,
            Failures: failures);

        return (report, result);
    }
}
