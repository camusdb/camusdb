/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Results;

/// <summary>One measured point of a concurrency sweep.</summary>
public sealed record SweepPoint(int Workers, RunSummary Summary, bool ReconciliationPassed, string Directory);

/// <summary>
/// Renders a concurrency sweep across its points.
///
/// <para>The sweep exists to find the saturation point, and the saturation point is not "the highest
/// number in the table". Throughput that rises while latency rises faster is a queue forming, not
/// capacity being used, so the report names the best <em>valid</em> point and states the latency it
/// was bought at. A point whose reconciliation failed is never a candidate at all.</para>
/// </summary>
public static class SweepReport
{
    public static string RenderCsv(IReadOnlyList<SweepPoint> points)
    {
        StringBuilder sb = new();
        sb.AppendLine("workers,completed_ops_per_sec,read_ops_per_sec,write_txns_per_sec,read_p50_ms,read_p99_ms," +
                      "write_p50_ms,write_p99_ms,commit_p99_ms,conflicts,schedule_drops,valid,reconciled");

        foreach (SweepPoint p in points.OrderBy(p => p.Workers))
        {
            sb.Append(p.Workers).Append(',')
              .Append(N(p.Summary.AchievedOpsPerSec)).Append(',')
              .Append(N(p.Summary.ReadOpsPerSec)).Append(',')
              .Append(N(p.Summary.WriteTxnsPerSec)).Append(',')
              .Append(N(p.Summary.ReadLatency.P50)).Append(',')
              .Append(N(p.Summary.ReadLatency.P99)).Append(',')
              .Append(N(p.Summary.WriteLatency.P50)).Append(',')
              .Append(N(p.Summary.WriteLatency.P99)).Append(',')
              .Append(N(p.Summary.WriteCommit.P99)).Append(',')
              .Append(p.Summary.Conflicts).Append(',')
              .Append(p.Summary.ScheduleDrops).Append(',')
              .Append(p.Summary.Valid ? "true" : "false").Append(',')
              .Append(p.ReconciliationPassed ? "true" : "false")
              .AppendLine();
        }

        return sb.ToString();
    }

    public static string RenderMarkdown(IReadOnlyList<SweepPoint> points)
    {
        List<SweepPoint> ordered = points.OrderBy(p => p.Workers).ToList();

        StringBuilder sb = new();
        sb.AppendLine("# Concurrency sweep").AppendLine();
        sb.AppendLine("Each row is a complete run: its own measured window, its own artifacts, and its own");
        sb.AppendLine("reconciliation. A row that is not both VALID and reconciled is not a capacity result.");
        sb.AppendLine();
        sb.AppendLine("| Workers | Completed ops/s | Write txns/s | Read p99 (ms) | Write p99 (ms) | Commit p99 (ms) | Conflicts | Valid | Reconciled |");
        sb.AppendLine("|---|---|---|---|---|---|---|---|---|");

        foreach (SweepPoint p in ordered)
        {
            sb.AppendLine($"| {p.Workers} | {N(p.Summary.AchievedOpsPerSec)} | {N(p.Summary.WriteTxnsPerSec)} | " +
                          $"{N(p.Summary.ReadLatency.P99)} | {N(p.Summary.WriteLatency.P99)} | {N(p.Summary.WriteCommit.P99)} | " +
                          $"{p.Summary.Conflicts} | {(p.Summary.Valid ? "yes" : "**no**")} | {(p.ReconciliationPassed ? "yes" : "**no**")} |");
        }
        sb.AppendLine();

        List<SweepPoint> usable = ordered.Where(p => p.Summary.Valid && p.ReconciliationPassed).ToList();
        if (usable.Count == 0)
        {
            sb.AppendLine("> No point was both valid and reconciled, so this sweep establishes no capacity.");
            return sb.ToString();
        }

        SweepPoint best = usable.MaxBy(p => p.Summary.AchievedOpsPerSec)!;
        sb.AppendLine($"- Highest valid throughput: **{N(best.Summary.AchievedOpsPerSec)} ops/s at {best.Workers} worker(s)**, " +
                      $"read p99 {N(best.Summary.ReadLatency.P99)} ms, write p99 {N(best.Summary.WriteLatency.P99)} ms.");

        SweepPoint? knee = FindKnee(usable);
        if (knee is not null)
        {
            sb.AppendLine($"- First point past the knee: **{knee.Workers} worker(s)** — throughput rose less than 10% over the");
            sb.AppendLine("  previous point while p99 latency rose more than 50%. Past this point the extra concurrency is");
            sb.AppendLine("  queueing, not capacity.");
        }
        else
        {
            sb.AppendLine("- No knee inside the swept range: throughput was still climbing without a latency blow-up at the");
            sb.AppendLine("  top point. The saturation point is above the range that was swept, so extend it before");
            sb.AppendLine("  concluding anything about capacity.");
        }
        sb.AppendLine();

        return sb.ToString();
    }

    /// <summary>
    /// The first point where added concurrency buys little throughput and costs a lot of tail latency.
    /// Both halves are required: throughput alone flattens for many reasons, and latency alone rises
    /// under any load increase. Together they are the queue forming.
    /// </summary>
    public static SweepPoint? FindKnee(IReadOnlyList<SweepPoint> ordered)
    {
        for (int i = 1; i < ordered.Count; i++)
        {
            SweepPoint previous = ordered[i - 1];
            SweepPoint current = ordered[i];

            double previousOps = previous.Summary.AchievedOpsPerSec;
            double previousP99 = Math.Max(previous.Summary.WriteLatency.P99, previous.Summary.ReadLatency.P99);
            double currentP99 = Math.Max(current.Summary.WriteLatency.P99, current.Summary.ReadLatency.P99);
            if (previousOps <= 0 || previousP99 <= 0)
                continue;

            double throughputGain = (current.Summary.AchievedOpsPerSec - previousOps) / previousOps;
            double latencyGrowth = (currentP99 - previousP99) / previousP99;

            if (throughputGain < 0.10 && latencyGrowth > 0.50)
                return current;
        }
        return null;
    }

    private static string N(double value) => value.ToString("F2", CultureInfo.InvariantCulture);
}
