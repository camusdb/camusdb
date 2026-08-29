/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Results;

/// <summary>One run's place in a baseline set.</summary>
public sealed record BaselineRun(string Directory, RunSummary Summary, bool Usable, string? Reason);

/// <summary>
/// What a set of repeated runs establishes, or fails to.
///
/// <para><see cref="Comparable"/> is separate from the numbers on purpose: a median across runs that
/// offered different work is not a baseline, it is an average of different experiments.</para>
/// </summary>
public sealed record BaselineResult(
    IReadOnlyList<BaselineRun> Runs,
    bool Comparable,
    IReadOnlyList<string> Incomparable,
    int UsableCount,
    double Median,
    double Min,
    double Max,
    double Spread,
    double CoefficientOfVariation,
    double MedianWriteTxnsPerSec,
    double MedianWriteP99Ms,
    double MedianReadP99Ms,
    IReadOnlyList<string> Warnings)
{
    /// <summary>
    /// A baseline that may anchor a later comparison: every run comparable, at least three of them
    /// usable, and the run-to-run variation under 10%.
    ///
    /// <para>The bar reads against the <b>coefficient of variation</b>, not <see cref="Spread"/>.
    /// Spread is max minus min, which grows with the number of runs — five draws from one
    /// distribution almost always show a wider range than three — so gating on it would punish the
    /// very repetitions that make a median trustworthy, and no amount of extra runs could ever
    /// satisfy it. The coefficient of variation is scale-free and stable as the sample grows, so it
    /// measures what the bar is actually asking about: how much one run of an unchanged system
    /// differs from the next. Spread stays in the report as context.</para>
    /// </summary>
    public bool Established => Comparable && UsableCount >= 3 && CoefficientOfVariation <= 0.10;
}

/// <summary>
/// Aggregates repeated runs of one workload into a baseline: the median throughput a later run is
/// measured against, and how much the repetitions disagreed.
///
/// <para>The variation is the point. A single run is a number; a baseline is a number with a known
/// spread, and without the spread there is no way to tell a real 15% improvement from two runs of an
/// unchanged system. A run that was INVALID or failed reconciliation is excluded from the statistics
/// and reported separately — it is not a slow result, it is not a result.</para>
/// </summary>
public static class BaselineSummary
{
    public static BaselineResult Build(IReadOnlyList<RunBundle> bundles)
    {
        if (bundles.Count == 0)
            throw new ArgumentException("A baseline needs at least one run.", nameof(bundles));

        List<BaselineRun> runs = new();
        foreach (RunBundle bundle in bundles)
        {
            string? reason = null;
            if (!bundle.Summary.Valid)
                reason = "the run is INVALID";

            runs.Add(new BaselineRun(bundle.Directory, bundle.Summary, reason is null, reason));
        }

        // Every run in a set must be comparable with the first, or the median spans different
        // experiments. This reuses the same blocking-field rules the pairwise comparison applies.
        List<string> incomparable = new();
        RunBundle first = bundles[0];
        for (int i = 1; i < bundles.Count; i++)
        {
            ComparisonResult comparison = RunComparison.Compare(first, bundles[i]);
            if (!comparison.Comparable)
            {
                incomparable.Add(
                    $"{bundles[i].Directory}: {string.Join(", ", comparison.Blockers.Select(b => $"{b.Name} {b.Baseline} != {b.Candidate}"))}");
            }
        }

        List<RunSummary> usable = runs.Where(r => r.Usable).Select(r => r.Summary).ToList();
        List<string> warnings = new();

        if (usable.Count < bundles.Count)
            warnings.Add($"{bundles.Count - usable.Count} of {bundles.Count} run(s) were excluded as invalid.");
        if (usable.Count < 3)
            warnings.Add("fewer than three usable runs: a spread computed from one or two runs says nothing about repeatability.");

        double[] throughput = usable.Select(s => s.AchievedOpsPerSec).OrderBy(v => v).ToArray();
        double median = Median(throughput);
        double min = throughput.Length > 0 ? throughput[0] : 0;
        double max = throughput.Length > 0 ? throughput[^1] : 0;
        double spread = median > 0 ? (max - min) / median : 0;
        double cv = CoefficientOfVariation(throughput);

        if (usable.Count >= 3 && cv > 0.10)
            warnings.Add(
                $"run-to-run variation is {cv:P1} (coefficient of variation), above the 10% bar: the baseline " +
                "is not repeatable enough to attribute a change of that size to anything but noise.");

        return new BaselineResult(
            runs,
            Comparable: incomparable.Count == 0,
            Incomparable: incomparable,
            UsableCount: usable.Count,
            Median: median,
            Min: min,
            Max: max,
            Spread: spread,
            CoefficientOfVariation: cv,
            MedianWriteTxnsPerSec: Median(usable.Select(s => s.WriteTxnsPerSec).OrderBy(v => v).ToArray()),
            MedianWriteP99Ms: Median(usable.Select(s => s.WriteLatency.P99).OrderBy(v => v).ToArray()),
            MedianReadP99Ms: Median(usable.Select(s => s.ReadLatency.P99).OrderBy(v => v).ToArray()),
            Warnings: warnings);
    }

    public static string Render(BaselineResult result)
    {
        StringBuilder sb = new();
        sb.AppendLine("# Baseline").AppendLine();
        sb.AppendLine("| Run | Completed ops/s | Write txns/s | Read p99 (ms) | Write p99 (ms) | Usable |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (BaselineRun run in result.Runs)
        {
            sb.AppendLine($"| `{run.Directory}` | {N(run.Summary.AchievedOpsPerSec)} | {N(run.Summary.WriteTxnsPerSec)} | " +
                          $"{N(run.Summary.ReadLatency.P99)} | {N(run.Summary.WriteLatency.P99)} | " +
                          $"{(run.Usable ? "yes" : "**no** — " + run.Reason)} |");
        }
        sb.AppendLine();

        if (!result.Comparable)
        {
            sb.AppendLine("## REFUSED — these runs are not one baseline").AppendLine();
            foreach (string reason in result.Incomparable)
                sb.AppendLine($"- {reason}");
            sb.AppendLine();
            sb.AppendLine("A median across runs that offered different work averages different experiments.");
            sb.AppendLine();
            return sb.ToString();
        }

        sb.AppendLine($"## Median: **{N(result.Median)} completed ops/s** over {result.UsableCount} usable run(s)").AppendLine();
        sb.AppendLine("| Statistic | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| Median completed ops/s | {N(result.Median)} |");
        sb.AppendLine($"| Min / Max | {N(result.Min)} / {N(result.Max)} |");
        sb.AppendLine($"| Spread (max-min) / median | {P(result.Spread)} — context only |");
        sb.AppendLine($"| Coefficient of variation | {P(result.CoefficientOfVariation)} — **the 10% bar reads here** |");
        sb.AppendLine($"| Median committed write txns/s | {N(result.MedianWriteTxnsPerSec)} |");
        sb.AppendLine($"| Median read p99 / write p99 (ms) | {N(result.MedianReadP99Ms)} / {N(result.MedianWriteP99Ms)} |");
        sb.AppendLine();

        sb.AppendLine(result.Established
            ? "**Baseline established.** Freeze the write p99 above as the latency budget and pass it to every"
            : "**Baseline NOT established.** Do not anchor a speedup claim on this set yet.");
        if (result.Established)
            sb.AppendLine($"later comparison: `compare --p99-budget-ms {result.MedianWriteP99Ms.ToString("F0", CultureInfo.InvariantCulture)}`.");
        sb.AppendLine();

        foreach (string warning in result.Warnings)
            sb.AppendLine($"> ⚠ {warning}").AppendLine();

        return sb.ToString();
    }

    private static double Median(double[] sorted)
    {
        if (sorted.Length == 0)
            return 0;
        int mid = sorted.Length / 2;
        return sorted.Length % 2 == 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    }

    private static double CoefficientOfVariation(double[] values)
    {
        if (values.Length < 2)
            return 0;

        double mean = values.Average();
        if (mean <= 0)
            return 0;

        double variance = values.Sum(v => (v - mean) * (v - mean)) / (values.Length - 1);
        return Math.Sqrt(variance) / mean;
    }

    private static string N(double value) => value.ToString("F2", CultureInfo.InvariantCulture);

    private static string P(double fraction) => (fraction * 100).ToString("F1", CultureInfo.InvariantCulture) + "%";
}
