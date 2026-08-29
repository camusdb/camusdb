/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using System.Text.Json;
using CamusDB.Workload.Cluster;

namespace CamusDB.Workload.Results;

/// <summary>Everything one run wrote that a comparison reads.</summary>
public sealed record RunBundle(string Directory, RunManifest Manifest, RunSummary Summary, ClusterFacts? Facts)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Loads a run directory. <c>cluster-facts.json</c> is optional — a run made without
    /// <c>--node-endpoint</c> has none — and its absence is reported later as an unverified build
    /// rather than treated as a match.
    /// </summary>
    public static RunBundle Load(string directory)
    {
        string manifestPath = Path.Combine(directory, "manifest.json");
        string summaryPath = Path.Combine(directory, "summary.json");
        if (!File.Exists(manifestPath) || !File.Exists(summaryPath))
            throw new FileNotFoundException($"'{directory}' is not a run directory (needs manifest.json and summary.json).");

        RunManifest manifest = JsonSerializer.Deserialize<RunManifest>(File.ReadAllText(manifestPath), Json)!;
        RunSummary summary = JsonSerializer.Deserialize<RunSummary>(File.ReadAllText(summaryPath), Json)!;

        string factsPath = Path.Combine(directory, "cluster-facts.json");
        ClusterFacts? facts = File.Exists(factsPath) ? ClusterProbe.Deserialize(File.ReadAllText(factsPath)) : null;

        return new RunBundle(directory, manifest, summary, facts);
    }
}

/// <summary>One field the two runs were checked on.</summary>
/// <param name="Blocking">True when a difference makes the pair incomparable rather than merely notable.</param>
public sealed record ComparisonField(string Name, string Baseline, string Candidate, bool Blocking);

/// <summary>
/// The verdict on a before/after pair.
///
/// <para><see cref="Comparable"/> is separate from <see cref="GatesPassed"/> on purpose. An
/// incomparable pair has no meaningful ratio at all — reporting "4.2x" for two runs on different data
/// shapes is worse than reporting nothing. A comparable pair that misses its target is a real result
/// that simply did not reach the bar.</para>
/// </summary>
public sealed record ComparisonResult(
    bool Comparable,
    bool BuildVerified,
    IReadOnlyList<ComparisonField> Blockers,
    IReadOnlyList<ComparisonField> Differences,
    IReadOnlyList<string> Waived,
    IReadOnlyList<string> Warnings,
    double BaselineOpsPerSec,
    double CandidateOpsPerSec,
    double? Ratio,
    IReadOnlyList<string> GateFailures)
{
    public bool GatesPassed => Comparable && GateFailures.Count == 0;
}

/// <summary>
/// Compares two run bundles and refuses the pair when a field that must not differ did.
///
/// <para>The refusal is the feature. A throughput ratio is only evidence when both runs offered the
/// same work to the same shape of data under the same durability settings; otherwise it measures the
/// change in the workload, not the change in the system. These mistakes are not obvious in a
/// directory of JSON, which is why the check is mechanical and on by default.</para>
///
/// <para>The build and durability equality comes from <see cref="ClusterFacts.DurabilityFingerprint"/>.
/// When either run has no cluster facts the comparison still proceeds, but reports that the equality
/// was never verified — a silent dependency bump is the most common way two "identical" runs differ.</para>
/// </summary>
public static class RunComparison
{
    public static ComparisonResult Compare(
        RunBundle baseline,
        RunBundle candidate,
        IReadOnlyCollection<string>? waive = null,
        double? requireRatio = null,
        double? requireOpsPerSec = null,
        double? p99BudgetMs = null)
    {
        HashSet<string> waived = new(waive ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
        List<ComparisonField> checks = new();

        void Check(string name, object? a, object? b, bool blocking)
            => checks.Add(new ComparisonField(name, Text(a), Text(b), blocking));

        RunManifest m1 = baseline.Manifest;
        RunManifest m2 = candidate.Manifest;

        // Blocking: the workload's shape and the data it ran against. A difference here changes what
        // was measured, so no ratio computed across it means anything.
        Check("mode", m1.Mode, m2.Mode, blocking: true);
        Check("workload", m1.WorkloadKind, m2.WorkloadKind, blocking: true);
        Check("protocol", m1.Protocol, m2.Protocol, blocking: true);
        Check("seed", m1.Seed, m2.Seed, blocking: true);
        Check("rows", m1.Rows, m2.Rows, blocking: true);
        Check("payload-bytes", m1.PayloadBytes, m2.PayloadBytes, blocking: true);
        Check("tables", m1.Tables, m2.Tables, blocking: true);
        Check("read-percent", m1.ReadPercent, m2.ReadPercent, blocking: true);
        Check("write-percent", m1.WritePercent, m2.WritePercent, blocking: true);
        Check("writes-per-transaction", m1.WritesPerTransaction, m2.WritesPerTransaction, blocking: true);
        Check("locking", m1.Locking, m2.Locking, blocking: true);
        Check("isolation", m1.Isolation, m2.Isolation, blocking: true);
        Check("schema-fingerprint", m1.SchemaFingerprint, m2.SchemaFingerprint, blocking: true);
        Check("expect-faults", m1.ExpectFaults, m2.ExpectFaults, blocking: true);
        Check("client-package", m1.ClientPackageVersion, m2.ClientPackageVersion, blocking: true);

        // An open-loop run is defined by its offered rate, so a different target is a different
        // experiment. A closed-loop run's rate is an outcome, so the same field is only notable there.
        bool openLoop = string.Equals(m1.Mode, "open", StringComparison.OrdinalIgnoreCase);
        Check("target-ops", m1.TargetOps, m2.TargetOps, blocking: openLoop);

        // Notable but legitimate: these are what an operator sweeps.
        Check("workers", m1.Workers, m2.Workers, blocking: false);
        Check("connections", m1.Connections, m2.Connections, blocking: false);
        Check("endpoint", m1.Endpoint, m2.Endpoint, blocking: false);
        Check("no-auto-prepare", m1.NoAutoPrepare, m2.NoAutoPrepare, blocking: false);
        Check("request-timeout", m1.RequestTimeoutSeconds, m2.RequestTimeoutSeconds, blocking: false);
        Check("measured-seconds", baseline.Summary.MeasuredSeconds, candidate.Summary.MeasuredSeconds, blocking: false);

        List<string> warnings = new();
        bool buildVerified = false;

        if (baseline.Facts is ClusterFacts f1 && candidate.Facts is ClusterFacts f2)
        {
            buildVerified = true;

            // The build is pinned as one field: no experiment deliberately varies a dependency
            // version, so there is nothing to gain from making it waivable piece by piece.
            Check("build", ClusterProbe.VersionFingerprint(f1.Nodes), ClusterProbe.VersionFingerprint(f2.Nodes), blocking: true);

            // Engine settings are named individually so an experiment can waive exactly the knob it
            // varies — `--allow engine.RaftWalGroupCommitLingerMs` — while every other durability
            // setting stays pinned. Waiving one combined fingerprint would excuse a storage-backend
            // or fsync change in the same breath as the intended one.
            IReadOnlyDictionary<string, string> e1 = ClusterProbe.EngineSettings(f1);
            IReadOnlyDictionary<string, string> e2 = ClusterProbe.EngineSettings(f2);

            if (e1.Count == 0 || e2.Count == 0)
            {
                // One side predates the engine-settings capture. Reporting "no differences" here would
                // be a false match: nothing was compared at all.
                if (e1.Count != e2.Count)
                    warnings.Add(
                        "resolved engine settings were NOT compared: " +
                        $"{(e1.Count == 0 ? "baseline" : "candidate")} captured none. " +
                        "A durability difference between these runs cannot be ruled out.");
            }
            else
            {
                foreach (string name in e1.Keys.Union(e2.Keys, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal))
                {
                    string v1 = e1.TryGetValue(name, out string? a) ? a : "(absent)";
                    string v2 = e2.TryGetValue(name, out string? b) ? b : "(absent)";
                    if (v1 != v2)
                        Check($"engine.{name}", v1, v2, blocking: true);
                }
            }
        }
        else
        {
            warnings.Add(
                "build and durability equality was NOT verified: " +
                $"{(baseline.Facts is null ? "baseline" : "candidate")} has no cluster-facts.json. " +
                "Re-run with --node-endpoint so a dependency or WAL-setting difference cannot pass unnoticed.");
        }

        List<ComparisonField> blockers = checks
            .Where(c => c.Blocking && c.Baseline != c.Candidate && !waived.Contains(c.Name))
            .ToList();

        List<string> waivedApplied = checks
            .Where(c => c.Blocking && c.Baseline != c.Candidate && waived.Contains(c.Name))
            .Select(c => $"{c.Name}: {c.Baseline} -> {c.Candidate}")
            .ToList();

        List<ComparisonField> differences = checks
            .Where(c => !c.Blocking && c.Baseline != c.Candidate)
            .ToList();

        bool comparable = blockers.Count == 0;

        double baseOps = baseline.Summary.AchievedOpsPerSec;
        double candOps = candidate.Summary.AchievedOpsPerSec;
        double? ratio = comparable && baseOps > 0 ? candOps / baseOps : null;

        List<string> gateFailures = new();
        if (!baseline.Summary.Valid)
            gateFailures.Add("the baseline run is INVALID; it cannot anchor a speedup claim.");
        if (!candidate.Summary.Valid)
            gateFailures.Add("the candidate run is INVALID; its throughput does not count.");

        if (comparable)
        {
            if (requireRatio is double needRatio)
            {
                if (ratio is null)
                    gateFailures.Add($"no ratio could be computed (baseline throughput is {N(baseOps)} ops/s).");
                else if (ratio.Value < needRatio)
                    gateFailures.Add($"speedup {N(ratio.Value, "F2")}x is below the required {N(needRatio, "F2")}x.");
            }

            if (requireOpsPerSec is double needOps && candOps < needOps)
                gateFailures.Add($"candidate {N(candOps)} ops/s is below the required floor of {N(needOps)} ops/s.");

            if (p99BudgetMs is double budget)
            {
                double worst = Math.Max(candidate.Summary.ReadLatency.P99, candidate.Summary.WriteLatency.P99);
                if (worst > budget)
                    gateFailures.Add(
                        $"candidate p99 {N(worst)} ms exceeds the frozen budget of {N(budget)} ms " +
                        $"(read p99 {N(candidate.Summary.ReadLatency.P99)}, write p99 {N(candidate.Summary.WriteLatency.P99)}).");
            }
        }

        return new ComparisonResult(
            comparable, buildVerified, blockers, differences, waivedApplied, warnings,
            baseOps, candOps, ratio, gateFailures);
    }

    /// <summary>Renders the verdict for a terminal and for <c>comparison.md</c>.</summary>
    public static string Render(RunBundle baseline, RunBundle candidate, ComparisonResult result)
    {
        StringBuilder sb = new();
        sb.AppendLine("# Run comparison").AppendLine();
        sb.AppendLine($"- Baseline : `{baseline.Directory}` — {N(result.BaselineOpsPerSec)} completed ops/s" +
                      $" ({(baseline.Summary.Valid ? "VALID" : "INVALID")})");
        sb.AppendLine($"- Candidate: `{candidate.Directory}` — {N(result.CandidateOpsPerSec)} completed ops/s" +
                      $" ({(candidate.Summary.Valid ? "VALID" : "INVALID")})");
        sb.AppendLine();

        if (!result.Comparable)
        {
            sb.AppendLine("## REFUSED — the runs are not comparable").AppendLine();
            sb.AppendLine("These fields must be identical for a before/after claim, and are not:").AppendLine();
            sb.AppendLine("| Field | Baseline | Candidate |");
            sb.AppendLine("|---|---|---|");
            foreach (ComparisonField f in result.Blockers)
                sb.AppendLine($"| {f.Name} | `{f.Baseline}` | `{f.Candidate}` |");
            sb.AppendLine();
            sb.AppendLine("No speedup is reported. Re-run the pair under matched settings, or waive a field");
            sb.AppendLine("explicitly with `--allow <field>` when the difference is the thing under test.");
            sb.AppendLine();
        }
        else
        {
            sb.AppendLine($"## Speedup: **{(result.Ratio is double r ? r.ToString("F2", CultureInfo.InvariantCulture) + "x" : "n/a")}**").AppendLine();
            sb.AppendLine("| Metric | Baseline | Candidate |");
            sb.AppendLine("|---|---|---|");
            AppendMetric(sb, "Completed ops/s", baseline.Summary.AchievedOpsPerSec, candidate.Summary.AchievedOpsPerSec);
            AppendMetric(sb, "Committed write txns/s", baseline.Summary.WriteTxnsPerSec, candidate.Summary.WriteTxnsPerSec);
            AppendMetric(sb, "Read ops/s", baseline.Summary.ReadOpsPerSec, candidate.Summary.ReadOpsPerSec);
            AppendMetric(sb, "Rows/s", baseline.Summary.RowsPerSec, candidate.Summary.RowsPerSec);
            AppendMetric(sb, "Write p50 (ms)", baseline.Summary.WriteLatency.P50, candidate.Summary.WriteLatency.P50);
            AppendMetric(sb, "Write p99 (ms)", baseline.Summary.WriteLatency.P99, candidate.Summary.WriteLatency.P99);
            AppendMetric(sb, "Read p99 (ms)", baseline.Summary.ReadLatency.P99, candidate.Summary.ReadLatency.P99);
            sb.AppendLine($"| Schedule drops | {baseline.Summary.ScheduleDrops} | {candidate.Summary.ScheduleDrops} |");
            sb.AppendLine();
        }

        if (result.Waived.Count > 0)
        {
            sb.AppendLine("## Waived differences").AppendLine();
            foreach (string w in result.Waived)
                sb.AppendLine($"- {w} (waived by `--allow`)");
            sb.AppendLine();
        }

        if (result.Differences.Count > 0)
        {
            sb.AppendLine("## Differences that do not block").AppendLine();
            sb.AppendLine("| Field | Baseline | Candidate |");
            sb.AppendLine("|---|---|---|");
            foreach (ComparisonField f in result.Differences)
                sb.AppendLine($"| {f.Name} | `{f.Baseline}` | `{f.Candidate}` |");
            sb.AppendLine();
        }

        sb.AppendLine("## Build and durability").AppendLine();
        if (result.BuildVerified)
        {
            sb.AppendLine($"- Build: `{ClusterProbe.VersionFingerprint(baseline.Facts!.Nodes)}` vs " +
                          $"`{ClusterProbe.VersionFingerprint(candidate.Facts!.Nodes)}`");
            int settingCount = ClusterProbe.EngineSettings(baseline.Facts).Count;
            int differing = result.Blockers.Concat(result.Differences)
                .Count(f => f.Name.StartsWith("engine.", StringComparison.Ordinal));
            sb.AppendLine($"- Resolved engine settings compared: {settingCount}; differing: {differing}" +
                          (result.Waived.Any(w => w.StartsWith("engine.", StringComparison.Ordinal))
                              ? " (some waived — see above)"
                              : ""));
        }
        else
        {
            sb.AppendLine("- **Not verified** — see the warning below.");
        }
        sb.AppendLine();

        foreach (string warning in result.Warnings)
            sb.AppendLine($"> ⚠ {warning}").AppendLine();

        sb.AppendLine($"## Gates: {(result.GatesPassed ? "PASS" : "FAIL")}").AppendLine();
        if (result.GateFailures.Count == 0)
            sb.AppendLine("- No gate failed.");
        foreach (string failure in result.GateFailures)
            sb.AppendLine($"- ✗ {failure}");
        sb.AppendLine();

        return sb.ToString();
    }

    /// <summary>
    /// Formats a number for an artifact. Always invariant: a comparison report is diffed, pasted and
    /// grepped, and a decimal separator that follows the operator's locale would make two identical
    /// verdicts differ as text.
    /// </summary>
    private static string N(double value, string format = "F1") => value.ToString(format, CultureInfo.InvariantCulture);

    private static void AppendMetric(StringBuilder sb, string name, double a, double b)
        => sb.AppendLine($"| {name} | {a.ToString("F2", CultureInfo.InvariantCulture)} | {b.ToString("F2", CultureInfo.InvariantCulture)} |");

    private static string Text(object? value) => value switch
    {
        null => "(unset)",
        bool b => b ? "true" : "false",
        double d => d.ToString("F3", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };
}
