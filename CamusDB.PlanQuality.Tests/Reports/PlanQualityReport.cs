/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Harness;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Reports;

/// <summary>
/// The Markdown report of one run: the gate result, the failures, the work by tag, every plan that
/// changed (old and new fingerprint), the worst estimates, and the timings. CI uploads it on every
/// run, also on success, so a reviewer sees plan changes that did not fail the gate.
/// </summary>
internal static class PlanQualityReport
{
    public static string Render(
        string title,
        string baselinePath,
        IReadOnlyList<QueryMeasurement> run,
        ComparisonResult comparison,
        HarnessSettings settings)
    {
        StringBuilder md = new();
        List<Finding> failures = comparison.Failures.ToList();
        List<Finding> notes = comparison.Findings.Where(f => !f.IsFailure).ToList();

        md.Append("# ").Append(title).Append("\n\n");
        md.Append(failures.Count == 0 ? "**Gate: PASS**" : $"**Gate: FAIL ({failures.Count})**");
        if (settings.UpdateBaseline)
            md.Append(" — baseline update run");
        if (settings.Only is not null)
            md.Append(" — partial run (PLAN_QUALITY_ONLY)");
        md.Append("\n\n");

        long baselineWork = comparison.Compared.Sum(c => c.Baseline.Work);
        long currentWork = comparison.Compared.Sum(c => c.Current.Work);
        long baselineReads = comparison.Compared.Sum(c => c.Baseline.StatementRowsRead);
        long currentReads = comparison.Compared.Sum(c => c.Current.StatementRowsRead);

        md.Append("| | |\n|---|---|\n");
        md.Append("| baseline | `").Append(Path.GetFileName(baselinePath)).Append("` |\n");
        md.Append("| queries run | ").Append(run.Count).Append(" |\n");
        md.Append("| queries compared | ").Append(comparison.Compared.Count).Append(" |\n");
        md.Append("| failures | ").Append(failures.Count).Append(" |\n");
        md.Append("| work regressions | ").Append(Count(notes, failures, FindingKind.WorkRegressed)).Append(" |\n");
        md.Append("| work improvements | ").Append(Count(notes, failures, FindingKind.WorkImproved)).Append(" |\n");
        md.Append("| plan changes | ").Append(Count(notes, failures, FindingKind.PlanChanged)).Append(" |\n");
        md.Append("| total work (baseline → now) | ").Append(baselineWork).Append(" → ").Append(currentWork).Append(" |\n");
        md.Append("| total statement rows read (baseline → now) | ").Append(baselineReads).Append(" → ").Append(currentReads).Append(" |\n");
        md.Append("| geometric mean of work ratios | ").Append(F(comparison.GeomeanWorkRatio, 3)).Append(" |\n\n");

        if (failures.Count > 0)
        {
            md.Append("## Failures\n\n");
            foreach (Finding f in failures)
                md.Append("- **").Append(f.Kind).Append("** `").Append(f.QueryId ?? "(run)").Append('`')
                  .Append(Tags(f.Tags)).Append(": ").Append(f.Message).Append('\n');
            md.Append('\n');
        }

        List<Finding> otherNotes = notes.Where(n => n.Kind != FindingKind.PlanChanged).ToList();
        if (otherNotes.Count > 0)
        {
            md.Append("## Notes (not failures)\n\n");
            foreach (Finding f in otherNotes)
                md.Append("- ").Append(f.Kind).Append(" `").Append(f.QueryId ?? "(run)").Append('`')
                  .Append(Tags(f.Tags)).Append(": ").Append(f.Message).Append('\n');
            md.Append('\n');
        }

        AppendWorkByTag(md, comparison);
        AppendPlanChanges(md, comparison, notes);
        AppendEstimates(md, run);
        AppendTimings(md, run);

        return md.ToString();
    }

    private static void AppendWorkByTag(StringBuilder md, ComparisonResult comparison)
    {
        if (comparison.Compared.Count == 0)
            return;

        md.Append("## Work by tag\n\n");
        md.Append("| tag | queries | work (baseline) | work (now) | geometric mean ratio |\n|---|---:|---:|---:|---:|\n");

        foreach (IGrouping<string, ComparedQuery> group in comparison.Compared
                     .SelectMany(c => c.Current.Tags.Select(tag => (tag, c)))
                     .GroupBy(x => x.tag, x => x.c, StringComparer.Ordinal)
                     .OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            md.Append("| ").Append(group.Key)
              .Append(" | ").Append(group.Count())
              .Append(" | ").Append(group.Sum(c => c.Baseline.Work))
              .Append(" | ").Append(group.Sum(c => c.Current.Work))
              .Append(" | ").Append(F(BaselineComparer.Geomean(group.Select(c => c.WorkRatio)), 3))
              .Append(" |\n");
        }

        md.Append('\n');
    }

    private static void AppendPlanChanges(StringBuilder md, ComparisonResult comparison, List<Finding> notes)
    {
        HashSet<string> changed = notes.Where(n => n.Kind == FindingKind.PlanChanged && n.QueryId is not null)
            .Select(n => n.QueryId!).ToHashSet(StringComparer.Ordinal);
        if (changed.Count == 0)
            return;

        md.Append("## Plan changes\n\n");
        foreach (ComparedQuery c in comparison.Compared.Where(c => changed.Contains(c.Current.Id)).OrderBy(c => c.Current.Id, StringComparer.Ordinal))
        {
            md.Append("### `").Append(c.Current.Id).Append('`').Append(Tags(c.Current.Tags))
              .Append(" — work ").Append(c.Baseline.Work).Append(" → ").Append(c.Current.Work)
              .Append(" (").Append(F(c.WorkRatio, 2)).Append("×)\n\n");
            md.Append("```diff\n");
            foreach (string line in c.Baseline.Plan)
                md.Append("- ").Append(line).Append('\n');
            foreach (string line in c.Current.Plan)
                md.Append("+ ").Append(line).Append('\n');
            md.Append("```\n\n");
        }
    }

    private static void AppendEstimates(StringBuilder md, IReadOnlyList<QueryMeasurement> run)
    {
        List<QueryMeasurement> worst = run.Where(m => m.Error is null).OrderByDescending(m => m.QErrorMax).ThenBy(m => m.Id, StringComparer.Ordinal).Take(10).ToList();
        if (worst.Count == 0)
            return;

        md.Append("## Worst estimates (q-error, not graded)\n\n");
        md.Append("| query | tags | median | max |\n|---|---|---:|---:|\n");
        foreach (QueryMeasurement m in worst)
            md.Append("| `").Append(m.Id).Append("` | ").Append(string.Join(", ", m.Tags))
              .Append(" | ").Append(F(m.QErrorMedian, 2)).Append(" | ").Append(F(m.QErrorMax, 2)).Append(" |\n");
        md.Append('\n');
    }

    private static void AppendTimings(StringBuilder md, IReadOnlyList<QueryMeasurement> run)
    {
        List<QueryMeasurement> ok = run.Where(m => m.Error is null).ToList();
        if (ok.Count == 0)
            return;

        md.Append("## Timings (not graded)\n\n");
        md.Append("Total planning time ").Append(F(ok.Sum(m => m.PlanningMs), 1))
          .Append(" ms, total execution time ").Append(F(ok.Sum(m => m.ExecutionMs), 1)).Append(" ms.\n\n");
        md.Append("| slowest queries | planning ms | execution ms | work | statement rows read | join steps costed |\n|---|---:|---:|---:|---:|---:|\n");
        foreach (QueryMeasurement m in ok.OrderByDescending(m => m.PlanningMs + m.ExecutionMs).ThenBy(m => m.Id, StringComparer.Ordinal).Take(10))
            md.Append("| `").Append(m.Id).Append("` | ").Append(F(m.PlanningMs, 2)).Append(" | ").Append(F(m.ExecutionMs, 2))
              .Append(" | ").Append(m.Work).Append(" | ").Append(m.StatementRowsRead).Append(" | ").Append(m.JoinStepsCosted).Append(" |\n");
        md.Append('\n');
    }

    private static int Count(List<Finding> notes, List<Finding> failures, FindingKind kind) =>
        notes.Count(n => n.Kind == kind) + failures.Count(f => f.Kind == kind);

    private static string Tags(IReadOnlyList<string> tags) => tags.Count == 0 ? "" : " (" + string.Join(", ", tags) + ")";

    private static string F(double value, int decimals) => value.ToString("F" + decimals, CultureInfo.InvariantCulture);
}
