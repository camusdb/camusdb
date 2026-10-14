/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Baselines;

/// <summary>
/// The limits of the gate. Every value is a first guess, to tune after the harness has run for some
/// weeks; change them here only, with the reason next to the value.
/// </summary>
internal sealed record GateThresholds
{
    /// <summary>
    /// A query fails when its work grows past this factor. 1.5 lets a plan move a little (one more
    /// index probe, a different build side) and still catches the plan changes that matter: a lost
    /// index, a cross product, a re-read inner side.
    /// </summary>
    public double QueryWorkRatio { get; init; } = 1.5;

    /// <summary>
    /// ... and also by more than this absolute amount. A tiny query that goes from 3 to 6 reads is a
    /// 2× change but no real cost; the floor keeps such a query from failing the gate.
    /// </summary>
    public long QueryWorkFloor { get; init; } = 100;

    /// <summary>
    /// The run fails when the geometric mean of all work ratios grows past this factor. It catches a
    /// small slowdown spread over many queries that no single query shows.
    /// </summary>
    public double GeomeanWorkRatio { get; init; } = 1.05;

    /// <summary>
    /// The report lists a query when its maximum q-error moves by more than this factor. It is not a
    /// failure: an estimate change matters through the plan it causes, and the work gate sees that.
    /// </summary>
    public double QErrorReportRatio { get; init; } = 2.0;

    public static GateThresholds Default { get; } = new();
}

/// <summary>What the comparer found. A kind is a failure or a note; see <see cref="Finding.IsFailure"/>.</summary>
internal enum FindingKind
{
    /// <summary>The query failed: an error or a timeout.</summary>
    QueryFailed,

    /// <summary>The result is different from the baseline.</summary>
    ResultChanged,

    /// <summary>The result is different from an independent oracle (SQLite for the JOB tier).</summary>
    OracleMismatch,

    /// <summary>The work grew past both limits of <see cref="GateThresholds"/>.</summary>
    WorkRegressed,

    /// <summary>The rows the whole statement read grew past the work limits. It catches a subquery that the plan nodes do not count.</summary>
    StatementReadsRegressed,

    /// <summary>The join-order search costed many more steps, with the same limits as the work.</summary>
    JoinStepsRegressed,

    /// <summary>The geometric mean of all work ratios grew past its limit.</summary>
    GeomeanRegressed,

    /// <summary>The corpus has a query that the baseline does not have.</summary>
    NewQuery,

    /// <summary>The baseline has a query that the corpus does not have.</summary>
    MissingQuery,

    /// <summary>The baseline is for a different data set, seed or scale.</summary>
    BaselineMismatch,

    /// <summary>The work went down past both limits. A note: update the baseline to keep the gain.</summary>
    WorkImproved,

    /// <summary>The plan fingerprint changed. A note: the work gate decides if the change is a regression.</summary>
    PlanChanged,

    /// <summary>The join order moved between cost-based and heuristic.</summary>
    JoinOrderPathChanged,

    /// <summary>The maximum q-error moved past <see cref="GateThresholds.QErrorReportRatio"/>.</summary>
    QErrorChanged,
}

internal sealed record Finding(FindingKind Kind, string? QueryId, IReadOnlyList<string> Tags, string Message)
{
    public bool IsFailure => Kind is FindingKind.QueryFailed or FindingKind.ResultChanged or FindingKind.OracleMismatch or FindingKind.WorkRegressed
        or FindingKind.StatementReadsRegressed or FindingKind.JoinStepsRegressed or FindingKind.GeomeanRegressed or FindingKind.NewQuery
        or FindingKind.MissingQuery or FindingKind.BaselineMismatch;
}

/// <summary>One query in both the baseline and the run, for the report.</summary>
internal sealed record ComparedQuery(BaselineEntry Baseline, QueryMeasurement Current)
{
    public double WorkRatio => Math.Max(1, Current.Work) / (double)Math.Max(1, Baseline.Work);
}

internal sealed record ComparisonResult(
    IReadOnlyList<Finding> Findings,
    IReadOnlyList<ComparedQuery> Compared,
    double GeomeanWorkRatio)
{
    public IEnumerable<Finding> Failures => Findings.Where(f => f.IsFailure);
}

/// <summary>
/// Compares a run with its baseline and lists the findings, failures first. It never reads a timing:
/// the gate uses only the numbers that repeat between runs.
/// </summary>
internal static class BaselineComparer
{
    public static ComparisonResult Compare(
        BaselineFile? baseline,
        BaselineFile expectedIdentity,
        IReadOnlyList<QueryMeasurement> run,
        GateThresholds thresholds,
        bool partialRun = false)
    {
        List<Finding> findings = new();
        List<ComparedQuery> compared = new();

        foreach (QueryMeasurement failed in run.Where(m => m.Error is not null))
            findings.Add(new Finding(FindingKind.QueryFailed, failed.Id, failed.Tags, failed.Error!));

        if (baseline is null)
        {
            findings.Add(new Finding(FindingKind.BaselineMismatch, null, [],
                "there is no baseline file for this tier and scale; run once with PLAN_QUALITY_UPDATE_BASELINE=1 to create it"));
            return new ComparisonResult(Sorted(findings), compared, 1);
        }

        if (baseline.DataSet != expectedIdentity.DataSet || baseline.Seed != expectedIdentity.Seed || baseline.Scale != expectedIdentity.Scale)
        {
            findings.Add(new Finding(FindingKind.BaselineMismatch, null, [],
                $"the baseline is for data set {baseline.DataSet}, seed {baseline.Seed}, scale {baseline.Scale}, " +
                $"but the run used {expectedIdentity.DataSet}, seed {expectedIdentity.Seed}, scale {expectedIdentity.Scale}"));
            return new ComparisonResult(Sorted(findings), compared, 1);
        }

        Dictionary<string, BaselineEntry> byId = baseline.Queries.ToDictionary(q => q.Id, StringComparer.Ordinal);
        HashSet<string> seen = new(StringComparer.Ordinal);

        foreach (QueryMeasurement current in run)
        {
            seen.Add(current.Id);

            if (!byId.TryGetValue(current.Id, out BaselineEntry? previous))
            {
                if (current.Error is null)
                    findings.Add(new Finding(FindingKind.NewQuery, current.Id, current.Tags,
                        "the query is not in the baseline; run with PLAN_QUALITY_UPDATE_BASELINE=1 to add it"));
                continue;
            }

            if (current.Error is not null)
                continue;

            compared.Add(new ComparedQuery(previous, current));
            CompareOne(previous, current, thresholds, findings);
        }

        if (!partialRun)
        {
            foreach (BaselineEntry missing in baseline.Queries.Where(q => !seen.Contains(q.Id)))
                findings.Add(new Finding(FindingKind.MissingQuery, missing.Id, missing.Tags,
                    "the baseline has this query but the corpus does not; run with PLAN_QUALITY_UPDATE_BASELINE=1 to remove it"));
        }

        double geomean = Geomean(compared.Select(c => c.WorkRatio));
        if (geomean > thresholds.GeomeanWorkRatio)
            findings.Add(new Finding(FindingKind.GeomeanRegressed, null, [],
                $"the geometric mean of the work ratios is {F(geomean, 3)}, over the limit of {F(thresholds.GeomeanWorkRatio, 3)}"));

        return new ComparisonResult(Sorted(findings), compared, geomean);
    }

    private static void CompareOne(BaselineEntry previous, QueryMeasurement current, GateThresholds thresholds, List<Finding> findings)
    {
        if (previous.ResultHash != current.ResultHash || previous.Rows != current.ResultRows)
            findings.Add(new Finding(FindingKind.ResultChanged, current.Id, current.Tags,
                $"the result changed: {previous.Rows} rows (hash {previous.ResultHash}) before, {current.ResultRows} rows (hash {current.ResultHash}) now"));

        if (Grew(previous.Work, current.Work, thresholds))
            findings.Add(new Finding(FindingKind.WorkRegressed, current.Id, current.Tags,
                $"work {previous.Work} → {current.Work} ({F(Ratio(previous.Work, current.Work), 2)}×)"));
        else if (Grew(current.Work, previous.Work, thresholds))
            findings.Add(new Finding(FindingKind.WorkImproved, current.Id, current.Tags,
                $"work {previous.Work} → {current.Work} ({F(Ratio(previous.Work, current.Work), 2)}×)"));

        if (Grew(previous.StatementRowsRead, current.StatementRowsRead, thresholds))
            findings.Add(new Finding(FindingKind.StatementReadsRegressed, current.Id, current.Tags,
                $"statement rows read {previous.StatementRowsRead} → {current.StatementRowsRead} ({F(Ratio(previous.StatementRowsRead, current.StatementRowsRead), 2)}×)"));

        if (Grew(previous.JoinStepsCosted, current.JoinStepsCosted, thresholds))
            findings.Add(new Finding(FindingKind.JoinStepsRegressed, current.Id, current.Tags,
                $"join steps costed {previous.JoinStepsCosted} → {current.JoinStepsCosted}"));

        if (!previous.Plan.SequenceEqual(current.Plan, StringComparer.Ordinal))
            findings.Add(new Finding(FindingKind.PlanChanged, current.Id, current.Tags,
                $"the plan changed (work {previous.Work} → {current.Work})"));

        if (!string.Equals(previous.JoinOrder, current.JoinOrder, StringComparison.Ordinal))
            findings.Add(new Finding(FindingKind.JoinOrderPathChanged, current.Id, current.Tags,
                $"join order {previous.JoinOrder ?? "none"} → {current.JoinOrder ?? "none"}"));

        double qRatio = Math.Max(previous.QErrorMax, current.QErrorMax) / Math.Max(1, Math.Min(previous.QErrorMax, current.QErrorMax));
        if (qRatio > thresholds.QErrorReportRatio)
            findings.Add(new Finding(FindingKind.QErrorChanged, current.Id, current.Tags,
                $"maximum q-error {F(previous.QErrorMax, 2)} → {F(current.QErrorMax, 2)}"));
    }

    /// <summary>True when <paramref name="after"/> is past both the ratio and the floor over <paramref name="before"/>.</summary>
    private static bool Grew(long before, long after, GateThresholds thresholds) =>
        after - before > thresholds.QueryWorkFloor && Ratio(before, after) > thresholds.QueryWorkRatio;

    private static double Ratio(long before, long after) => Math.Max(1, after) / (double)Math.Max(1, before);

    public static double Geomean(IEnumerable<double> ratios)
    {
        double sum = 0;
        int count = 0;
        foreach (double ratio in ratios)
        {
            sum += Math.Log(ratio);
            count++;
        }
        return count == 0 ? 1 : Math.Exp(sum / count);
    }

    private static List<Finding> Sorted(List<Finding> findings) =>
        findings.OrderBy(f => f.IsFailure ? 0 : 1).ThenBy(f => f.Kind).ThenBy(f => f.QueryId, StringComparer.Ordinal).ToList();

    private static string F(double value, int decimals) => value.ToString("F" + decimals, CultureInfo.InvariantCulture);
}
