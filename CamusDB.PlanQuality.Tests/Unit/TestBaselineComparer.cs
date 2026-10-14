/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Unit;

/// <summary>Each rule of the gate, on synthetic measurements.</summary>
[TestFixture]
[Category("PlanQuality")]
public sealed class TestBaselineComparer
{
    private static readonly BaselineFile Identity = new("small", "retail", "1", "1", []);

    private static QueryMeasurement M(
        string id,
        long work = 1000,
        string hash = "h",
        long rows = 1,
        long statementRows = 500,
        long steps = 10,
        string? joinOrder = "cost-based",
        double qMax = 2,
        string[]? plan = null,
        string? error = null) =>
        new(id, ["tag"], rows, hash, work, statementRows, 1.5, qMax, joinOrder, steps,
            plan ?? ["table-scan(table=t)"], 1.0, 2.0, [], error);

    private static BaselineFile Baseline(params QueryMeasurement[] queries) =>
        BaselineFile.Create("small", "retail", "1", "1", queries);

    private static List<FindingKind> Kinds(ComparisonResult result) => result.Findings.Select(f => f.Kind).ToList();

    private static ComparisonResult Compare(BaselineFile? baseline, params QueryMeasurement[] run) =>
        BaselineComparer.Compare(baseline, Identity, run, GateThresholds.Default);

    [Test]
    public void SameNumbers_NoFinding()
    {
        ComparisonResult result = Compare(Baseline(M("a"), M("b")), M("a"), M("b"));

        Assert.That(result.Findings, Is.Empty);
        Assert.AreEqual(1.0, result.GeomeanWorkRatio, 1e-9);
    }

    [Test]
    public void NoBaseline_Fails()
    {
        Assert.That(Kinds(Compare(null, M("a"))), Is.EqualTo(new[] { FindingKind.BaselineMismatch }));
    }

    [Test]
    public void BaselineForAnotherScale_Fails()
    {
        BaselineFile other = BaselineFile.Create("small", "retail", "1", "4", [M("a")]);

        Assert.That(Kinds(Compare(other, M("a"))), Is.EqualTo(new[] { FindingKind.BaselineMismatch }));
    }

    [Test]
    public void ChangedResult_Fails()
    {
        ComparisonResult result = Compare(Baseline(M("a")), M("a", hash: "other"));

        Assert.That(result.Failures.Select(f => f.Kind), Is.EqualTo(new[] { FindingKind.ResultChanged }));
    }

    [Test]
    public void WorkOverRatioAndFloor_Fails()
    {
        ComparisonResult result = Compare(Baseline(M("a", work: 1000)), M("a", work: 1600));

        Assert.That(result.Failures.Select(f => f.Kind), Does.Contain(FindingKind.WorkRegressed));
    }

    [Test]
    public void WorkOverRatioButUnderFloor_DoesNotFailTheQuery()
    {
        // 3 → 60 is 20× but only 57 more reads, under the floor of 100. Among 63 unchanged queries the
        // one change also keeps the geometric mean under its limit, so the run passes.
        List<QueryMeasurement> before = Enumerable.Range(0, 63).Select(i => i == 0 ? M("q0", work: 3) : M("q" + i)).ToList();
        List<QueryMeasurement> after = Enumerable.Range(0, 63).Select(i => i == 0 ? M("q0", work: 60) : M("q" + i)).ToList();

        ComparisonResult result = BaselineComparer.Compare(Baseline([.. before]), Identity, after, GateThresholds.Default);

        Assert.That(result.Failures, Is.Empty);
        Assert.That(result.GeomeanWorkRatio, Is.GreaterThan(1.0).And.LessThan(GateThresholds.Default.GeomeanWorkRatio));
    }

    [Test]
    public void WorkUnderRatio_DoesNotFailTheQueryButTheGeomeanDoes()
    {
        // 1.4× on every query: no query is over 1.5×, but the whole run is 40% slower.
        ComparisonResult result = Compare(Baseline(M("a"), M("b")), M("a", work: 1400), M("b", work: 1400));

        Assert.That(result.Failures.Select(f => f.Kind), Is.EqualTo(new[] { FindingKind.GeomeanRegressed }));
        Assert.AreEqual(1.4, result.GeomeanWorkRatio, 1e-9);
    }

    [Test]
    public void LessWork_IsANoteNotAFailure()
    {
        ComparisonResult result = Compare(Baseline(M("a", work: 5000)), M("a", work: 1000));

        Assert.That(result.Failures, Is.Empty);
        Assert.That(Kinds(result), Does.Contain(FindingKind.WorkImproved));
    }

    [Test]
    public void StatementReadsOverTheLimits_Fail()
    {
        ComparisonResult result = Compare(Baseline(M("a", statementRows: 500)), M("a", statementRows: 50_000));

        Assert.That(result.Failures.Select(f => f.Kind), Is.EqualTo(new[] { FindingKind.StatementReadsRegressed }));
    }

    [Test]
    public void MoreJoinStepsCosted_Fail()
    {
        ComparisonResult result = Compare(Baseline(M("a", steps: 800)), M("a", steps: 4000));

        Assert.That(result.Failures.Select(f => f.Kind), Is.EqualTo(new[] { FindingKind.JoinStepsRegressed }));
    }

    [Test]
    public void ChangedPlanWithSameWork_IsANote()
    {
        ComparisonResult result = Compare(Baseline(M("a")), M("a", plan: ["index-range-scan(index=i)"]));

        Assert.That(result.Failures, Is.Empty);
        Assert.That(Kinds(result), Is.EqualTo(new[] { FindingKind.PlanChanged }));
    }

    [Test]
    public void JoinOrderPathAndQError_AreNotes()
    {
        ComparisonResult result = Compare(Baseline(M("a", qMax: 2)), M("a", joinOrder: "heuristic", qMax: 50));

        Assert.That(result.Failures, Is.Empty);
        Assert.That(Kinds(result), Is.EquivalentTo(new[] { FindingKind.JoinOrderPathChanged, FindingKind.QErrorChanged }));
    }

    [Test]
    public void NewQuery_Fails()
    {
        ComparisonResult result = Compare(Baseline(M("a")), M("a"), M("b"));

        Assert.That(result.Failures.Select(f => (f.Kind, f.QueryId)), Is.EqualTo(new[] { (FindingKind.NewQuery, (string?)"b") }));
    }

    [Test]
    public void RemovedQuery_Fails_ExceptInAPartialRun()
    {
        Assert.That(Compare(Baseline(M("a"), M("b")), M("a")).Failures.Select(f => f.Kind),
            Is.EqualTo(new[] { FindingKind.MissingQuery }));

        ComparisonResult partial = BaselineComparer.Compare(Baseline(M("a"), M("b")), Identity, [M("a")], GateThresholds.Default, partialRun: true);
        Assert.That(partial.Findings, Is.Empty);
    }

    [Test]
    public void FailedQuery_Fails()
    {
        ComparisonResult result = Compare(Baseline(M("a")), M("a", error: "timed out"));

        Assert.That(result.Failures.Select(f => f.Kind), Is.EqualTo(new[] { FindingKind.QueryFailed }));
    }

    [Test]
    public void FailuresComeBeforeNotes()
    {
        ComparisonResult result = Compare(Baseline(M("a"), M("b", work: 5000)),
            M("a", hash: "x"), M("b", work: 1000));

        Assert.IsTrue(result.Findings[0].IsFailure);
        Assert.IsFalse(result.Findings[^1].IsFailure);
    }
}
