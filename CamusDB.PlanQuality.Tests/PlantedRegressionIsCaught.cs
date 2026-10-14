/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

using NUnit.Framework;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Data;
using CamusDB.PlanQuality.Tests.Engine;
using CamusDB.PlanQuality.Tests.Harness;
using CamusDB.PlanQuality.Tests.Metrics;
using CamusDB.PlanQuality.Tests.Reports;

namespace CamusDB.PlanQuality.Tests;

/// <summary>
/// Proves that the gate can fail. It plants a known planner regression, an engine with the
/// cost-based join order off, runs the join queries where the declared order is a bad order, and
/// compares them with the committed baseline. The gate must report a work regression, and the
/// report must name the query and show the plan diff. A gate that passes this run is a gate that
/// passes everything.
///
/// <para>It reads the committed baseline and never writes it, also when the update switch is set.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("PlanQuality")]
public sealed class PlantedRegressionIsCaught
{
    // A selective filter far from the first declared table: the cost-based order starts there, the
    // declared order does not.
    private static readonly string[] JoinQueries =
    [
        "jc-7-selective-far-end", "jw-12-selective-far-end", "jc-5-comma-scrambled", "jw-10", "jw-12",
    ];

    [Test]
    public async Task DeclaredJoinOrder_FailsTheGateWithAReadableReport()
    {
        HarnessSettings settings = HarnessSettings.FromEnvironment() with { UpdateBaseline = false, Only = JoinQueries.ToHashSet(StringComparer.Ordinal) };
        string scale = "1";
        BaselineFile? baseline = BaselineFile.TryLoad(HarnessSettings.BaselinePath("small", scale));
        Assume.That(baseline, Is.Not.Null, "no committed small-tier baseline at scale 1");

        List<CorpusQuery> corpus = CorpusReader.ReadDirectory(HarnessSettings.CorpusDirectory("small"));

        await using PlanQualityEngine engine = await PlanQualityEngine.StartAsync(options => options with { CostBasedJoinOrderEnabled = false });
        await RetailDataSet.LoadAsync(engine, RetailDataSet.DefaultSeed, scale: 1);

        List<QueryMeasurement> run = await TierRunner.MeasureAsync(engine, corpus, settings);
        Assert.AreEqual(JoinQueries.Length, run.Count);

        BaselineFile identity = new("small", "retail", RetailDataSet.DefaultSeed.ToString(CultureInfo.InvariantCulture), scale, []);
        ComparisonResult comparison = BaselineComparer.Compare(baseline, identity, run, GateThresholds.Default, partialRun: true);

        List<Finding> regressions = comparison.Failures.Where(f => f.Kind == FindingKind.WorkRegressed).ToList();
        Assert.That(regressions, Is.Not.Empty, "the declared join order must regress at least one of: " + string.Join(", ", JoinQueries));
        Assert.That(comparison.Failures.Where(f => f.Kind == FindingKind.ResultChanged), Is.Empty,
            "a different join order must return the same rows");

        string report = PlanQualityReport.Render("planted regression", "small-sf1.json", run, comparison, settings);
        Assert.That(report, Does.Contain("**Gate: FAIL"));
        Assert.That(report, Does.Contain("`" + regressions[0].QueryId + "`"));
        Assert.That(report, Does.Contain("```diff"), "the report shows the old and the new plan");

        TestContext.Progress.WriteLine($"planted regression caught: {string.Join("; ", regressions.Select(r => r.QueryId + " " + r.Message))}");
    }
}
