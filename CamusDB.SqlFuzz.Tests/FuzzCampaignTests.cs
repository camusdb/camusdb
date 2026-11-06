/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.SqlFuzz.Tests.Campaign;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests;

/// <summary>
/// The fuzz campaigns: one test per oracle, each with the time budget of <c>SQLFUZZ_BUDGET_SECONDS</c>
/// and the seed of <c>SQLFUZZ_SEED</c> (see <see cref="FuzzSettings"/>). A test fails when its oracle
/// found a disagreement, or when its checks did not compare enough (see <see cref="MinimumCompletedShare"/>);
/// the report names the seed, and the output folder holds the original and the shrunk statements and a
/// regression test for each distinct finding.
///
/// <para>The campaigns run one at a time: each boots its own embedded node, and the statement time
/// limit of the hang oracle is not reliable on a machine that runs other suites at the same time.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("SqlFuzz")]
internal sealed class FuzzCampaignTests
{
    /// <summary>The smallest share of checks that must complete a comparison (pass or fail) for a campaign to count.</summary>
    private const double MinimumCompletedShare = 0.5;

    /// <summary>A variant with at least this many attempts must complete at least one comparison.</summary>
    private const int MinimumAttemptsToJudge = 20;

    [Test]
    public Task ErrorClassification() => RunAsync(OracleKind.ErrorClassification);

    [Test]
    public Task Tlp() => RunAsync(OracleKind.Tlp);

    [Test]
    public Task NoRec() => RunAsync(OracleKind.NoRec);

    [Test]
    public Task PlanEquivalence() => RunAsync(OracleKind.PlanEquivalence);

    [Test]
    public Task DmlIntegrity() => RunAsync(OracleKind.DmlIntegrity);

    private static async Task RunAsync(OracleKind kind)
    {
        FuzzSettings settings = FuzzSettings.FromEnvironment(Path.Combine(TestContext.CurrentContext.WorkDirectory, "sqlfuzz-findings"));
        if (settings.ReplayOracle is { } only && only != kind)
            Assert.Ignore($"SQLFUZZ_ORACLE selects {only}");

        FuzzCampaign campaign = new(settings, log: line => TestContext.Progress.WriteLine(line));
        CampaignResult result = await campaign.RunAsync(kind);

        TestContext.Progress.WriteLine(result.Report);
        Directory.CreateDirectory(settings.OutputDirectory);
        await File.WriteAllTextAsync(Path.Combine(settings.OutputDirectory, $"summary-{FuzzSettings.OracleName(kind)}.txt"), result.Report, FindingWriter.Utf8);

        Assert.That(result.Stats.Checks, Is.GreaterThan(0), "the campaign ran no check");
        Assert.That(result.DistinctSignatures, Is.Zero, result.Report);

        // A skipped check compared nothing, so a campaign of skips proves nothing. Outside a replay of one
        // round: at least half of the checks completed a comparison, and so did every variant with
        // enough attempts to judge it.
        if (settings.ReplayRound is null)
        {
            Assert.That(result.Stats.Completed, Is.GreaterThan(0), "no check completed a comparison\n" + result.Report);
            Assert.That((double)result.Stats.Completed / result.Stats.Checks, Is.GreaterThanOrEqualTo(MinimumCompletedShare),
                $"only {result.Stats.Completed} of {result.Stats.Checks} checks completed a comparison\n" + result.Report);
            List<string> idle = result.Stats.Variants.Where(v => v.Attempts >= MinimumAttemptsToJudge && v.Completed == 0).Select(v => v.Variant).ToList();
            Assert.That(idle, Is.Empty, "variants that never completed a comparison\n" + result.Report);
        }
    }
}
