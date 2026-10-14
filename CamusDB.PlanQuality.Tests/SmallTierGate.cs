/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;

using NUnit.Framework;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Data;
using CamusDB.PlanQuality.Tests.Engine;
using CamusDB.PlanQuality.Tests.Harness;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests;

/// <summary>
/// The small tier: the tagged corpus under <c>Corpus/small</c> on the seeded retail data set,
/// compared with <c>Baselines/small-sf{scale}.json</c>. CI runs it on every pull request at scale 1,
/// and the nightly workflow runs it at a larger scale. A failure message lists every failure; the
/// Markdown report next to the test output has the plan diffs.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("PlanQuality")]
public sealed class SmallTierGate
{
    [Test]
    public async Task SmallTier_MatchesTheBaseline()
    {
        HarnessSettings settings = HarnessSettings.FromEnvironment();
        List<CorpusQuery> corpus = CorpusReader.ReadDirectory(HarnessSettings.CorpusDirectory("small"));

        await using PlanQualityEngine engine = await PlanQualityEngine.StartAsync();

        Stopwatch load = Stopwatch.StartNew();
        await RetailDataSet.LoadAsync(engine, RetailDataSet.DefaultSeed, settings.Scale);
        TestContext.Progress.WriteLine(FormattableString.Invariant($"retail data set loaded at scale {settings.Scale} in {load.Elapsed.TotalSeconds:0.0} s"));

        Stopwatch measure = Stopwatch.StartNew();
        List<QueryMeasurement> run = await TierRunner.MeasureAsync(engine, corpus, settings);
        TestContext.Progress.WriteLine(FormattableString.Invariant($"{run.Count} queries measured in {measure.Elapsed.TotalSeconds:0.0} s"));

        string scale = settings.Scale.ToString(CultureInfo.InvariantCulture);
        BaselineFile identity = new("small", "retail", RetailDataSet.DefaultSeed.ToString(CultureInfo.InvariantCulture), scale, []);

        await TierRunner.GateAsync(
            $"Plan quality: small tier, scale {scale}",
            identity,
            HarnessSettings.BaselinePath("small", scale),
            engine,
            corpus,
            run,
            settings);
    }
}

/// <summary>Assembly setup: the same thread-pool floor as the main suite, so node start-up does not wait on thread injection.</summary>
[SetUpFixture]
public sealed class PlanQualityAssemblySetup
{
    [OneTimeSetUp]
    public void Configure()
    {
        ThreadPool.GetMinThreads(out int workerMin, out int completionMin);
        ThreadPool.SetMinThreads(Math.Max(workerMin, 32), completionMin);
    }
}
