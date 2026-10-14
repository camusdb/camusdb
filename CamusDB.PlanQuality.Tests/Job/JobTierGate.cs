/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Engine;
using CamusDB.PlanQuality.Tests.Harness;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Job;

/// <summary>
/// The large tier: the 113 Join Order Benchmark queries on synthetic IMDB data, compared with
/// <c>Baselines/job-sf{scale}.json</c> and, for correctness, with SQLite on the same rows.
///
/// <para>It needs a work directory from <c>scripts/plan-quality/job/prepare.sh</c>, named by
/// <c>JOB_DIR</c>, and the scale it was prepared at, in <c>JOB_SCALE</c> (the baseline of each scale
/// is its own file). Without <c>JOB_DIR</c> the test is skipped, so the pull-request job, which does
/// not download the benchmark, reports it as skipped and not as failed. The nightly workflow
/// prepares the directory and runs this test.</para>
///
/// <para>The JOB queries return one row of <c>MIN()</c> values each. A row that differs from SQLite
/// is a failure even when it matches the baseline: the baseline only says the result did not
/// change, the oracle says it is right.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("PlanQualityJob")]
public sealed class JobTierGate
{
    [Test]
    public async Task JobTier_MatchesTheBaselineAndSqlite()
    {
        string? workDirectory = Environment.GetEnvironmentVariable("JOB_DIR");
        if (string.IsNullOrWhiteSpace(workDirectory))
            Assert.Ignore("JOB_DIR is not set; prepare it with scripts/plan-quality/job/prepare.sh <dir> <scale>");

        string scale = Environment.GetEnvironmentVariable("JOB_SCALE")
            ?? throw new InvalidOperationException("JOB_SCALE must name the scale that JOB_DIR was prepared at, for example 0.001");

        HarnessSettings settings = HarnessSettings.FromEnvironment();
        JobDataSet dataSet = JobDataSet.Open(workDirectory);
        List<CorpusQuery> corpus = dataSet.Queries();
        Dictionary<string, List<string?>> oracle = dataSet.OracleRows();

        await using PlanQualityEngine engine = await PlanQualityEngine.StartAsync();

        Stopwatch load = Stopwatch.StartNew();
        await dataSet.LoadAsync(engine);
        TestContext.Progress.WriteLine(FormattableString.Invariant($"JOB data set loaded at scale {scale} in {load.Elapsed.TotalSeconds:0.0} s"));

        List<Finding> oracleFailures = new();
        void CheckOracle(CorpusQuery query, IReadOnlyList<QueryResultRow> rows)
        {
            if (!oracle.TryGetValue(query.Id, out List<string?>? expected))
            {
                oracleFailures.Add(new Finding(FindingKind.OracleMismatch, query.Id, query.Tags, "SQLite has no result for this query"));
                return;
            }

            List<string?> actual = rows.Count == 1 ? rows[0].Row.Values.Select(JobDataSet.OracleText).ToList() : [];
            if (rows.Count != 1 || !actual.SequenceEqual(expected))
                oracleFailures.Add(new Finding(FindingKind.OracleMismatch, query.Id, query.Tags,
                    $"CamusDB returned [{string.Join(", ", actual.Select(v => v ?? "NULL"))}] in {rows.Count} row(s), SQLite [{string.Join(", ", expected.Select(v => v ?? "NULL"))}]"));
        }

        Stopwatch measure = Stopwatch.StartNew();
        List<QueryMeasurement> run = await TierRunner.MeasureAsync(engine, corpus, settings, CheckOracle);
        TestContext.Progress.WriteLine(FormattableString.Invariant($"{run.Count} JOB queries measured in {measure.Elapsed.TotalSeconds:0.0} s"));

        BaselineFile identity = new("job", "job-synthetic-imdb", "gen_job.py", scale, []);

        await TierRunner.GateAsync(
            $"Plan quality: Join Order Benchmark, scale {scale}",
            identity,
            HarnessSettings.BaselinePath("job", scale),
            engine,
            corpus,
            run,
            settings,
            oracleFailures);
    }
}
