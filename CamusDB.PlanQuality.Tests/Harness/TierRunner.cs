/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Engine;
using CamusDB.PlanQuality.Tests.Estimates;
using CamusDB.PlanQuality.Tests.Metrics;
using CamusDB.PlanQuality.Tests.Reports;

namespace CamusDB.PlanQuality.Tests.Harness;

/// <summary>
/// Runs a corpus on a loaded engine and applies the gate. Shared by the small tier and the JOB tier,
/// so both tiers measure, compare, report and update in the same way.
/// </summary>
internal static class TierRunner
{
    /// <summary>
    /// Measures each query: one <c>EXPLAIN (ANALYZE)</c> for the plan and its counters, then one plain
    /// run for the result. A query that fails or passes <see cref="HarnessSettings.QueryTimeout"/> is
    /// recorded as failed, and the run goes on with the next query. <paramref name="onResult"/>, when
    /// given, sees the result rows of each query that ran, for a tier that checks them against an
    /// oracle.
    /// </summary>
    public static async Task<List<QueryMeasurement>> MeasureAsync(
        PlanQualityEngine engine,
        IReadOnlyList<CorpusQuery> corpus,
        HarnessSettings settings,
        Action<CorpusQuery, IReadOnlyList<QueryResultRow>>? onResult = null)
    {
        List<QueryMeasurement> measurements = new(corpus.Count);

        foreach (CorpusQuery query in corpus)
        {
            if (settings.Only is not null && !settings.Only.Contains(query.Id))
                continue;

            measurements.Add(await MeasureOneAsync(engine, query, settings.QueryTimeout, onResult).ConfigureAwait(false));
        }

        return measurements;
    }

    public static async Task<QueryMeasurement> MeasureOneAsync(
        PlanQualityEngine engine,
        CorpusQuery query,
        TimeSpan timeout,
        Action<CorpusQuery, IReadOnlyList<QueryResultRow>>? onResult = null)
    {
        using CancellationTokenSource cts = new(timeout);
        try
        {
            List<QueryResultRow> analyze = await engine.QueryAsync("EXPLAIN (ANALYZE) " + query.Sql, cts.Token).ConfigureAwait(false);
            List<QueryResultRow> result = await engine.QueryAsync(query.Sql, cts.Token).ConfigureAwait(false);
            long? statementRowsRead = await engine.StatementRowsReadAsync(query.Sql, cts.Token).ConfigureAwait(false);
            if (statementRowsRead is null)
                return QueryMeasurement.Failed(query.Id, query.Tags, $"the slow query log has no entry for the query ({query.Source})");

            onResult?.Invoke(query, result);

            return PlanMeasures.Build(query, analyze, result, statementRowsRead.Value);
        }
        catch (Exception ex) when (cts.IsCancellationRequested)
        {
            return QueryMeasurement.Failed(query.Id, query.Tags, $"timed out after {timeout.TotalSeconds:0} s ({ex.GetType().Name})");
        }
        catch (Exception ex)
        {
            return QueryMeasurement.Failed(query.Id, query.Tags, $"{ex.GetType().Name}: {ex.Message} ({query.Source})");
        }
    }

    /// <summary>
    /// Runs each query on the reference engine (both cost-based switches off) and returns a message
    /// for each query whose result is different from the run under test. Only a baseline update runs
    /// this check: it is slow, because the reference engine keeps the declared join order.
    /// </summary>
    public static async Task<List<string>> ReferenceMismatchesAsync(
        PlanQualityEngine engine, IReadOnlyList<CorpusQuery> corpus, IReadOnlyList<QueryMeasurement> run, HarnessSettings settings)
    {
        (CommandExecutor reference, _) = await engine.CreateEngineAsync(options => options with
        {
            CostBasedJoinOrderEnabled = false,
            CostBasedAccessPathEnabled = false,
        }).ConfigureAwait(false);

        Dictionary<string, QueryMeasurement> byId = run.ToDictionary(m => m.Id, StringComparer.Ordinal);
        List<string> mismatches = new();

        foreach (CorpusQuery query in corpus)
        {
            if (!byId.TryGetValue(query.Id, out QueryMeasurement? measured) || measured.Error is not null)
                continue;

            using CancellationTokenSource cts = new(settings.QueryTimeout * 4);
            try
            {
                List<QueryResultRow> rows = await PlanQualityEngine.QueryAsync(reference, engine.DatabaseName, query.Sql, cts.Token)
                    .ConfigureAwait(false);
                (string hash, long count) = PlanMeasures.ResultHash(rows);

                if (hash != measured.ResultHash || count != measured.ResultRows)
                    mismatches.Add($"{query.Id}: the reference engine returned {count} rows (hash {hash}), the engine under test {measured.ResultRows} rows (hash {measured.ResultHash})");
            }
            catch (Exception ex)
            {
                mismatches.Add($"{query.Id}: the reference engine failed: {ex.GetType().Name}: {ex.Message}");
            }
        }

        return mismatches;
    }

    /// <summary>
    /// Compares the run with the baseline, writes the report, and either updates the baseline or
    /// asserts that the gate passed. The NUnit assertion message holds the failures, so the CI log
    /// shows them without the report.
    /// </summary>
    public static async Task GateAsync(
        string title,
        BaselineFile identity,
        string baselinePath,
        PlanQualityEngine engine,
        IReadOnlyList<CorpusQuery> corpus,
        List<QueryMeasurement> run,
        HarnessSettings settings,
        IReadOnlyList<Finding>? extraFailures = null)
    {
        bool partial = settings.Only is not null;
        BaselineFile? baseline = BaselineFile.TryLoad(baselinePath);
        ComparisonResult comparison = BaselineComparer.Compare(baseline, identity, run, GateThresholds.Default, partial);

        List<Finding> findings = comparison.Findings.ToList();
        if (extraFailures is not null)
            findings.InsertRange(0, extraFailures);
        comparison = comparison with { Findings = findings };

        // The estimate records: one for each node with an estimate and an actual count, with the
        // trace of the estimate. They go next to the run, never into the repository.
        List<EstimateRecord> estimates = TrainingSet.FromRun(run, identity.Tier, identity.Scale);

        string report = PlanQualityReport.Render(title, baselinePath, run, comparison, settings, estimates);
        Directory.CreateDirectory(settings.ReportDirectory);
        string stem = Path.GetFileNameWithoutExtension(baselinePath);
        string reportPath = Path.Combine(settings.ReportDirectory, stem + "-report.md");
        await File.WriteAllTextAsync(reportPath, report).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path.Combine(settings.ReportDirectory, stem + "-run.json"),
            JsonSerializer.Serialize(run, new JsonSerializerOptions { WriteIndented = true })).ConfigureAwait(false);
        TrainingSet.Save(estimates, Path.Combine(settings.ReportDirectory, stem + "-estimates.json"));

        TrainingSet.Summary estimateSummary = TrainingSet.Summarize(estimates);
        TestContext.Progress.WriteLine(FormattableString.Invariant(
            $"estimates: {estimateSummary.Nodes} nodes, {estimateSummary.Over100} with q-error > 100, median {estimateSummary.Median:0.00}, p95 {estimateSummary.P95:0.0}, unknown rule {estimateSummary.UnknownShare:P1}"));

        TestContext.Progress.WriteLine($"plan-quality report: {reportPath}");
        TestContext.AddTestAttachment(reportPath);

        List<string> failures = comparison.Failures.Select(f => $"[{f.Kind}] {f.QueryId ?? "(run)"}: {f.Message}").ToList();

        if (settings.UpdateBaseline)
        {
            if (partial)
                Assert.Fail("PLAN_QUALITY_UPDATE_BASELINE=1 needs the whole corpus; unset PLAN_QUALITY_ONLY");

            List<string> errors = run.Where(m => m.Error is not null).Select(m => $"{m.Id}: {m.Error}").ToList();
            if (errors.Count > 0)
                Assert.Fail("the baseline was not written, because these queries failed:\n" + string.Join("\n", errors));

            if (extraFailures is { Count: > 0 })
                Assert.Fail("the baseline was not written:\n" + string.Join("\n", extraFailures.Select(f => $"{f.QueryId}: {f.Message}")));

            Stopwatch referenceTime = Stopwatch.StartNew();
            List<string> mismatches = await ReferenceMismatchesAsync(engine, corpus, run, settings).ConfigureAwait(false);
            TestContext.Progress.WriteLine(FormattableString.Invariant($"reference check: {run.Count} queries in {referenceTime.Elapsed.TotalSeconds:0.0} s"));
            if (mismatches.Count > 0)
                Assert.Fail("the baseline was not written, because the reference engine disagrees:\n" + string.Join("\n", mismatches));

            BaselineFile updated = BaselineFile.Create(identity.Tier, identity.DataSet, identity.Seed, identity.Scale, run);
            updated.Save(baselinePath);
            TestContext.Progress.WriteLine($"baseline written: {baselinePath} ({updated.Queries.Count} queries)");
            return;
        }

        if (failures.Count > 0)
            Assert.Fail($"{failures.Count} plan-quality failure(s); report: {reportPath}\n" + string.Join("\n", failures));
    }
}
