/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Data;
using CamusDB.PlanQuality.Tests.Engine;
using CamusDB.PlanQuality.Tests.Harness;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests;

/// <summary>
/// The gate is only useful if the counters it compares repeat: the same plan on the same data must
/// give the same numbers on every run and on every engine. This fixture proves it for a sample of
/// the corpus that covers each shape group. It builds two engines independently, each with its own
/// node and its own load of the seeded data, runs the sample twice on the first engine and once on
/// the second, and compares everything except the timings.
///
/// <para>It also compares the statistics, through <c>SHOW STATISTICS</c> without the analysis time.
/// The explicit <c>ANALYZE</c> reads each table in key order up to its row limit, and every table
/// here is under that limit, so the statistics repeat. Auto-analyze does not: its reservoir sample
/// is seeded by the time of the run, which is why the harness turns it off.</para>
///
/// <para>Known residual risk: a row id is a timestamp, a per-process value and a counter that starts
/// at a random value. If that counter wraps inside one second during a load, two rows change order
/// in a full scan. No counter in the corpus depends on scan order (every ordered limit breaks ties
/// on a unique column), so the risk is limited to a plan whose early exit depends on it.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("PlanQuality")]
public sealed class PlanCountersRepeat
{
    [Test]
    public async Task Counters_RepeatBetweenRunsAndBetweenEngines()
    {
        HarnessSettings settings = HarnessSettings.FromEnvironment();
        List<CorpusQuery> corpus = CorpusReader.ReadDirectory(HarnessSettings.CorpusDirectory("small"));

        // Every third query: each file contributes, so each shape group is in the sample.
        List<CorpusQuery> sample = corpus.Where((_, index) => index % 3 == 0).ToList();

        List<QueryMeasurement> firstRun;
        List<QueryMeasurement> secondRun;
        List<QueryMeasurement> otherEngine;
        List<string> firstStatistics;
        List<string> otherStatistics;

        await using (PlanQualityEngine first = await PlanQualityEngine.StartAsync())
        {
            await RetailDataSet.LoadAsync(first, RetailDataSet.DefaultSeed, scale: 1);
            firstStatistics = await StatisticsAsync(first);
            firstRun = await MeasureAllAsync(first, sample, settings);
            secondRun = await MeasureAllAsync(first, sample, settings);
        }

        await using (PlanQualityEngine other = await PlanQualityEngine.StartAsync())
        {
            await RetailDataSet.LoadAsync(other, RetailDataSet.DefaultSeed, scale: 1);
            otherStatistics = await StatisticsAsync(other);
            otherEngine = await MeasureAllAsync(other, sample, settings);
        }

        List<string> differences = new();

        Assert.That(firstRun.Where(m => m.Error is not null).Select(m => $"{m.Id}: {m.Error}"), Is.Empty, "a sample query failed");

        for (int i = 0; i < firstStatistics.Count; i++)
        {
            if (firstStatistics[i] != otherStatistics[i])
                differences.Add($"statistics differ between engines:\n  {firstStatistics[i]}\n  {otherStatistics[i]}");
        }

        for (int i = 0; i < sample.Count; i++)
        {
            Compare("second run", firstRun[i], secondRun[i], differences);
            Compare("other engine", firstRun[i], otherEngine[i], differences);
        }

        Assert.That(differences, Is.Empty, string.Join("\n", differences));
        TestContext.Progress.WriteLine($"{sample.Count} queries and {firstStatistics.Count} statistics rows repeat over two runs and two engines");
    }

    private static async Task<List<QueryMeasurement>> MeasureAllAsync(PlanQualityEngine engine, List<CorpusQuery> queries, HarnessSettings settings)
    {
        List<QueryMeasurement> measurements = new(queries.Count);
        foreach (CorpusQuery query in queries)
            measurements.Add(await TierRunner.MeasureOneAsync(engine, query, settings.QueryTimeout));
        return measurements;
    }

    /// <summary>Every statistics row of every table, without <c>last_analyzed</c>, which is a time.</summary>
    private static async Task<List<string>> StatisticsAsync(PlanQualityEngine engine)
    {
        List<string> lines = new();
        foreach (string table in RetailDataSet.Tables)
        {
            List<QueryResultRow> rows = await engine.QueryAsync("SHOW STATISTICS FOR " + table, CancellationToken.None);
            foreach (QueryResultRow row in rows)
            {
                StringBuilder line = new(table);
                foreach (KeyValuePair<string, ColumnValue> cell in row.Row.OrderBy(c => c.Key, StringComparer.Ordinal))
                {
                    if (cell.Key == "last_analyzed")
                        continue;
                    line.Append(' ').Append(cell.Key).Append('=').Append(PlanMeasures.Render(cell.Value));
                }
                lines.Add(line.ToString());
            }
        }
        return lines;
    }

    private static void Compare(string what, QueryMeasurement expected, QueryMeasurement actual, List<string> differences)
    {
        void Check<T>(string field, T a, T b)
        {
            if (!EqualityComparer<T>.Default.Equals(a, b))
                differences.Add($"{expected.Id} ({what}): {field} {a} != {b}");
        }

        Check("error", expected.Error, actual.Error);
        Check("result hash", expected.ResultHash, actual.ResultHash);
        Check("result rows", expected.ResultRows, actual.ResultRows);
        Check("work", expected.Work, actual.Work);
        Check("statement rows read", expected.StatementRowsRead, actual.StatementRowsRead);
        Check("join order", expected.JoinOrder, actual.JoinOrder);
        Check("join steps costed", expected.JoinStepsCosted, actual.JoinStepsCosted);
        Check("q-error median", expected.QErrorMedian, actual.QErrorMedian);
        Check("q-error max", expected.QErrorMax, actual.QErrorMax);
        Check("plan", string.Join(" | ", expected.Plan), string.Join(" | ", actual.Plan));
        Check("node count", expected.Nodes.Count, actual.Nodes.Count);

        for (int n = 0; n < Math.Min(expected.Nodes.Count, actual.Nodes.Count); n++)
        {
            PlanNodeMeasure a = expected.Nodes[n];
            PlanNodeMeasure b = actual.Nodes[n];
            Check($"node {n} estimated rows", a.EstimatedRows, b.EstimatedRows);
            Check($"node {n} actual rows", a.ActualRows, b.ActualRows);
            Check($"node {n} rows read", a.RowsRead, b.RowsRead);
            Check($"node {n} kv lookups", a.KvLookups, b.KvLookups);
            Check($"node {n} kv scan entries", a.KvScanEntries, b.KvScanEntries);
        }
    }
}
