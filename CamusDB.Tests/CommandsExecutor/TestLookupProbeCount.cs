/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Controllers.Queries;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The probes of an index-lookup step are the distinct keys of its left input, estimated by
/// <see cref="CostEstimator.ExpectedDistinctKeys"/> from the key column's distinct count and its
/// table's row count. The numbers are the Join Order Benchmark's cast table at scale 0.003: 108,733
/// cast rows over 12,483 distinct people, joined for the movies one keyword names, which the search
/// estimates at 3,784 cast rows. Before, every one of those rows counted as a probe of <c>name</c>
/// and a hash join over the whole table won by a few percent; the chain produces 4,928 rows with
/// 2,147 distinct people, and the probe plan runs a third faster.
/// </summary>
[TestFixture]
public sealed class TestLookupProbeCount
{
    private const double CastRows = 108_733;
    private const double People = 12_483;
    private const double ChainRows = 3_784;
    private const double NameRows = 12_502;
    private const double NamesMatched = 252;

    [Test]
    public void ExpectedDistinctKeys_IsBelowTheRowsAndTheDistinctCount()
    {
        foreach (double rows in new[] { 1.0, 10.0, 500.0, 3_784.0, 50_000.0, 108_733.0 })
        {
            double keys = CostEstimator.ExpectedDistinctKeys(rows, People, CastRows);
            Assert.That(keys, Is.GreaterThan(0).And.LessThanOrEqualTo(Math.Min(rows, People) + 1e-9), $"rows {rows}");
        }
    }

    [Test]
    public void ExpectedDistinctKeys_CastRowsOfAFewMovies_RepeatPeople()
    {
        // 12,483 · (1 − (1 − 3,784 / 108,733)^(108,733 / 12,483)) = 3,314: twelve percent of the rows
        // repeat a key the memo answers. The old bound counted every row.
        Assert.That(CostEstimator.ExpectedDistinctKeys(ChainRows, People, CastRows), Is.EqualTo(3314).Within(1));
    }

    [Test]
    public void ExpectedDistinctKeys_UniqueKey_IsTheRows()
    {
        Assert.That(CostEstimator.ExpectedDistinctKeys(500, 7_584, 7_584), Is.EqualTo(500).Within(1e-9));
    }

    [Test]
    public void ExpectedDistinctKeys_WholeTable_IsEveryKey()
    {
        Assert.That(CostEstimator.ExpectedDistinctKeys(CastRows, People, CastRows), Is.EqualTo(People));
        Assert.That(CostEstimator.ExpectedDistinctKeys(CastRows * 10, People, CastRows), Is.EqualTo(People));
    }

    [Test]
    public void ExpectedDistinctKeys_UnknownCounts_KeepTheOldBound()
    {
        Assert.That(CostEstimator.ExpectedDistinctKeys(500, 100, double.PositiveInfinity), Is.EqualTo(100));
        Assert.That(CostEstimator.ExpectedDistinctKeys(50, 100, double.PositiveInfinity), Is.EqualTo(50));
        Assert.That(CostEstimator.ExpectedDistinctKeys(500, double.PositiveInfinity, CastRows), Is.EqualTo(500));
        Assert.That(CostEstimator.ExpectedDistinctKeys(0, People, CastRows), Is.EqualTo(0));
    }

    [Test]
    public void StepCost_RepeatedKeys_ProbeUndercutsTheHashJoin()
    {
        IndexLookupProfile sampled = new(UniqueIndex: true, MatchesPerProbe: 1, LeftKeyDistinct: People, LeftTableRows: CastRows);
        IndexLookupProfile bounded = sampled with { LeftTableRows = double.PositiveInfinity };

        double hash = CostEstimator.HashJoinStepCost(ChainRows, NamesMatched, NameRows);

        // Every row a probe: 3,784 · 0.65 + 3,784 · 4 = 17,594 against a hash join at 16,311.
        Assert.That(CostEstimator.IndexLookupJoinStepCost(ChainRows, bounded), Is.GreaterThan(hash));
        // 3,314 probes: 2,460 + 13,256 = 15,716, under the hash join.
        Assert.That(CostEstimator.IndexLookupJoinStepCost(ChainRows, sampled), Is.LessThan(hash));
        Assert.That(CostEstimator.PrefersHashOverIndexLookup(ChainRows, NamesMatched, NameRows, sampled), Is.False);
    }

    [Test]
    public void StepCost_KeyRowsBound_ScalesByTheNonNullShare()
    {
        IndexLookupProfile probe = new(UniqueIndex: true, MatchesPerProbe: 1, LeftKeyDistinct: 10_000, LeftNonNullFraction: 0.5, LeftKeyRows: 400);

        // 1,000 left rows, half with a key; the key's table has 400 rows in the input, 200 of them keyed.
        double expected = 1000 * CostEstimator.MemoHitWeight + 200 * 4.0;
        Assert.That(CostEstimator.IndexLookupJoinStepCost(1000, probe), Is.EqualTo(expected).Within(1e-9));
    }
}
