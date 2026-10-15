/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Statistics.Models;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The equi-join selectivity over value distributions: the textbook estimate when no value is
/// heavy, the product of the shared heavy values when both sides are skewed on them.
/// </summary>
[TestFixture]
public sealed class TestJoinKeyDistribution
{
    [Test]
    public void TwoUniformColumns_GiveTheTextbookSelectivity()
    {
        double? selectivity = JoinKeyDistribution.JoinSelectivity(JoinKeyDistribution.Uniform(100), JoinKeyDistribution.Uniform(40));

        Assert.AreEqual(1.0 / 100, selectivity!.Value, 1e-12);
    }

    [Test]
    public void HeavyValueOnBothSides_MultipliesItsShares()
    {
        // Half of a and 40% of b hold the value 7; the rest spreads over 99 and 49 other values.
        JoinKeyDistribution a = Distribution((7, 0.5), residualDistinct: 99);
        JoinKeyDistribution b = Distribution((7, 0.4), residualDistinct: 49);

        double expected = 0.5 * 0.4 + 0.5 * 0.6 / 99;

        Assert.AreEqual(expected, JoinKeyDistribution.JoinSelectivity(a, b)!.Value, 1e-12);
        Assert.AreEqual(expected, JoinKeyDistribution.JoinSelectivity(b, a)!.Value, 1e-12);
    }

    [Test]
    public void HeavyValueOnOneSide_AgainstAUniformColumn_IsTheTextbookSelectivity()
    {
        // The heavy value meets one of the 100 uniform values: no skew on the other side, no
        // change against 1 / max(distinct).
        JoinKeyDistribution a = Distribution((7, 0.5), residualDistinct: 99);
        JoinKeyDistribution b = JoinKeyDistribution.Uniform(100);

        Assert.AreEqual(1.0 / 100, JoinKeyDistribution.JoinSelectivity(a, b)!.Value, 1e-12);
    }

    [Test]
    public void HeavyValuesOfOneSideAbsentOnTheOther_MeetTheResidualAtItsAverageFrequency()
    {
        JoinKeyDistribution a = Distribution((7, 0.5), residualDistinct: 99);
        JoinKeyDistribution b = Distribution((9, 0.4), residualDistinct: 49);

        // 7 against b's residual, 9 against a's residual, then the two residuals.
        double expected = 0.5 * (0.6 / 49) + 0.4 * (0.5 / 99) + 0.5 * 0.6 / 99;

        Assert.AreEqual(expected, JoinKeyDistribution.JoinSelectivity(a, b)!.Value, 1e-12);
        Assert.AreEqual(expected, JoinKeyDistribution.JoinSelectivity(b, a)!.Value, 1e-12);
    }

    [Test]
    public void HeavyValuesOfDifferentTypes_GiveNoEstimate()
    {
        JoinKeyDistribution a = Distribution((7, 0.5), residualDistinct: 99);
        JoinKeyDistribution b = JoinKeyDistribution.FromHistogram(new ColumnHistogram
        {
            TotalRows = 10,
            Buckets = [new ColumnHistogramBucket { UpperBound = new ScalarBound { Type = ColumnType.String, StrValue = "7" }, CumulativeRows = 10, DistinctInBucket = 1 }],
        }, distinct: 1)!;

        Assert.IsNull(JoinKeyDistribution.JoinSelectivity(a, b));
    }

    [Test]
    public void FromHistogram_TakesTheBucketsOfOneValueAsHeavy_AndMergesARun()
    {
        // 100 rows in buckets of 10: the value 7 fills buckets 2 to 4 (and part of bucket 1,
        // which is not counted); 1,000 distinct values in all.
        ColumnHistogram histogram = new()
        {
            TotalRows = 100,
            MinValue = Int(1),
            Buckets =
            [
                new() { UpperBound = Int(7), CumulativeRows = 10, DistinctInBucket = 4 },
                new() { UpperBound = Int(7), CumulativeRows = 20, DistinctInBucket = 1 },
                new() { UpperBound = Int(7), CumulativeRows = 30, DistinctInBucket = 1 },
                new() { UpperBound = Int(7), CumulativeRows = 40, DistinctInBucket = 1 },
                new() { UpperBound = Int(9), CumulativeRows = 50, DistinctInBucket = 2 },
                new() { UpperBound = Int(9), CumulativeRows = 60, DistinctInBucket = 1 },
                new() { UpperBound = Int(500), CumulativeRows = 100, DistinctInBucket = 40 },
            ],
        };

        JoinKeyDistribution distribution = JoinKeyDistribution.FromHistogram(histogram, distinct: 1000)!;

        Assert.AreEqual(2, distribution.HeavyValues.Count);
        Assert.AreEqual(7, distribution.HeavyValues[0].Value.LongValue);
        Assert.AreEqual(0.3, distribution.HeavyValues[0].Fraction, 1e-12);
        Assert.AreEqual(9, distribution.HeavyValues[1].Value.LongValue);
        Assert.AreEqual(0.1, distribution.HeavyValues[1].Fraction, 1e-12);
        Assert.AreEqual(0.6, distribution.ResidualFraction, 1e-12);
        Assert.AreEqual(998, distribution.ResidualDistinct, 1e-12);
    }

    [Test]
    public void FromHistogram_EmptyHistogram_IsNull()
    {
        Assert.IsNull(JoinKeyDistribution.FromHistogram(new ColumnHistogram { TotalRows = 0 }, distinct: 10));
    }

    [Test]
    public void FromHistogram_MoreHeavyValuesThanTheDistinctCount_KeepsOneResidualValue()
    {
        // A distinct count from a sketch can fall below the heavy values the histogram shows.
        ColumnHistogram histogram = new()
        {
            TotalRows = 20,
            Buckets =
            [
                new() { UpperBound = Int(1), CumulativeRows = 10, DistinctInBucket = 1 },
                new() { UpperBound = Int(2), CumulativeRows = 20, DistinctInBucket = 1 },
            ],
        };

        JoinKeyDistribution distribution = JoinKeyDistribution.FromHistogram(histogram, distinct: 1)!;

        Assert.AreEqual(0.0, distribution.ResidualFraction, 1e-12);
        Assert.AreEqual(1.0, distribution.ResidualDistinct, 1e-12);
    }

    private static ScalarBound Int(long value) => new() { Type = ColumnType.Integer64, LongValue = value };

    /// <summary>A distribution with one heavy integer value; the rest of the rows over <paramref name="residualDistinct"/> values.</summary>
    private static JoinKeyDistribution Distribution((long Value, double Fraction) heavy, long residualDistinct)
    {
        const long rows = 1000;
        long heavyRows = (long)(heavy.Fraction * rows);

        List<ColumnHistogramBucket> buckets =
        [
            new() { UpperBound = Int(heavy.Value), CumulativeRows = heavyRows, DistinctInBucket = 1 },
            new() { UpperBound = Int(heavy.Value + 1_000_000), CumulativeRows = rows, DistinctInBucket = residualDistinct },
        ];

        return JoinKeyDistribution.FromHistogram(new ColumnHistogram { TotalRows = rows, Buckets = buckets }, residualDistinct + 1)!;
    }
}
