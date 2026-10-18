/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Statistics.Models;

namespace CamusDB.Tests.Statistics;

/// <summary>
/// A heavy value fills whole buckets of an equi-depth histogram. Its equality fraction is the
/// share of the bucket its run starts in plus the whole buckets that follow, the cumulative
/// fraction at the value counts the rows through the last bucket of the run, and an inclusive
/// range bound at the value carries its whole mass once.
/// </summary>
[TestFixture]
public sealed class TestColumnHistogramHeavyValues
{
    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };

    /// <summary>
    /// 1,000 rows. The value 5 starts in the first bucket, which holds 100 rows of 4 distinct
    /// values, then fills a whole bucket of 500 rows; the last bucket holds 400 rows of 40 values.
    /// </summary>
    private static ColumnHistogram RunStartsMidBucket() => new()
    {
        TotalRows = 1000,
        MinValue = Int(2),
        Buckets =
        [
            new() { UpperBound = Int(5),  CumulativeRows = 100,  DistinctInBucket = 4 },
            new() { UpperBound = Int(5),  CumulativeRows = 600,  DistinctInBucket = 1 },
            new() { UpperBound = Int(45), CumulativeRows = 1000, DistinctInBucket = 40 },
        ],
    };

    [Test]
    public void EqualityFraction_OfAHeavyValue_AddsItsWholeBucketsToItsShareOfTheFirst()
    {
        ColumnHistogram hist = RunStartsMidBucket();

        // 100 / 4 of the first bucket plus the whole second bucket: 525 of 1,000 rows. The
        // density of the first bucket alone would say 25.
        Assert.That(hist.EqualityFraction(Int(5)), Is.EqualTo(0.525).Within(1e-9));
        Assert.That(hist.IsHeavyValue(Int(5)), Is.True);
    }

    [Test]
    public void EqualityFraction_OfAValueInsideABucket_IsTheBucketDensity()
    {
        ColumnHistogram hist = RunStartsMidBucket();

        Assert.That(hist.EqualityFraction(Int(3)), Is.EqualTo(0.025).Within(1e-9), "100 rows over 4 values");
        Assert.That(hist.IsHeavyValue(Int(3)), Is.False);
        Assert.That(hist.EqualityFraction(Int(20)), Is.EqualTo(0.01).Within(1e-9), "400 rows over 40 values");
        Assert.That(hist.IsHeavyValue(Int(45)), Is.False, "the upper bound of a bucket of many values is not heavy");
    }

    [Test]
    public void CumulativeFraction_AtAHeavyValue_CountsThroughTheLastBucketOfItsRun()
    {
        ColumnHistogram hist = RunStartsMidBucket();

        Assert.That(hist.CumulativeFraction(Int(5)), Is.EqualTo(0.6).Within(1e-9), "rows at or below 5 end with the second bucket");
        Assert.That(hist.RangeFraction(Int(5), loInclusive: true, null, hiInclusive: true), Is.EqualTo(0.925).Within(1e-9), "rows above 5 plus the value's own rows");
        Assert.That(hist.RangeFraction(Int(5), loInclusive: false, null, hiInclusive: true), Is.EqualTo(0.4).Within(1e-9), "rows above 5");
        Assert.That(hist.RangeFraction(null, loInclusive: false, Int(5), hiInclusive: false), Is.EqualTo(0.075).Within(1e-9), "rows below 5");
    }

    [Test]
    public void AValueThatStartsAtABucketBoundary_IsHeavyFromItsFirstBucket()
    {
        ColumnHistogram hist = new()
        {
            TotalRows = 1000,
            MinValue = Int(5),
            Buckets =
            [
                new() { UpperBound = Int(5),  CumulativeRows = 500,  DistinctInBucket = 1 },
                new() { UpperBound = Int(45), CumulativeRows = 1000, DistinctInBucket = 40 },
            ],
        };

        Assert.That(hist.IsHeavyValue(Int(5)), Is.True);
        Assert.That(hist.EqualityFraction(Int(5)), Is.EqualTo(0.5).Within(1e-9));
        Assert.That(hist.CumulativeFraction(Int(5)), Is.EqualTo(0.5).Within(1e-9));
    }

    [Test]
    public void AValueOutsideTheDomain_IsNotHeavy()
    {
        ColumnHistogram hist = RunStartsMidBucket();

        Assert.That(hist.IsHeavyValue(Int(1)), Is.False);
        Assert.That(hist.IsHeavyValue(Int(99)), Is.False);
        Assert.That(hist.EqualityFraction(Int(99)), Is.EqualTo(0.0));
    }
}
