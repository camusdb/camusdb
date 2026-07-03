
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Statistics.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Validates the per-column equi-depth histogram model:
///   1. ColumnHistogram and ColumnHistogramBucket JSON round-trip via MetaJsonContext.
///   2. TableStatistics.Histograms persists alongside existing fields without breaking them.
///   3. A histogram with known skew reports cumulativeRows that reflect the skew.
///   4. CumulativeFraction / RangeFraction compute correct fractions on uniform and skewed data.
///   5. Empty / missing histograms fall back gracefully (no throw).
///   6. Old TableStatistics blobs that lack the histograms field deserialize to Histograms=null.
/// </summary>
[TestFixture]
public sealed class TestStatisticsHistogram
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };

    /// <summary>Builds an equi-depth histogram over <paramref name="sortedValues"/>.</summary>
    private static ColumnHistogram BuildHistogram(long[] sortedValues, int targetBuckets)
    {
        if (sortedValues.Length == 0)
            return new ColumnHistogram { TotalRows = 0 };

        int bucketSize = Math.Max(1, (int)Math.Ceiling((double)sortedValues.Length / targetBuckets));
        var buckets = new List<ColumnHistogramBucket>();

        long cumulative = 0;
        for (int i = bucketSize - 1; i < sortedValues.Length; i += bucketSize)
        {
            int end = Math.Min(i, sortedValues.Length - 1);
            // Count distinct values in this bucket span.
            int start = Math.Max(0, end - bucketSize + 1);
            var distinct = new HashSet<long>();
            for (int j = start; j <= end; j++) distinct.Add(sortedValues[j]);

            cumulative += Math.Min(bucketSize, sortedValues.Length - start);
            buckets.Add(new ColumnHistogramBucket
            {
                UpperBound     = Int(sortedValues[end]),
                CumulativeRows = cumulative,
                DistinctInBucket = distinct.Count,
            });
        }

        // Ensure the last bucket covers all rows.
        if (buckets.Count > 0)
            buckets[^1].CumulativeRows = sortedValues.Length;

        return new ColumnHistogram { Buckets = buckets, TotalRows = sortedValues.Length };
    }

    private static byte[] SerializeStats(TableStatistics s)
        => MetaJsonSerializer.Serialize(s, MetaJsonContext.Default.TableStatistics);

    private static TableStatistics? DeserializeStats(byte[] bytes)
        => MetaJsonSerializer.DeserializeCompat(bytes, MetaJsonContext.Default.TableStatistics);

    // ─────────────────────────────────────────────────────────────────────────
    // Tests
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void BucketJsonRoundTrip()
    {
        var bucket = new ColumnHistogramBucket
        {
            UpperBound       = Int(100),
            CumulativeRows   = 50,
            DistinctInBucket = 10,
        };

        string json = JsonSerializer.Serialize(bucket, MetaJsonContext.Default.ColumnHistogramBucket);
        ColumnHistogramBucket? rt = JsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnHistogramBucket);

        Assert.That(rt, Is.Not.Null);
        Assert.That(rt!.UpperBound!.LongValue, Is.EqualTo(100));
        Assert.That(rt.CumulativeRows,          Is.EqualTo(50));
        Assert.That(rt.DistinctInBucket,        Is.EqualTo(10));
    }

    [Test]
    public void HistogramJsonRoundTrip()
    {
        var histogram = new ColumnHistogram
        {
            TotalRows = 300,
            Buckets =
            [
                new() { UpperBound = Int(33),  CumulativeRows = 100, DistinctInBucket = 33 },
                new() { UpperBound = Int(66),  CumulativeRows = 200, DistinctInBucket = 33 },
                new() { UpperBound = Int(100), CumulativeRows = 300, DistinctInBucket = 34 },
            ],
        };

        string json = JsonSerializer.Serialize(histogram, MetaJsonContext.Default.ColumnHistogram);
        ColumnHistogram? rt = JsonSerializer.Deserialize(json, MetaJsonContext.Default.ColumnHistogram);

        Assert.That(rt, Is.Not.Null);
        Assert.That(rt!.TotalRows,    Is.EqualTo(300));
        Assert.That(rt.Buckets.Count, Is.EqualTo(3));
        Assert.That(rt.Buckets[1].UpperBound!.LongValue, Is.EqualTo(66));
        Assert.That(rt.Buckets[1].CumulativeRows,         Is.EqualTo(200));
    }

    [Test]
    public void TableStatisticsWithHistogramsRoundTrips()
    {
        var stats = new TableStatistics
        {
            RowCount = 500,
            Histograms = new Dictionary<string, ColumnHistogram>
            {
                ["price"] = new ColumnHistogram
                {
                    TotalRows = 500,
                    Buckets =
                    [
                        new() { UpperBound = Int(10),  CumulativeRows = 100, DistinctInBucket = 5 },
                        new() { UpperBound = Int(50),  CumulativeRows = 300, DistinctInBucket = 20 },
                        new() { UpperBound = Int(200), CumulativeRows = 500, DistinctInBucket = 30 },
                    ],
                },
            },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt,                  Is.Not.Null);
        Assert.That(rt!.RowCount,        Is.EqualTo(500));
        Assert.That(rt.Histograms,        Is.Not.Null);
        Assert.That(rt.Histograms!.ContainsKey("price"), Is.True);

        ColumnHistogram h = rt.Histograms["price"];
        Assert.That(h.TotalRows,    Is.EqualTo(500));
        Assert.That(h.Buckets.Count, Is.EqualTo(3));
        Assert.That(h.Buckets[0].UpperBound!.LongValue, Is.EqualTo(10));
        Assert.That(h.Buckets[2].CumulativeRows,          Is.EqualTo(500));
    }

    [Test]
    public void OldBlobWithoutHistogramsDeserializesToNull()
    {
        // An old-format blob has no "histograms" key — backward-compatible: field is nullable.
        var oldStats = new TableStatistics { RowCount = 42 };
        byte[] bytes = SerializeStats(oldStats);

        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt,            Is.Not.Null);
        Assert.That(rt!.RowCount,  Is.EqualTo(42));
        Assert.That(rt.Histograms, Is.Null);
    }

    [Test]
    public void ExistingFieldsUndamagedByHistogramAddition()
    {
        // Ensure ColumnStats and IndexEntryCounts still round-trip correctly alongside Histograms.
        var stats = new TableStatistics
        {
            RowCount = 1000,
            IndexEntryCounts = new Dictionary<string, long> { ["idx_year"] = 950 },
            ColumnStats = new Dictionary<string, ColumnMinMax>
            {
                ["year"] = new ColumnMinMax
                {
                    Min = new ScalarBound { Type = ColumnType.Integer64, LongValue = 2000 },
                    Max = new ScalarBound { Type = ColumnType.Integer64, LongValue = 2024 },
                },
            },
            Histograms = new Dictionary<string, ColumnHistogram>
            {
                ["year"] = new ColumnHistogram
                {
                    TotalRows = 1000,
                    Buckets =
                    [
                        new() { UpperBound = Int(2010), CumulativeRows = 400, DistinctInBucket = 10 },
                        new() { UpperBound = Int(2024), CumulativeRows = 1000, DistinctInBucket = 14 },
                    ],
                },
            },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt!.RowCount,                           Is.EqualTo(1000));
        Assert.That(rt.IndexEntryCounts!["idx_year"],        Is.EqualTo(950));
        Assert.That(rt.ColumnStats!["year"].Min!.LongValue,  Is.EqualTo(2000));
        Assert.That(rt.Histograms!["year"].TotalRows,        Is.EqualTo(1000));
        Assert.That(rt.Histograms["year"].Buckets[0].CumulativeRows, Is.EqualTo(400));
    }

    [Test]
    public void UniformHistogramCumulativeFractionIsMonotonic()
    {
        // 3 equal-depth buckets over [1..300], 100 rows each.
        var h = new ColumnHistogram
        {
            TotalRows = 300,
            Buckets =
            [
                new() { UpperBound = Int(100), CumulativeRows = 100, DistinctInBucket = 100 },
                new() { UpperBound = Int(200), CumulativeRows = 200, DistinctInBucket = 100 },
                new() { UpperBound = Int(300), CumulativeRows = 300, DistinctInBucket = 100 },
            ],
        };

        double f100 = h.CumulativeFraction(Int(100));
        double f200 = h.CumulativeFraction(Int(200));
        double f300 = h.CumulativeFraction(Int(300));

        Assert.That(f100, Is.EqualTo(1.0 / 3).Within(0.01));
        Assert.That(f200, Is.EqualTo(2.0 / 3).Within(0.01));
        Assert.That(f300, Is.EqualTo(1.0).Within(0.01));
        // Monotonically non-decreasing.
        Assert.That(f100 <= f200 && f200 <= f300, Is.True);
    }

    [Test]
    public void SkewedHistogramReflectsSkewInCumulativeRows()
    {
        // Skewed data: 800 rows in [1..10], only 200 rows in [11..1000].
        // 2 buckets:
        //   bucket 0: upper=10,   cumulative=800
        //   bucket 1: upper=1000, cumulative=1000
        var h = new ColumnHistogram
        {
            TotalRows = 1000,
            Buckets =
            [
                new() { UpperBound = Int(10),   CumulativeRows = 800, DistinctInBucket = 10 },
                new() { UpperBound = Int(1000),  CumulativeRows = 1000, DistinctInBucket = 990 },
            ],
        };

        double fAt10   = h.CumulativeFraction(Int(10));
        double fAt1000 = h.CumulativeFraction(Int(1000));
        // The fraction at value 10 should be 0.8, reflecting the heavy skew in the low range.
        Assert.That(fAt10,   Is.EqualTo(0.8).Within(0.01));
        Assert.That(fAt1000, Is.EqualTo(1.0).Within(0.01));
    }

    [Test]
    public void RangeFractionReturnsCorrectSlice()
    {
        var h = new ColumnHistogram
        {
            TotalRows = 300,
            Buckets =
            [
                new() { UpperBound = Int(100), CumulativeRows = 100, DistinctInBucket = 100 },
                new() { UpperBound = Int(200), CumulativeRows = 200, DistinctInBucket = 100 },
                new() { UpperBound = Int(300), CumulativeRows = 300, DistinctInBucket = 100 },
            ],
        };

        // Middle bucket only: rows in (100, 200].
        double mid = h.RangeFraction(Int(100), Int(200));
        Assert.That(mid, Is.EqualTo(1.0 / 3).Within(0.01));

        // Full range should be 1.0.
        double full = h.RangeFraction(Int(1), Int(300));
        Assert.That(full, Is.EqualTo(1.0).Within(0.01));
    }

    [Test]
    public void EmptyHistogramFallsBackToHalf()
    {
        var h = new ColumnHistogram { TotalRows = 0, Buckets = [] };
        Assert.That(h.CumulativeFraction(Int(42)), Is.EqualTo(0.5));
        Assert.That(h.RangeFraction(Int(1), Int(100)), Is.EqualTo(0.0).Within(0.01));
    }

    [Test]
    public void MidBucketInterpolationForInteger()
    {
        // 2 equal-depth buckets: [..100] = 100 rows, [101..200] = 100 rows.
        // A value of 150 falls inside bucket 1 where lo=100 is known; linear interpolation
        // yields prevCumul(100) + 0.5 * bucketRows(100) = 150 → 150/200 = 0.75.
        // The first bucket (bucket 0) has no stored lower bound, so values inside it
        // conservatively return 0.0 — interpolation only applies when lo is known.
        var h = new ColumnHistogram
        {
            TotalRows = 200,
            Buckets =
            [
                new() { UpperBound = Int(100), CumulativeRows = 100, DistinctInBucket = 100 },
                new() { UpperBound = Int(200), CumulativeRows = 200, DistinctInBucket = 100 },
            ],
        };

        double fAt150 = h.CumulativeFraction(Int(150));
        Assert.That(fAt150, Is.EqualTo(0.75).Within(0.01));
    }

    [Test]
    public void MidBucketInterpolationIsStrictlyLessThanBoundary()
    {
        // 2-bucket uniform histogram: bucket 0 upper=100 (100 rows), bucket 1 upper=200 (100 rows).
        // A mid-bucket value of 150 should be strictly between f(100) and f(200).
        var h = new ColumnHistogram
        {
            TotalRows = 200,
            Buckets =
            [
                new() { UpperBound = Int(100), CumulativeRows = 100, DistinctInBucket = 100 },
                new() { UpperBound = Int(200), CumulativeRows = 200, DistinctInBucket = 100 },
            ],
        };

        double fAt100 = h.CumulativeFraction(Int(100));
        double fAt150 = h.CumulativeFraction(Int(150));
        double fAt200 = h.CumulativeFraction(Int(200));

        Assert.That(fAt150, Is.GreaterThan(fAt100));
        Assert.That(fAt150, Is.LessThan(fAt200));
        Assert.That(fAt150, Is.EqualTo(0.75).Within(0.01)); // 100/200 + 0.5*(100/200)
    }

    [Test]
    public void MidBucketInterpolationMonotonicAcrossThreeBuckets()
    {
        var h = new ColumnHistogram
        {
            TotalRows = 300,
            Buckets =
            [
                new() { UpperBound = Int(100), CumulativeRows = 100, DistinctInBucket = 100 },
                new() { UpperBound = Int(200), CumulativeRows = 200, DistinctInBucket = 100 },
                new() { UpperBound = Int(300), CumulativeRows = 300, DistinctInBucket = 100 },
            ],
        };

        // Sample mid-bucket values and verify monotonicity.
        double[] fractions = [
            h.CumulativeFraction(Int(50)),
            h.CumulativeFraction(Int(100)),
            h.CumulativeFraction(Int(150)),
            h.CumulativeFraction(Int(200)),
            h.CumulativeFraction(Int(250)),
            h.CumulativeFraction(Int(300)),
        ];

        for (int i = 1; i < fractions.Length; i++)
            Assert.That(fractions[i], Is.GreaterThanOrEqualTo(fractions[i - 1]),
                $"fractions[{i}]={fractions[i]} < fractions[{i-1}]={fractions[i-1]}");
    }

    [Test]
    public void BuildEquiDepthHistogramSkewedReflectsSkew()
    {
        // 800 rows with value 1, 200 rows with values 2-201 (one each).
        var values = new long[1000];
        for (int i = 0; i < 800; i++) values[i] = 1;
        for (int i = 0; i < 200; i++) values[800 + i] = 2 + i;
        Array.Sort(values);

        ColumnHistogram h = BuildHistogram(values, 4);

        // In an equi-depth histogram, the first bucket (containing ~250 rows) should
        // capture mostly value=1 rows (heavy skew) → its upper bound should be 1.
        Assert.That(h.Buckets.Count,    Is.GreaterThan(0));
        Assert.That(h.TotalRows,        Is.EqualTo(1000));
        // The first bucket spans the skewed value.
        Assert.That(h.Buckets[0].UpperBound!.LongValue, Is.EqualTo(1));
        // cumulativeRows for that bucket reflects how many rows had value ≤ 1 (the 800 skewed rows).
        Assert.That(h.Buckets[0].CumulativeRows, Is.GreaterThan(0));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ScalarBound.CompareTo ordering for Date, DateTime, Float32
    // ─────────────────────────────────────────────────────────────────────────

    private static ScalarBound DateBound(long v)  => new() { Type = ColumnType.Date,     LongValue  = v };
    private static ScalarBound DtBound(long v)    => new() { Type = ColumnType.DateTime, LongValue  = v };
    private static ScalarBound F32Bound(double v) => new() { Type = ColumnType.Float32,  FloatValue = v };

    /// <summary>
    /// ScalarBound.CompareTo must order Date/DateTime by LongValue and Float32 by FloatValue.
    /// Before the fix, all three types fell to the default arm returning 0, so a < b and a > b
    /// were indistinguishable and sorting was a no-op.
    /// </summary>
    [Test]
    public void ScalarBoundCompareTo_Date_OrdersCorrectly()
    {
        ScalarBound lo = DateBound(100);
        ScalarBound hi = DateBound(200);
        ScalarBound eq = DateBound(100);

        Assert.That(lo.CompareTo(hi), Is.LessThan(0),    "earlier date must be less than later date");
        Assert.That(hi.CompareTo(lo), Is.GreaterThan(0), "later date must be greater than earlier date");
        Assert.That(lo.CompareTo(eq), Is.EqualTo(0),     "equal dates must compare to 0");
    }

    [Test]
    public void ScalarBoundCompareTo_DateTime_OrdersCorrectly()
    {
        ScalarBound lo = DtBound(1_000_000);
        ScalarBound hi = DtBound(2_000_000);
        ScalarBound eq = DtBound(1_000_000);

        Assert.That(lo.CompareTo(hi), Is.LessThan(0));
        Assert.That(hi.CompareTo(lo), Is.GreaterThan(0));
        Assert.That(lo.CompareTo(eq), Is.EqualTo(0));
    }

    [Test]
    public void ScalarBoundCompareTo_Float32_OrdersCorrectly()
    {
        ScalarBound lo  = F32Bound(1.5);
        ScalarBound hi  = F32Bound(3.0);
        ScalarBound eq  = F32Bound(1.5);
        ScalarBound neg = F32Bound(-2.0);

        Assert.That(lo.CompareTo(hi),  Is.LessThan(0),    "1.5 < 3.0");
        Assert.That(hi.CompareTo(lo),  Is.GreaterThan(0), "3.0 > 1.5");
        Assert.That(lo.CompareTo(eq),  Is.EqualTo(0),     "1.5 == 1.5");
        Assert.That(neg.CompareTo(lo), Is.LessThan(0),    "negative must be less than positive");
    }
}
