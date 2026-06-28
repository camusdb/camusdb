
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// Equi-depth histogram for one column. Built by <c>ANALYZE</c> and refreshed on demand;
/// not maintained incrementally on DML (too costly). Between rebuilds the histogram drifts —
/// that is acceptable because statistics are advisory.
///
/// <see cref="Buckets"/> is ordered by <see cref="ColumnHistogramBucket.UpperBound"/> ascending.
/// The first bucket captures all values from the column minimum up to its upper bound.
/// The last bucket's upper bound equals the column maximum.
///
/// <see cref="TotalRows"/> is the number of non-null rows scanned when the histogram was built.
/// It may differ from <see cref="TableStatistics.RowCount"/> when sampling was used.
/// </summary>
public sealed class ColumnHistogram
{
    /// <summary>Ordered list of equi-depth buckets (ascending by upper bound).</summary>
    [JsonPropertyName("buckets")]
    public List<ColumnHistogramBucket> Buckets { get; set; } = [];

    /// <summary>Total non-null rows observed when this histogram was last built.</summary>
    [JsonPropertyName("totalRows")]
    public long TotalRows { get; set; }

    /// <summary>
    /// Returns the estimated fraction of rows satisfying <c>column ≤ value</c>.
    /// Returns 0.0 when <paramref name="value"/> is below the first bucket's upper bound,
    /// 1.0 when above the last, and 0.5 on an empty histogram.
    ///
    /// For values that fall strictly inside a bucket (between the previous bucket's upper
    /// bound and this bucket's upper bound) linear interpolation over the bucket's row span
    /// is applied, assuming a uniform value distribution within the bucket.
    /// </summary>
    public double CumulativeFraction(ScalarBound value)
    {
        if (Buckets.Count == 0 || TotalRows <= 0)
            return 0.5;

        // Value is below the lowest recorded upper bound → no rows qualify.
        ColumnHistogramBucket first = Buckets[0];
        if (first.UpperBound is not null && value.CompareTo(first.UpperBound) < 0)
            return 0.0;

        for (int i = 0; i < Buckets.Count; i++)
        {
            ColumnHistogramBucket b = Buckets[i];
            if (b.UpperBound is null)
                continue;

            int cmp = value.CompareTo(b.UpperBound);
            if (cmp == 0)
                return (double)b.CumulativeRows / TotalRows;

            if (cmp < 0)
            {
                // value is strictly inside bucket i.
                // Interpolate linearly between the previous bucket's upper bound (exclusive)
                // and this bucket's upper bound (inclusive), assuming uniform spread.
                long prevCumul = i == 0 ? 0 : Buckets[i - 1].CumulativeRows;
                long bucketRows = b.CumulativeRows - prevCumul;
                if (bucketRows <= 0)
                    return (double)b.CumulativeRows / TotalRows;

                // ScalarBound.CompareTo returns an ordinal distance only for numeric types
                // (integers and floats). For non-numeric types (String, Id) we cannot
                // compute a meaningful fractional position within the bucket, so we round
                // up to the bucket boundary as a conservative over-estimate.
                double fraction = FractionWithinBucket(value, i);
                return (prevCumul + fraction * bucketRows) / TotalRows;
            }
        }

        return 1.0;
    }

    /// <summary>
    /// Returns the estimated fraction of rows satisfying <c>column &gt; lo AND column ≤ hi</c>
    /// (half-open on the low side: <c>(lo, hi]</c>). Should account for the inclusive low
    /// bound <c>lo ≤ col</c> (SQL BETWEEN / <c>≥</c>) by subtracting an equality-selectivity
    /// term for <c>lo</c> if needed.
    /// Falls back to 0.5 on an empty histogram.
    /// </summary>
    public double RangeFraction(ScalarBound? lo, ScalarBound? hi)
    {
        double upper = hi is not null ? CumulativeFraction(hi) : 1.0;
        double lower = lo is not null ? CumulativeFraction(lo) : 0.0;
        return Math.Max(0.0, upper - lower);
    }

    // Returns the fractional position of value within bucket i, in [0, 1).
    // For Integer64 and Float64 the position is interpolated by value distance.
    // For other types (String, Id) interpolation is not meaningful; returns 0.5
    // (mid-bucket) as a neutral estimate.
    private double FractionWithinBucket(ScalarBound value, int bucketIndex)
    {
        ScalarBound? lo = bucketIndex == 0 ? null : Buckets[bucketIndex - 1].UpperBound;
        ScalarBound? hi = Buckets[bucketIndex].UpperBound;

        if (lo is null || hi is null || lo.Type != hi.Type || value.Type != hi.Type)
            return 0.5;

        return value.Type switch
        {
            ColumnType.Integer64 when hi.LongValue != lo.LongValue =>
                Math.Clamp((double)(value.LongValue - lo.LongValue) / (hi.LongValue - lo.LongValue), 0.0, 1.0),

            ColumnType.Float64 when hi.FloatValue != lo.FloatValue =>
                Math.Clamp((value.FloatValue - lo.FloatValue) / (hi.FloatValue - lo.FloatValue), 0.0, 1.0),

            _ => 0.5,
        };
    }
}
