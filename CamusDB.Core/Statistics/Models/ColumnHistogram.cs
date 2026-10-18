
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
    /// Smallest value observed when the histogram was built — the lower boundary of the first
    /// bucket. Without it the first bucket (which holds ~1/B of all rows, spanning
    /// [column-min, first upper bound]) cannot be interpolated: values inside it would either
    /// estimate 0 rows (badly under-pricing a range scan that actually touches the whole
    /// bucket) or the whole bucket. Null on histograms persisted before this field existed —
    /// interpolation then falls back to mid-bucket.
    /// </summary>
    [JsonPropertyName("minValue")]
    public ScalarBound? MinValue { get; set; }

    /// <summary>
    /// Returns the estimated fraction of rows satisfying <c>column ≤ value</c>.
    /// Returns 0.0 when <paramref name="value"/> is below <see cref="MinValue"/>,
    /// 1.0 when above the last bucket, and 0.5 on an empty histogram.
    ///
    /// For values that fall strictly inside a bucket (between the previous bucket's upper
    /// bound — or <see cref="MinValue"/> for the first bucket — and this bucket's upper bound)
    /// linear interpolation over the bucket's row span is applied, assuming a uniform value
    /// distribution within the bucket.
    /// </summary>
    public double CumulativeFraction(ScalarBound value)
    {
        if (Buckets.Count == 0 || TotalRows <= 0)
            return 0.5;

        // Below the observed column minimum → no rows qualify. Values inside the FIRST bucket
        // (≥ min but below its upper bound) must NOT short-circuit to 0: the first bucket holds
        // ~1/B of all rows and is interpolated like any other bucket by the loop below.
        if (MinValue is not null)
        {
            if (value.CompareTo(MinValue) < 0)
                return 0.0;
        }
        else if (Buckets[0].UpperBound is { } firstUpper && value.CompareTo(firstUpper) < 0)
        {
            // Legacy histogram persisted before MinValue existed: below-min cannot be told apart
            // from inside-the-first-bucket, so keep the historical conservative 0. The next
            // ANALYZE rebuilds the histogram with MinValue and enables first-bucket interpolation.
            return 0.0;
        }

        for (int i = 0; i < Buckets.Count; i++)
        {
            ColumnHistogramBucket b = Buckets[i];
            if (b.UpperBound is null)
                continue;

            int cmp = value.CompareTo(b.UpperBound);
            if (cmp == 0)
            {
                // A value that fills whole buckets is the upper bound of each of them; the rows
                // at or below it end with the last bucket of that run, not the first. Stopping
                // at the first would leave most of a heavy value's rows above it, and an
                // inclusive bound would then count them twice with the equality mass.
                while (i + 1 < Buckets.Count && Buckets[i + 1].UpperBound is { } sameBound && value.CompareTo(sameBound) == 0)
                    i++;

                return (double)Buckets[i].CumulativeRows / TotalRows;
            }

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
    /// (half-open: <c>(lo, hi]</c>). Prefer the inclusivity-aware overload — on a skewed
    /// low-NDV column the equality mass at a bound can be most of the table, so treating an
    /// inclusive <c>≥ lo</c> as exclusive drops that entire mass from the estimate.
    /// Falls back to 0.5 on an empty histogram.
    /// </summary>
    public double RangeFraction(ScalarBound? lo, ScalarBound? hi) =>
        RangeFraction(lo, loInclusive: false, hi, hiInclusive: true);

    /// <summary>
    /// Inclusivity-aware range fraction. The cumulative base covers <c>(lo, hi]</c>; an
    /// inclusive lower bound adds the equality mass at <paramref name="lo"/> and an exclusive
    /// upper bound subtracts the equality mass at <paramref name="hi"/> (bucket-density
    /// estimates, see <see cref="EqualityFraction"/>).
    /// </summary>
    public double RangeFraction(ScalarBound? lo, bool loInclusive, ScalarBound? hi, bool hiInclusive)
    {
        double upper = hi is not null ? CumulativeFraction(hi) : 1.0;
        double lower = lo is not null ? CumulativeFraction(lo) : 0.0;
        double fraction = upper - lower;

        if (lo is not null && loInclusive)
            fraction += EqualityFraction(lo);

        if (hi is not null && !hiInclusive)
            fraction -= EqualityFraction(hi);

        return Math.Clamp(fraction, 0.0, 1.0);
    }

    /// <summary>
    /// Estimated fraction of rows equal to <paramref name="value"/>: the share the value holds
    /// of the bucket its run starts in, <c>bucketRows / distinctInBucket</c>, plus every
    /// following bucket that holds the value alone (the same upper bound and
    /// <see cref="ColumnHistogramBucket.DistinctInBucket"/> 1). A value common enough to fill
    /// whole buckets of the sorted rows is counted by those buckets; the density of the one
    /// bucket its run starts in would price a value that holds half the table like any other
    /// value of that bucket. The bucket where the run ends is not counted, because the histogram
    /// does not say how much of it the value holds, so a heavy value's fraction is a slight
    /// underestimate. Returns 0 for out-of-domain values and on an empty histogram.
    /// </summary>
    public double EqualityFraction(ScalarBound value)
    {
        if (Buckets.Count == 0 || TotalRows <= 0)
            return 0.0;

        if (MinValue is not null && value.CompareTo(MinValue) < 0)
            return 0.0;

        for (int i = 0; i < Buckets.Count; i++)
        {
            ColumnHistogramBucket b = Buckets[i];
            if (b.UpperBound is null)
                continue;

            int cmp = value.CompareTo(b.UpperBound);
            if (cmp > 0)
                continue;

            long prevCumul = i == 0 ? 0 : Buckets[i - 1].CumulativeRows;
            long bucketRows = b.CumulativeRows - prevCumul;
            long distinct = Math.Max(1, b.DistinctInBucket);
            double rows = bucketRows <= 0 ? 0.0 : (double)bucketRows / distinct;

            if (cmp == 0)
                rows += WholeBucketRows(value, i + 1);

            return rows / TotalRows;
        }

        // Above every bucket's upper bound → outside the observed domain.
        return 0.0;
    }

    /// <summary>
    /// True when <paramref name="value"/> fills at least one whole bucket: a heavy value, whose
    /// share <see cref="EqualityFraction"/> reads from its buckets instead of a distinct count.
    /// The first bucket of a value's run is the one its rows start in, so the whole buckets are
    /// the ones after it under the same upper bound; a run that starts at a bucket boundary
    /// fills its first bucket too. False for a value outside the histogram's domain.
    /// </summary>
    public bool IsHeavyValue(ScalarBound value)
    {
        if (MinValue is not null && value.CompareTo(MinValue) < 0)
            return false;

        for (int i = 0; i < Buckets.Count; i++)
        {
            ColumnHistogramBucket b = Buckets[i];
            if (b.UpperBound is null)
                continue;

            int cmp = value.CompareTo(b.UpperBound);
            if (cmp > 0)
                continue;

            return cmp == 0 && (b.DistinctInBucket == 1 || WholeBucketRows(value, i + 1) > 0);
        }

        return false;
    }

    // The rows of the buckets from index `from` on that hold `value` alone (the value as their
    // upper bound and one distinct value): the whole buckets of the value's run.
    private double WholeBucketRows(ScalarBound value, int from)
    {
        double rows = 0;

        for (int j = from; j < Buckets.Count; j++)
        {
            ColumnHistogramBucket next = Buckets[j];
            if (next.UpperBound is null || next.DistinctInBucket != 1 || value.CompareTo(next.UpperBound) != 0)
                break;

            rows += next.CumulativeRows - Buckets[j - 1].CumulativeRows;
        }

        return rows;
    }

    // Returns the fractional position of value within bucket i, in [0, 1).
    // For Integer64 and Float64 the position is interpolated by value distance.
    // For other types (String, Id) interpolation is not meaningful; returns 0.5
    // (mid-bucket) as a neutral estimate. The first bucket interpolates from MinValue
    // (mid-bucket when absent, e.g. a histogram persisted before MinValue existed).
    private double FractionWithinBucket(ScalarBound value, int bucketIndex)
    {
        ScalarBound? lo = bucketIndex == 0 ? MinValue : Buckets[bucketIndex - 1].UpperBound;
        ScalarBound? hi = Buckets[bucketIndex].UpperBound;

        if (lo is null || hi is null)
            return 0.5;

        if (lo.Type == ColumnType.Integer64 && hi.Type == ColumnType.Integer64 && value.Type == ColumnType.Integer64)
        {
            return hi.LongValue != lo.LongValue
                ? Math.Clamp((double)(value.LongValue - lo.LongValue) / (hi.LongValue - lo.LongValue), 0.0, 1.0)
                : 0.5;
        }

        // FLOAT64, FLOAT32 and NUMERIC buckets, and a numeric constant of another type (an INT64
        // literal against a NUMERIC column), interpolate as doubles.
        if (value.TryToDouble(out double v) && lo.TryToDouble(out double l) && hi.TryToDouble(out double h) && h != l)
            return Math.Clamp((v - l) / (h - l), 0.0, 1.0);

        return 0.5;
    }
}
