
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// One bucket in an equi-depth column histogram.
///
/// Equi-depth means each bucket captures roughly the same number of rows; the buckets
/// therefore have variable-width value ranges. <see cref="UpperBound"/> is the inclusive
/// upper boundary of the bucket. <see cref="CumulativeRows"/> is the total number of rows
/// whose column value falls at or below <see cref="UpperBound"/>. <see cref="DistinctInBucket"/>
/// is the approximate number of distinct values within this bucket.
///
/// Consumers interpolate linearly within a bucket to estimate the row fraction for a point
/// or range predicate that partially overlaps the bucket.
/// </summary>
public sealed class ColumnHistogramBucket
{
    /// <summary>Inclusive upper bound of this bucket (same typed representation as <see cref="ColumnMinMax"/>).</summary>
    [JsonPropertyName("ub")]
    public ScalarBound? UpperBound { get; set; }

    /// <summary>
    /// Cumulative row count: the number of non-null rows with a column value ≤ <see cref="UpperBound"/>.
    /// Monotonically non-decreasing across buckets.
    /// </summary>
    [JsonPropertyName("cr")]
    public long CumulativeRows { get; set; }

    /// <summary>Approximate number of distinct values within this bucket.</summary>
    [JsonPropertyName("db")]
    public long DistinctInBucket { get; set; }
}
