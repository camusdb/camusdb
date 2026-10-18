/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// One of the most common values of a column, with the number of rows that hold it, as
/// <c>ANALYZE</c> counted them in the rows the histogram was built from
/// (<see cref="ColumnHistogram.TopValues"/>). An equi-depth histogram resolves a value only when
/// it fills a whole bucket, one percent of the rows at the default bucket count; the common values
/// below that line (the popular movies of a cast table, each with half a percent of the rows) are
/// where a skewed join or an equality on a common value gets its size, so <c>ANALYZE</c> keeps them
/// by name. <see cref="Rows"/> is a count within <see cref="ColumnHistogram.TotalRows"/>, so a
/// histogram built from a sample still gives a share.
/// </summary>
public sealed class ColumnTopValue
{
    /// <summary>The value, in the same typed form as a bucket bound.</summary>
    [JsonPropertyName("v")]
    public ScalarBound? Value { get; set; }

    /// <summary>The rows that hold the value, out of <see cref="ColumnHistogram.TotalRows"/>.</summary>
    [JsonPropertyName("n")]
    public long Rows { get; set; }
}
