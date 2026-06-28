/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// Persisted per-column min/max statistics.
///
/// Maintained as a running estimate: inserts expand the range; deletes and updates to indexed
/// columns may let the range drift upward (stale min/max are acceptable — they produce conservative
/// selectivity estimates, which is the safe direction for a cost model).
///
/// Both <see cref="Min"/> and <see cref="Max"/> are null when no values have been observed yet
/// (the column has only ever received <c>NULL</c> inserts, or no inserts have been tracked).
/// </summary>
public sealed class ColumnMinMax
{
    [JsonPropertyName("min")]
    public ScalarBound? Min { get; set; }

    [JsonPropertyName("max")]
    public ScalarBound? Max { get; set; }
}
