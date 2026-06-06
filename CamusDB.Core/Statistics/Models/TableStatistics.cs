
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// Lightweight advisory statistics for one table. Persisted to Kahuna KV after each DML
/// and reloaded on database open. All values are best-effort estimates — consumers must
/// treat them as hints and never rely on them for correctness.
///
/// R8: <see cref="RowCount"/> tracked and persisted.
/// R9b: <see cref="IndexEntryCounts"/> and <see cref="ColumnStats"/> fully populated.
/// </summary>
public sealed class TableStatistics
{
    /// <summary>
    /// Approximate number of rows in the table.
    /// <c>-1</c> means the count has never been recorded (treat as unknown).
    /// </summary>
    [JsonPropertyName("rowCount")]
    public long RowCount { get; set; } = -1;

    /// <summary>
    /// Approximate entry counts per index (key = index name).
    /// Incremented on inserts, decremented on deletes (approximate — null-column entries
    /// may differ slightly from reality). Null when no DML has been tracked yet.
    /// </summary>
    [JsonPropertyName("indexCounts")]
    public Dictionary<string, long>? IndexEntryCounts { get; set; }

    /// <summary>
    /// Per-column running min/max observed across inserts and updates (R9b).
    /// Key = column name; value = <see cref="ColumnMinMax"/> with typed <see cref="ScalarBound"/>
    /// fields. Only indexed columns are tracked; unindexed columns are absent from this dict.
    /// May drift stale on deletes (drift is acceptable — conservative estimates are safe).
    /// </summary>
    [JsonPropertyName("colStats")]
    public Dictionary<string, ColumnMinMax>? ColumnStats { get; set; }
}
