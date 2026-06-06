
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
/// R8 implementation scope: only <see cref="RowCount"/> is tracked and persisted.
/// <see cref="IndexEntryCounts"/> is declared for schema compatibility but is never populated
/// by the current <see cref="CamusDB.Core.Statistics.StatisticsManager"/>; all entries will
/// be <c>null</c>.  Per-column <c>Min</c>/<c>Max</c> histograms were also deferred.
///
/// R9 cost-model note: the planner will only have row-count estimates available from R8.
/// Index selectivity and column-value range estimates require a future statistics pass that
/// populates <see cref="IndexEntryCounts"/> (e.g. by hooking <c>IndexWriter</c>) and adds
/// column min/max fields.
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
    /// Reserved for a future statistics pass — always <c>null</c> in the R8 implementation.
    /// R9 cost-model callers must treat a missing entry as unknown selectivity.
    /// </summary>
    [JsonPropertyName("indexCounts")]
    public Dictionary<string, long>? IndexEntryCounts { get; set; }
}
