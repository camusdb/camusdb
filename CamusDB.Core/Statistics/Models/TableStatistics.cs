
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
/// <see cref="RowCount"/> tracked and persisted.
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

    /// <summary>
    /// Per-column equi-depth histograms built by <c>ANALYZE</c>. Null until the first
    /// <c>ANALYZE</c> run. Between <c>ANALYZE</c> runs, buckets may drift; that is acceptable
    /// because statistics are advisory. Old blobs that lack this field deserialize to null
    /// (backward-compatible: the estimator falls back to min/max when no histogram is present).
    /// Key = column name (same namespace as <see cref="ColumnStats"/>).
    /// </summary>
    [JsonPropertyName("histograms")]
    public Dictionary<string, ColumnHistogram>? Histograms { get; set; }

    /// <summary>
    /// Approximate distinct-value count (NDV) per column, built by <c>ANALYZE</c>.
    /// Key = column name. A uniform column reports NDV ≈ <see cref="RowCount"/>; a
    /// low-cardinality column reports a small NDV. Null until the first <c>ANALYZE</c> run.
    ///
    /// v1 method: exact scan-based count (HyperLogLog / sampling deferred to a later release).
    /// Used by the cardinality estimator for equality selectivity (1/NDV) and join cardinality
    /// (|A ⋈ B| ≈ |A|·|B| / max(NDV_A, NDV_B)).
    /// </summary>
    [JsonPropertyName("colNdv")]
    public Dictionary<string, long>? ColumnNdv { get; set; }

    /// <summary>
    /// Approximate distinct-value count for multi-column index/PK key prefixes, built by
    /// <c>ANALYZE</c>. Key = column-tuple signature produced by
    /// <see cref="StatisticsManager.KeyTupleSignature"/>: columns joined with <c>","</c>
    /// in index order (e.g. <c>"city,year"</c> for a composite key on those two columns).
    /// Null until the first <c>ANALYZE</c> run.
    ///
    /// Used by the cardinality estimator when predicate columns form the prefix of a composite
    /// index — <see cref="KeyNdv"/> is more accurate than the independence product of individual
    /// <see cref="ColumnNdv"/> values when the columns are correlated.
    /// </summary>
    [JsonPropertyName("keyNdv")]
    public Dictionary<string, long>? KeyNdv { get; set; }
}
