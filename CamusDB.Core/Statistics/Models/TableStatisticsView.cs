/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// An immutable point-in-time copy of one table's statistics, produced for introspection
/// (<c>SHOW STATISTICS</c>) rather than for the cost model.
///
/// <para>Why a copy rather than the live cache entry: the entry's dictionaries are mutated by
/// concurrent DML under a lock, so handing them to a row projection that yields lazily would let a
/// reader enumerate a dictionary while an insert is writing to it. Everything here is snapshotted
/// under that lock, so the projection can be as lazy as it likes.</para>
///
/// <para><see cref="ColumnStats"/> entries are fresh <see cref="ColumnMinMax"/> instances because the
/// live ones are updated in place. <see cref="ColumnHistogram"/> instances are shared instead of
/// deep-copied: <c>ANALYZE</c> replaces them wholesale rather than editing buckets, so a shared
/// reference can never be seen half-rebuilt — treat them as read-only.</para>
///
/// <para>Every value is advisory and may be absent: a table that has never been written to and never
/// analyzed yields a view with a null <see cref="RowCount"/> and empty dictionaries.</para>
/// </summary>
public sealed class TableStatisticsView
{
    /// <summary>Estimated row count, or null when no count has ever been recorded.</summary>
    public long? RowCount { get; init; }

    /// <summary>Estimated entry count per index name. Empty when no DML has been tracked.</summary>
    public IReadOnlyDictionary<string, long> IndexEntryCounts { get; init; } =
        new Dictionary<string, long>(StringComparer.Ordinal);

    /// <summary>Per-column observed min/max. Only indexed columns are tracked.</summary>
    public IReadOnlyDictionary<string, ColumnMinMax> ColumnStats { get; init; } =
        new Dictionary<string, ColumnMinMax>(StringComparer.Ordinal);

    /// <summary>Per-column equi-depth histograms. Empty until an <c>ANALYZE</c> has run.</summary>
    public IReadOnlyDictionary<string, ColumnHistogram> Histograms { get; init; } =
        new Dictionary<string, ColumnHistogram>(StringComparer.Ordinal);

    /// <summary>Per-column approximate distinct-value counts. Empty until an <c>ANALYZE</c> has run.</summary>
    public IReadOnlyDictionary<string, long> ColumnNdv { get; init; } =
        new Dictionary<string, long>(StringComparer.Ordinal);

    /// <summary>
    /// Approximate distinct-value counts per composite-index key prefix, keyed by
    /// <see cref="StatisticsManager.KeyTupleSignature"/> output.
    /// </summary>
    public IReadOnlyDictionary<string, long> KeyNdv { get; init; } =
        new Dictionary<string, long>(StringComparer.Ordinal);

    /// <summary>Row mutations observed since the last completed <c>ANALYZE</c>.</summary>
    public long MutationsSinceAnalyze { get; init; }

    /// <summary>Snapshot timestamp the last <c>ANALYZE</c> read; the zero timestamp when never analyzed.</summary>
    public HLCTimestamp LastAnalyzedAt { get; init; }

    /// <summary>
    /// True when the values came from this node's live cache entry (and therefore include mutations
    /// it has not flushed yet); false when they were point-read from the persisted blob. Recorded so
    /// callers can explain a discrepancy between nodes rather than guess at one.
    /// </summary>
    public bool FromLocalCache { get; init; }
}
