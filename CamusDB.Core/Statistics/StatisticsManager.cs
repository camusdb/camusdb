
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Statistics;

/// <summary>
/// Manages lightweight advisory table statistics.
///
/// Row count per table.
/// Per-index entry counts and per-column (indexed columns only) running min/max.
///
/// All values are best-effort estimates — the planner uses them as hints and never relies
/// on them for correctness.
///
/// Thread-safety:
///   <see cref="Entry.RowCount"/> and <see cref="Entry.IndexEntries"/> values are updated
///   with <see cref="Interlocked"/> operations.
///   <see cref="Entry.ColumnStats"/> is protected by <see cref="Entry.ColumnStatsLock"/>
///   (read-modify-write on struct fields requires a lock).
///   Flushes are fire-and-forget; at most one runs per table at a time.
///
/// Key layout in Kahuna: <c>{dbId}:stats:{tableId}</c> where <c>dbId</c> is
/// <see cref="CamusDB.Core.CommandsExecutor.Models.DatabaseDescriptor.Id"/> — the opaque
/// identifier, not the user-facing name, so keys are stable across future renames.
/// </summary>
public sealed class StatisticsManager
{
    // Holds the mutable in-memory snapshot for one table.
    private sealed class Entry
    {
        // Semantics depend on Loaded:
        //   Loaded=false: accumulated DML delta since entry creation (pending base merge).
        //   Loaded=true:  absolute count (base + all deltas applied so far).
        public long RowCount;

        // true once the Kahuna base has been merged into RowCount.
        public bool Loaded;

        // CAS one-shot guard: 0=no load attempted yet, 1=load in progress or done.
        public int LoadAttempted;

        // Environment.TickCount64 of the last completed flush. 0 = never flushed.
        public long LastFlushTicks;

        // 1 while a flush cycle owns this table; 0 otherwise.
        public int FlushPending;

        // Per-index entry counts. Same Loaded/delta semantics as RowCount.
        // Key = index name; value = entry count (or pending delta if !Loaded).
        public readonly ConcurrentDictionary<string, long> IndexEntries = new(StringComparer.Ordinal);

        // Per-column min/max. Only indexed columns are tracked.
        // Key = column name; value = running min/max.
        public readonly Dictionary<string, ColumnMinMax> ColumnStats = new(StringComparer.Ordinal);
        public readonly object ColumnStatsLock = new();

        // Per-column equi-depth histograms. Set wholesale by ANALYZE; not maintained
        // incrementally. Protected by ColumnStatsLock (same lock as ColumnStats for simplicity).
        public Dictionary<string, ColumnHistogram>? Histograms;

        // Per-column NDV and per-key-tuple NDV. Set wholesale by ANALYZE.
        // Protected by ColumnStatsLock.
        public Dictionary<string, long>? ColumnNdv;
        public Dictionary<string, long>? KeyNdv;
    }

    private readonly ConcurrentDictionary<string, Entry> _cache = new(StringComparer.Ordinal);

    private readonly ILogger<ICamusDB> _logger;

    public StatisticsManager(ILogger<ICamusDB> logger)
    {
        _logger = logger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public query API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Returns the estimated row count, or null if no statistics have been collected.</summary>
    public long? GetRowCountEstimate(DatabaseDescriptor database, TableDescriptor table)
    {
        string key = CacheKey(database.Id, table.Id);
        if (_cache.TryGetValue(key, out Entry? entry) && entry.Loaded)
            return entry.RowCount >= 0 ? entry.RowCount : null;

        _ = Task.Run(() => LoadAndCacheAsync(database, table));
        return null;
    }

    /// <summary>
    /// Returns the estimated entry count for <paramref name="indexName"/> in <paramref name="table"/>,
    /// or null if no statistics have been collected yet.
    /// </summary>
    public long? GetIndexEntryCount(DatabaseDescriptor database, TableDescriptor table, string indexName)
    {
        string key = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry) || !entry.Loaded)
            return null;

        return entry.IndexEntries.TryGetValue(indexName, out long count) && count >= 0 ? count : null;
    }

    /// <summary>
    /// Returns the persisted min/max bounds for <paramref name="columnName"/> in <paramref name="table"/>,
    /// or null if no statistics have been observed for that column.
    /// </summary>
    public ColumnMinMax? GetColumnMinMax(DatabaseDescriptor database, TableDescriptor table, string columnName)
    {
        string key = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry) || !entry.Loaded)
            return null;

        lock (entry.ColumnStatsLock)
        {
            return entry.ColumnStats.TryGetValue(columnName, out ColumnMinMax? mm) ? mm : null;
        }
    }

    /// <summary>
    /// Returns the equi-depth histogram for <paramref name="columnName"/>, or null if no
    /// <c>ANALYZE</c> has been run for that column yet.
    /// </summary>
    public ColumnHistogram? GetColumnHistogram(DatabaseDescriptor database, TableDescriptor table, string columnName)
    {
        string key = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry) || !entry.Loaded)
            return null;

        lock (entry.ColumnStatsLock)
        {
            return entry.Histograms is not null &&
                   entry.Histograms.TryGetValue(columnName, out ColumnHistogram? h) ? h : null;
        }
    }

    /// <summary>
    /// Returns the canonical column-tuple signature used as a key in <see cref="GetKeyNdv"/>.
    /// </summary>
    public static string KeyTupleSignature(IReadOnlyList<string> columns)
    {
        foreach (string col in columns)
        {
            if (col.Contains(',', StringComparison.Ordinal))
                throw new ArgumentException(
                    $"Column name '{col}' contains the key-tuple delimiter ',' — cannot build an unambiguous signature.",
                    nameof(columns));
        }
        return string.Join(",", columns);
    }

    /// <summary>
    /// Returns the approximate distinct-value count for <paramref name="columnName"/>, or null
    /// if <c>ANALYZE</c> has not been run for that column yet.
    /// </summary>
    public long? GetColumnNdv(DatabaseDescriptor database, TableDescriptor table, string columnName)
    {
        string key = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry) || !entry.Loaded)
            return null;

        lock (entry.ColumnStatsLock)
        {
            return entry.ColumnNdv is not null &&
                   entry.ColumnNdv.TryGetValue(columnName, out long ndv) ? ndv : null;
        }
    }

    /// <summary>
    /// Returns the approximate distinct-value count for a multi-column key prefix, or null if
    /// not available. <paramref name="columns"/> must be in index-key order.
    /// </summary>
    public long? GetKeyNdv(DatabaseDescriptor database, TableDescriptor table, IReadOnlyList<string> columns)
    {
        string key = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry) || !entry.Loaded)
            return null;

        string sig = KeyTupleSignature(columns);
        lock (entry.ColumnStatsLock)
        {
            return entry.KeyNdv is not null &&
                   entry.KeyNdv.TryGetValue(sig, out long ndv) ? ndv : null;
        }
    }

    /// <summary>
    /// Replaces the in-memory NDV sets for <paramref name="table"/> and persists them.
    /// Called by <c>ANALYZE</c> after rebuilding all column and key-tuple NDV counts.
    /// </summary>
    public async Task SetNdvAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Dictionary<string, long>? columnNdv,
        Dictionary<string, long>? keyNdv)
    {
        await LoadByIdAsync(database, table.Id).ConfigureAwait(false);

        string key = CacheKey(database.Id, table.Id);
        Entry entry = GetOrCreateEntry(database, table, key);

        lock (entry.ColumnStatsLock)
        {
            if (columnNdv is not null) entry.ColumnNdv = columnNdv;
            if (keyNdv    is not null) entry.KeyNdv    = keyNdv;
        }

        await FlushAsync(database, table).ConfigureAwait(false);
    }

    /// <summary>
    /// Replaces the in-memory histogram set for <paramref name="table"/> and persists it.
    /// Called by <c>ANALYZE</c> after rebuilding all column histograms.
    /// </summary>
    public async Task SetHistogramsAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Dictionary<string, ColumnHistogram> histograms)
    {
        await LoadByIdAsync(database, table.Id).ConfigureAwait(false);

        string key = CacheKey(database.Id, table.Id);
        Entry entry = GetOrCreateEntry(database, table, key);

        lock (entry.ColumnStatsLock)
        {
            entry.Histograms = histograms;
        }

        await FlushAsync(database, table).ConfigureAwait(false);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public DML tracking API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Records that <paramref name="delta"/> rows were inserted and fires a background flush.
    /// No index/column tracking — use the overload with <paramref name="rowValues"/> for additional stats.
    /// </summary>
    public void TrackInsert(DatabaseDescriptor database, TableDescriptor table, int delta)
    {
        if (delta <= 0) return;
        ApplyRowDelta(database, table, delta);
        ScheduleFlush(database, table);
    }

    /// <summary>
    /// Records that <paramref name="delta"/> rows were inserted, tracking per-index entry
    /// counts and per-column min/max for all indexed columns.
    /// </summary>
    public void TrackInsert(
        DatabaseDescriptor database,
        TableDescriptor table,
        int delta,
        IReadOnlyList<Dictionary<string, ColumnValue>> rowValues)
    {
        if (delta <= 0) return;

        ApplyRowDelta(database, table, delta);

        string key = CacheKey(database.Id, table.Id);
        Entry entry = GetOrCreateEntry(database, table, key);

        // Collect the set of writable indexes once to avoid repeated dict lookup.
        IReadOnlyList<TableIndexSchema> writableIndexes = GetWritableIndexes(table);

        foreach (Dictionary<string, ColumnValue> row in rowValues)
            ApplyInsertStats(entry, writableIndexes, row);

        ScheduleFlush(database, table);
    }

    /// <summary>
    /// Records that <paramref name="delta"/> rows were deleted and schedules a background flush.
    /// Decrements entry counts for all writable indexes by <paramref name="delta"/> (approximate —
    /// null-column rows may not have had entries for all indexes).
    /// Min/max is not recomputed on delete (let drift; advisory only).
    /// </summary>
    public void TrackDelete(DatabaseDescriptor database, TableDescriptor table, int delta)
    {
        if (delta <= 0) return;

        ApplyRowDelta(database, table, -delta);

        string key = CacheKey(database.Id, table.Id);
        Entry entry = GetOrCreateEntry(database, table, key);

        foreach (TableIndexSchema index in GetWritableIndexes(table))
        {
            entry.IndexEntries.AddOrUpdate(
                index.Name,
                addValue: -delta,
                updateValueFactory: (_, cur) => Math.Max(0, cur - delta));
        }

        ScheduleFlush(database, table);
    }

    /// <summary>
    /// Records that rows were updated.
    /// Updates min/max for any indexed columns present in <paramref name="updatedValues"/>.
    /// Entry counts are not changed: a non-null→non-null update keeps the same index entry.
    /// (Updates from null→non-null or non-null→null require the old row value which is not
    /// available at this call site; the count drift is acceptable for advisory stats.)
    /// </summary>
    public void TrackUpdate(
        DatabaseDescriptor database,
        TableDescriptor table,
        int delta,
        IReadOnlyDictionary<string, ColumnValue>? updatedValues)
    {
        if (updatedValues is null || updatedValues.Count == 0)
            return;

        string key = CacheKey(database.Id, table.Id);
        Entry entry = GetOrCreateEntry(database, table, key);

        IReadOnlyList<TableIndexSchema> writableIndexes = GetWritableIndexes(table);

        lock (entry.ColumnStatsLock)
        {
            foreach (TableIndexSchema index in writableIndexes)
            {
                if (index.Columns is not { Length: > 0 })
                    continue;

                string col = index.Columns[0];
                if (!updatedValues.TryGetValue(col, out ColumnValue? newVal))
                    continue;

                if (newVal.Type is ColumnType.Null)
                    continue; // null does not produce an index entry; don't expand range

                UpdateMinMax(entry.ColumnStats, col, newVal);
            }
        }

        ScheduleFlush(database, table);
    }

    // Keep old no-op overload signature for callers that don't supply updated values.
    public void TrackUpdate(DatabaseDescriptor database, TableDescriptor table, int delta) { }

    /// <summary>Explicitly flushes statistics for one table to Kahuna, awaiting completion.</summary>
    public async Task FlushAsync(DatabaseDescriptor database, TableDescriptor table)
    {
        try
        {
            await FlushInternalAsync(database, table).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Explicit stats flush failed for table {Table}", table.Name);
        }
    }

    /// <summary>Flushes in-memory statistics for every opened table before closing a database.</summary>
    public async Task FlushAllAsync(DatabaseDescriptor database)
    {
        string prefix = string.Concat(database.Id, ":");

        foreach (KeyValuePair<string, Entry> kv in _cache)
        {
            if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            string tableId = kv.Key[prefix.Length..];

            if (!database.TableDescriptors.TryGetValue(tableId, out Nito.AsyncEx.AsyncLazy<TableDescriptor>? lazy))
            {
                lazy = database.TableDescriptors.Values
                    .FirstOrDefault(l => l.IsStarted && l.Task.IsCompletedSuccessfully && l.Task.Result.Id == tableId);
            }

            if (lazy is null || !lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
                continue;

            TableDescriptor table = lazy.Task.Result;
            try
            {
                await FlushInternalAsync(database, table).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Close-hook stats flush failed for table {Table}", table.Name);
            }
        }
    }

    /// <summary>Loads persisted statistics from Kahuna for the given table ID into the cache.</summary>
    public async Task LoadByIdAsync(DatabaseDescriptor database, string tableId)
    {
        string key = CacheKey(database.Id, tableId);
        if (_cache.TryGetValue(key, out Entry? existing) && existing.Loaded)
            return;

        try
        {
            Entry entry = _cache.GetOrAdd(key, _ => new Entry());

            if (Interlocked.CompareExchange(ref entry.LoadAttempted, 1, 0) != 0)
                return;

            TableStatistics? loaded = await LoadFromKahunaByIdAsync(database, tableId).ConfigureAwait(false);

            if (loaded is not null)
            {
                MergeBaseIntoEntry(entry, loaded.RowCount >= 0 ? loaded.RowCount : 0);
                MergeIndexCounts(entry, loaded.IndexEntryCounts);
                MergeColumnStats(entry, loaded.ColumnStats);
                MergeHistograms(entry, loaded.Histograms);
                MergeNdv(entry, loaded.ColumnNdv, loaded.KeyNdv);
            }

            entry.Loaded = true;
        }
        catch (Exception ex)
        {
            if (_cache.TryGetValue(key, out Entry? failed))
                Interlocked.Exchange(ref failed.LoadAttempted, 0);

            _logger.LogWarning(ex, "Stats load failed for table id {TableId}", tableId);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal tracking helpers
    // ─────────────────────────────────────────────────────────────────────────

    private void ApplyRowDelta(DatabaseDescriptor database, TableDescriptor table, long delta)
    {
        string key = CacheKey(database.Id, table.Id);

        bool isFirstEntry = false;
        Entry entry = _cache.GetOrAdd(key, _ =>
        {
            isFirstEntry = true;
            return new Entry();
        });

        Interlocked.Add(ref entry.RowCount, delta);

        if (entry.Loaded)
        {
            long current = Interlocked.Read(ref entry.RowCount);
            if (current < 0)
                Interlocked.CompareExchange(ref entry.RowCount, 0, current);
        }

        if (isFirstEntry)
            _ = Task.Run(() => LoadAndCacheAsync(database, table));
    }

    private Entry GetOrCreateEntry(DatabaseDescriptor database, TableDescriptor table, string key) =>
        _cache.GetOrAdd(key, _ => new Entry());

    private static IReadOnlyList<TableIndexSchema> GetWritableIndexes(TableDescriptor table)
    {
        if (table.Indexes is not { Count: > 0 })
            return [];

        var result = new List<TableIndexSchema>(table.Indexes.Count);
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (SchemaElementStateRules.IsWritable(kv.Value))
                result.Add(kv.Value);
        }
        return result;
    }

    private static void ApplyInsertStats(
        Entry entry,
        IReadOnlyList<TableIndexSchema> writableIndexes,
        Dictionary<string, ColumnValue> row)
    {
        lock (entry.ColumnStatsLock)
        {
            foreach (TableIndexSchema index in writableIndexes)
            {
                if (index.Columns is not { Length: > 0 })
                    continue;

                string col = index.Columns[0];
                if (!row.TryGetValue(col, out ColumnValue? val) || val.Type == ColumnType.Null)
                    continue; // null-valued columns don't produce index entries

                // Entry count: +1 for this row's contribution to this index.
                entry.IndexEntries.AddOrUpdate(index.Name, addValue: 1, updateValueFactory: (_, c) => c + 1);

                // Min/max: expand the tracked range.
                UpdateMinMax(entry.ColumnStats, col, val);
            }
        }
    }

    // Must be called under entry.ColumnStatsLock.
    private static void UpdateMinMax(Dictionary<string, ColumnMinMax> stats, string col, ColumnValue val)
    {
        if (val.Type is ColumnType.Null or ColumnType.Bool)
            return; // unordered types: not useful for range selectivity

        ScalarBound bound = ScalarBound.FromColumnValue(val);

        if (!stats.TryGetValue(col, out ColumnMinMax? mm))
        {
            stats[col] = new ColumnMinMax { Min = bound, Max = bound };
            return;
        }

        if (mm.Min is null || bound.CompareTo(mm.Min) < 0)
            mm.Min = bound;

        if (mm.Max is null || bound.CompareTo(mm.Max) > 0)
            mm.Max = bound;
    }

    private static void MergeBaseIntoEntry(Entry entry, long loadedBase)
    {
        long pending, merged;
        do
        {
            pending = Interlocked.Read(ref entry.RowCount);
            merged = Math.Max(0, loadedBase + pending);
        } while (Interlocked.CompareExchange(ref entry.RowCount, merged, pending) != pending);
    }

    private static void MergeIndexCounts(Entry entry, Dictionary<string, long>? persisted)
    {
        if (persisted is null) return;
        foreach (KeyValuePair<string, long> kv in persisted)
        {
            entry.IndexEntries.AddOrUpdate(
                kv.Key,
                addValue: kv.Value,
                // in-memory delta (accumulated before load) is added to the persisted base
                updateValueFactory: (_, delta) => Math.Max(0, kv.Value + delta));
        }
    }

    private static void MergeColumnStats(Entry entry, Dictionary<string, ColumnMinMax>? persisted)
    {
        if (persisted is null) return;

        lock (entry.ColumnStatsLock)
        {
            foreach (KeyValuePair<string, ColumnMinMax> kv in persisted)
            {
                if (!entry.ColumnStats.TryGetValue(kv.Key, out ColumnMinMax? existing))
                {
                    // No in-memory obs yet — take the persisted value directly.
                    entry.ColumnStats[kv.Key] = new ColumnMinMax
                    {
                        Min = kv.Value.Min,
                        Max = kv.Value.Max,
                    };
                    continue;
                }

                // Merge: take the wider range of persisted vs. in-memory.
                if (kv.Value.Min is not null && (existing.Min is null || kv.Value.Min.CompareTo(existing.Min) < 0))
                    existing.Min = kv.Value.Min;

                if (kv.Value.Max is not null && (existing.Max is null || kv.Value.Max.CompareTo(existing.Max) > 0))
                    existing.Max = kv.Value.Max;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Flush scheduling
    // ─────────────────────────────────────────────────────────────────────────

    private void ScheduleFlush(DatabaseDescriptor database, TableDescriptor table)
    {
        int interval = CamusDBConfig.StatsFlushIntervalMs;
        if (interval < 0)
            return;

        string key = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry))
            return;

        if (Interlocked.CompareExchange(ref entry.FlushPending, 1, 0) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                if (interval > 0)
                {
                    long elapsed = Environment.TickCount64 - Interlocked.Read(ref entry.LastFlushTicks);
                    long wait = interval - elapsed;
                    if (wait > 0)
                        await Task.Delay((int)Math.Min(wait, interval)).ConfigureAwait(false);
                }

                await FlushInternalAsync(database, table).ConfigureAwait(false);
                Interlocked.Exchange(ref entry.LastFlushTicks, Environment.TickCount64);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Stats flush failed for table {Table}", table.Name);
            }
            finally
            {
                Interlocked.Exchange(ref entry.FlushPending, 0);
            }
        });
    }

    private async Task FlushInternalAsync(DatabaseDescriptor database, TableDescriptor table)
    {
        string cacheKey = CacheKey(database.Id, table.Id);
        if (!_cache.TryGetValue(cacheKey, out Entry? entry))
            return;

        if (!entry.Loaded)
            return;

        // Snapshot all stats under the column-stats lock to avoid partial reads.
        Dictionary<string, long>? indexSnapshot = null;
        Dictionary<string, ColumnMinMax>? colSnapshot = null;
        Dictionary<string, ColumnHistogram>? histSnapshot = null;
        Dictionary<string, long>? colNdvSnapshot = null;
        Dictionary<string, long>? keyNdvSnapshot = null;

        lock (entry.ColumnStatsLock)
        {
            if (entry.IndexEntries.Count > 0)
                indexSnapshot = new Dictionary<string, long>(entry.IndexEntries, StringComparer.Ordinal);

            if (entry.ColumnStats.Count > 0)
            {
                colSnapshot = new Dictionary<string, ColumnMinMax>(entry.ColumnStats.Count, StringComparer.Ordinal);
                foreach (KeyValuePair<string, ColumnMinMax> kv in entry.ColumnStats)
                {
                    colSnapshot[kv.Key] = new ColumnMinMax
                    {
                        Min = kv.Value.Min,
                        Max = kv.Value.Max,
                    };
                }
            }

            // Histograms and NDV dicts are replaced wholesale by ANALYZE — shallow copies suffice.
            if (entry.Histograms is { Count: > 0 })
                histSnapshot = new Dictionary<string, ColumnHistogram>(entry.Histograms, StringComparer.Ordinal);

            if (entry.ColumnNdv is { Count: > 0 })
                colNdvSnapshot = new Dictionary<string, long>(entry.ColumnNdv, StringComparer.Ordinal);

            if (entry.KeyNdv is { Count: > 0 })
                keyNdvSnapshot = new Dictionary<string, long>(entry.KeyNdv, StringComparer.Ordinal);
        }

        TableStatistics snapshot = new()
        {
            RowCount         = Interlocked.Read(ref entry.RowCount),
            IndexEntryCounts = indexSnapshot,
            ColumnStats      = colSnapshot,
            Histograms       = histSnapshot,
            ColumnNdv        = colNdvSnapshot,
            KeyNdv           = keyNdvSnapshot,
        };

        byte[] bytes = MetaJsonSerializer.Serialize(snapshot, MetaJsonContext.Default.TableStatistics);

        string kahunaKey = KahunaKey(database.Id, table.Id);
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            // Fold the write and its lock into the coordinator working set so the commit persists the stats
            // blob; a fresh id per call is sufficient (no retry loop here).
            (KeyValueResponseType lockType, _, KeyValueDurability lockDurability, _) =
                await database.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
                    tx.TransactionId, kahunaKey, 0, KeyValueDurability.Persistent, CancellationToken.None,
                    coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()
                ).ConfigureAwait(false);

            if (lockType != KeyValueResponseType.Locked)
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                return;
            }

            tx.TrackLock(kahunaKey, lockDurability);

            (KeyValueResponseType setType, _, _) = await database.Kahuna.Kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, kahunaKey, bytes, null, -1,
                KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()
            ).ConfigureAwait(false);

            if (setType == KeyValueResponseType.Set)
            {
                tx.TrackModified(kahunaKey, KeyValueDurability.Persistent);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            else
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }

    private async Task LoadAndCacheAsync(DatabaseDescriptor database, TableDescriptor table)
    {
        string key = CacheKey(database.Id, table.Id);

        if (_cache.TryGetValue(key, out Entry? existing) && existing.Loaded)
            return;

        try
        {
            Entry entry = _cache.GetOrAdd(key, _ => new Entry());

            if (Interlocked.CompareExchange(ref entry.LoadAttempted, 1, 0) != 0)
                return;

            TableStatistics? loaded = await LoadFromKahunaAsync(database, table).ConfigureAwait(false);

            if (loaded is not null)
            {
                MergeBaseIntoEntry(entry, loaded.RowCount >= 0 ? loaded.RowCount : 0);
                MergeIndexCounts(entry, loaded.IndexEntryCounts);
                MergeColumnStats(entry, loaded.ColumnStats);
                MergeHistograms(entry, loaded.Histograms);
                MergeNdv(entry, loaded.ColumnNdv, loaded.KeyNdv);
            }

            entry.Loaded = true;
        }
        catch (Exception ex)
        {
            if (_cache.TryGetValue(key, out Entry? failed))
                Interlocked.Exchange(ref failed.LoadAttempted, 0);

            _logger.LogWarning(ex, "Stats load failed for table {Table}", table.Name);
        }
    }

    private Task<TableStatistics?> LoadFromKahunaAsync(DatabaseDescriptor database, TableDescriptor table)
        => LoadFromKahunaByIdAsync(database, table.Id);

    private async Task<TableStatistics?> LoadFromKahunaByIdAsync(DatabaseDescriptor database, string tableId)
    {
        string kahunaKey = KahunaKey(database.Id, tableId);

        // Statistics are advisory metadata read for planning/cost estimation; a stale or
        // missing value is harmless. Use a synthetic read-only transaction (HLCTimestamp.Zero)
        // so Kahuna performs a non-transactional read-committed point read — no
        // START-TRANSACTION / ROLLBACK round-trips and no MVCC context, matching how the
        // table scan itself reads rows. Previously this opened a full interactive transaction
        // per stats load and immediately rolled it back.
        KvTransaction tx = database.Transactions.CreateReadOnlyTransaction();

        try
        {
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
                await database.Kahuna.Kahuna.LocateAndTryGetValue(
                    tx.TransactionId, kahunaKey, -1,
                    Kommander.Time.HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (getType == KeyValueResponseType.Get && entry?.Value is not null)
                return MetaJsonSerializer.DeserializeCompat(entry.Value, MetaJsonContext.Default.TableStatistics);

            return null;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    // Histograms/NDV are set wholesale by ANALYZE. On load, take the persisted dict when no
    // in-memory version exists yet; otherwise the in-process ANALYZE result takes precedence.
    private static void MergeHistograms(Entry entry, Dictionary<string, ColumnHistogram>? persisted)
    {
        if (persisted is null || persisted.Count == 0) return;

        lock (entry.ColumnStatsLock)
        {
            entry.Histograms ??= new Dictionary<string, ColumnHistogram>(persisted, StringComparer.Ordinal);
        }
    }

    private static void MergeNdv(Entry entry, Dictionary<string, long>? persistedCol, Dictionary<string, long>? persistedKey)
    {
        if (persistedCol is null && persistedKey is null) return;

        lock (entry.ColumnStatsLock)
        {
            if (persistedCol is { Count: > 0 })
                entry.ColumnNdv ??= new Dictionary<string, long>(persistedCol, StringComparer.Ordinal);

            if (persistedKey is { Count: > 0 })
                entry.KeyNdv ??= new Dictionary<string, long>(persistedKey, StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// Replaces the in-memory row count, column min/max, and index entry counts for
    /// <paramref name="table"/> with freshly-scanned values from <c>ANALYZE</c>. Does not
    /// trigger a flush — callers must call <see cref="SetHistogramsAsync"/> or
    /// <see cref="SetNdvAsync"/> afterwards (both flush internally). Marks the entry as Loaded.
    /// </summary>
    internal void SeedColumnStats(
        DatabaseDescriptor database,
        TableDescriptor table,
        long rowCount,
        Dictionary<string, ColumnMinMax> columnStats,
        Dictionary<string, long> indexCounts)
    {
        string key = CacheKey(database.Id, table.Id);
        Entry entry = _cache.GetOrAdd(key, _ => new Entry());

        Interlocked.Exchange(ref entry.RowCount, rowCount);

        lock (entry.ColumnStatsLock)
        {
            entry.ColumnStats.Clear();
            foreach (KeyValuePair<string, ColumnMinMax> kv in columnStats)
                entry.ColumnStats[kv.Key] = kv.Value;
        }

        foreach (KeyValuePair<string, long> kv in indexCounts)
            entry.IndexEntries[kv.Key] = kv.Value;

        entry.Loaded = true;
    }

    /// <summary>
    /// Seeds a known row count for <paramref name="table"/> without a Kahuna round-trip.
    /// Marks the entry as <c>Loaded</c> so <see cref="GetRowCountEstimate"/> returns it immediately.
    /// Intended for unit tests only; do not call from production paths.
    /// </summary>
    internal void SeedRowCountForTesting(DatabaseDescriptor database, TableDescriptor table, long rowCount)
    {
        string key = CacheKey(database.Id, table.Id);
        Entry entry = _cache.GetOrAdd(key, _ => new Entry());
        Interlocked.Exchange(ref entry.RowCount, rowCount);
        entry.Loaded = true;
    }

    /// <summary>
    /// Drops the cached in-memory entry for <paramref name="table"/> so the next access
    /// reloads it from Kahuna. Intended for unit tests only.
    /// </summary>
    internal void EvictForTesting(DatabaseDescriptor database, TableDescriptor table)
        => _cache.TryRemove(CacheKey(database.Id, table.Id), out _);

    /// <summary>
    /// When true, the join planner emits <c>MergeJoinNode</c> for any inner equi-join instead
    /// of the normal cost-based algorithm selection. Intended for unit tests only.
    /// </summary>
    internal bool ForceMergeJoinForTesting { get; set; }

    /// <summary>
    /// When true, the join planner emits <c>HashJoinNode</c> for any inner equi-join instead
    /// of the normal algorithm selection. Intended for parity-sweep tests only.
    /// </summary>
    internal bool ForceHashJoinForTesting { get; set; }

    /// <summary>
    /// When true, the join planner emits <c>NestedLoopJoinNode</c> for every inner join
    /// regardless of available indexes or equi-keys. Intended for parity-sweep tests only.
    /// </summary>
    internal bool ForceNestedLoopForTesting { get; set; }

    /// <summary>
    /// Counts how many times the Grace hash join has fallen back to nested-loop for a single
    /// skewed partition that could not be split below the threshold within the recursion depth
    /// limit. Incremented by <c>QueryJoinExecutor.JoinPartitionAsync</c>. Test-only; not thread-safe.
    /// </summary>
    internal int HashJoinNljPartitionFallbackCount { get; set; }

    /// <summary>
    /// Counts how many times a hash join routed to the Grace/hybrid partitioning path because the
    /// build side exceeded <c>CamusDBConfig.SpillEffectiveThreshold</c> with spill enabled.
    /// Incremented by <c>QueryJoinExecutor.GraceHashJoinAsync</c>. Lets a test prove the Grace path
    /// was actually taken (vs the in-memory hash join) independent of result values. Test-only;
    /// not thread-safe.
    /// </summary>
    internal int HashJoinGracePathCount { get; set; }

    /// <summary>
    /// Counts how many times <c>QueryAggregator.AggregatePartitionAsync</c> recursively
    /// repartitioned a GROUP BY partition because its distinct-group count exceeded
    /// <c>CamusDBConfig.SpillEffectiveThreshold</c>. Lets a test prove the recursion path was
    /// taken rather than unbounded dictionary growth. Test-only; not thread-safe.
    /// </summary>
    internal int GroupByPartitionRecursionCount { get; set; }

    /// <summary>
    /// Counts how many times <c>InSubqueryExecutor.MaterializeAsync</c> overflowed the
    /// in-memory buffer to a spill file when collecting the value set for an uncorrelated
    /// IN/NOT IN subquery. A positive value proves the spill path was taken rather than
    /// accumulating all values in a plain <c>List&lt;ColumnValue&gt;</c>. Test-only; not thread-safe.
    /// </summary>
    internal int InSubqueryValueListSpillCount { get; set; }

    /// <summary>
    /// Tracks the largest single batch size passed to
    /// <see cref="KvTableStore.DeleteRowsBatch"/> during a DELETE operation. When the DELETE
    /// mutation phase runs in bounded chunks this value never exceeds the configured chunk
    /// size. A value larger than the chunk size proves the unbounded single-batch path was
    /// taken. Test-only; not thread-safe.
    /// </summary>
    internal int DeleteBatchMaxChunkSeen { get; set; }

    private static string CacheKey(string dbId, string tableId)
        => string.Concat(dbId, ":", tableId);

    private static string KahunaKey(string dbId, string tableId)
        => string.Concat(dbId, ":stats:", tableId);
}
