
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Statistics;

/// <summary>
/// Manages lightweight advisory table statistics (R8).
///
/// Statistics are loaded lazily from Kahuna KV on the first call to
/// <see cref="GetRowCountEstimate"/> and flushed back asynchronously after each DML
/// operation that changes the row count.  All values are best-effort estimates —
/// the planner uses them as hints and never relies on them for correctness.
///
/// Thread-safety: the in-memory cache uses a <see cref="ConcurrentDictionary"/> and
/// <see cref="Interlocked"/> for atomic counter updates.  Flushes are fire-and-forget
/// background tasks; at most one flush per table runs at a time (guarded by a
/// per-table <see cref="SemaphoreSlim"/>).
///
/// Key layout in Kahuna: <c>{dbname}:stats:{tableId}</c>
///
/// R8 scope: only <see cref="TableStatistics.RowCount"/> is tracked. Index entry counts
/// (<see cref="TableStatistics.IndexEntryCounts"/>) and per-column min/max histograms are
/// declared in the model but never populated here. The R9 cost model will therefore only
/// have row counts available; index selectivity estimates require a future pass that hooks
/// the index writer path.
/// </summary>
public sealed class StatisticsManager
{
    // Holds the mutable in-memory snapshot for one table.
    // RowCount is accessed exclusively via Interlocked so no extra locking is needed.
    private sealed class Entry
    {
        // Semantics depend on Loaded:
        //   Loaded=false: accumulated DML delta since entry creation (pending base merge).
        //   Loaded=true:  absolute count (base + all deltas applied so far).
        public long RowCount;

        // true once the Kahuna base has been merged into RowCount.
        public bool Loaded;

        // CAS one-shot guard: 0=no load attempted yet, 1=load in progress or done.
        // Prevents two concurrent background loads from double-adding the base.
        public int LoadAttempted;

        // Environment.TickCount64 of the last completed flush. 0 = never flushed.
        public long LastFlushTicks;

        // 1 while a flush cycle owns this table (debounce guard); 0 otherwise.
        public int FlushPending;
    }

    // Cache key → mutable in-memory entry.
    private readonly ConcurrentDictionary<string, Entry> _cache = new(StringComparer.Ordinal);

    private readonly ILogger<ICamusDB> _logger;

    public StatisticsManager(ILogger<ICamusDB> logger)
    {
        _logger = logger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the estimated row count for <paramref name="table"/>, or <c>null</c> if no
    /// statistics have been collected yet.  May trigger a lazy Kahuna load on the first call.
    /// </summary>
    public long? GetRowCountEstimate(DatabaseDescriptor database, TableDescriptor table)
    {
        string key = CacheKey(database.Name, table.Id);
        if (_cache.TryGetValue(key, out Entry? entry) && entry.Loaded)
            return entry.RowCount >= 0 ? entry.RowCount : null;

        // Trigger a background load; return null for this call.
        _ = Task.Run(() => LoadAndCacheAsync(database, table));
        return null;
    }

    /// <summary>
    /// Returns the estimated row count using raw cache-key components, bypassing the need for a
    /// fully-opened <see cref="TableDescriptor"/>. Useful when only the table ID is known
    /// (e.g. after a database reopen before any table-opening DML).
    /// Returns <c>null</c> when no in-memory entry has been loaded yet.
    /// </summary>
    public long? GetRowCountEstimate(string dbname, string tableId)
    {
        string key = CacheKey(dbname, tableId);
        if (_cache.TryGetValue(key, out Entry? entry) && entry.Loaded)
            return entry.RowCount >= 0 ? entry.RowCount : null;
        return null;
    }

    /// <summary>
    /// Records that <paramref name="delta"/> rows were inserted and fires a background flush.
    /// </summary>
    public void TrackInsert(DatabaseDescriptor database, TableDescriptor table, int delta)
    {
        if (delta <= 0)
            return;

        ApplyDelta(database, table, delta);
        ScheduleFlush(database, table);
    }

    /// <summary>
    /// Records that <paramref name="delta"/> rows were deleted and schedules a background flush.
    /// </summary>
    public void TrackDelete(DatabaseDescriptor database, TableDescriptor table, int delta)
    {
        if (delta <= 0)
            return;

        ApplyDelta(database, table, -delta);
        ScheduleFlush(database, table);
    }

    // Updates are row-preserving; no row-count change. Placeholder for index-count tracking later.
    public void TrackUpdate(DatabaseDescriptor database, TableDescriptor table, int delta) { }

    /// <summary>
    /// Explicitly flushes the in-memory statistics for <paramref name="table"/> to Kahuna,
    /// awaiting completion. Use before closing a database to guarantee persistence.
    /// No-op if there is nothing cached for the table.
    /// </summary>
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

    /// <summary>
    /// Flushes in-memory statistics for every table in <paramref name="database"/> that has
    /// been opened and touched by DML in this session. Call this before closing a database
    /// to guarantee the tail deltas are persisted even when the debounce cycle has not fired.
    /// </summary>
    public async Task FlushAllAsync(DatabaseDescriptor database)
    {
        string prefix = string.Concat(database.Name, ":");

        foreach (KeyValuePair<string, Entry> kv in _cache)
        {
            if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            // Extract the tableId portion after the db-name prefix.
            string tableId = kv.Key[prefix.Length..];

            // Resolve the TableDescriptor only if the lazy has already been started
            // (i.e. the table was opened during this session). We do not want to trigger
            // a table open solely to flush stats for a table that was never accessed.
            if (!database.TableDescriptors.TryGetValue(tableId, out Nito.AsyncEx.AsyncLazy<TableDescriptor>? lazy))
            {
                // TableDescriptors is keyed by table name; try to find by matching Id.
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

    /// <summary>
    /// Loads persisted statistics from Kahuna for the given table ID and populates the
    /// in-memory cache. Call this when only the table ID is available (e.g. after reopen)
    /// and you need the estimate to be available for <see cref="GetRowCountEstimate(string,string)"/>.
    /// </summary>
    public async Task LoadByIdAsync(DatabaseDescriptor database, string tableId)
    {
        string key = CacheKey(database.Name, tableId);
        if (_cache.TryGetValue(key, out Entry? existing) && existing.Loaded)
            return;

        try
        {
            Entry entry = _cache.GetOrAdd(key, _ => new Entry());

            if (Interlocked.CompareExchange(ref entry.LoadAttempted, 1, 0) != 0)
                return; // another task already owns this load

            TableStatistics? loaded = await LoadFromKahunaByIdAsync(database, tableId).ConfigureAwait(false);

            if (loaded is not null)
                MergeBaseIntoEntry(entry, loaded.RowCount >= 0 ? loaded.RowCount : 0);

            entry.Loaded = true;
        }
        catch (Exception ex)
        {
            // Release the one-shot guard so a transient failure does not permanently wedge
            // this table's stats (no load → never Loaded → never flushed) for the session.
            if (_cache.TryGetValue(key, out Entry? failed))
                Interlocked.Exchange(ref failed.LoadAttempted, 0);

            _logger.LogWarning(ex, "Stats load failed for table id {TableId} — estimates will be unavailable", tableId);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internals
    // ─────────────────────────────────────────────────────────────────────────

    private void ApplyDelta(DatabaseDescriptor database, TableDescriptor table, long delta)
    {
        string key = CacheKey(database.Name, table.Id);

        // New entries start with Loaded=false so the lazy load can later merge the Kahuna
        // base onto this delta rather than discarding it.
        bool isFirstEntry = false;
        Entry entry = _cache.GetOrAdd(key, _ =>
        {
            isFirstEntry = true;
            return new Entry();
        });

        Interlocked.Add(ref entry.RowCount, delta);

        // Clamp only when loaded (RowCount is absolute). Pre-load, RowCount is a pending
        // delta that may legitimately be negative (e.g. deletes before the base is known).
        if (entry.Loaded)
        {
            long current = Interlocked.Read(ref entry.RowCount);
            if (current < 0)
                Interlocked.CompareExchange(ref entry.RowCount, 0, current);
        }

        // For newly-seen tables, kick off a background load so GetRowCountEstimate can
        // return an estimate as soon as the load finishes (even if the Kahuna key is absent —
        // the load will set Loaded=true with the pending delta as the absolute count).
        if (isFirstEntry)
            _ = Task.Run(() => LoadAndCacheAsync(database, table));
    }

    // Atomically merges the loaded base onto the pending delta in one CAS loop.
    // This is safe against concurrent DML updating RowCount mid-merge.
    private static void MergeBaseIntoEntry(Entry entry, long loadedBase)
    {
        long pending, merged;
        do
        {
            pending = Interlocked.Read(ref entry.RowCount);
            merged = Math.Max(0, loadedBase + pending);
        } while (Interlocked.CompareExchange(ref entry.RowCount, merged, pending) != pending);
    }

    /// <summary>
    /// Schedules a background flush honoring <see cref="CamusDBConfig.StatsFlushIntervalMs"/>:
    /// at most one flush cycle runs per table at a time, and when an interval is configured the
    /// cycle waits out the remaining time since the last flush before persisting — so a write
    /// burst collapses into roughly one disk write per interval. Changes that arrive while a
    /// cycle is in flight are captured by it (the flush reads the latest count) or by the next
    /// cycle the following DML schedules.
    /// </summary>
    private void ScheduleFlush(DatabaseDescriptor database, TableDescriptor table)
    {
        int interval = CamusDBConfig.StatsFlushIntervalMs;

        // -1 → never auto-flush; persistence happens only via an explicit FlushAsync (e.g. on close).
        if (interval < 0)
            return;

        string key = CacheKey(database.Name, table.Id);
        if (!_cache.TryGetValue(key, out Entry? entry))
            return;

        // Debounce: only one in-flight flush cycle per table.
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
                _logger.LogWarning(ex, "Stats flush failed for table {Table} — statistics may be stale", table.Name);
            }
            finally
            {
                Interlocked.Exchange(ref entry.FlushPending, 0);
            }
        });
    }

    private async Task FlushInternalAsync(DatabaseDescriptor database, TableDescriptor table)
    {
        string cacheKey = CacheKey(database.Name, table.Id);
        if (!_cache.TryGetValue(cacheKey, out Entry? entry))
            return;

        // Do not flush while the base is unloaded. RowCount is a pending delta at this point,
        // not an absolute count — writing it would corrupt the persisted base in Kahuna.
        if (!entry.Loaded)
            return;

        TableStatistics snapshot = new() { RowCount = Interlocked.Read(ref entry.RowCount) };
        byte[] bytes = MetaJsonSerializer.Serialize(snapshot, MetaJsonContext.Default.TableStatistics);

        string kahunaKey = KahunaKey(database.Name, table.Id);
        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);
        try
        {
            (KeyValueResponseType lockType, _, KeyValueDurability lockDurability) =
                await database.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
                    tx.TransactionId, kahunaKey, 0, KeyValueDurability.Persistent, CancellationToken.None
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
                KeyValueDurability.Persistent, CancellationToken.None
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
        string key = CacheKey(database.Name, table.Id);

        if (_cache.TryGetValue(key, out Entry? existing) && existing.Loaded)
            return;

        try
        {
            Entry entry = _cache.GetOrAdd(key, _ => new Entry());

            if (Interlocked.CompareExchange(ref entry.LoadAttempted, 1, 0) != 0)
                return; // another task already owns this load

            TableStatistics? loaded = await LoadFromKahunaAsync(database, table).ConfigureAwait(false);

            if (loaded is not null)
                MergeBaseIntoEntry(entry, loaded.RowCount >= 0 ? loaded.RowCount : 0);

            entry.Loaded = true;
        }
        catch (Exception ex)
        {
            // Release the one-shot guard so a transient failure does not permanently wedge
            // this table's stats (no load → never Loaded → never flushed) for the session.
            if (_cache.TryGetValue(key, out Entry? failed))
                Interlocked.Exchange(ref failed.LoadAttempted, 0);

            _logger.LogWarning(ex, "Stats load failed for table {Table} — estimates will be unavailable", table.Name);
        }
    }

    private Task<TableStatistics?> LoadFromKahunaAsync(DatabaseDescriptor database, TableDescriptor table)
        => LoadFromKahunaByIdAsync(database, table.Id);

    private async Task<TableStatistics?> LoadFromKahunaByIdAsync(DatabaseDescriptor database, string tableId)
    {
        string kahunaKey = KahunaKey(database.Name, tableId);
        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);

        try
        {
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
                await database.Kahuna.Kahuna.LocateAndTryGetValue(
                    tx.TransactionId, kahunaKey, -1,
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

    private static string CacheKey(string dbName, string tableId)
        => string.Concat(dbName, ":", tableId);

    private static string KahunaKey(string dbName, string tableId)
        => string.Concat(dbName, ":stats:", tableId);
}
