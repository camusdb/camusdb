
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Cache;

/// <summary>
/// Per-node in-memory query result cache. Implements <see cref="IQueryResultCache"/> with
/// an LRU entry dictionary, byte accounting, TTL expiry, and a background sweep.
///
/// <para><b>Concurrency model:</b> all mutations to <c>_entries</c>, <c>_lru*</c>, byte
/// counters, and <c>_depIndex</c> are protected by <c>_lock</c>. Read-path probe and LRU
/// promotion also hold <c>_lock</c> so LRU order stays correct under concurrent access.
/// The lock is held for short operations only; it is never held across Kahuna calls or I/O.</para>
///
/// <para><b>Publish protocol:</b> <see cref="TryPublishAsync"/> delegates the final insert to
/// <see cref="CachePublishGate.TryPublishUnderGeneration"/>, which holds its own write lock
/// to be mutually exclusive with <see cref="CachePublishGate.CommitWrite"/>. The store action
/// passed to the gate acquires <c>_lock</c> to update the dictionary and LRU list.</para>
///
/// <para><b>Byte estimation:</b> per-entry byte cost is approximated at store time by
/// <see cref="EstimateBytes"/>. The estimate is deliberately coarse (over-estimate rather
/// than under-estimate) so the byte cap is never silently violated.</para>
/// </summary>
public sealed class QueryResultCache : IQueryResultCache, IDisposable
{
    private sealed class CacheEntry
    {
        public string EntryId        { get; }
        public string Fingerprint    { get; }
        public string CacheName      { get; }
        public string DatabaseId     { get; }
        public CachedQueryResult Result { get; }
        public QueryDependencySet Deps { get; }
        public long ByteSize         { get; }
        public long ExpiresAtMs      { get; }  // Environment.TickCount64 ticks

        public LinkedListNode<string>? LruNode;  // node in _lruList; null if evicted

        public CacheEntry(
            string entryId,
            string fingerprint,
            string cacheName,
            string databaseId,
            CachedQueryResult result,
            QueryDependencySet deps,
            long byteSize,
            long expiresAtMs)
        {
            EntryId     = entryId;
            Fingerprint = fingerprint;
            CacheName   = cacheName;
            DatabaseId  = databaseId;
            Result      = result;
            Deps        = deps;
            ByteSize    = byteSize;
            ExpiresAtMs = expiresAtMs;
        }
    }

    public CachePublishGate PublishGate { get; } = new();

    private readonly Lock _lock = new();

    // Primary lookup: fingerprint → entry
    private readonly Dictionary<string, CacheEntry> _entries =
        new(StringComparer.Ordinal);

    // Secondary index: "{databaseId}:{cacheName}" → set of fingerprints
    private readonly Dictionary<string, HashSet<string>> _byCacheName =
        new(StringComparer.Ordinal);

    // Secondary index: databaseId → set of fingerprints
    private readonly Dictionary<string, HashSet<string>> _byDatabaseId =
        new(StringComparer.Ordinal);

    // LRU: head = most-recently-used, tail = least-recently-used
    private readonly LinkedList<string> _lruList = new();

    private long _totalBytes;

    private readonly DependencyIndex _depIndex = new();

    // Single-flight: fingerprint → (TCS, waiterCount). The TCS carries only "did the owner publish?"
    // — never the result object, so waiters must re-probe — and is what owners signal and waiters
    // await. waiterCount is incremented each time a non-owner caller picks up the slot; it lets
    // tests poll until at least one waiter is blocking in WaitAsync. Protected by _lock.
    private readonly Dictionary<string, (TaskCompletionSource<bool> Tcs, int WaiterCount)> _inFlight =
        new(StringComparer.Ordinal);

    private readonly Timer _sweepTimer;

    /// <param name="sweepIntervalMs">
    /// How often the background sweep removes expired entries.
    /// Pass a negative value (or omit) to disable the automatic sweep — the timer is created but
    /// never fires (<see cref="Timeout.Infinite"/>). Tests that call <see cref="Sweep"/> manually
    /// should pass -1 so the background timer does not race with their assertions.
    /// Pass 0 to use <see cref="CamusDBConfig.QueryResultCacheSweepIntervalMs"/>.
    /// </param>
    public QueryResultCache(int sweepIntervalMs = -1)
    {
        int interval = sweepIntervalMs > 0
            ? sweepIntervalMs
            : sweepIntervalMs == 0
                ? CamusDBConfig.QueryResultCacheSweepIntervalMs
                : Timeout.Infinite;

        _sweepTimer = new Timer(_ => Sweep(), null, interval, interval);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // IQueryResultCache implementation
    // ──────────────────────────────────────────────────────────────────────────

    /// <inheritdoc />
    public Task<CachedQueryResult?> TryGetAsync(
        string databaseId,
        string cacheName,
        string fingerprint,
        CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(fingerprint, out CacheEntry? entry))
                return Task.FromResult<CachedQueryResult?>(null);

            // TTL check
            if (Environment.TickCount64 > entry.ExpiresAtMs)
            {
                RemoveEntryLocked(entry);
                return Task.FromResult<CachedQueryResult?>(null);
            }

            // Promote to MRU
            if (entry.LruNode is not null)
            {
                _lruList.Remove(entry.LruNode);
                _lruList.AddFirst(entry.LruNode);
            }

            return Task.FromResult<CachedQueryResult?>(entry.Result);
        }
    }

    /// <inheritdoc />
    public Task<(CachedQueryResult Result, QueryDependencySet Deps)?> TryGetWithDepsAsync(
        string databaseId,
        string cacheName,
        string fingerprint,
        CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(fingerprint, out CacheEntry? entry))
                return Task.FromResult<(CachedQueryResult, QueryDependencySet)?>(null);

            if (Environment.TickCount64 > entry.ExpiresAtMs)
            {
                RemoveEntryLocked(entry);
                return Task.FromResult<(CachedQueryResult, QueryDependencySet)?>(null);
            }

            if (entry.LruNode is not null)
            {
                _lruList.Remove(entry.LruNode);
                _lruList.AddFirst(entry.LruNode);
            }

            return Task.FromResult<(CachedQueryResult, QueryDependencySet)?>((entry.Result, entry.Deps));
        }
    }

    /// <inheritdoc />
    public void InvalidateEntry(string databaseId, string cacheName, string fingerprint)
    {
        lock (_lock)
        {
            if (_entries.TryGetValue(fingerprint, out CacheEntry? entry))
                RemoveEntryLocked(entry);
        }
    }

    /// <inheritdoc />
    public Task<(QueryCacheStatus Status, QueryCacheBypassReason Reason)> TryPublishAsync(
        CachedQueryResult result,
        CacheGenerationToken generationToken,
        QueryDependencySet? deps = null,
        CancellationToken cancellationToken = default)
    {
        // Per-entry row and byte caps — belt-and-suspenders; CachedQueryRunner's _capBreached
        // flag catches these before TryPublishAsync is called in normal usage.
        if (result.Rows.Count > CamusDBConfig.QueryResultCacheMaxEntryRows)
            return Task.FromResult((QueryCacheStatus.EvictedBeforePublish, QueryCacheBypassReason.OversizedResult));

        long estimatedBytes = EstimateBytes(result.Rows);
        if (estimatedBytes > CamusDBConfig.QueryResultCacheMaxEntryBytes)
            return Task.FromResult((QueryCacheStatus.EvictedBeforePublish, QueryCacheBypassReason.OversizedResult));

        int ttlMs = result.HintTtlMs ?? CamusDBConfig.QueryResultCacheDefaultTtlMs;

        long now = Environment.TickCount64;
        long expiresAtMs = now + ttlMs;

        // Stamp creation time for age computation in the response envelope.
        result = result with { CreatedAtMs = now };

        var entry = new CacheEntry(
            entryId:    result.ResultFingerprint,
            fingerprint: result.ResultFingerprint,
            cacheName:  result.CacheName,
            databaseId: result.DatabaseId,
            result:     result,
            deps:       deps ?? QueryDependencySet.Empty,
            byteSize:   estimatedBytes,
            expiresAtMs: expiresAtMs);

        bool stored = false;

        bool gateAllowed = PublishGate.TryPublishUnderGeneration(generationToken, () =>
        {
            lock (_lock)
            {
                // Idempotent: if a concurrent miss already stored this fingerprint, skip.
                if (_entries.ContainsKey(entry.Fingerprint))
                {
                    stored = true; // treat as stored (caller gets Miss = was published)
                    return;
                }

                // Global byte cap: evict LRU until we have room.
                // This eviction path (total bytes exceeded → drain LRU) is not directly
                // tested; only the per-entry byte cap is. A dedicated test that fills the
                // cache past QueryResultCacheMaxBytes and asserts the oldest entry is
                // evicted is still missing.
                while (_totalBytes + entry.ByteSize > CamusDBConfig.QueryResultCacheMaxBytes
                       && _lruList.Count > 0)
                {
                    EvictLruLocked();
                }

                // Global entry cap
                while (_entries.Count >= CamusDBConfig.QueryResultCacheMaxEntries
                       && _lruList.Count > 0)
                {
                    EvictLruLocked();
                }

                // If caps still exceeded after draining all LRU, bypass
                if (_totalBytes + entry.ByteSize > CamusDBConfig.QueryResultCacheMaxBytes)
                    return;
                if (_entries.Count >= CamusDBConfig.QueryResultCacheMaxEntries)
                    return;

                InsertEntryLocked(entry);
                stored = true;
            }
        });

        if (stored)
            return Task.FromResult((QueryCacheStatus.Miss, QueryCacheBypassReason.None));

        // Generation fence rejected: a write committed into this keyspace during the query.
        if (!gateAllowed)
            return Task.FromResult((QueryCacheStatus.EvictedBeforePublish, QueryCacheBypassReason.InFlightWrite));

        // Gate allowed but caps still exceeded after LRU drain.
        return Task.FromResult((QueryCacheStatus.EvictedBeforePublish, QueryCacheBypassReason.OversizedResult));
    }

    /// <inheritdoc />
    public void InvalidateByModifiedKeys(
        IReadOnlyCollection<(string key, KeyValueDurability durability)> modifiedKeys)
    {
        if (modifiedKeys.Count == 0)
            return;

        lock (_lock)
        {
            HashSet<string> toRemove = new(StringComparer.Ordinal);

            foreach ((string key, _) in modifiedKeys)
            {
                // Derive the keyspace bucket prefix from the full KV key.
                // Key format: "{dbId}:{tableId}:r/{rowId}" or "{dbId}:{tableId}:i:{indexId}/..."
                // Bucket prefix: everything up to and including ":r" or ":i:{indexId}"
                string keyspace = ExtractKeyspaceBucket(key);
                if (keyspace.Length > 0)
                {
                    foreach (string id in _depIndex.FindByKeyspace(keyspace))
                        toRemove.Add(id);
                }

                // Also check exact point deps
                foreach (string id in _depIndex.FindByPoint(key))
                    toRemove.Add(id);
            }

            foreach (string fp in toRemove)
            {
                if (_entries.TryGetValue(fp, out CacheEntry? entry))
                    RemoveEntryLocked(entry);
            }
        }
    }

    /// <inheritdoc />
    public void InvalidateByTableId(string databaseId, string tableId)
    {
        lock (_lock)
        {
            List<string> toRemove = new(
                _depIndex.FindByTableSchema(databaseId, tableId));

            foreach (string fp in toRemove)
            {
                if (_entries.TryGetValue(fp, out CacheEntry? entry))
                    RemoveEntryLocked(entry);
            }
        }
    }

    /// <inheritdoc />
    public void InvalidateDatabase(string databaseId)
    {
        lock (_lock)
        {
            if (!_byDatabaseId.TryGetValue(databaseId, out HashSet<string>? fps))
                return;

            // Snapshot before mutating — RemoveEntryLocked modifies _byDatabaseId
            List<string> snapshot = new(fps);
            foreach (string fp in snapshot)
            {
                if (_entries.TryGetValue(fp, out CacheEntry? entry))
                    RemoveEntryLocked(entry);
            }
        }
    }

    /// <inheritdoc />
    public void InvalidateCacheName(string databaseId, string cacheName)
    {
        lock (_lock)
        {
            string compositeKey = databaseId + ":" + cacheName;
            if (!_byCacheName.TryGetValue(compositeKey, out HashSet<string>? fps))
                return;

            List<string> snapshot = new(fps);
            foreach (string fp in snapshot)
            {
                if (_entries.TryGetValue(fp, out CacheEntry? entry))
                    RemoveEntryLocked(entry);
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Internal helpers — all must be called while holding _lock
    // ──────────────────────────────────────────────────────────────────────────

    private void InsertEntryLocked(CacheEntry entry)
    {
        LinkedListNode<string> node = _lruList.AddFirst(entry.Fingerprint);
        entry.LruNode = node;
        _entries[entry.Fingerprint] = entry;
        _totalBytes += entry.ByteSize;

        // Update secondary indexes
        string compositeKey = entry.DatabaseId + ":" + entry.CacheName;
        if (!_byCacheName.TryGetValue(compositeKey, out HashSet<string>? byName))
            _byCacheName[compositeKey] = byName = new HashSet<string>(StringComparer.Ordinal);
        byName.Add(entry.Fingerprint);

        if (!_byDatabaseId.TryGetValue(entry.DatabaseId, out HashSet<string>? byDb))
            _byDatabaseId[entry.DatabaseId] = byDb = new HashSet<string>(StringComparer.Ordinal);
        byDb.Add(entry.Fingerprint);

        // Dep index
        _depIndex.Add(entry.EntryId, entry.Deps, entry.DatabaseId);
    }

    private void RemoveEntryLocked(CacheEntry entry)
    {
        if (!_entries.Remove(entry.Fingerprint))
            return; // already removed

        _totalBytes -= entry.ByteSize;
        if (entry.LruNode is not null)
        {
            _lruList.Remove(entry.LruNode);
            entry.LruNode = null;
        }

        // Secondary indexes
        string compositeKey = entry.DatabaseId + ":" + entry.CacheName;
        if (_byCacheName.TryGetValue(compositeKey, out HashSet<string>? byName))
            byName.Remove(entry.Fingerprint);

        if (_byDatabaseId.TryGetValue(entry.DatabaseId, out HashSet<string>? byDb))
            byDb.Remove(entry.Fingerprint);

        // Dep index
        _depIndex.Remove(entry.EntryId, entry.Deps, entry.DatabaseId);
    }

    private void EvictLruLocked()
    {
        LinkedListNode<string>? tail = _lruList.Last;
        if (tail is null) return;

        if (_entries.TryGetValue(tail.Value, out CacheEntry? entry))
            RemoveEntryLocked(entry);
        else
            _lruList.RemoveLast();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // TTL sweep
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Removes all entries whose TTL has elapsed. Called by the background timer
    /// and may be called manually in tests.
    /// </summary>
    public void Sweep()
    {
        long now = Environment.TickCount64;
        List<CacheEntry>? expired = null;

        lock (_lock)
        {
            foreach (CacheEntry entry in _entries.Values)
            {
                if (now > entry.ExpiresAtMs)
                    (expired ??= new()).Add(entry);
            }

            if (expired is not null)
                foreach (CacheEntry e in expired)
                    RemoveEntryLocked(e);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Byte estimation
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Estimates the memory cost of a single <see cref="QueryResultRow"/> in bytes. Used by
    /// <see cref="CachedQueryRunner"/> to detect a cap breach row-by-row during accumulation,
    /// and by <see cref="EstimateBytes"/> for the final per-entry check. Intentionally
    /// over-estimates to ensure the byte cap is never silently violated.
    /// </summary>
    internal static long EstimateRowBytes(QueryResultRow row)
    {
        const int RowOverhead = 128;  // Dictionary, QueryResultRow struct, GC header, etc.

        long total = RowOverhead;
        foreach (KeyValuePair<string, ColumnValue> col in row.Row)
        {
            total += col.Key.Length * 2 + 32;  // column name
            total += EstimateColumnValueBytes(col.Value);
        }
        return total;
    }

    private static long EstimateBytes(IReadOnlyList<QueryResultRow> rows)
    {
        long total = 0;
        foreach (QueryResultRow row in rows)
            total += EstimateRowBytes(row);
        return total;
    }

    private static long EstimateColumnValueBytes(ColumnValue v)
    {
        return v.Type switch
        {
            ColumnType.Null      => 16,
            ColumnType.Bool      => 16,
            ColumnType.Integer64 => 24,
            ColumnType.Float64   => 24,
            ColumnType.Float32   => 24,
            ColumnType.Date      => 24,
            ColumnType.DateTime  => 24,
            ColumnType.Id        => 48,  // 24-char hex + object overhead
            ColumnType.Uuid      => 32,  // two 64-bit halves + object overhead
            ColumnType.String    => 16 + ((long)(v.StrValue?.Length ?? 0)) * 2,
            ColumnType.Bytes     => 32 + (long)(v.BytesValue?.Length ?? 0),
            ColumnType.Array     => 32 + (v.ArrayValues?.Count ?? 0) * 32,
            _                    => 32,
        };
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Keyspace extraction
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Derives the keyspace bucket prefix from a full KV key so it can be matched against
    /// <see cref="CachePublishGate"/> keyspaces and <see cref="DependencyIndex"/> range deps.
    ///
    /// <para>Real KV key formats (from <c>KvTableStore</c>):</para>
    /// <list type="bullet">
    ///   <item><description>Row:   <c>{dbId}:{tableId}:r/{rowIdHex24}</c>
    ///     → bucket <c>{dbId}:{tableId}:r</c></description></item>
    ///   <item><description>Index: <c>{dbId}:{tableId}:i:{indexId}/{encodedKey}</c>
    ///     → bucket <c>{dbId}:{tableId}:i:{indexId}</c></description></item>
    /// </list>
    ///
    /// <para>Returns an empty string if the key does not match either pattern.</para>
    ///
    /// <para>Internal (not private) so the mapping can be unit-tested directly against the real
    /// <c>KvTableStore</c> key formats without populating dependency sets.</para>
    /// </summary>
    internal static string ExtractKeyspaceBucket(string key)
    {
        // First colon separates dbId from tableId.
        int first = key.IndexOf(':');
        if (first < 0) return "";

        // Second colon separates tableId from the segment type (:r or :i).
        int second = key.IndexOf(':', first + 1);
        if (second < 0) return "";

        // The character after the second colon is the segment type.
        if (second + 2 >= key.Length) return "";

        char segment = key[second + 1];

        if (segment == 'r')
        {
            // Row key: "{dbId}:{tableId}:r/{rowIdHex24}"
            // Bucket is everything up to and including ":r" (the slash is NOT part of the bucket).
            int slashPos = key.IndexOf('/', second + 2);
            return slashPos >= 0 ? key[..slashPos] : key[..(second + 2)];
        }

        if (segment == 'i')
        {
            // Index key: "{dbId}:{tableId}:i:{indexId}/{encodedKey}"
            // There is a colon (not a slash) immediately after ":i", then the indexId, then "/".
            // Bucket = "{dbId}:{tableId}:i:{indexId}" — up to but not including the first "/".
            int colonAfterI = second + 2;   // points at ':' before indexId
            if (colonAfterI >= key.Length || key[colonAfterI] != ':') return "";

            int slashAfterIndexId = key.IndexOf('/', colonAfterI + 1);
            return slashAfterIndexId >= 0 ? key[..slashAfterIndexId] : key;
        }

        return "";
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Diagnostic helpers (test access)
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>Current count of live (non-expired) entries in the cache.</summary>
    internal int EntryCount { get { lock (_lock) return _entries.Count; } }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="fingerprint"/> is currently registered as an
    /// in-flight owner (i.e. <see cref="EnterSingleFlight"/> was called and
    /// <see cref="ExitSingleFlight"/> has not yet been called for it).
    /// </summary>
    internal bool HasInFlightSlot(string fingerprint)
    {
        lock (_lock)
            return _inFlight.ContainsKey(fingerprint);
    }

    /// <summary>
    /// Returns the number of callers that have called <see cref="EnterSingleFlight"/> as a
    /// waiter (IsOwner == false) for <paramref name="fingerprint"/> and have not yet been
    /// signalled. Used by tests to poll until at least one waiter has reached
    /// <see cref="SingleFlightSlot.WaitAsync"/>, eliminating the fixed-delay race.
    /// </summary>
    internal int WaiterCount(string fingerprint)
    {
        lock (_lock)
            return _inFlight.TryGetValue(fingerprint, out var e) ? e.WaiterCount : 0;
    }

    /// <summary>
    /// Returns the fingerprint of any live entry stored under <paramref name="cacheName"/> for
    /// <paramref name="databaseId"/>, or <c>null</c> if none exists. Used by tests to discover
    /// the fingerprint after an initial cold miss so they can pre-occupy the in-flight slot for
    /// the same fingerprint in subsequent single-flight tests.
    /// </summary>
    internal string? GetFirstFingerprintByCacheName(string databaseId, string cacheName)
    {
        string compositeKey = databaseId + ":" + cacheName;
        lock (_lock)
        {
            if (!_byCacheName.TryGetValue(compositeKey, out HashSet<string>? fps))
                return null;
            foreach (string fp in fps)
                return fp;
            return null;
        }
    }

    /// <summary>Current total byte estimate across all live entries.</summary>
    internal long TotalBytes { get { lock (_lock) return _totalBytes; } }

    /// <summary>
    /// Directly inserts an entry into the cache, bypassing normal publish-gate and fingerprint
    /// checks. Used by strict-validation tests to inject stale entries that could not have been
    /// produced by the normal miss path (e.g. an entry with <see cref="HLCTimestamp.Zero"/> as
    /// <c>CachedAt</c> simulating a cross-node stale hit).
    /// </summary>
    internal void InjectEntryForTest(CachedQueryResult result, QueryDependencySet deps, int ttlMs = 60_000)
    {
        long byteSize = EstimateBytes(result.Rows);
        long expiresAtMs = Environment.TickCount64 + ttlMs;

        var entry = new CacheEntry(
            entryId:     result.ResultFingerprint,
            fingerprint: result.ResultFingerprint,
            cacheName:   result.CacheName,
            databaseId:  result.DatabaseId,
            result:      result,
            deps:        deps,
            byteSize:    byteSize,
            expiresAtMs: expiresAtMs);

        lock (_lock)
        {
            if (_entries.ContainsKey(entry.Fingerprint))
                return;

            entry.LruNode = _lruList.AddFirst(entry.Fingerprint);
            _entries[entry.Fingerprint] = entry;
            _totalBytes += byteSize;

            if (!_byCacheName.TryGetValue(entry.CacheName, out HashSet<string>? byName))
                _byCacheName[entry.CacheName] = byName = [];
            byName.Add(entry.Fingerprint);

            if (!_byDatabaseId.TryGetValue(entry.DatabaseId, out HashSet<string>? byDb))
                _byDatabaseId[entry.DatabaseId] = byDb = [];
            byDb.Add(entry.Fingerprint);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Single-flight
    // ──────────────────────────────────────────────────────────────────────────

    /// <inheritdoc />
    public SingleFlightSlot EnterSingleFlight(string fingerprint)
    {
        lock (_lock)
        {
            if (_inFlight.TryGetValue(fingerprint, out (TaskCompletionSource<bool> Tcs, int WaiterCount) existing))
            {
                // Another request is already computing this fingerprint. Return a distinct waiter
                // slot backed by the owner's Task so WaitAsync will unblock when ExitSingleFlight
                // signals. IsOwner=false so the caller does NOT call ExitSingleFlight.
                _inFlight[fingerprint] = (existing.Tcs, existing.WaiterCount + 1);
                return new SingleFlightSlot(isOwner: false, existing.Tcs.Task);
            }

            // First caller: become the owner. The TCS stays in _inFlight; ExitSingleFlight
            // removes it and calls TrySetResult to wake all waiters.
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _inFlight[fingerprint] = (tcs, 0);
            return new SingleFlightSlot(isOwner: true, tcs.Task);
        }
    }

    /// <inheritdoc />
    public void ExitSingleFlight(string fingerprint, bool published)
    {
        TaskCompletionSource<bool>? tcs;
        lock (_lock)
        {
            if (!_inFlight.Remove(fingerprint, out (TaskCompletionSource<bool> Tcs, int) entry))
                return;
            tcs = entry.Tcs;
        }

        // Signal outside the lock: continuations run asynchronously (RunContinuationsAsynchronously)
        // so they cannot re-enter _lock on this thread. Only the flag crosses the boundary — waiters
        // re-probe the cache themselves, so an entry evicted between publish and this signal cannot
        // be served to them.
        tcs.TrySetResult(published);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Dispose
    // ──────────────────────────────────────────────────────────────────────────

    public void Dispose()
    {
        _sweepTimer.Dispose();
    }
}
