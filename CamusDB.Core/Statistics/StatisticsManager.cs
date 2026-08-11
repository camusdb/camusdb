
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Linq;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Storage.Kv;
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

        // Serializes the Kahuna base load for this table. A concurrent caller waits for the
        // in-flight load instead of returning early: returning while Loaded is still false would
        // let the caller mutate and flush an entry whose base has not been merged yet, and
        // FlushInternalAsync silently drops writes for a not-yet-loaded entry.
        public readonly SemaphoreSlim LoadLock = new(1, 1);

        // Environment.TickCount64 of the last completed flush. 0 = never flushed.
        public long LastFlushTicks;

        // 1 while a flush cycle owns this table; 0 otherwise.
        public int FlushPending;

        // Environment.TickCount64 of the last background-load attempt fired by
        // GetRowCountEstimate. Rate-limits load retries: without it, a persistently failing
        // Kahuna load (all attempts serialized on LoadLock) becomes a per-query task storm.
        public long LastLoadAttemptTicks;

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

        // Row mutations (insert/update/delete) accumulated since the last completed ANALYZE.
        // Updated with Interlocked; drives auto-analyze staleness. Same Loaded/delta merge
        // semantics as RowCount: pre-load increments are added to the persisted base on load.
        public long MutationsSinceAnalyze;

        // HLC timestamp the last successful ANALYZE read (advisory). Guarded by ColumnStatsLock.
        public Kommander.Time.HLCTimestamp LastAnalyzedAt;

        // Generation of the last histogram/NDV publish on this node (see GetAnalyzeGeneration).
        public long AnalyzeGeneration;

        // Set when the table is dropped: fences a background flush already holding this entry
        // reference so it cannot re-create the persisted stats key after the drop deleted it.
        public volatile bool Dropped;

        // Serializes flushes for this entry so the delta baselines below have a single writer.
        public readonly SemaphoreSlim FlushLock = new(1, 1);

        // Counter values already reflected in the persisted blob as of the last successful load
        // or flush BY THIS NODE. The difference between the live counters and these baselines is
        // this node's unflushed local delta — the only part a flush may add to the persisted
        // value. Writing the absolute local view instead would last-writer-wins clobber deltas
        // flushed by other nodes tracking DML on the same table. Guarded by FlushLock.
        public long FlushedRowCount;
        public long FlushedMutations;
        public Dictionary<string, long> FlushedIndexEntries = new(StringComparer.Ordinal);

        // 1 after an ANALYZE publish: the next flush writes this node's absolute view instead of
        // delta-merging, because the scan already reconciled the true counts (and the correction
        // arithmetic in PublishAsync preserved concurrent DML). Consumed with Interlocked.
        public int ForceAbsoluteFlush;
    }

    /// <summary>
    /// Process-wide counter backing <see cref="Entry.AnalyzeGeneration"/>. Global (static) so a
    /// generation value can never repeat after an entry is evicted and rebuilt — a per-entry
    /// counter restarting at zero could coincidentally match a stale cached value.
    /// </summary>
    private static long globalAnalyzeGeneration;

    private readonly ConcurrentDictionary<string, Entry> _cache = new(StringComparer.Ordinal);

    private readonly ILogger<ICamusDB> _logger;

    public StatisticsManager(ILogger<ICamusDB> logger)
    {
        _logger = logger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public query API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns this node's generation of the last histogram/NDV publish for <paramref name="table"/>
    /// (0 when statistics were never published on this node). The plan cache includes this value in
    /// its dependency fingerprint so that an ANALYZE invalidates cached access-path decisions —
    /// without it, the first plan per query shape would be frozen forever regardless of stats
    /// refreshes. Per-node by design: caches and stats entries are both node-local.
    /// </summary>
    public long GetAnalyzeGeneration(DatabaseDescriptor database, TableDescriptor table)
    {
        return _cache.TryGetValue(CacheKey(database.Id, table.Id), out Entry? entry)
            ? Volatile.Read(ref entry.AnalyzeGeneration)
            : 0;
    }

    /// <summary>
    /// True once this table carries column statistics produced by a real ANALYZE — histograms
    /// published in-process or loaded from the persisted blob, or a recorded analyze timestamp.
    /// Cost-based access-path selection keys off this rather than the DML-maintained row count:
    /// a bare row count engages the cost model with fixed fallback selectivities, which is enough
    /// to wrongly abandon a correct index or covered path on small, never-analyzed tables (and
    /// auto-analyze staleness thresholds mean a small table may never earn histograms at all).
    /// Returns false while the background stats load is still pending — the caller's preceding
    /// <see cref="GetRowCountEstimate"/> call fires that load, so the answer converges.
    /// </summary>
    public bool HasAnalyzedStatistics(DatabaseDescriptor database, TableDescriptor table)
    {
        if (!_cache.TryGetValue(CacheKey(database.Id, table.Id), out Entry? entry))
            return false;

        lock (entry.ColumnStatsLock)
            return entry.Histograms is { Count: > 0 } || !entry.LastAnalyzedAt.IsNull();
    }

    // Minimum interval between background-load attempts fired by GetRowCountEstimate for one
    // table. Purely a retry-storm guard; a successful load makes the check moot (Loaded=true).
    private const int LoadRetryBackoffMs = 1_000;

    /// <summary>Returns the estimated row count, or null if no statistics have been collected.</summary>
    public long? GetRowCountEstimate(DatabaseDescriptor database, TableDescriptor table)
    {
        string key = CacheKey(database.Id, table.Id);
        Entry entry = _cache.GetOrAdd(key, _ => new Entry());

        if (entry.Loaded)
            return entry.RowCount >= 0 ? entry.RowCount : null;

        // Fire at most one background load attempt per backoff window: every planner call lands
        // here while the entry is unloaded, and an unconditional Task.Run per call piles up
        // tasks serialized on the entry's LoadLock — a per-query retry storm when the load
        // keeps failing. CAS on the attempt timestamp elects a single firer per window.
        long now = Environment.TickCount64;
        long last = Interlocked.Read(ref entry.LastLoadAttemptTicks);
        if ((last == 0 || now - last >= LoadRetryBackoffMs)
            && Interlocked.CompareExchange(ref entry.LastLoadAttemptTicks, now, last) == last)
        {
            _ = Task.Run(() => LoadAndCacheAsync(database, table));
        }

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

            Volatile.Write(ref entry.AnalyzeGeneration, Interlocked.Increment(ref globalAnalyzeGeneration));
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

            Volatile.Write(ref entry.AnalyzeGeneration, Interlocked.Increment(ref globalAnalyzeGeneration));
        }

        await FlushAsync(database, table).ConfigureAwait(false);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Auto-analyze staleness API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Number of row mutations accumulated since the last completed <c>ANALYZE</c>, or 0 if the
    /// table's statistics are not loaded. The background auto-analyze scheduler reads this to
    /// decide whether a table is stale; it is also the baseline an in-flight ANALYZE captures so
    /// concurrent churn during the (throttled) scan is preserved on completion.
    /// </summary>
    public long GetMutationsSinceAnalyze(DatabaseDescriptor database, TableDescriptor table)
    {
        string key = CacheKey(database.Id, table.Id);
        return _cache.TryGetValue(key, out Entry? entry) ? Interlocked.Read(ref entry.MutationsSinceAnalyze) : 0;
    }

    /// <summary>
    /// Returns true when <paramref name="table"/>'s statistics are stale enough to warrant a
    /// background <c>ANALYZE</c>, using the CockroachDB-style rule
    /// <c>mutations &gt;= fractionStaleRows · rowCount + minStaleRows</c>:
    ///
    /// <list type="bullet">
    ///   <item>A never-analyzed table with tracked data (<c>RowCount &lt; 0</c>) is stale.</item>
    ///   <item>Below <paramref name="minStaleRows"/> mutations is never stale — small tables and
    ///         light churn don't trigger constant re-analysis.</item>
    ///   <item>An empty, never-mutated table is not stale.</item>
    /// </list>
    ///
    /// Returns false when statistics are not yet loaded (the caller shouldn't force a load just to
    /// test staleness — an unopened/untouched table has nothing worth analyzing).
    /// </summary>
    public bool IsStale(DatabaseDescriptor database, TableDescriptor table, double fractionStaleRows, long minStaleRows) =>
        IsStaleByKey(CacheKey(database.Id, table.Id), fractionStaleRows, minStaleRows);

    /// <summary>
    /// Staleness test keyed by table id, for the cluster-visible discovery path: the elected leader
    /// loads persisted staleness for a table it may not have a <see cref="TableDescriptor"/> for yet
    /// (via <see cref="LoadByIdAsync"/>) and asks whether it is worth opening and analyzing.
    /// </summary>
    public bool IsStale(DatabaseDescriptor database, string tableId, double fractionStaleRows, long minStaleRows) =>
        IsStaleByKey(CacheKey(database.Id, tableId), fractionStaleRows, minStaleRows);

    private bool IsStaleByKey(string key, double fractionStaleRows, long minStaleRows)
    {
        if (!_cache.TryGetValue(key, out Entry? entry) || !entry.Loaded)
            return false;

        return EvaluateStaleness(
            Interlocked.Read(ref entry.RowCount),
            Interlocked.Read(ref entry.MutationsSinceAnalyze),
            fractionStaleRows,
            minStaleRows);
    }

    /// <summary>
    /// The staleness arithmetic itself, over a row count and a mutation count from any source. Shared
    /// by the cached test and the cache-free probe so the two can never drift into disagreeing about
    /// what "stale" means — a probe that answered differently from the cache would make a table's
    /// eligibility for ANALYZE depend on whether this node happened to have it open.
    /// </summary>
    private static bool EvaluateStaleness(long rowCount, long mutations, double fractionStaleRows, long minStaleRows)
    {
        // Has tracked data but no histograms/NDV have ever been built for it.
        if (rowCount < 0)
            return true;

        if (mutations < minStaleRows)
            return false;

        return mutations >= (long)(fractionStaleRows * rowCount) + minStaleRows;
    }

    /// <summary>
    /// Answers "is this table stale enough to analyze?" for a table this node may never have opened,
    /// <b>without creating a cache entry</b> — background discovery must be able to look at every table
    /// in the cluster without that inspection itself becoming resident state.
    ///
    /// <para><b>Being cache-neutral is the point, not an implementation detail.</b> The obvious
    /// alternative — load the table's statistics and then ask <see cref="IsStale(DatabaseDescriptor,
    /// string, double, long)"/> — allocates an <c>Entry</c> (two semaphores and several dictionaries)
    /// for every table merely being <em>checked</em>. Across a cluster of thousands of databases that
    /// turns a periodic freshness poll into an unbounded memory leak. Do not "simplify" this back onto
    /// the loading path.</para>
    ///
    /// <para>A table this node already tracks is answered from the live entry, which is both free and
    /// strictly more accurate: it includes mutations this node has not flushed yet. Only a table with
    /// no entry pays a point read of the persisted blob, and that read leaves no trace.</para>
    /// </summary>
    public async Task<bool> IsStaleWithoutCachingAsync(
        EmbeddedKahuna node,
        string dbId,
        string tableId,
        double fractionStaleRows,
        long minStaleRows,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(node);

        // Already tracked here: use it. Reading the persisted blob instead would ignore this node's
        // unflushed mutations and under-report staleness for exactly the tables that are busiest.
        if (_cache.TryGetValue(CacheKey(dbId, tableId), out Entry? cached) && cached.Loaded)
        {
            return EvaluateStaleness(
                Interlocked.Read(ref cached.RowCount),
                Interlocked.Read(ref cached.MutationsSinceAnalyze),
                fractionStaleRows,
                minStaleRows);
        }

        TableStatistics? persisted;
        try
        {
            persisted = await ReadPersistedStatisticsAsync(node, dbId, tableId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Advisory metadata: an unreadable blob means "no evidence this table is stale", never a
            // failed discovery tick.
            _logger.LogWarning(ex, "Stats freshness probe failed for table id {TableId}", tableId);
            return false;
        }

        if (persisted is null)
            return false; // never persisted: nothing has been tracked, so nothing is stale

        // Mirrors what the cache would hold after a load with no local deltas: the merge clamps both
        // values at zero (see MergeBaseIntoEntry / MergeStaleness).
        return EvaluateStaleness(
            Math.Max(0, persisted.RowCount),
            Math.Max(0, persisted.MutationsSinceAnalyze),
            fractionStaleRows,
            minStaleRows);
    }

    /// <summary>
    /// Point-reads a table's persisted statistics blob straight off the shared node, given only its
    /// database and table ids. Deliberately takes no <see cref="DatabaseDescriptor"/>: the caller is
    /// discovery, and requiring a descriptor is what would force the database open.
    /// </summary>
    private static async Task<TableStatistics?> ReadPersistedStatisticsAsync(
        EmbeddedKahuna node, string dbId, string tableId, CancellationToken cancellationToken)
    {
        // HLCTimestamp.Zero is the synthetic read-only context used elsewhere for advisory reads: a
        // non-transactional read-committed point read, with no START/ROLLBACK round-trips.
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            Kommander.Time.HLCTimestamp.Zero,
            KahunaKey(dbId, tableId),
            -1,
            Kommander.Time.HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        if (getType == KeyValueResponseType.Get && entry?.Value is not null)
            return MetaJsonSerializer.DeserializeCompat(entry.Value, MetaJsonContext.Default.TableStatistics);

        return null;
    }

    /// <summary>
    /// Test-only seam: how many tables currently hold a cache entry. Exposed so a test can assert that
    /// a discovery sweep left no residue (see <see cref="IsStaleWithoutCachingAsync"/>).
    /// </summary>
    internal int CachedTableCount => _cache.Count;

    /// <summary>
    /// Marks a completed <c>ANALYZE</c>: records the snapshot timestamp it read and subtracts the
    /// mutation baseline it consumed so any churn that happened <em>during</em> the scan remains
    /// counted (the table is not falsely reported fresh). Persists the update.
    ///
    /// <paramref name="mutationsConsumed"/> must be the value <see cref="GetMutationsSinceAnalyze"/>
    /// returned at the <b>start</b> of the scan — not a re-read at the end — so concurrent mutations
    /// survive. Passing 0 leaves the counter untouched (used by callers that don't track a baseline).
    /// </summary>
    public async Task MarkAnalyzedAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Kommander.Time.HLCTimestamp analyzedAt,
        long mutationsConsumed)
    {
        await LoadByIdAsync(database, table.Id).ConfigureAwait(false);

        string key = CacheKey(database.Id, table.Id);
        Entry entry = GetOrCreateEntry(database, table, key);

        if (mutationsConsumed > 0)
        {
            long current, next;
            do
            {
                current = Interlocked.Read(ref entry.MutationsSinceAnalyze);
                next = Math.Max(0, current - mutationsConsumed);
            } while (Interlocked.CompareExchange(ref entry.MutationsSinceAnalyze, next, current) != current);
        }

        if (!analyzedAt.IsNull())
        {
            lock (entry.ColumnStatsLock)
                entry.LastAnalyzedAt = analyzedAt;
        }

        await FlushAsync(database, table).ConfigureAwait(false);
    }

    /// <summary>
    /// Snapshot of a table's live tracked counters at the moment an <c>ANALYZE</c> scan starts. The
    /// scan reads a fixed MVCC snapshot, so writes that commit while it runs update these same live
    /// counters underneath it; <see cref="PublishAsync"/> uses this baseline to apply only the scan's
    /// <em>correction</em> (scanned − baseline) to the current live value, preserving that concurrent
    /// churn instead of clobbering it back to snapshot-time values.
    /// </summary>
    public sealed class AnalyzeBaseline
    {
        public required long RowCount { get; init; }
        public required Dictionary<string, long> IndexCounts { get; init; }
        public required long Mutations { get; init; }
    }

    /// <summary>
    /// Captures the live counter baseline for a table before an <c>ANALYZE</c> scan. Forces the entry
    /// loaded first so <see cref="AnalyzeBaseline.RowCount"/> is the absolute tracked count (not a raw
    /// pre-load delta), which is what <see cref="PublishAsync"/>'s correction arithmetic requires.
    /// </summary>
    public async Task<AnalyzeBaseline> CaptureAnalyzeBaselineAsync(DatabaseDescriptor database, TableDescriptor table)
    {
        await LoadByIdAsync(database, table.Id).ConfigureAwait(false);
        Entry entry = GetOrCreateEntry(database, table, CacheKey(database.Id, table.Id));

        var indexCounts = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, long> kv in entry.IndexEntries)
            indexCounts[kv.Key] = kv.Value;

        return new AnalyzeBaseline
        {
            RowCount = Interlocked.Read(ref entry.RowCount),
            IndexCounts = indexCounts,
            Mutations = Interlocked.Read(ref entry.MutationsSinceAnalyze),
        };
    }

    /// <summary>
    /// Atomically publishes one complete statistics generation from a finished <c>ANALYZE</c> scan,
    /// replacing the three-flush <c>Seed</c>/<c>SetHistograms</c>/<c>SetNdv</c> sequence. Two guarantees:
    ///
    /// <list type="bullet">
    ///   <item><b>Post-snapshot deltas survive.</b> Row and per-index counts are updated by the scan's
    ///   <em>correction</em> (<paramref name="scannedRowCount"/> − <see cref="AnalyzeBaseline.RowCount"/>)
    ///   applied to the current live value, so a write that committed after the scan snapshot but before
    ///   publication is preserved rather than lost. Min/max is <em>widened</em>, never replaced.
    ///   Histograms and NDV are rebuilt wholesale (they are not tracked incrementally, so there is no
    ///   delta to preserve). Index counts for indexes neither scanned nor currently writable
    ///   (dropped/unreadable) are removed.</item>
    ///   <item><b>One generation, one transaction.</b> All fields are mutated under the entry lock and
    ///   persisted in a single flush, so a concurrent planner and the persisted blob never observe a
    ///   mixture of old and new fields.</item>
    /// </list>
    ///
    /// <paramref name="scanComplete"/> must be false for a sampled scan: <paramref name="scannedRowCount"/>
    /// is then a sample size, not the true count, so the row-count correction is skipped and the live
    /// tracked count is kept.
    /// </summary>
    public async Task PublishAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        long scannedRowCount,
        bool scanComplete,
        Dictionary<string, ColumnMinMax> scannedMinMax,
        Dictionary<string, long> scannedIndexCounts,
        Dictionary<string, ColumnHistogram> histograms,
        Dictionary<string, long> columnNdv,
        Dictionary<string, long>? keyNdv,
        AnalyzeBaseline baseline,
        Kommander.Time.HLCTimestamp analyzedAt)
    {
        await LoadByIdAsync(database, table.Id).ConfigureAwait(false);
        Entry entry = GetOrCreateEntry(database, table, CacheKey(database.Id, table.Id));

        var writableIndexNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (TableIndexSchema ix in GetWritableIndexes(table))
            writableIndexNames.Add(ix.Name);

        lock (entry.ColumnStatsLock)
        {
            // Row count: apply the scan's correction to the live count (preserving concurrent DML).
            // A sampled scan's count is not the true size, so it must not correct the live count.
            if (scanComplete)
                ApplyDelta(ref entry.RowCount, scannedRowCount - baseline.RowCount);

            // Per-index counts: same correction, then prune counts for dropped/unreadable indexes.
            foreach ((string name, long scanned) in scannedIndexCounts)
            {
                baseline.IndexCounts.TryGetValue(name, out long baseCount);
                long correction = scanned - baseCount;
                entry.IndexEntries.AddOrUpdate(name, Math.Max(0, scanned), (_, live) => Math.Max(0, live + correction));
            }
            foreach (string name in entry.IndexEntries.Keys.ToList())
            {
                if (!scannedIndexCounts.ContainsKey(name) && !writableIndexNames.Contains(name))
                    entry.IndexEntries.TryRemove(name, out _);
            }

            // Min/max: replace with the freshly scanned truth. Correcting delete-drift (a deleted
            // extreme that DML tracking never lowers) is the whole point of recomputing bounds, so a
            // merge that only ever widens would defeat it. Bounds are advisory and self-healing, so the
            // rare case of a concurrent insert committing outside the scanned range after the snapshot
            // is re-captured by the next DML that touches that column — unlike row/index counts above,
            // there is no cardinality correctness riding on it.
            entry.ColumnStats.Clear();
            foreach ((string col, ColumnMinMax mm) in scannedMinMax)
                entry.ColumnStats[col] = new ColumnMinMax { Min = mm.Min, Max = mm.Max };

            // Histograms / NDV: rebuilt wholesale by ANALYZE (no incremental delta to preserve).
            entry.Histograms = histograms;
            entry.ColumnNdv = columnNdv;
            if (keyNdv is not null)
                entry.KeyNdv = keyNdv;

            // Staleness: subtract the baseline consumed (so churn during the scan is not lost), record ts.
            ApplyDelta(ref entry.MutationsSinceAnalyze, -baseline.Mutations);

            if (!analyzedAt.IsNull())
                entry.LastAnalyzedAt = analyzedAt;

            entry.Loaded = true;

            // New histogram/NDV generation published: cached plan decisions made against the
            // old statistics are now invalid (the plan cache carries this generation in its deps).
            Volatile.Write(ref entry.AnalyzeGeneration, Interlocked.Increment(ref globalAnalyzeGeneration));

            // The scan reconciled the true counts, so the flush below must overwrite the
            // persisted counters with this node's view instead of delta-merging onto them.
            Interlocked.Exchange(ref entry.ForceAbsoluteFlush, 1);
        }

        // One flush → the whole generation reaches Kahuna in a single transaction.
        await FlushAsync(database, table).ConfigureAwait(false);
    }

    // CAS-applies a signed delta to an Interlocked-managed counter, flooring at zero.
    private static void ApplyDelta(ref long counter, long delta)
    {
        long cur, next;
        do
        {
            cur = Interlocked.Read(ref counter);
            next = Math.Max(0, cur + delta);
        } while (Interlocked.CompareExchange(ref counter, next, cur) != cur);
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
        AddMutations(database, table, delta);
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
        AddMutations(database, table, delta);

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
        AddMutations(database, table, delta);

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
        // Every updated row is a mutation for staleness purposes, even when no indexed column
        // changed (so no min/max work happens below). Count before the early return.
        if (delta > 0)
            AddMutations(database, table, delta);

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

    /// <summary>
    /// Loads persisted statistics from Kahuna for the given table ID into the cache. Awaiting this
    /// guarantees the entry is loaded (or the load failed and was logged) — a caller that races an
    /// already-running load waits for it rather than proceeding against an unloaded entry.
    /// </summary>
    public Task LoadByIdAsync(DatabaseDescriptor database, string tableId)
        => EnsureLoadedAsync(database, tableId, tableId);

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

    // Accumulates row mutations since the last ANALYZE. The count is a monotonically-growing
    // total (inserts + updates + deletes) — deletes count as churn too, so a table that is fully
    // rewritten still trips the staleness threshold even though its row count barely moved.
    private void AddMutations(DatabaseDescriptor database, TableDescriptor table, long count)
    {
        if (count <= 0) return;
        Entry entry = GetOrCreateEntry(database, table, CacheKey(database.Id, table.Id));
        Interlocked.Add(ref entry.MutationsSinceAnalyze, count);
    }

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
        int interval = database.Options.StatsFlushIntervalMs;
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

        if (!entry.Loaded || entry.Dropped)
            return;

        // Serialize flushes per entry: the delta baselines (Flushed*) must have a single
        // writer, and an explicit FlushAsync (ANALYZE publish, close hook) can otherwise run
        // concurrently with a scheduled background flush.
        await entry.FlushLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await FlushLockedAsync(database, table, entry).ConfigureAwait(false);
        }
        finally
        {
            entry.FlushLock.Release();
        }
    }

    private async Task FlushLockedAsync(DatabaseDescriptor database, TableDescriptor table, Entry entry)
    {
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
            RowCount              = Interlocked.Read(ref entry.RowCount),
            IndexEntryCounts      = indexSnapshot,
            ColumnStats           = colSnapshot,
            Histograms            = histSnapshot,
            ColumnNdv             = colNdvSnapshot,
            KeyNdv                = keyNdvSnapshot,
            MutationsSinceAnalyze = Interlocked.Read(ref entry.MutationsSinceAnalyze),
            LastAnalyzedAt        = ReadLastAnalyzedAt(entry),
        };

        // Consume the ANALYZE overwrite flag; restored in the catch so a failed flush retries
        // with the same semantics.
        bool absolute = Interlocked.Exchange(ref entry.ForceAbsoluteFlush, 0) == 1;

        string kahunaKey = KahunaKey(database.Id, table.Id);
        // Background priority: flushing the statistics blob is maintenance, not user work, so it
        // yields the door to foreground traffic on a saturated node. A deferred flush costs only
        // optimizer freshness.
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            priority: TransactionPriority.Background
        ).ConfigureAwait(false);
        try
        {
            // Fold the write and its lock into the coordinator working set so the commit persists the stats
            // blob; a fresh id per call is sufficient (no retry loop here).
            (KeyValueResponseType lockType, _, _, _) =
                await database.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
                    tx.TransactionId, kahunaKey, 0, KeyValueDurability.Persistent, CancellationToken.None,
                    coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()
                ).ConfigureAwait(false);

            if (lockType != KeyValueResponseType.Locked)
            {
                if (absolute)
                    Interlocked.Exchange(ref entry.ForceAbsoluteFlush, 1);
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                return;
            }

            // Read-merge-write under the exclusive key lock: another node tracking DML on the
            // same table flushes its own deltas into this blob, and blindly writing our absolute
            // view would last-writer-wins discard them. Instead, add only this node's unflushed
            // delta (live − Flushed* baseline) on top of the current persisted counters. An
            // ANALYZE publish (absolute=true) skips the merge for counters — the scan already
            // reconciled the truth — but still resolves histograms/NDV by newest analyze.
            TableStatistics toWrite = snapshot;
            if (!absolute)
            {
                (KeyValueResponseType getType, ReadOnlyKeyValueEntry? persistedEntry) =
                    await database.Kahuna.Kahuna.LocateAndTryGetValue(
                        tx.TransactionId, kahunaKey, -1,
                        Kommander.Time.HLCTimestamp.Zero,
                        KeyValueDurability.Persistent, CancellationToken.None
                    ).ConfigureAwait(false);

                if (getType == KeyValueResponseType.Get && persistedEntry?.Value is not null)
                {
                    TableStatistics? persisted = MetaJsonSerializer.DeserializeCompat(
                        persistedEntry.Value, MetaJsonContext.Default.TableStatistics);

                    if (persisted is not null)
                        toWrite = MergeForFlush(snapshot, persisted, entry);
                }
            }

            byte[] bytes = MetaJsonSerializer.Serialize(toWrite, MetaJsonContext.Default.TableStatistics);

            (KeyValueResponseType setType, _, _) = await database.Kahuna.Kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, kahunaKey, bytes, null, -1,
                KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()
            ).ConfigureAwait(false);

            if (setType == KeyValueResponseType.Set)
            {
                // Test-only: fault after the value is staged but before commit, to prove the publish
                // is a single atomic transaction — the rollback leaves the persisted blob entirely at
                // its prior generation with no partial fields written.
                if (FailFlushForTesting)
                    throw new InvalidOperationException("Injected flush failure (test-only)");

                // Re-check the drop fence as late as possible: a DROP TABLE that raced this flush
                // deleted the persisted key, and committing now would resurrect it as an orphan.
                if (entry.Dropped)
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    return;
                }

                tx.TrackModified(kahunaKey, KeyValueDurability.Persistent);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

                // Advance the baselines: everything up to this snapshot is now reflected in the
                // persisted blob and must not be re-added by the next delta-merge.
                entry.FlushedRowCount  = snapshot.RowCount;
                entry.FlushedMutations = snapshot.MutationsSinceAnalyze;
                entry.FlushedIndexEntries = indexSnapshot is not null
                    ? new Dictionary<string, long>(indexSnapshot, StringComparer.Ordinal)
                    : new Dictionary<string, long>(StringComparer.Ordinal);
            }
            else
            {
                if (absolute)
                    Interlocked.Exchange(ref entry.ForceAbsoluteFlush, 1);
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }
        catch
        {
            if (absolute)
                Interlocked.Exchange(ref entry.ForceAbsoluteFlush, 1);
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Merges this node's local statistics snapshot onto the currently persisted blob for the
    /// counter fields (persisted value + this node's unflushed delta, floored at zero), takes
    /// the widest min/max union, and resolves histograms/NDV by whichever side has the newest
    /// <c>LastAnalyzedAt</c>. Runs under the flush's exclusive key lock, so the read-merge-write
    /// is atomic with respect to other nodes' flushes.
    /// </summary>
    private static TableStatistics MergeForFlush(TableStatistics local, TableStatistics persisted, Entry entry)
    {
        long persistedRow = persisted.RowCount >= 0 ? persisted.RowCount : 0;

        TableStatistics merged = new()
        {
            RowCount = Math.Max(0, persistedRow + (local.RowCount - entry.FlushedRowCount)),
            MutationsSinceAnalyze = Math.Max(0,
                Math.Max(0, persisted.MutationsSinceAnalyze) + (local.MutationsSinceAnalyze - entry.FlushedMutations)),
        };

        // Index counts: persisted base + this node's unflushed delta per index. Indexes only the
        // other node knows about are preserved; indexes only we know about start from our value.
        Dictionary<string, long> indexCounts = new(StringComparer.Ordinal);
        if (persisted.IndexEntryCounts is not null)
        {
            foreach (KeyValuePair<string, long> kv in persisted.IndexEntryCounts)
                indexCounts[kv.Key] = kv.Value;
        }
        if (local.IndexEntryCounts is not null)
        {
            foreach (KeyValuePair<string, long> kv in local.IndexEntryCounts)
            {
                entry.FlushedIndexEntries.TryGetValue(kv.Key, out long flushed);
                indexCounts.TryGetValue(kv.Key, out long persistedCount);
                indexCounts[kv.Key] = Math.Max(0, persistedCount + (kv.Value - flushed));
            }
        }
        merged.IndexEntryCounts = indexCounts.Count > 0 ? indexCounts : null;

        // Histograms / NDV / analyzed-at travel together: whichever side ran ANALYZE most
        // recently wins wholesale (they are one generation, never field-mixed).
        bool persistedAnalyzeNewer = persisted.LastAnalyzedAt.CompareTo(local.LastAnalyzedAt) > 0;
        TableStatistics analyzeSource = persistedAnalyzeNewer ? persisted : local;
        merged.Histograms     = analyzeSource.Histograms;
        merged.ColumnNdv      = analyzeSource.ColumnNdv;
        merged.KeyNdv         = analyzeSource.KeyNdv;
        merged.LastAnalyzedAt = analyzeSource.LastAnalyzedAt;

        // Min/max: widest union of both sides — advisory bounds, so widening is always safe and
        // the next ANALYZE re-narrows any delete drift.
        Dictionary<string, ColumnMinMax>? colStats = null;
        if (persisted.ColumnStats is not null || local.ColumnStats is not null)
        {
            colStats = new Dictionary<string, ColumnMinMax>(StringComparer.Ordinal);

            if (persisted.ColumnStats is not null)
            {
                foreach (KeyValuePair<string, ColumnMinMax> kv in persisted.ColumnStats)
                    colStats[kv.Key] = new ColumnMinMax { Min = kv.Value.Min, Max = kv.Value.Max };
            }

            if (local.ColumnStats is not null)
            {
                foreach (KeyValuePair<string, ColumnMinMax> kv in local.ColumnStats)
                {
                    if (!colStats.TryGetValue(kv.Key, out ColumnMinMax? existing))
                    {
                        colStats[kv.Key] = new ColumnMinMax { Min = kv.Value.Min, Max = kv.Value.Max };
                        continue;
                    }

                    if (kv.Value.Min is not null && (existing.Min is null || kv.Value.Min.CompareTo(existing.Min) < 0))
                        existing.Min = kv.Value.Min;

                    if (kv.Value.Max is not null && (existing.Max is null || kv.Value.Max.CompareTo(existing.Max) > 0))
                        existing.Max = kv.Value.Max;
                }
            }
        }
        merged.ColumnStats = colStats;

        return merged;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Drop / eviction
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Evicts the in-memory statistics entry for a dropped table. Without this every table ever
    /// touched leaks an <c>Entry</c> (plus its <c>SemaphoreSlim</c>) for the process lifetime.
    /// Sets the entry's drop fence first so a background flush already holding the entry
    /// reference aborts instead of re-creating the persisted stats key after the drop.
    /// Used alone by the deferred (recoverable-orphan) DROP TABLE path, which intentionally
    /// keeps the persisted blob: a later RELINK restores the table and reloads it, and the
    /// orphan reclaimer deletes it with the rest of the table keyspace otherwise.
    /// </summary>
    public void EvictTableStats(DatabaseDescriptor database, string tableId)
    {
        if (_cache.TryRemove(CacheKey(database.Id, tableId), out Entry? entry))
            entry.Dropped = true;
    }

    /// <summary>
    /// Evicts every cached entry belonging to a database, for use when this node stops holding that
    /// database open. Returns how many entries were released.
    ///
    /// <para>Statistics are per table and the cache is process-wide, so without this the cost of
    /// having <em>once</em> served a database outlives the database being closed: its tables keep an
    /// <c>Entry</c> each — two semaphores and several dictionaries — until the process ends. Reopening
    /// reloads them from the persisted blob, which is the same thing a cold node does, so nothing is
    /// lost beyond the reload.</para>
    ///
    /// <para>Entries are fenced as dropped on the way out, exactly as a dropped table's entry is, so a
    /// background flush still holding a reference aborts instead of writing a stale view back over
    /// state another node may have advanced.</para>
    /// </summary>
    public int EvictDatabaseStats(string dbId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dbId);

        string prefix = string.Concat(dbId, ":");
        int evicted = 0;

        foreach (string key in _cache.Keys)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            if (_cache.TryRemove(key, out Entry? entry))
            {
                entry.Dropped = true;
                evicted++;
            }
        }

        return evicted;
    }

    /// <summary>
    /// Evicts the in-memory entry (see <see cref="EvictTableStats"/>) and deletes the persisted
    /// <c>{dbId}:stats:{tableId}</c> blob inside the caller's DDL transaction. Used by the
    /// immediate (FORCE / branch) DROP TABLE path, which otherwise orphans the blob until the
    /// database itself is dropped. The delete joins the DDL transaction so an aborted drop
    /// restores the blob along with the table.
    /// </summary>
    public async Task DropTableStatsAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
    {
        EvictTableStats(database, tableId);

        string kahunaKey = KahunaKey(database.Id, tableId);

        (KeyValueResponseType lockType, _, _, _) =
            await database.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, kahunaKey, 0, KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()
            ).ConfigureAwait(false);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Failed to acquire lock on stats key '{kahunaKey}': {lockType}"
            );

        (KeyValueResponseType deleteType, _, _) = await database.Kahuna.Kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId, kahunaKey, KeyValueDurability.Persistent, CancellationToken.None,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()
        ).ConfigureAwait(false);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Failed to delete stats key '{kahunaKey}': {deleteType}"
            );

        tx.TrackModified(kahunaKey, KeyValueDurability.Persistent);
    }

    private Task LoadAndCacheAsync(DatabaseDescriptor database, TableDescriptor table)
        => EnsureLoadedAsync(database, table.Id, table.Name);

    /// <summary>
    /// Merges the persisted Kahuna base into the cached entry and marks it <c>Loaded</c>, at most
    /// once per table. Concurrent callers serialize on the entry's load lock and the losers see
    /// <c>Loaded == true</c> when they acquire it, so every caller that awaits this observes a
    /// loaded entry — an early return while a load is still in flight would let ANALYZE overwrite
    /// and flush stats that <see cref="FlushInternalAsync"/> then drops (it skips unloaded entries),
    /// and would make every getter report "no statistics" until the background load happened to
    /// finish. A failed load leaves the entry unloaded so the next caller retries.
    /// </summary>
    private async Task EnsureLoadedAsync(DatabaseDescriptor database, string tableId, string tableLabel)
    {
        string key = CacheKey(database.Id, tableId);

        if (_cache.TryGetValue(key, out Entry? existing) && existing.Loaded)
            return;

        Entry entry = _cache.GetOrAdd(key, _ => new Entry());

        await entry.LoadLock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (entry.Loaded)
                return;

            TableStatistics? loaded = await LoadFromKahunaByIdAsync(database, tableId).ConfigureAwait(false);

            if (loaded is not null)
            {
                MergeBaseIntoEntry(entry, loaded.RowCount >= 0 ? loaded.RowCount : 0);
                MergeIndexCounts(entry, loaded.IndexEntryCounts);
                MergeColumnStats(entry, loaded.ColumnStats);
                MergeHistograms(entry, loaded.Histograms);
                MergeNdv(entry, loaded.ColumnNdv, loaded.KeyNdv);
                MergeStaleness(entry, loaded.MutationsSinceAnalyze, loaded.LastAnalyzedAt);

                // Record the persisted base as the flush baseline: everything already in the
                // blob must not be re-added by a later delta-merge flush, while local deltas
                // accumulated before this load (merged on top above) remain unflushed.
                entry.FlushedRowCount  = loaded.RowCount >= 0 ? loaded.RowCount : 0;
                entry.FlushedMutations = Math.Max(0, loaded.MutationsSinceAnalyze);
                if (loaded.IndexEntryCounts is not null)
                    entry.FlushedIndexEntries = new Dictionary<string, long>(loaded.IndexEntryCounts, StringComparer.Ordinal);
            }

            entry.Loaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Stats load failed for table {Table}", tableLabel);
        }
        finally
        {
            entry.LoadLock.Release();
        }
    }

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
            if (entry.Histograms is not null)
                return;

            entry.Histograms = new Dictionary<string, ColumnHistogram>(persisted, StringComparer.Ordinal);

            // Installing loaded histograms is a histogram publish on this node: bump the analyze
            // generation so plans cached before the load (planned rule-based, without histograms)
            // are invalidated and re-planned against the real statistics.
            Volatile.Write(ref entry.AnalyzeGeneration, Interlocked.Increment(ref globalAnalyzeGeneration));
        }
    }

    // Merge persisted staleness state on load. Mutations follow the same base+pending rule as
    // RowCount (any churn tracked before the load is added to the persisted base). LastAnalyzedAt
    // is taken from the persisted value only when no in-memory ANALYZE has set it this session.
    private static void MergeStaleness(Entry entry, long persistedMutations, Kommander.Time.HLCTimestamp persistedLastAnalyzedAt)
    {
        if (persistedMutations > 0)
        {
            long pending, merged;
            do
            {
                pending = Interlocked.Read(ref entry.MutationsSinceAnalyze);
                merged = Math.Max(0, persistedMutations + pending);
            } while (Interlocked.CompareExchange(ref entry.MutationsSinceAnalyze, merged, pending) != pending);
        }

        if (!persistedLastAnalyzedAt.IsNull())
        {
            lock (entry.ColumnStatsLock)
            {
                if (entry.LastAnalyzedAt.IsNull())
                    entry.LastAnalyzedAt = persistedLastAnalyzedAt;
            }
        }
    }

    private static Kommander.Time.HLCTimestamp ReadLastAnalyzedAt(Entry entry)
    {
        lock (entry.ColumnStatsLock)
            return entry.LastAnalyzedAt;
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
    /// Test-only: when set, the next (and every) stats flush faults after staging the value but before
    /// commit, so a test can prove a failed publish leaves the persisted blob atomically unchanged.
    /// </summary>
    internal volatile bool FailFlushForTesting;

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
    /// build side exceeded <c>CamusDBOptions.SpillEffectiveThreshold</c> with spill enabled.
    /// Incremented by <c>QueryJoinExecutor.GraceHashJoinAsync</c>. Lets a test prove the Grace path
    /// was actually taken (vs the in-memory hash join) independent of result values. Test-only;
    /// not thread-safe.
    /// </summary>
    internal int HashJoinGracePathCount { get; set; }

    /// <summary>
    /// Counts how many times <c>QueryAggregator.AggregatePartitionAsync</c> recursively
    /// repartitioned a GROUP BY partition because its distinct-group count exceeded
    /// <c>CamusDBOptions.SpillEffectiveThreshold</c>. Lets a test prove the recursion path was
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
