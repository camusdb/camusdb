
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// A single entry in the SQL parser AST cache.
/// </summary>
/// <remarks>
/// <see cref="ExpirationTicks"/> is mutable so the cache can extend it on each hit (sliding TTL)
/// without allocating a new entry. All other fields are immutable after construction.
/// </remarks>
internal sealed class ParsedSqlCacheEntry
{
    public NodeAst Ast { get; }

    /// <summary>
    /// Estimated bytes this entry retains — the AST, its literal strings and the SQL text used as
    /// the key. Stored on the entry so a removal can decrement the running total without deriving
    /// the estimate a second time, and so the total stays consistent if the estimate changes.
    /// </summary>
    public long EstimatedBytes { get; }

    /// <summary>
    /// Absolute expiry expressed as <see cref="Environment.TickCount64"/> milliseconds.
    /// A monotonic source avoids wall-clock adjustment surprises.
    /// </summary>
    public long ExpirationTicks { get; set; }

    public ParsedSqlCacheEntry(NodeAst ast, long expirationTicks, long estimatedBytes)
    {
        Ast = ast;
        ExpirationTicks = expirationTicks;
        EstimatedBytes = estimatedBytes;
    }
}

/// <summary>
/// Per-<see cref="CommandsExecutor.CommandExecutor"/> cache of parsed SQL ASTs, keyed by the
/// exact SQL string.
/// </summary>
/// <remarks>
/// Entries use a sliding TTL: each cache hit extends <see cref="ParsedSqlCacheEntry.ExpirationTicks"/>
/// by the configured TTL so a frequently-used statement is never evicted while in use.
/// <para>
/// Thread safety: <see cref="ConcurrentDictionary{TKey,TValue}"/> provides safe concurrent reads and
/// writes. A brief double-parse is acceptable when two threads race to insert the same new key (last
/// writer wins; both trees are structurally equal). No lock is held on the parse path.
/// </para>
/// <para>
/// Caching is correct because a <see cref="NodeAst"/> returned by
/// <see cref="SQLParserProcessor.Parse(string)"/> is immutable after it is returned — see the invariant
/// documented on <see cref="NodeAst"/> and <see cref="SQLParserProcessor"/>.
/// </para>
/// <para>
/// <b>Max-entries cap policy:</b> when the dictionary already holds <see cref="_maxEntries"/>
/// entries, new statements are <em>not</em> cached until the background sweep reclaims expired entries.
/// This is intentionally the simplest possible policy — allocation-free, contention-free, and sufficient
/// to bound memory against floods of unique ad-hoc SQL. The TTL, cap, and sweep cadence can be swapped
/// at runtime via <see cref="Retune"/>, which also trims an over-cap population.
/// </para>
/// <para>
/// <b>Lifecycle:</b> the background sweep task starts lazily on the first <see cref="Store"/>
/// call and is stopped cleanly via <see cref="DisposeAsync"/>, which is called from
/// <see cref="CommandsExecutor.CommandExecutor.DisposeAsync"/>.
/// </para>
/// </remarks>
public sealed class SqlParserCache : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ParsedSqlCacheEntry> _cache = new();

    // Not readonly: Retune swaps these at runtime. _ttlMs is read/written via Volatile so a torn
    // read of the 64-bit value is impossible on 32-bit runtimes; the ints are atomic by themselves.
    private long _ttlMs;
    private volatile int _maxEntries;
    private volatile int _sweepSeconds;
    private long _maxBytes;

    // Running estimate of what the live entries retain. Approximate by construction: it is updated
    // outside the dictionary's own atomicity, so a concurrent add and remove can leave it slightly
    // off. It bounds growth; it is not an invariant, and every Store re-checks it.
    private long _approxBytes;

    private readonly ILogger? _logger;

    // Background sweeper state — one per instance, started lazily on first Store() call.
    private CancellationTokenSource? _sweepCts;
    private Task? _sweepTask;
    private readonly Lock _sweepLock = new();

    // ── Observable counters ────────────────────────────────────────────────────

    private long _hits;
    private long _misses;
    private long _evictions;

    // Counters as of the last line the sweep loop logged, so an idle cache stops repeating itself.
    // Touched only by the single sweep task, hence no synchronization.
    private long _loggedHits = -1;
    private long _loggedMisses = -1;
    private long _loggedEvictions = -1;
    private int _loggedCount = -1;

    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);
    public long Evictions => Interlocked.Read(ref _evictions);

    /// <summary>Current number of live entries in the cache.</summary>
    public int Count => _cache.Count;

    /// <summary>
    /// Estimated bytes the live entries retain. Approximate: see <see cref="EstimateBytes"/> for how
    /// an entry's cost is derived and why it is an estimate rather than a measurement.
    /// </summary>
    public long ApproxBytes => Interlocked.Read(ref _approxBytes);

    /// <summary>
    /// Multiplier from SQL text length to retained bytes. A parsed statement retains far more than
    /// its text: one <see cref="NodeAst"/> per token with nine child references, plus a string per
    /// literal, plus the text itself as the dictionary key.
    ///
    /// <para>Derived on 2026-09-15 by filling a cache with unique INSERT statements and reading the
    /// managed heap. Before this bound existed, 2,048 statements of 500 rows each held about
    /// 1,348 MiB; with a 64 MiB budget the same load holds about 79 MiB. The per-entry ratio implied
    /// by those two runs differs (about 20x and about 40x), because a whole-heap delta also counts
    /// transient garbage, and that noise is a larger share once few entries survive. So treat 32 as
    /// an order-of-magnitude constant: the budget is a bound on growth, not an accounting of bytes,
    /// and the heap will sit somewhat above it rather than below.</para>
    /// </summary>
    private const long EstimatedBytesPerSqlChar = 32;

    /// <summary>Estimated retained bytes for a statement of this text length.</summary>
    private static long EstimateBytes(string sql) => (long)sql.Length * EstimatedBytesPerSqlChar;

    /// <summary>Volatile view of the sliding TTL so a concurrent <see cref="Retune"/> is never torn.</summary>
    private long TtlMs => Volatile.Read(ref _ttlMs);

    /// <summary>
    /// Returns <see langword="true"/> when the cache is active (TTL &gt; 0).
    /// When <see langword="false"/> every <see cref="TryGet"/> returns a miss and every
    /// <see cref="Store"/> is a no-op; parsing behaviour is identical to pre-cache.
    /// </summary>
    public bool IsEnabled => TtlMs > 0;

    // ── Construction ───────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a new cache instance.
    /// </summary>
    /// <param name="logger">
    /// Optional logger; receives a <c>Debug</c> line after a sweep whose counters differ from the
    /// previously logged ones, so an idle cache does not repeat an identical line every interval.
    /// </param>
    /// <param name="ttlSeconds">
    /// Sliding TTL in seconds. <c>0</c> or negative disables the cache entirely (every parse
    /// re-lexes from scratch). Maps to <see cref="CamusDBOptions.SqlParserCacheTtlSeconds"/>.
    /// </param>
    /// <param name="maxEntries">
    /// Maximum entries the dictionary may hold. <c>0</c> = unbounded.
    /// Maps to <see cref="CamusDBOptions.SqlParserCacheMaxEntries"/>.
    /// </param>
    /// <param name="sweepSeconds">
    /// Interval between background eviction sweeps in seconds; clamped to 1 if &lt;= 0.
    /// Maps to <see cref="CamusDBOptions.SqlParserCacheSweepSeconds"/>.
    /// </param>
    public SqlParserCache(ILogger? logger, int ttlSeconds, int maxEntries, int sweepSeconds, long maxBytes = 0)
    {
        _logger = logger;
        _ttlMs = (long)ttlSeconds * 1000;
        _maxEntries = maxEntries;
        _sweepSeconds = sweepSeconds > 0 ? sweepSeconds : 60;
        _maxBytes = maxBytes;
    }

    /// <summary>
    /// Applies a newly published configuration to a live cache: swaps the TTL, entry cap, and sweep
    /// cadence, then trims to the new cap. The sweep loop reads <c>_sweepSeconds</c> at the top of
    /// each iteration, so a new cadence takes effect after the currently running delay elapses; a
    /// disposed cache's sweeper is never resurrected (this method starts nothing).
    ///
    /// <para>When the new cap is smaller than the current population, entries are removed in
    /// ascending <see cref="ParsedSqlCacheEntry.ExpirationTicks"/> order — the entries that would
    /// die soonest go first. There is no LRU order to honor (the insert policy is
    /// stop-inserting-when-full), so nearest-expiry is the only defensible eviction order.</para>
    /// </summary>
    internal void Retune(int ttlSeconds, int maxEntries, int sweepSeconds, long maxBytes)
    {
        Volatile.Write(ref _ttlMs, (long)ttlSeconds * 1000);
        _maxEntries = maxEntries;
        _sweepSeconds = sweepSeconds > 0 ? sweepSeconds : 60;
        Volatile.Write(ref _maxBytes, maxBytes);

        bool overEntries = maxEntries > 0 && _cache.Count > maxEntries;
        bool overBytes = maxBytes > 0 && Interlocked.Read(ref _approxBytes) > maxBytes;

        if (!overEntries && !overBytes)
            return;

        // Snapshot, order by nearest expiry, and remove until at/below both caps. Concurrent stores
        // may race this trim; the caps bound growth, they are not exact invariants, and the next
        // Store re-checks them anyway.
        List<KeyValuePair<string, ParsedSqlCacheEntry>> snapshot = new(_cache);
        snapshot.Sort(static (a, b) => a.Value.ExpirationTicks.CompareTo(b.Value.ExpirationTicks));

        foreach (KeyValuePair<string, ParsedSqlCacheEntry> pair in snapshot)
        {
            if ((maxEntries <= 0 || _cache.Count <= maxEntries)
                && (maxBytes <= 0 || Interlocked.Read(ref _approxBytes) <= maxBytes))
                break;

            RemoveEntry(pair.Key);
        }
    }

    // ── Lookup ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Tries to retrieve a live (non-expired) cached AST for <paramref name="sql"/>.
    /// On a hit the entry's expiration is slid forward by the configured TTL.
    /// Always returns <see langword="false"/> when the cache is disabled.
    /// </summary>
    public bool TryGet(string sql, out NodeAst? ast)
    {
        long ttlMs = TtlMs;
        if (ttlMs <= 0)
        {
            ast = null;
            Interlocked.Increment(ref _misses);
            return false;
        }

        if (_cache.TryGetValue(sql, out ParsedSqlCacheEntry? entry))
        {
            long now = Environment.TickCount64;
            if (entry.ExpirationTicks >= now)
            {
                entry.ExpirationTicks = now + ttlMs;
                ast = entry.Ast;
                Interlocked.Increment(ref _hits);
                return true;
            }
            // Expired — remove eagerly so the sweep doesn't have to race with us.
            RemoveEntry(sql);
        }

        ast = null;
        Interlocked.Increment(ref _misses);
        return false;
    }

    // ── Insert ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Inserts a successfully-parsed AST into the cache using the configured TTL.
    /// No-op when the cache is disabled or when the max-entries cap is reached.
    /// </summary>
    public void Store(string sql, NodeAst ast) => Store(sql, ast, TtlMs);

    /// <summary>
    /// Inserts a successfully-parsed AST with an explicit TTL in milliseconds.
    /// Intended for tests that need precise expiry control; production code uses
    /// <see cref="Store(string, NodeAst)"/>.
    /// </summary>
    public void Store(string sql, NodeAst ast, long ttlMs)
    {
        if (ttlMs <= 0)
            return;  // disabled — no-op

        int maxEntries = _maxEntries;
        if (maxEntries > 0 && _cache.Count >= maxEntries)
            return;  // cap reached — skip silently; sweep will make room

        long cost = EstimateBytes(sql);

        // Byte budget: make room rather than refuse. Refusing would be the cheaper policy, but it
        // would also make a stream of large unique statements — a logical restore — parse the same
        // text more than once per request: the transport reads the root node type before the engine
        // parses it for execution, and the routing collector parses it again afterwards. Those
        // repeats are cache hits today, and they stay hits only if the statement is cached at all.
        // Evicting to fit keeps every within-request hit while bounding what the cache retains.
        if (!MakeRoomForBytes(cost))
            return;  // one entry alone exceeds the whole budget — leave it uncached

        long expiresAt = Environment.TickCount64 + ttlMs;
        ParsedSqlCacheEntry newEntry = new(ast, expiresAt, cost);

        if (_cache.TryAdd(sql, newEntry))
        {
            Interlocked.Add(ref _approxBytes, cost);
        }
        else
        {
            // Another thread inserted first; slide its expiration so it stays hot.
            if (_cache.TryGetValue(sql, out ParsedSqlCacheEntry? existing))
                existing.ExpirationTicks = expiresAt;
        }

        EnsureSweepRunning();
    }

    /// <summary>
    /// Evicts nearest-expiry entries until <paramref name="cost"/> more bytes fit inside the budget.
    /// Returns <see langword="false"/> when the entry could never fit, which leaves it uncached.
    ///
    /// <para>Nearest-expiry is the same order <see cref="Retune"/> uses, and for the same reason:
    /// the insert policy is not an LRU, so there is no recency order to honor, and the entries
    /// closest to dying anyway are the cheapest to lose.</para>
    /// </summary>
    private bool MakeRoomForBytes(long cost)
    {
        long maxBytes = Volatile.Read(ref _maxBytes);

        if (maxBytes <= 0)
            return true;   // unbounded

        if (cost > maxBytes)
            return false;  // never fits, whatever we evict

        if (Interlocked.Read(ref _approxBytes) + cost <= maxBytes)
            return true;

        List<KeyValuePair<string, ParsedSqlCacheEntry>> snapshot = new(_cache);
        snapshot.Sort(static (a, b) => a.Value.ExpirationTicks.CompareTo(b.Value.ExpirationTicks));

        foreach (KeyValuePair<string, ParsedSqlCacheEntry> pair in snapshot)
        {
            if (Interlocked.Read(ref _approxBytes) + cost <= maxBytes)
                break;

            RemoveEntry(pair.Key);
        }

        // A concurrent store may have taken the room we just freed. The budget bounds growth rather
        // than holding exactly, so admit the entry either way and let the next store re-check.
        return true;
    }

    /// <summary>
    /// Removes one entry and decrements the byte estimate, counting an eviction. The single place
    /// that removes an entry outside <see cref="TryGet"/>, so the running total cannot drift from
    /// the dictionary through a path that forgot to decrement it.
    /// </summary>
    private bool RemoveEntry(string sql)
    {
        if (!_cache.TryRemove(sql, out ParsedSqlCacheEntry? removed))
            return false;

        Interlocked.Add(ref _approxBytes, -removed.EstimatedBytes);
        Interlocked.Increment(ref _evictions);
        return true;
    }

    // ── Background sweeper ─────────────────────────────────────────────────────

    private void EnsureSweepRunning()
    {
        if (_sweepTask is not null)
            return;

        lock (_sweepLock)
        {
            if (_sweepTask is not null)
                return;

            CancellationTokenSource cts = new();
            _sweepCts = cts;
            CancellationToken token = cts.Token;   // extract struct while cts is alive
            _sweepTask = Task.Run(() => SweepLoopAsync(token));
        }
    }

    private async Task SweepLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(_sweepSeconds), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            Sweep();

            if (_logger is not null)
                LogSweepIfChanged();
        }
    }

    /// <summary>
    /// Emits the sweep line only when a counter moved since the last one emitted.
    /// </summary>
    /// <remarks>
    /// The counters are cumulative, so an unchanged tuple means no lookup, insert or eviction
    /// happened during the whole interval — a line that says nothing but repeats forever on an
    /// idle server, drowning the debug log. Suppressing the repeats keeps every line meaningful:
    /// a line appearing at all now means the cache saw traffic since the previous one.
    /// </remarks>
    private void LogSweepIfChanged()
    {
        int count = Count;
        long hits = Hits, misses = Misses, evictions = Evictions;

        if (count == _loggedCount && hits == _loggedHits && misses == _loggedMisses && evictions == _loggedEvictions)
            return;

        _loggedCount = count;
        _loggedHits = hits;
        _loggedMisses = misses;
        _loggedEvictions = evictions;

        Log.LogParserCacheSweep(_logger!, count, hits, misses, evictions);
    }

    /// <summary>
    /// Removes all entries whose <see cref="ParsedSqlCacheEntry.ExpirationTicks"/> is in the past.
    /// Called by the background sweeper; also exposed for tests.
    /// </summary>
    public void Sweep()
    {
        long now = Environment.TickCount64;

        foreach (KeyValuePair<string, ParsedSqlCacheEntry> pair in _cache)
        {
            if (pair.Value.ExpirationTicks < now)
                RemoveEntry(pair.Key);
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Stops the background sweep task and waits for it to exit cleanly.
    /// Called automatically by <see cref="DisposeAsync"/>.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        Task? task;
        CancellationTokenSource? cts;

        lock (_sweepLock)
        {
            task = _sweepTask;
            cts = _sweepCts;
            _sweepTask = null;
            _sweepCts = null;
        }

        if (cts is not null)
            await cts.CancelAsync().ConfigureAwait(false);

        if (task is not null)
        {
            try { await task.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }

        // Dispose only after the task has exited — the task's lambda captures the CTS token struct,
        // but the CTS itself must outlive the task to avoid ObjectDisposedException on finalisation.
        cts?.Dispose();
    }

    /// <summary>
    /// Removes all entries and resets counters. Useful for deterministic test setup.
    /// Does not stop the background sweeper.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
        Interlocked.Exchange(ref _hits, 0);
        Interlocked.Exchange(ref _misses, 0);
        Interlocked.Exchange(ref _evictions, 0);
    }
}
