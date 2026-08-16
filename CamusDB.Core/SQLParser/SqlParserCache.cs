
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
    /// Absolute expiry expressed as <see cref="Environment.TickCount64"/> milliseconds.
    /// A monotonic source avoids wall-clock adjustment surprises.
    /// </summary>
    public long ExpirationTicks { get; set; }

    public ParsedSqlCacheEntry(NodeAst ast, long expirationTicks)
    {
        Ast = ast;
        ExpirationTicks = expirationTicks;
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
    public SqlParserCache(ILogger? logger, int ttlSeconds, int maxEntries, int sweepSeconds)
    {
        _logger = logger;
        _ttlMs = (long)ttlSeconds * 1000;
        _maxEntries = maxEntries;
        _sweepSeconds = sweepSeconds > 0 ? sweepSeconds : 60;
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
    internal void Retune(int ttlSeconds, int maxEntries, int sweepSeconds)
    {
        Volatile.Write(ref _ttlMs, (long)ttlSeconds * 1000);
        _maxEntries = maxEntries;
        _sweepSeconds = sweepSeconds > 0 ? sweepSeconds : 60;

        if (maxEntries <= 0 || _cache.Count <= maxEntries)
            return;

        // Snapshot, order by nearest expiry, and remove until at/below the cap. Concurrent stores
        // may race this trim; the cap is a bound on growth, not an exact invariant, and the next
        // Store re-checks it anyway.
        List<KeyValuePair<string, ParsedSqlCacheEntry>> snapshot = new(_cache);
        snapshot.Sort(static (a, b) => a.Value.ExpirationTicks.CompareTo(b.Value.ExpirationTicks));

        foreach (KeyValuePair<string, ParsedSqlCacheEntry> pair in snapshot)
        {
            if (_cache.Count <= maxEntries)
                break;

            if (_cache.TryRemove(pair.Key, out _))
                Interlocked.Increment(ref _evictions);
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
            _cache.TryRemove(sql, out _);
            Interlocked.Increment(ref _evictions);
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

        long expiresAt = Environment.TickCount64 + ttlMs;
        ParsedSqlCacheEntry newEntry = new(ast, expiresAt);

        if (!_cache.TryAdd(sql, newEntry))
        {
            // Another thread inserted first; slide its expiration so it stays hot.
            if (_cache.TryGetValue(sql, out ParsedSqlCacheEntry? existing))
                existing.ExpirationTicks = expiresAt;
        }

        EnsureSweepRunning();
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
            {
                if (_cache.TryRemove(pair.Key, out _))
                    Interlocked.Increment(ref _evictions);
            }
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
