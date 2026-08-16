
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Coverage for the background sweep, the max-entries cap, and the sweep's logging cadence.
/// Each test owns its own <see cref="SqlParserCache"/> instance so tests are fully
/// independent and require no [NonParallelizable] attribute.
/// </summary>
[TestFixture]
public class TestSQLParserCacheSweep
{
    // ── Sweep removes expired, keeps live ─────────────────────────────────────

    [Test]
    public async Task Sweep_RemovesExpiredEntries()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);

        NodeAst ast = SQLParserProcessor.Parse("SELECT id FROM users WHERE x = 1");
        cache.Store("SELECT id FROM users WHERE x = 1", ast, ttlMs: 1L);  // 1 ms
        Assert.That(cache.Count, Is.EqualTo(1));

        Thread.Sleep(10);

        cache.Sweep();
        Assert.That(cache.Count, Is.EqualTo(0), "Expired entry must be removed by Sweep");
    }

    [Test]
    public async Task Sweep_PreservesLiveEntries()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        const string sql = "SELECT id FROM orders WHERE active = true";

        SQLParserProcessor.Parse(sql, cache);    // populates cache with 300 s TTL
        Assert.That(cache.Count, Is.EqualTo(1));

        cache.Sweep();   // nothing expired yet
        Assert.That(cache.Count, Is.EqualTo(1), "Live entry must survive a sweep");
    }

    [Test]
    public async Task Sweep_MixedExpiredAndLive_OnlyExpiredRemoved()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);

        NodeAst a1 = SQLParserProcessor.Parse("SELECT a FROM t1 WHERE id = 1");
        NodeAst a2 = SQLParserProcessor.Parse("SELECT b FROM t2 WHERE id = 2");
        NodeAst a3 = SQLParserProcessor.Parse("SELECT c FROM t3 WHERE id = 3");

        cache.Store("SELECT a FROM t1 WHERE id = 1", a1, ttlMs: 300_000L);   // survives
        cache.Store("SELECT b FROM t2 WHERE id = 2", a2, ttlMs: 1L);         // expires
        cache.Store("SELECT c FROM t3 WHERE id = 3", a3, ttlMs: 300_000L);   // survives
        Assert.That(cache.Count, Is.EqualTo(3));

        Thread.Sleep(10);

        cache.Sweep();
        Assert.That(cache.Count, Is.EqualTo(2), "Only the expired entry must be removed");
    }

    [Test]
    public void Sweep_EmptyCache_DoesNotThrow()
    {
        SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        Assert.That(cache.Count, Is.EqualTo(0));
        Assert.DoesNotThrow(() => cache.Sweep());
    }

    // ── Max-entries cap ────────────────────────────────────────────────────────

    [Test]
    public async Task MaxEntries_Cap_CacheDoesNotGrowBeyondLimit()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 3, sweepSeconds: 60);

        for (int i = 1; i <= 10; i++)
            SQLParserProcessor.Parse($"SELECT id FROM t WHERE col{i} = {i}", cache);

        Assert.That(cache.Count, Is.LessThanOrEqualTo(3),
            "Cache must not grow beyond maxEntries");
    }

    [Test]
    public async Task MaxEntries_CapOfOne_FirstEntryStoredSubsequentDropped()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 1, sweepSeconds: 60);

        SQLParserProcessor.Parse("SELECT id FROM a WHERE x = 1", cache);
        int countAfterFirst = cache.Count;

        SQLParserProcessor.Parse("SELECT id FROM b WHERE y = 2", cache);
        int countAfterSecond = cache.Count;

        Assert.That(countAfterFirst, Is.EqualTo(1));
        Assert.That(countAfterSecond, Is.EqualTo(1), "Second entry must be dropped when cap = 1");
    }

    [Test]
    public async Task MaxEntries_ZeroCap_CacheIsUnbounded()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 0, sweepSeconds: 60);

        for (int i = 1; i <= 20; i++)
            SQLParserProcessor.Parse($"SELECT id FROM t WHERE col = {i}", cache);

        Assert.That(cache.Count, Is.EqualTo(20),
            "With maxEntries = 0 (unbounded) all entries must be stored");
    }

    [Test]
    public async Task MaxEntries_HitOnExistingKey_DoesNotCountAsNewEntry()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2, sweepSeconds: 60);

        SQLParserProcessor.Parse("SELECT id FROM t WHERE x = 1", cache);
        SQLParserProcessor.Parse("SELECT id FROM t WHERE x = 2", cache);
        Assert.That(cache.Count, Is.EqualTo(2));

        // Repeated parse of an existing key — must be a cache hit, count stays at 2.
        SQLParserProcessor.Parse("SELECT id FROM t WHERE x = 1", cache);
        Assert.That(cache.Count, Is.EqualTo(2));
    }

    // ── DisposeAsync — sweeper shuts down cleanly ─────────────────────────────

    [Test]
    public async Task DisposeAsync_CanBeCalledWhenSweepNeverStarted()
    {
        // DisposeAsync before any Store() (sweeper never launched) must not throw.
        SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        await cache.DisposeAsync();
    }

    [Test]
    public async Task DisposeAsync_AfterSweepStarted_CompletesWithoutHang()
    {
        SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);

        // Trigger the sweeper by storing an entry.
        SQLParserProcessor.Parse("SELECT id FROM t WHERE id = 99", cache);

        Task disposeTask = cache.DisposeAsync().AsTask();
        bool completed = await Task.WhenAny(disposeTask, Task.Delay(5000)) == disposeTask;

        Assert.That(completed, Is.True, "DisposeAsync must complete without hanging");
    }

    [Test]
    public async Task DisposeAsync_IsIdempotent()
    {
        SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        SQLParserProcessor.Parse("SELECT id FROM t WHERE id = 100", cache);

        await cache.DisposeAsync();
        Assert.DoesNotThrowAsync(() => cache.DisposeAsync().AsTask());
    }

    // ── Eviction counter ──────────────────────────────────────────────────────

    [Test]
    public async Task Sweep_EvictionCounter_IncreasesForRemovedEntries()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);

        long before = cache.Evictions;

        NodeAst ast = SQLParserProcessor.Parse("SELECT id FROM ev_test WHERE x = 1");
        cache.Store("SELECT id FROM ev_test WHERE x = 1", ast, ttlMs: 1L);

        Thread.Sleep(10);
        cache.Sweep();

        Assert.That(cache.Evictions, Is.GreaterThan(before),
            "Eviction counter must increment when the sweep removes an entry");
    }

    // ── Sweep logging cadence ─────────────────────────────────────────────────

    /// <summary>
    /// The sweep line reports cumulative counters, so on an idle server it would otherwise repeat
    /// an identical line every interval forever. It must be emitted only when something moved.
    /// </summary>
    [Test]
    public async Task SweepLogging_IdleCache_DoesNotRepeatAnIdenticalLine()
    {
        CountingLogger logger = new();
        await using SqlParserCache cache = new(logger, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 1);

        SQLParserProcessor.Parse("SELECT id FROM t WHERE x = 1", cache);   // starts the sweeper

        // Long enough for several sweeps; only the first has anything new to say.
        await Task.Delay(3500);
        Assert.That(logger.Count, Is.EqualTo(1),
            "an idle cache must log the sweep once, not once per interval");

        // Traffic moves the counters, so the next sweep is worth a line again.
        SQLParserProcessor.Parse("SELECT id FROM t WHERE x = 2", cache);

        await Task.Delay(2500);
        Assert.That(logger.Count, Is.EqualTo(2),
            "a sweep following real cache activity must still be logged");
    }

    /// <summary>Counts <c>Debug</c> lines; the sweep is the only thing the cache logs.</summary>
    private sealed class CountingLogger : ILogger
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Interlocked.Increment(ref _count);
    }
}
