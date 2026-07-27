/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Per-fingerprint single-flight gate.
///
/// Validates that concurrent cold-cache requests for the same fingerprint execute the query
/// plan only once. The first caller (owner) computes and publishes; all concurrent callers
/// (waiters) block until the owner signals, then re-probe the cache and serve the published
/// entry. Waiters that time out before the owner finishes compute independently (no blocked
/// result is discarded).
///
/// Also validates that owner cancellation or failure wakes waiters with a not-published signal
/// (so they compute independently) rather than blocking them until timeout, and — the safety
/// property the re-probe exists for — that a waiter which joins after a committed write evicted
/// the entry executes live instead of being handed the pre-write rows.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryResultCacheSingleFlight : CommandsExecutor.BaseTest
{
    private QueryResultCache? _cache;

    protected override CommandExecutor CreateCommandExecutor()
    {
        _cache = new QueryResultCache(sweepIntervalMs: -1);
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: _cache);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task SetupRobotsTable(string dbname, DatabaseDescriptor db, CommandExecutor exec)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await exec.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE robots (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)", null));

        KvTransaction ins = await db.Transactions.BeginAsync();
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO robots (id, name) VALUES (\"r1\", \"alpha\")", null));
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO robots (id, name) VALUES (\"r2\", \"beta\")", null));
        await db.Transactions.CommitAsync(ins);
    }

    private static async Task<CacheMetadataHolder> RunHintedQuery(
        string dbname, CommandExecutor exec, string cacheName)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        string sql = $"SELECT * FROM robots{{cache={cacheName}}}";
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await exec.ExecuteSQLQuery(new ExecuteSQLTicket(readTx, dbname, sql, null), meta);
        await foreach (QueryResultRow _ in cursor) { }
        return meta;
    }

    // ── Unit tests: SingleFlightSlot mechanism in isolation ───────────────────

    [Test]
    public void EnterSingleFlight_FirstCaller_IsOwner()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        SingleFlightSlot slot = cache.EnterSingleFlight("fp-1");
        Assert.IsTrue(slot.IsOwner, "First caller for a fingerprint must be the owner");
        cache.ExitSingleFlight("fp-1", published: false);  // cleanup
    }

    [Test]
    public void EnterSingleFlight_SecondConcurrentCaller_IsWaiter()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        SingleFlightSlot owner = cache.EnterSingleFlight("fp-2");
        SingleFlightSlot waiter = cache.EnterSingleFlight("fp-2");

        Assert.IsTrue(owner.IsOwner);
        Assert.IsFalse(waiter.IsOwner, "Second concurrent caller must be a waiter");

        cache.ExitSingleFlight("fp-2", published: false);  // cleanup
    }

    [Test]
    public void EnterSingleFlight_AfterExit_NewCallerIsOwner()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        SingleFlightSlot first = cache.EnterSingleFlight("fp-3");
        Assert.IsTrue(first.IsOwner);
        cache.ExitSingleFlight("fp-3", published: false);

        // After exit the in-flight entry is removed; next caller is the owner.
        SingleFlightSlot second = cache.EnterSingleFlight("fp-3");
        Assert.IsTrue(second.IsOwner, "After ExitSingleFlight, next caller must be the owner again");
        cache.ExitSingleFlight("fp-3", published: false);
    }

    [Test]
    public async Task WaitAsync_OwnerSignalsPublished_WaiterIsToldToReprobe()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        const string fp = "fp-waiter-success";

        SingleFlightSlot owner = cache.EnterSingleFlight(fp);
        SingleFlightSlot waiter = cache.EnterSingleFlight(fp);

        Task<bool> waitTask = waiter.WaitAsync(5_000, CancellationToken.None);

        cache.ExitSingleFlight(fp, published: true);

        bool published = await waitTask;
        Assert.IsTrue(published, "Waiter must be told the owner published so it re-probes the cache");
    }

    [Test]
    public async Task WaitAsync_OwnerFails_WaiterIsToldToExecuteLive()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        const string fp = "fp-waiter-failure";

        SingleFlightSlot owner = cache.EnterSingleFlight(fp);
        SingleFlightSlot waiter = cache.EnterSingleFlight(fp);

        Task<bool> waitTask = waiter.WaitAsync(5_000, CancellationToken.None);

        cache.ExitSingleFlight(fp, published: false);

        bool published = await waitTask;
        Assert.IsFalse(published, "An owner failure must leave the waiter to execute independently");
    }

    [Test]
    public async Task WaitAsync_Timeout_ReturnsFalse()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        const string fp = "fp-waiter-timeout";

        SingleFlightSlot owner = cache.EnterSingleFlight(fp);
        SingleFlightSlot waiter = cache.EnterSingleFlight(fp);

        // Waiter with very short timeout — owner does NOT signal.
        bool published = await waiter.WaitAsync(50 /*ms*/, CancellationToken.None);

        Assert.IsFalse(published, "A waiter that times out before the owner signals must execute independently");

        // Cleanup: signal owner so in-flight state is properly cleared.
        cache.ExitSingleFlight(fp, published: false);
    }

    [Test]
    public async Task WaitAsync_MultipleWaiters_AllReceiveTheSameSignal()
    {
        using var cache = new QueryResultCache(sweepIntervalMs: -1);
        const string fp = "fp-multi-waiter";

        SingleFlightSlot owner = cache.EnterSingleFlight(fp);
        SingleFlightSlot w1 = cache.EnterSingleFlight(fp);
        SingleFlightSlot w2 = cache.EnterSingleFlight(fp);
        SingleFlightSlot w3 = cache.EnterSingleFlight(fp);

        Task<bool>[] waitTasks = [
            w1.WaitAsync(5_000, CancellationToken.None),
            w2.WaitAsync(5_000, CancellationToken.None),
            w3.WaitAsync(5_000, CancellationToken.None),
        ];

        cache.ExitSingleFlight(fp, published: true);

        bool[] received = await Task.WhenAll(waitTasks);
        Assert.That(received, Has.All.True, "All waiters must receive the owner's published signal");
    }

    // ── Integration tests: single-flight through the full query path ───────────

    /// <summary>
    /// Deterministic end-to-end test of the waiter path.
    ///
    /// Strategy:
    /// 1. Run one cold query to discover the exact fingerprint the planner+builder will produce
    ///    for this query shape.
    /// 2. Evict that entry so the cache is cold again.
    /// 3. Pre-occupy the fingerprint as a <em>fake owner</em> by calling
    ///    <see cref="QueryResultCache.EnterSingleFlight"/> before the real query fires.
    /// 4. Launch the real query in a background task — it probes the cache (miss), hits the
    ///    single-flight gate, and blocks as a waiter.
    /// 5. Store an entry under that fingerprint whose rows are <em>not in the table</em> (a
    ///    sentinel value), then signal the fake owner as having published.
    /// 6. Assert the real query returned the sentinel rows and reported <c>Status = Hit</c>,
    ///    proving it re-probed the cache and served the entry rather than executing the plan.
    ///
    /// The rows reach the waiter through its own cache probe, never through the slot: the slot
    /// only carries the published flag.
    /// </summary>
    [Test]
    public async Task WaiterPath_EndToEnd_ReprobesAndServesPublishedEntryWithoutExecutingPlan()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "sf_waiter_e2e";

        // ── Step 1: warm run to discover the fingerprint ──────────────────────
        CacheMetadataHolder warmMeta = await RunHintedQuery(dbname, exec, cacheName);
        Assert.That(warmMeta.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "Warm-up run must be a cache miss");

        string? fp = _cache!.GetFirstFingerprintByCacheName(db.Id, cacheName);
        Assert.IsNotNull(fp, "Fingerprint must be discoverable after a cold miss");

        // ── Step 2: evict so the cache is cold again ──────────────────────────
        _cache.InvalidateEntry(db.Id, cacheName, fp!);
        Assert.That(_cache.EntryCount, Is.EqualTo(0), "Cache must be empty after eviction");

        // ── Step 3: pre-occupy as a fake owner ────────────────────────────────
        SingleFlightSlot fakeOwnerSlot = _cache.EnterSingleFlight(fp!);
        Assert.IsTrue(fakeOwnerSlot.IsOwner, "Pre-occupying the slot must make us the owner");

        // ── Step 4: launch real query in background — it will block as waiter ─
        // Build a sentinel result: one row with a column value that cannot appear
        // in the robots table (the column is named "sentinel" and doesn't exist there).
        ColumnValue sentinelValue = new(ColumnType.String, "single-flight-sentinel");
        var sentinelRow = new QueryResultRow(
            ObjectIdGenerator.Generate(),
            new Dictionary<string, ColumnValue> { ["sentinel"] = sentinelValue });

        CachedQueryResult injectedResult = new(
            CacheName: cacheName,
            DatabaseId: db.Id,
            Rows: [sentinelRow],
            ResultFingerprint: fp!,
            CachedAt: Kommander.Time.HLCTimestamp.Zero,
            Status: QueryCacheStatus.Miss);

        // The real query task is created but not yet awaited — it runs concurrently.
        var collectedRows = new List<QueryResultRow>();
        var meta = new CacheMetadataHolder();
        Task<CacheMetadataHolder> queryTask = Task.Run(async () =>
        {
            KvTransaction readTx = KvTransaction.CreateReadOnly();
            string sql = $"SELECT * FROM robots{{cache={cacheName}}}";
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await exec.ExecuteSQLQuery(new ExecuteSQLTicket(readTx, dbname, sql, null), meta);
            await foreach (QueryResultRow row in cursor)
                collectedRows.Add(row);
            return meta;
        });

        // Poll until the background query has called EnterSingleFlight as a waiter (WaiterCount
        // increments atomically under _lock the moment EnterSingleFlight returns a waiter slot).
        // This replaces a fixed Task.Delay that would race under a loaded CI: if the signal fires
        // before the query reaches EnterSingleFlight, the query becomes the owner rather than a
        // waiter and the test becomes a false negative.
        int pollMs = 0;
        while (_cache!.WaiterCount(fp!) == 0 && pollMs < 5_000)
        {
            await Task.Delay(10);
            pollMs += 10;
        }
        Assert.That(_cache.WaiterCount(fp!), Is.GreaterThanOrEqualTo(1),
            "Timed out waiting for the background query to register as a waiter");

        // ── Step 5: store the sentinel entry, then signal the fake owner ──────
        // The entry must exist in the cache before the signal: the waiter re-probes rather than
        // receiving rows through the slot, so a signal with nothing stored would make it execute
        // the plan instead.
        _cache.InjectEntryForTest(injectedResult, QueryDependencySet.Empty);
        _cache.ExitSingleFlight(fp!, published: true);

        // ── Step 6: assert the query returned the injected rows ───────────────
        CacheMetadataHolder queryMeta = await queryTask;

        Assert.That(queryMeta.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "A waiter whose re-probe finds the published entry must report Status=Hit");

        Assert.That(collectedRows, Has.Count.EqualTo(1),
            "Exactly the sentinel row must be returned (not the table's two real rows)");
        Assert.That(collectedRows[0].Row.ContainsKey("sentinel"), Is.True,
            "The returned row must carry the sentinel column, proving plan was not executed");
        Assert.That(collectedRows[0].Row["sentinel"].StrValue, Is.EqualTo("single-flight-sentinel"),
            "The sentinel value must match what was injected, proving the waiter path served it");
    }

    /// <summary>
    /// A query that begins after an entry was invalidated must never be served that entry through
    /// the single-flight slot.
    ///
    /// Ordering (fully deterministic, no timing assumptions):
    /// 1. an owner occupies the slot and stores an entry carrying sentinel rows;
    /// 2. the entry is invalidated, as a committed write's cache invalidation would do;
    /// 3. a new query probes the cache, misses, and joins the slot as a waiter;
    /// 4. the owner signals that it published.
    ///
    /// The waiter learns only that something was published; its own re-probe finds nothing, so it
    /// executes live and returns current table rows. Handing it the owner's materialized result
    /// instead would serve rows that predate the write which evicted them.
    /// </summary>
    [Test]
    public async Task WaiterJoiningAfterInvalidation_ExecutesLiveInsteadOfServingEvictedEntry()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "sf_late_waiter";

        // Discover the fingerprint, then clear the cache.
        await RunHintedQuery(dbname, exec, cacheName);
        string? fp = _cache!.GetFirstFingerprintByCacheName(db.Id, cacheName);
        Assert.IsNotNull(fp, "Fingerprint must be discoverable after a cold miss");
        _cache.InvalidateEntry(db.Id, cacheName, fp!);

        // Step 1: an owner holds the slot and has stored an entry with rows that cannot come from
        // the table, so serving them is unambiguous evidence of the detached-result path.
        SingleFlightSlot ownerSlot = _cache.EnterSingleFlight(fp!);
        Assert.IsTrue(ownerSlot.IsOwner);

        var sentinelRow = new QueryResultRow(
            ObjectIdGenerator.Generate(),
            new Dictionary<string, ColumnValue>
            {
                ["sentinel"] = new(ColumnType.String, "pre-invalidation-rows")
            });

        CachedQueryResult ownerResult = new(
            CacheName: cacheName,
            DatabaseId: db.Id,
            Rows: [sentinelRow],
            ResultFingerprint: fp!,
            CachedAt: HLCTimestamp.Zero,
            Status: QueryCacheStatus.Miss);

        _cache.InjectEntryForTest(ownerResult, QueryDependencySet.Empty);

        // Step 2: a committed write's invalidation removes the entry while the slot is still open.
        _cache.InvalidateEntry(db.Id, cacheName, fp!);
        Assert.That(_cache.EntryCount, Is.EqualTo(0),
            "The entry must be gone before the waiter joins, mirroring a committed write's eviction");

        // Step 3: a new query starts, misses, and joins the open slot as a waiter.
        var collectedRows = new List<QueryResultRow>();
        var meta = new CacheMetadataHolder();
        Task queryTask = Task.Run(async () =>
        {
            KvTransaction readTx = KvTransaction.CreateReadOnly();
            string sql = $"SELECT * FROM robots{{cache={cacheName}}}";
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await exec.ExecuteSQLQuery(new ExecuteSQLTicket(readTx, dbname, sql, null), meta);
            await foreach (QueryResultRow row in cursor)
                collectedRows.Add(row);
        });

        int pollMs = 0;
        while (_cache.WaiterCount(fp!) == 0 && pollMs < 5_000)
        {
            await Task.Delay(10);
            pollMs += 10;
        }
        Assert.That(_cache.WaiterCount(fp!), Is.GreaterThanOrEqualTo(1),
            "Timed out waiting for the query to register as a waiter");

        // Step 4: the owner reports success — but its entry no longer exists.
        _cache.ExitSingleFlight(fp!, published: true);

        await queryTask;

        Assert.That(collectedRows.Any(r => r.Row.ContainsKey("sentinel")), Is.False,
            "A waiter that joined after the invalidation must not receive the evicted rows");
        Assert.That(collectedRows, Has.Count.EqualTo(2),
            "The waiter must execute live and return the table's current rows");
        Assert.That(meta.Status, Is.Not.EqualTo(QueryCacheStatus.Hit),
            "Serving a Hit here would mean the waiter trusted an entry that no longer exists");
    }

    /// <summary>
    /// Verifies that after the single-flight owner publishes, subsequent (non-concurrent)
    /// requests see a regular cache Hit — single-flight doesn't interfere with the warm-cache
    /// path.
    /// </summary>
    [Test]
    public async Task AfterSingleFlight_SubsequentRequests_AreRegularCacheHits()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "sf_warm_cache";

        // First request: cold miss (owner, no contention).
        CacheMetadataHolder first = await RunHintedQuery(dbname, exec, cacheName);
        Assert.That(first.Status, Is.EqualTo(QueryCacheStatus.Miss));

        // Second request (non-concurrent): warm hit.
        CacheMetadataHolder second = await RunHintedQuery(dbname, exec, cacheName);
        Assert.That(second.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "Second non-concurrent request must be a regular cache hit");
    }

    /// <summary>
    /// Verifies that when the owner signals failure (null), waiters compute independently
    /// rather than blocking until timeout. After the fake owner fails, the real query
    /// executes the plan, gets a Miss, and publishes to the cache.
    /// </summary>
    [Test]
    public async Task WaiterPath_OwnerFails_WaiterComputesIndependently()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupRobotsTable(dbname, db, exec);

        const string cacheName = "sf_owner_fail";

        // Discover the fingerprint via a warm run, then evict.
        await RunHintedQuery(dbname, exec, cacheName);
        string? fp = _cache!.GetFirstFingerprintByCacheName(db.Id, cacheName);
        Assert.IsNotNull(fp);
        _cache.InvalidateEntry(db.Id, cacheName, fp!);

        // Pre-occupy as fake owner.
        _cache.EnterSingleFlight(fp!);

        // Launch real query as waiter.
        Task<CacheMetadataHolder> queryTask = RunHintedQuery(dbname, exec, cacheName);

        // Poll until the background query has registered as a waiter; see WaiterPath_EndToEnd
        // for the rationale — a fixed delay races under loaded CI.
        int pollMs = 0;
        while (_cache!.WaiterCount(fp!) == 0 && pollMs < 5_000)
        {
            await Task.Delay(10);
            pollMs += 10;
        }
        Assert.That(_cache.WaiterCount(fp!), Is.GreaterThanOrEqualTo(1),
            "Timed out waiting for the background query to register as a waiter");

        // Owner fails: signal that nothing was published.
        _cache.ExitSingleFlight(fp!, published: false);

        // The waiter should have fallen through to independent execution and produced a Miss.
        CacheMetadataHolder result = await queryTask;
        Assert.That(result.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "After owner failure, the waiter must compute independently and produce a Miss");
    }
}
