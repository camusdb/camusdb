
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Config;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using Kommander.Time;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Tests for the strict-validation read path (<c>{cache=name, strict}</c>).
///
/// Strict cache hits are validated against current KV state before rows are served.
/// These tests exercise:
/// <list type="bullet">
///   <item><description>TTL expiry: expired entries are swept on probe and return a miss.</description></item>
///   <item><description>StrictValidator: stale point/range deps are detected and entries evicted.</description></item>
///   <item><description>Probe limit: too many deps cause fail-closed eviction.</description></item>
///   <item><description>Autocommit bypass: strict SELECTs with ReadTimestamp=Zero never publish.</description></item>
///   <item><description>Non-strict entries are served without KV validation.</description></item>
/// </list>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryResultCacheStrictValidation : CommandsExecutor.BaseTest
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

    private QueryResultCache Cache => _cache!;

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task<List<QueryResultRow>> SelectAllStrict(
        string dbname, CommandExecutor executor, string cacheName)
    {
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                $"SELECT * FROM orders{{cache={cacheName}, strict}}",
                null));
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return rows;
    }

    private static async Task<List<QueryResultRow>> SelectAll(
        string dbname, CommandExecutor executor, string cacheName)
    {
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                $"SELECT * FROM orders{{cache={cacheName}}}",
                null));
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return rows;
    }

    private static async Task CreateOrdersTable(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbname,
            "CREATE TABLE orders (id STRING NOT NULL PRIMARY KEY, amount int64 NOT NULL)",
            null));
    }

    private static async Task InsertOrder(string dbname, DatabaseDescriptor database, CommandExecutor executor,
        string id, int amount)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"INSERT INTO orders (id, amount) VALUES (\"{id}\", {amount})", null));
        await database.Transactions.CommitAsync(tx);
    }

    private static CachedQueryResult MakeStrictResult(
        string cacheName, string dbId, IReadOnlyList<QueryResultRow> rows, string fp = "test-fp")
        => new(
            CacheName: cacheName,
            DatabaseId: dbId,
            Rows: rows,
            ResultFingerprint: fp,
            CachedAt: HLCTimestamp.Zero,
            Status: QueryCacheStatus.Miss,
            HintIsStrict: true);

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: TTL expiry — expired entry is swept on probe, returns miss
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// An entry injected with a 1 ms TTL must be reported as a miss once it expires.
    /// <see cref="QueryResultCache.TryGetWithDepsAsync"/> removes the expired entry on probe.
    /// </summary>
    [Test]
    public async Task TryGetWithDeps_ExpiredEntry_ReturnsNullAndRemovesEntry()
    {
        const string dbId = "db1";
        const string cacheName = "orders";
        const string fp = "test-fp-ttl";

        var result = new CachedQueryResult(
            CacheName: cacheName,
            DatabaseId: dbId,
            Rows: [],
            ResultFingerprint: fp,
            CachedAt: HLCTimestamp.Zero,
            Status: QueryCacheStatus.Miss,
            HintIsStrict: true);

        Cache.InjectEntryForTest(result, QueryDependencySet.Empty, ttlMs: 1);
        Assert.That(Cache.EntryCount, Is.EqualTo(1));

        await Task.Delay(5); // let TTL expire

        var hit = await Cache.TryGetWithDepsAsync(dbId, cacheName, fp);
        Assert.That(hit, Is.Null, "Expired entry must return null on probe");
        Assert.That(Cache.EntryCount, Is.EqualTo(0), "Expired entry must be removed on probe");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: StrictValidator — stale range dep detected, entry invalid
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="StrictValidator.ValidateAsync"/> returns <c>false</c> when any key in a
    /// range dep has <c>LastModified &gt; CachedAt</c>. An injected entry with
    /// <c>CachedAt=HLCTimestamp.Zero</c> is guaranteed stale because all committed rows
    /// have <c>LastModified &gt; Zero</c>.
    /// </summary>
    [Test]
    public async Task StrictValidator_StaleRangeDep_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "o1", 10);

        // First SELECT populates cache entry (non-strict).
        List<QueryResultRow> staleRows = await SelectAll(dbname, executor, "orders_val");

        TableSchema ordersSchema = database.Schema.Tables["orders"];

        var staleResult = MakeStrictResult("orders_val", database.Id, staleRows, "test-fp-range");

        // Range dep pointing to the real orders row keyspace.
        var deps = new QueryDependencySet(
            rangeDeps: [$"{database.Id}:{ordersSchema.Id}:r"],
            pointDeps: [],
            schemaDeps: []);

        // Validate: range scan will find rows with LastModified > Zero → stale.
        bool valid = await StrictValidator.ValidateAsync(
            staleResult, deps, database, TestNode!.Kahuna, default);

        Assert.That(valid, Is.False,
            "A range dep covering rows written after CachedAt=Zero must fail validation");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2b: StrictValidator — stale point dep (LastModified > CachedAt) → invalid
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="StrictValidator.ValidateAsync"/> returns <c>false</c> when a point dep's
    /// <c>LastModified</c> is greater than the entry's <c>CachedAt</c>. An injected result
    /// with <c>CachedAt=HLCTimestamp.Zero</c> guarantees this: every committed row has
    /// <c>LastModified &gt; Zero</c>.
    /// </summary>
    [Test]
    public async Task StrictValidator_StalePointDep_LastModifiedAfterCachedAt_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "o1", 10);

        List<QueryResultRow> fetchedRows = await SelectAll(dbname, executor, "pt_stale_tmp");
        Assert.That(fetchedRows, Has.Count.EqualTo(1));

        TableSchema ordersSchema = database.Schema.Tables["orders"];
        string rowPointKey = $"{database.Id}:{ordersSchema.Id}:r/{fetchedRows[0].RowId}";

        var staleResult = MakeStrictResult("pt_stale", database.Id, fetchedRows, "test-fp-point-stale");

        // Point dep: the row was committed after Zero, so LastModified > CachedAt=Zero.
        var deps = new QueryDependencySet(
            rangeDeps: [],
            pointDeps: [rowPointKey],
            schemaDeps: []);

        bool valid = await StrictValidator.ValidateAsync(
            staleResult, deps, database, TestNode!.Kahuna, default);

        Assert.That(valid, Is.False,
            "A point dep whose row has LastModified > CachedAt must fail validation (stale update detected)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2c: StrictValidator — absent point dep (deleted row) → invalid
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="StrictValidator.ValidateAsync"/> returns <c>false</c> when a point dep
    /// key no longer exists in Kahuna (<c>DoesNotExist</c>), indicating the row was physically
    /// deleted. This is the critical case that range scans cannot detect — a deleted key has no
    /// <c>LastModified</c> entry, so without the point dep the validator would incorrectly
    /// return <c>true</c> and serve stale rows that include the deleted row.
    /// </summary>
    [Test]
    public async Task StrictValidator_AbsentPointDep_DeletedRow_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "o1", 10);

        // Capture the row before it is deleted.
        List<QueryResultRow> fetchedRows = await SelectAll(dbname, executor, "pt_del_tmp");
        Assert.That(fetchedRows, Has.Count.EqualTo(1));

        TableSchema ordersSchema = database.Schema.Tables["orders"];
        string rowPointKey = $"{database.Id}:{ordersSchema.Id}:r/{fetchedRows[0].RowId}";

        // Delete the row so its KV key no longer exists in Kahuna.
        KvTransaction txDel = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txDel, dbname, "DELETE FROM orders WHERE id = \"o1\"", null));
        await database.Transactions.CommitAsync(txDel);

        var staleResult = MakeStrictResult("pt_del", database.Id, fetchedRows, "test-fp-point-del");

        // Point dep: the key was deleted — DoesNotExist in Kahuna.
        var deps = new QueryDependencySet(
            rangeDeps: [],
            pointDeps: [rowPointKey],
            schemaDeps: []);

        bool valid = await StrictValidator.ValidateAsync(
            staleResult, deps, database, TestNode!.Kahuna, default);

        Assert.That(valid, Is.False,
            "A point dep whose row was deleted (DoesNotExist) must fail validation");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: StrictValidator — schema version mismatch detected
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="StrictValidator.ValidateAsync"/> returns <c>false</c> when a schema dep's
    /// version no longer matches the live schema version. This simulates a column add/drop
    /// after the entry was cached.
    /// </summary>
    [Test]
    public async Task StrictValidator_SchemaDep_WrongVersion_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);

        TableSchema ordersSchema = database.Schema.Tables["orders"];
        int currentVersion = ordersSchema.Version;

        var staleResult = MakeStrictResult("orders_schema", database.Id, [], "test-fp-schema");

        // Schema dep claims version is currentVersion + 99 — can never match.
        var deps = new QueryDependencySet(
            rangeDeps: [],
            pointDeps: [],
            schemaDeps: [(ordersSchema.Id!, currentVersion + 99)]);

        bool valid = await StrictValidator.ValidateAsync(
            staleResult, deps, database, TestNode!.Kahuna, default);

        Assert.That(valid, Is.False,
            "A schema dep with an outdated version must fail validation");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: StrictValidator — correct schema and empty keyspace passes
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="StrictValidator.ValidateAsync"/> returns <c>true</c> when all schema deps
    /// match the live version and all dep sets are empty (no KV probes needed).
    /// </summary>
    [Test]
    public async Task StrictValidator_ValidSchemaNoPointOrRange_ReturnsTrue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);

        TableSchema ordersSchema = database.Schema.Tables["orders"];

        // Make a fake result whose CachedAt is far in the future so even if probed, nothing
        // would be newer (but since there are no KV deps to probe, this is moot).
        var validResult = new CachedQueryResult(
            CacheName: "orders_valid",
            DatabaseId: database.Id,
            Rows: [],
            ResultFingerprint: "test-fp-valid",
            CachedAt: new HLCTimestamp(0, long.MaxValue, 0),
            Status: QueryCacheStatus.Miss,
            HintIsStrict: true);

        var deps = new QueryDependencySet(
            rangeDeps: [],
            pointDeps: [],
            schemaDeps: [(ordersSchema.Id!, ordersSchema.Version)]);

        bool valid = await StrictValidator.ValidateAsync(
            validResult, deps, database, TestNode!.Kahuna, default);

        Assert.That(valid, Is.True,
            "Matching schema dep with no KV deps must pass validation");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: StrictValidator — probe limit exceeded, fail closed
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// When point deps exceed <see cref="CamusDBConfig.QueryResultCacheStrictValidationMaxKeys"/>,
    /// <see cref="StrictValidator.ValidateAsync"/> must return <c>false</c> (fail closed).
    /// </summary>
    [Test]
    public async Task StrictValidator_ProbeLimitExceeded_ReturnsFalseFailClosed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);

        var staleResult = MakeStrictResult("orders_limit", database.Id, [], "test-fp-limit");

        int origLimit = CamusDBConfig.QueryResultCacheStrictValidationMaxKeys;
        CamusDBConfig.QueryResultCacheStrictValidationMaxKeys = 2;
        try
        {
            // 3 point deps exceeds the limit of 2.
            var deps = new QueryDependencySet(
                rangeDeps: [],
                pointDeps: ["fake-key-1", "fake-key-2", "fake-key-3"],
                schemaDeps: []);

            bool valid = await StrictValidator.ValidateAsync(
                staleResult, deps, database, TestNode!.Kahuna, default);

            Assert.That(valid, Is.False,
                "Exceeding the probe limit must fail closed (return false)");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheStrictValidationMaxKeys = origLimit;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6: InvalidateEntry removes a specific entry by fingerprint
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="QueryResultCache.InvalidateEntry"/> must remove exactly the targeted
    /// fingerprint and leave other entries intact.
    /// </summary>
    [Test]
    public async Task InvalidateEntry_RemovesTargetedFingerprint_LeavesOthersIntact()
    {
        // CreateDatabase triggers CreateCommandExecutor which initializes _cache.
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        var r1 = new CachedQueryResult(
            CacheName: "c1", DatabaseId: "db1", Rows: [], ResultFingerprint: "fp-1",
            CachedAt: HLCTimestamp.Zero, Status: QueryCacheStatus.Miss, HintIsStrict: false);
        var r2 = new CachedQueryResult(
            CacheName: "c2", DatabaseId: "db1", Rows: [], ResultFingerprint: "fp-2",
            CachedAt: HLCTimestamp.Zero, Status: QueryCacheStatus.Miss, HintIsStrict: false);

        Cache.InjectEntryForTest(r1, QueryDependencySet.Empty);
        Cache.InjectEntryForTest(r2, QueryDependencySet.Empty);
        Assert.That(Cache.EntryCount, Is.EqualTo(2));

        Cache.InvalidateEntry("db1", "c1", "fp-1");
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "InvalidateEntry must remove exactly the targeted fingerprint");

        Cache.InvalidateEntry("db1", "c1", "fp-1"); // idempotent
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "InvalidateEntry on missing fingerprint must be a no-op");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 7: strict hint + autocommit never publishes
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// An autocommit SELECT runs with <see cref="KvTransaction.CreateReadOnly"/>, which sets
    /// <c>ReadTimestamp=HLCTimestamp.Zero</c>. A <c>{cache=name, strict}</c> hint on such a
    /// query must bypass publication because <c>CachedAt=Zero</c> would make every subsequent
    /// strict validation fail.
    /// </summary>
    [Test]
    public async Task StrictHint_AutocommitReadOnly_DoesNotPopulateCache()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "o1", 99);

        // Autocommit strict SELECT — ReadTimestamp=Zero, must not publish.
        List<QueryResultRow> rows = await SelectAllStrict(dbname, executor, "strict_auto");
        Assert.That(rows, Has.Count.EqualTo(1), "Live rows must still be returned");
        Assert.That(Cache.EntryCount, Is.EqualTo(0),
            "Strict query with CachedAt=Zero must never populate the cache");

        // Running it again also misses (never populates).
        List<QueryResultRow> rows2 = await SelectAllStrict(dbname, executor, "strict_auto");
        Assert.That(rows2, Has.Count.EqualTo(1));
        Assert.That(Cache.EntryCount, Is.EqualTo(0), "Cache must remain empty after repeated strict autocommit reads");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 8: non-strict entries ARE published and served without validation
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A non-strict SELECT with <c>ReadTimestamp=Zero</c> (autocommit) still publishes to
    /// the cache. A second SELECT must serve the cached rows and not probe Kahuna.
    /// </summary>
    [Test]
    public async Task NonStrictHint_AutocommitReadOnly_PublishesAndServes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "o1", 7);

        // First non-strict SELECT — cache miss, publishes.
        List<QueryResultRow> first = await SelectAll(dbname, executor, "ns_cache");
        Assert.That(first, Has.Count.EqualTo(1));
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "Non-strict autocommit SELECT must publish to the cache");

        // Second SELECT — must be a cache hit (no new publish).
        List<QueryResultRow> second = await SelectAll(dbname, executor, "ns_cache");
        Assert.That(second, Has.Count.EqualTo(1), "Second SELECT must return cached rows");
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "Cache must still have exactly one entry after a hit");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 9: strict entry with truncated point deps is never published
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// When a strict query fetches more rows than <see cref="CamusDBConfig.QueryResultCacheMaxPointDeps"/>,
    /// <see cref="QueryDependencyCollector.PointDepsTruncated"/> is set. A physical delete of
    /// an untracked row would be invisible to strict validation — the deleted key is gone from
    /// Kahuna and the range scan finds nothing newer than <c>CachedAt</c>. Publishing such an
    /// entry would cause <see cref="StrictValidator"/> to incorrectly return <see langword="true"/>
    /// and serve stale rows. The publish must be bypassed.
    ///
    /// <para>Non-strict entries are unaffected: same-node <c>InvalidateByModifiedKeys</c> range
    /// matching catches deletes regardless of point capping.</para>
    /// </summary>
    [Test]
    public async Task StrictCollector_PointDepsTruncated_BypassesPublish()
    {
        // Lower the point-dep cap to 1 so a 2-row result forces truncation.
        int origPointCap = CamusDBConfig.QueryResultCacheMaxPointDeps;
        CamusDBConfig.QueryResultCacheMaxPointDeps = 1;
        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            TrackDatabase(dbname, executor);

            await CreateOrdersTable(dbname, database, executor);
            await InsertOrder(dbname, database, executor, "o1", 1);
            await InsertOrder(dbname, database, executor, "o2", 2);

            // Non-strict SELECT with 2 rows: point-dep cap=1, one dep truncated, but non-strict
            // entries are exempt — the entry IS published (range dep covers the bucket).
            List<QueryResultRow> nonStrictRows = await SelectAll(dbname, executor, "ns_ptcap");
            Assert.That(nonStrictRows, Has.Count.EqualTo(2));
            Assert.That(Cache.EntryCount, Is.EqualTo(1),
                "Non-strict entry must still publish when point deps are truncated");

            Cache.InvalidateCacheName(database.Id, "ns_ptcap");

            // Strict SELECT with 2 rows: point-dep cap=1, one dep truncated, must bypass publish.
            List<QueryResultRow> strictRows = await SelectAllStrict(dbname, executor, "s_ptcap");
            Assert.That(strictRows, Has.Count.EqualTo(2), "Live rows must still be delivered");
            Assert.That(Cache.EntryCount, Is.EqualTo(0),
                "Strict entry with truncated point deps must be bypassed, not published");

            // Running again still bypasses (the condition is structural, not transient).
            List<QueryResultRow> strictRows2 = await SelectAllStrict(dbname, executor, "s_ptcap");
            Assert.That(strictRows2, Has.Count.EqualTo(2));
            Assert.That(Cache.EntryCount, Is.EqualTo(0),
                "Strict entry with truncated point deps must never populate the cache");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxPointDeps = origPointCap;
        }
    }
}
