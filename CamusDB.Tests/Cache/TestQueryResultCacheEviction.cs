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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Tests for <c>EVICT CACHE 'name'</c> and <c>EVICT CACHE ALL</c> manual eviction.
///
/// Verifies that the SQL statements drop the expected cache entries, leave unrelated
/// entries intact, and are safe to call when the cache is disabled (null).
/// </summary>
[TestFixture]
public sealed class TestQueryResultCacheEviction : CommandsExecutor.BaseTest
{
    private QueryResultCache? _cache;

    protected override CommandExecutor CreateCommandExecutor()
    {
        _cache = new QueryResultCache(CamusDBConfig.Ambient, sweepIntervalMs: -1);
        CommandValidator validator = new(CamusDBConfig.Ambient);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, CamusDBConfig.Ambient,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: _cache);
    }

    private QueryResultCache Cache => _cache!;

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

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

    private static async Task<CacheMetadataHolder> SelectWithCache(
        string dbname, CommandExecutor executor, string cacheName)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                $"SELECT * FROM orders{{cache={cacheName}}}",
                null), meta);
        await foreach (QueryResultRow _ in cursor) { }
        return meta;
    }

    private static async Task EvictByName(string dbname, DatabaseDescriptor database, CommandExecutor executor, string cacheName)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"EVICT CACHE '{cacheName}'", null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task EvictAll(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "EVICT CACHE ALL", null));
        await database.Transactions.CommitAsync(tx);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. EVICT CACHE 'name' drops entries in that family, leaving other names.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictCacheByName_DropsTargetFamily_LeavesOtherNames()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "x", 1);

        // Warm up two distinct cache families.
        CacheMetadataHolder firstA = await SelectWithCache(dbname, executor, "family_a");
        CacheMetadataHolder firstB = await SelectWithCache(dbname, executor, "family_b");
        Assert.That(firstA.Status, Is.EqualTo(QueryCacheStatus.Miss), "family_a cold");
        Assert.That(firstB.Status, Is.EqualTo(QueryCacheStatus.Miss), "family_b cold");

        // Both should hit on the second query before eviction.
        CacheMetadataHolder hitA = await SelectWithCache(dbname, executor, "family_a");
        CacheMetadataHolder hitB = await SelectWithCache(dbname, executor, "family_b");
        Assert.That(hitA.Status, Is.EqualTo(QueryCacheStatus.Hit), "family_a warm");
        Assert.That(hitB.Status, Is.EqualTo(QueryCacheStatus.Hit), "family_b warm");

        // Evict only family_a.
        await EvictByName(dbname, database, executor, "family_a");

        // family_a should miss (evicted); family_b should still hit (untouched).
        CacheMetadataHolder afterA = await SelectWithCache(dbname, executor, "family_a");
        CacheMetadataHolder afterB = await SelectWithCache(dbname, executor, "family_b");

        Assert.That(afterA.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "family_a should miss after EVICT CACHE 'family_a'");
        Assert.That(afterB.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "family_b should still hit after evicting family_a");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. EVICT CACHE ALL drops every entry for the database.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictCacheAll_DropsAllEntriesForDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "y", 2);

        // Warm up two families.
        await SelectWithCache(dbname, executor, "all_a");
        await SelectWithCache(dbname, executor, "all_b");

        // Both hit before eviction.
        CacheMetadataHolder hitA = await SelectWithCache(dbname, executor, "all_a");
        CacheMetadataHolder hitB = await SelectWithCache(dbname, executor, "all_b");
        Assert.That(hitA.Status, Is.EqualTo(QueryCacheStatus.Hit), "all_a warm");
        Assert.That(hitB.Status, Is.EqualTo(QueryCacheStatus.Hit), "all_b warm");

        // Evict everything.
        await EvictAll(dbname, database, executor);

        // Both should miss after EVICT CACHE ALL.
        CacheMetadataHolder afterA = await SelectWithCache(dbname, executor, "all_a");
        CacheMetadataHolder afterB = await SelectWithCache(dbname, executor, "all_b");

        Assert.That(afterA.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "all_a should miss after EVICT CACHE ALL");
        Assert.That(afterB.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "all_b should miss after EVICT CACHE ALL");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. EVICT CACHE ALL does not affect entries for a different database.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictCacheAll_DoesNotEvictOtherDatabase()
    {
        (string dbnameA, DatabaseDescriptor dbA, CommandExecutor execA) = await CreateDatabase();
        (string dbnameB, DatabaseDescriptor dbB, CommandExecutor execB) = await CreateDatabase();

        // Both databases share the same cache instance (same executor CreateCommandExecutor logic).
        // Since they are separate executors, we need to share the same cache. In this test setup,
        // each CreateDatabase call returns the same executor (from the fixture), so the cache is
        // the same object. But the eviction is scoped to the database id.
        //
        // Here we create two separate executors both pointing at the same QueryResultCache object
        // so we can verify isolation by database id.
        QueryResultCache sharedCache = new(CamusDBConfig.Ambient, sweepIntervalMs: -1);

        CommandExecutor execForA = new(new CommandValidator(CamusDBConfig.Ambient), new CatalogsManager(logger), logger, CamusDBConfig.Ambient,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false, cache: sharedCache);
        CommandExecutor execForB = new(new CommandValidator(CamusDBConfig.Ambient), new CatalogsManager(logger), logger, CamusDBConfig.Ambient,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false, cache: sharedCache);

        await CreateOrdersTable(dbnameA, dbA, execForA);
        await InsertOrder(dbnameA, dbA, execForA, "z1", 3);

        await CreateOrdersTable(dbnameB, dbB, execForB);
        await InsertOrder(dbnameB, dbB, execForB, "z2", 4);

        // Warm up cache for both databases under the same family name.
        await SelectWithCacheExec(dbnameA, execForA, "shared_family");
        await SelectWithCacheExec(dbnameB, execForB, "shared_family");

        // Both should hit.
        CacheMetadataHolder hitA = await SelectWithCacheExec(dbnameA, execForA, "shared_family");
        CacheMetadataHolder hitB = await SelectWithCacheExec(dbnameB, execForB, "shared_family");
        Assert.That(hitA.Status, Is.EqualTo(QueryCacheStatus.Hit), "dbA warm");
        Assert.That(hitB.Status, Is.EqualTo(QueryCacheStatus.Hit), "dbB warm");

        // Evict ALL for database A only.
        KvTransaction tx = await dbA.Transactions.BeginAsync();
        await execForA.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbnameA, "EVICT CACHE ALL", null));
        await dbA.Transactions.CommitAsync(tx);

        // Database A should miss; database B's entry should be preserved.
        CacheMetadataHolder afterA = await SelectWithCacheExec(dbnameA, execForA, "shared_family");
        CacheMetadataHolder afterB = await SelectWithCacheExec(dbnameB, execForB, "shared_family");

        Assert.That(afterA.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "dbA should miss after its EVICT CACHE ALL");
        Assert.That(afterB.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "dbB entry should survive eviction of a different database");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. EVICT CACHE when cache is disabled (null) is a no-op.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictCache_WhenCacheDisabled_IsNoOp()
    {
        // Build an executor with cache: null (disabled).
        CommandExecutor disabledExecutor = new(
            new CommandValidator(CamusDBConfig.Ambient), new CatalogsManager(logger), logger, CamusDBConfig.Ambient,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
            cache: null);

        (string dbname, DatabaseDescriptor database, _) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        // Neither of these should throw.
        Assert.DoesNotThrowAsync(async () =>
            await disabledExecutor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                "EVICT CACHE 'anything'", null)));

        Assert.DoesNotThrowAsync(async () =>
            await disabledExecutor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                KvTransaction.CreateReadOnly(), dbname, "EVICT CACHE ALL", null)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Evicting a non-existent name is a no-op (idempotent).
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictCacheByName_NonExistentName_IsNoOp()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "w", 5);

        // Warm one family.
        await SelectWithCache(dbname, executor, "real_family");
        CacheMetadataHolder hit = await SelectWithCache(dbname, executor, "real_family");
        Assert.That(hit.Status, Is.EqualTo(QueryCacheStatus.Hit));

        // Evict a name that was never populated — should not throw and should not disturb real_family.
        await EvictByName(dbname, database, executor, "phantom_family");

        CacheMetadataHolder stillHit = await SelectWithCache(dbname, executor, "real_family");
        Assert.That(stillHit.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "real_family untouched after evicting phantom_family");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Eviction cleans dependency indexes (re-population works after evict).
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictAndRepopulate_WorksCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "r1", 100);

        // Populate.
        await SelectWithCache(dbname, executor, "repop");
        CacheMetadataHolder firstHit = await SelectWithCache(dbname, executor, "repop");
        Assert.That(firstHit.Status, Is.EqualTo(QueryCacheStatus.Hit));

        // Evict.
        await EvictByName(dbname, database, executor, "repop");

        // Miss (re-executes and re-populates).
        CacheMetadataHolder missAfterEvict = await SelectWithCache(dbname, executor, "repop");
        Assert.That(missAfterEvict.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "first query after eviction should re-populate");

        // Second query after repopulation should hit again.
        CacheMetadataHolder secondHit = await SelectWithCache(dbname, executor, "repop");
        Assert.That(secondHit.Status, Is.EqualTo(QueryCacheStatus.Hit),
            "second query after repopulation should hit");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. EVICT CACHE with a mixed-case name matches the hint's lowercased family.
    //    {cache=FooBar} stores the family as "foobar"; EVICT CACHE 'FooBar' must
    //    still evict it.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EvictCacheByName_MixedCaseName_MatchesLowercasedFamily()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "m1", 7);

        // Warm the cache with a mixed-case hint — the identifier production lowercases it to "recentorders".
        await SelectWithCache(dbname, executor, "RecentOrders");
        CacheMetadataHolder warm = await SelectWithCache(dbname, executor, "RecentOrders");
        Assert.That(warm.Status, Is.EqualTo(QueryCacheStatus.Hit), "entry stored under 'recentorders'");

        // Evict using the same mixed-case string in the EVICT statement.
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "EVICT CACHE 'RecentOrders'", null));
        await database.Transactions.CommitAsync(tx);

        // Should miss — eviction must have matched the lowercased stored name.
        CacheMetadataHolder afterEvict = await SelectWithCache(dbname, executor, "RecentOrders");
        Assert.That(afterEvict.Status, Is.EqualTo(QueryCacheStatus.Miss),
            "EVICT CACHE 'RecentOrders' must evict the entry stored under 'recentorders'");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task<CacheMetadataHolder> SelectWithCacheExec(
        string dbname, CommandExecutor executor, string cacheName)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTx, dbname,
                $"SELECT * FROM orders{{cache={cacheName}}}",
                null), meta);
        await foreach (QueryResultRow _ in cursor) { }
        return meta;
    }
}
