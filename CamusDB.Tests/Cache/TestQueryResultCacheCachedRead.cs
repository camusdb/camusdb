
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
using CamusDB.Core.Config;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.Tests.Cache;

/// <summary>
/// End-to-end tests for the cached read path (the integration point where a SELECT with a
/// <c>{cache=name}</c> hint probes the cache, stores on miss, and returns stored rows on hit).
///
/// Each test uses a real <see cref="QueryResultCache"/> injected via
/// <see cref="CreateCommandExecutor"/>. The cache instance is accessible via
/// <see cref="Cache"/> so tests can inspect <see cref="QueryResultCache.EntryCount"/>
/// without computing fingerprints directly.
///
/// Several tests also verify that commit-driven invalidation is correct: a committed write on
/// the same table must evict the cached entry, and the next SELECT must re-populate it with
/// fresh rows.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryResultCacheCachedRead : CommandsExecutor.BaseTest
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

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Cold SELECT stores an entry; subsequent SELECT is served from cache.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ColdSelect_StoresEntry_WarmSelectReturnsSameRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "a", 10);

        // Cold: miss → stores entry.
        List<QueryResultRow> rows1 = await SelectAll(dbname, executor, "orders_all");
        Assert.That(rows1, Has.Count.EqualTo(1), "Cold SELECT must return the seeded row");
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "A cache miss must store exactly one entry");

        // Warm: hit → same rows without touching storage.
        List<QueryResultRow> rows2 = await SelectAll(dbname, executor, "orders_all");
        Assert.That(rows2, Has.Count.EqualTo(1), "Warm SELECT must return the same number of rows as the cold one");
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "A cache hit must not store a second entry");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Committed INSERT evicts the entry; the next SELECT re-populates.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task InsertAfterCacheFill_EvictsEntry_NextSelectReturnsFreshRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "a", 10);

        // Populate cache with 1-row entry.
        List<QueryResultRow> rows1 = await SelectAll(dbname, executor, "orders_all");
        Assert.That(rows1, Has.Count.EqualTo(1));
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "Entry must be in cache after cold SELECT");

        // Committed INSERT via CommitAsync hook → evicts the 1-row entry.
        await InsertOrder(dbname, database, executor, "b", 20);
        Assert.That(Cache.EntryCount, Is.EqualTo(0), "CommitAsync hook must evict the cached entry");

        // Cache miss again → re-executes against storage, gets both rows, stores new entry.
        List<QueryResultRow> rows2 = await SelectAll(dbname, executor, "orders_all");
        Assert.That(rows2, Has.Count.EqualTo(2), "Post-insert SELECT must return both rows");
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "New cache entry must be stored after re-population");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. UPDATE evicts; next SELECT sees updated value.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task UpdateAfterCacheFill_EvictsEntry_NextSelectReturnsUpdatedRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "upd", 1);

        await SelectAll(dbname, executor, "orders_upd");
        Assert.That(Cache.EntryCount, Is.EqualTo(1));

        KvTransaction txUpd = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txUpd, dbname,
            "UPDATE orders SET amount = 99 WHERE id = \"upd\"", null));
        await database.Transactions.CommitAsync(txUpd);

        Assert.That(Cache.EntryCount, Is.EqualTo(0), "UPDATE commit must evict the cached entry");

        List<QueryResultRow> rows = await SelectAll(dbname, executor, "orders_upd");
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Row["amount"].LongValue, Is.EqualTo(99L),
            "Cached SELECT after UPDATE must see the updated amount");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Rolled-back INSERT must NOT evict; the warm hit is still served.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task RolledBackInsert_DoesNotEvictEntry_WarmHitStillServed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "x", 5);

        await SelectAll(dbname, executor, "orders_rb");
        Assert.That(Cache.EntryCount, Is.EqualTo(1));

        // Rolled-back INSERT: AbortWrite in KvTransactionsManager → no eviction.
        KvTransaction txRb = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txRb, dbname,
            "INSERT INTO orders (id, amount) VALUES (\"y\", 99)", null));
        await database.Transactions.RollbackIfNotCompletedAsync(txRb);

        Assert.That(Cache.EntryCount, Is.EqualTo(1), "Rollback must not evict cached entries");

        List<QueryResultRow> rows = await SelectAll(dbname, executor, "orders_rb");
        Assert.That(rows, Has.Count.EqualTo(1), "Warm SELECT after rollback must return the pre-rollback result");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. A SELECT inside an explicit read-write transaction must bypass the cache.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExplicitTransaction_BypassesCache_DoesNotPopulate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "z", 7);

        // Use an explicit RW transaction — IsReadOnly is false → cache bypassed.
        KvTransaction explicitTx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(explicitTx, dbname,
                "SELECT * FROM orders{cache=orders_explicit}", null));
        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        await database.Transactions.RollbackIfNotCompletedAsync(explicitTx);

        Assert.That(rows, Has.Count.EqualTo(1), "SELECT inside explicit transaction must return live rows");
        Assert.That(Cache.EntryCount, Is.EqualTo(0),
            "SELECT inside an explicit transaction must not populate the cache");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5b. Explicit Serializable read-only transaction must also bypass the cache.
    //     BeginAsync(Serializable, ReadOnly) returns a KvTransaction with a real
    //     Kahuna TransactionId (non-zero) and a pinned read snapshot; serving it a
    //     cached entry from a different snapshot would break its isolation guarantee.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExplicitSerializableReadOnlyTransaction_BypassesCache_DoesNotPopulate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "ro", 42);

        // Populate the cache with an autocommit SELECT first so EntryCount reaches 1.
        await SelectAll(dbname, executor, "orders_exro");
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "Autocommit SELECT must populate the cache");

        // Now run the same SELECT inside an explicit Serializable RO transaction.
        // TransactionId != Zero → guard must bypass the cache; EntryCount must stay at 1.
        KvTransaction serRoTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(serRoTx, dbname,
                    "SELECT * FROM orders{cache=orders_exro}", null));
            List<QueryResultRow> rows = new();
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);

            Assert.That(rows, Has.Count.EqualTo(1),
                "Explicit RO transaction must return live rows");
            Assert.That(Cache.EntryCount, Is.EqualTo(1),
                "Explicit Serializable RO transaction must not add a second cache entry");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(serRoTx);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Multiple consecutive warm hits all return the same rows.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task MultipleWarmHits_AllReturnSameRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "m1", 11);
        await InsertOrder(dbname, database, executor, "m2", 22);

        // Cold
        List<QueryResultRow> cold = await SelectAll(dbname, executor, "orders_multi");
        Assert.That(cold, Has.Count.EqualTo(2));
        Assert.That(Cache.EntryCount, Is.EqualTo(1));

        for (int i = 0; i < 5; i++)
        {
            List<QueryResultRow> warm = await SelectAll(dbname, executor, "orders_multi");
            Assert.That(warm, Has.Count.EqualTo(2), $"Warm hit {i + 1} must return 2 rows");
            Assert.That(Cache.EntryCount, Is.EqualTo(1), $"Warm hit {i + 1} must not add entries");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. Row cap breach: all rows reach the consumer; cache stays empty.
    //    DrainAsync must bail out of buffering as soon as the cap is crossed
    //    and must NOT hold the full result set in memory after that point.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task RowCapBreach_AllRowsDelivered_CacheNotPopulated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);
        await InsertOrder(dbname, database, executor, "c1", 1);
        await InsertOrder(dbname, database, executor, "c2", 2);
        await InsertOrder(dbname, database, executor, "c3", 3);

        int savedRowCap = CamusDBConfig.QueryResultCacheMaxEntryRows;
        try
        {
            // Lower the cap below the number of rows in the table so DrainAsync breaches it.
            CamusDBConfig.QueryResultCacheMaxEntryRows = 2;

            List<QueryResultRow> rows = await SelectAll(dbname, executor, "orders_cap");

            Assert.That(rows, Has.Count.EqualTo(3),
                "All rows must reach the consumer even when the cap is breached");
            Assert.That(Cache.EntryCount, Is.EqualTo(0),
                "A cap-breached query must not store an entry in the cache");
        }
        finally
        {
            CamusDBConfig.QueryResultCacheMaxEntryRows = savedRowCap;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 8. Empty table: cold miss stores a 0-row entry; warm hit returns 0 rows.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EmptyTable_ColdMissStoresZeroRowEntry_WarmHitReturnsZeroRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateOrdersTable(dbname, database, executor);

        List<QueryResultRow> cold = await SelectAll(dbname, executor, "orders_empty");
        Assert.That(cold, Has.Count.EqualTo(0));
        Assert.That(Cache.EntryCount, Is.EqualTo(1), "Empty result must still populate the cache");

        List<QueryResultRow> warm = await SelectAll(dbname, executor, "orders_empty");
        Assert.That(warm, Has.Count.EqualTo(0));
        Assert.That(Cache.EntryCount, Is.EqualTo(1));
    }
}
