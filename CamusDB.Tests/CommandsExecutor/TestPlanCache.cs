/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for the query plan cache (Phase F of the cost-based optimizer).
///
/// The cache stores the access-path decision (which index or full scan) keyed by
/// QueryShapeId — a literal-independent structural fingerprint. On a hit the planner
/// re-binds the current query's predicates into the cached decision, skipping
/// access-path enumeration.
///
/// For join queries the cache stores the alias ordering only (not the full ON-predicate AST),
/// so that ON-literal values from the current query are always applied — not frozen from the
/// first query of that shape.
///
/// Assertions use PlanCache.Hits / Misses / Evictions counters exposed via
/// CommandExecutor.PlanCache (internal, visible to tests via InternalsVisibleTo).
/// </summary>
[TestFixture]
public sealed class TestPlanCache : BaseTest
{
    // Enable the cache for every test in this fixture; restore the default (false) on teardown
    // so that parallel or sequentially-run fixtures are not affected by the flag.
    [SetUp]
    public void EnableCache() => CamusDBConfig.PlanCacheEnabled = true;

    [TearDown]
    public void DisableCache() => CamusDBConfig.PlanCacheEnabled = false;

    // ─── Schema helpers ───────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        CreateProductsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "products",
            columns:
            [
                new("id",       ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
                new("price",    ColumnType.Integer64),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",           [new("id",       OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "products_cat",  [new("category", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "products",
            values:
            [
                new() { { "id", new(ColumnType.Id, "aaaaaaaaaaaaaaaaaaaaaaaa") }, { "category", new(ColumnType.String, "electronics") }, { "price", new(ColumnType.Integer64, 999L) } },
                new() { { "id", new(ColumnType.Id, "bbbbbbbbbbbbbbbbbbbbbbbb") }, { "category", new(ColumnType.String, "books")       }, { "price", new(ColumnType.Integer64,  29L) } },
                new() { { "id", new(ColumnType.Id, "cccccccccccccccccccccccc") }, { "category", new(ColumnType.String, "electronics") }, { "price", new(ColumnType.Integer64, 499L) } },
            ]));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    // ─── Tests ────────────────────────────────────────────────────────────────

    [Test]
    public async Task SameQuery_SecondCallHitsCache()
    {
        // First execution is a cache miss (cold); second is a hit because the shape is identical.
        (string dbname, _, CommandExecutor executor) = await CreateProductsTable();

        const string sql = "SELECT id, price FROM products WHERE category = \"electronics\"";

        long hitsBefore   = executor.PlanCache.Hits;
        long missesBefore = executor.PlanCache.Misses;

        for (int i = 0; i < 2; i++)
        {
            DatabaseDescriptor db = await executor.OpenDatabase(dbname);
            KvTransaction txn = await db.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor) rows.Add(row);
            await db.Transactions.CommitAsync(txn);
        }

        Assert.AreEqual(missesBefore + 1, executor.PlanCache.Misses, "Cold miss expected on first call.");
        Assert.AreEqual(hitsBefore  + 1, executor.PlanCache.Hits,   "Warm hit expected on second call.");
    }

    [Test]
    public async Task DifferentLiterals_SameShapeHitsCache()
    {
        // Two queries that differ only in the literal value share the same QueryShapeId
        // and therefore reuse the same plan cache entry (one miss, one hit).
        //
        // Result-equivalence check: the cache hit must return the rows for its own literal
        // ("books"), not the rows frozen from the first query ("electronics"). A counter-only
        // assertion would not catch a regression where the cached plan returns the wrong rows.
        (string dbname, _, CommandExecutor executor) = await CreateProductsTable();

        long hitsBefore   = executor.PlanCache.Hits;
        long missesBefore = executor.PlanCache.Misses;

        async Task<List<long>> ExecuteForCategory(string category)
        {
            string sql = $"SELECT id, price FROM products WHERE category = \"{category}\"";
            DatabaseDescriptor db = await executor.OpenDatabase(dbname);
            KvTransaction txn = await db.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<long> prices = [];
            await foreach (QueryResultRow row in cursor)
                prices.Add(row.Row["price"].LongValue);
            await db.Transactions.CommitAsync(txn);
            return prices;
        }

        // Q1 — cold miss; seeds the cache with "electronics" as the literal.
        List<long> electronicsRows = await ExecuteForCategory("electronics");

        // Q2 — cache hit; must return "books" rows, not "electronics" rows from Q1.
        List<long> booksRows = await ExecuteForCategory("books");

        Assert.AreEqual(missesBefore + 1, executor.PlanCache.Misses,
            "Only one miss for the first literal; second literal reuses the cached shape.");
        Assert.AreEqual(hitsBefore + 1, executor.PlanCache.Hits,
            "Second execution with a different literal must be a cache hit.");

        // CreateProductsTable inserts: electronics(999), books(29), electronics(499).
        Assert.AreEqual(2, electronicsRows.Count, "Two electronics rows expected.");
        Assert.AreEqual(1, booksRows.Count,       "One books row expected.");

        // The cache hit must have used "books", not "electronics" — different result sets.
        Assert.IsFalse(
            new HashSet<long>(electronicsRows).SetEquals(booksRows),
            "Cache hit must return the current query's rows (books), not the cached query's rows (electronics).");
    }

    [Test]
    public async Task SchemaChange_InvalidatesCacheEntry()
    {
        // Execute the query once (miss), then add a column to bump the schema version,
        // then execute the same query again — must miss because the schema hash changed.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateProductsTable();

        const string sql = "SELECT id FROM products WHERE category = \"electronics\"";

        // First execution — cold miss.
        {
            KvTransaction txn = await database.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await foreach (QueryResultRow _ in cursor) { }
            await database.Transactions.CommitAsync(txn);
        }

        long hitsAfterFirst   = executor.PlanCache.Hits;
        long missesAfterFirst = executor.PlanCache.Misses;

        // Add a column — increments the table's SchemaVersion.
        await executor.AlterTable(new AlterTableTicket(
            databaseName: dbname,
            tableName: "products",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("stock", ColumnType.Integer64)));

        // Second execution — should miss because schema version changed.
        {
            DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);
            KvTransaction txn2 = await db2.Transactions.BeginAsync();
            ExecuteSQLTicket ticket2 = new(txnState: txn2, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor2) = await executor.ExecuteSQLQuery(ticket2);
            await foreach (QueryResultRow _ in cursor2) { }
            await db2.Transactions.CommitAsync(txn2);
        }

        Assert.AreEqual(hitsAfterFirst, executor.PlanCache.Hits,
            "No new hits expected — schema change must invalidate the cached entry.");
        Assert.AreEqual(missesAfterFirst + 1, executor.PlanCache.Misses,
            "Stale schema entry must count as a miss.");
    }

    [Test]
    public async Task LruEviction_OldestEntryDropped()
    {
        // Fill a tiny cache (capacity = 2) with three distinct query shapes.
        // The first shape should be evicted, and Evictions must equal 1.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateProductsTable();

        // Overwrite the shared cache with a tiny one.
        executor.PlanCache.Clear();
        int originalMax = CamusDBConfig.PlanCacheMaxEntries;
        CamusDBConfig.PlanCacheMaxEntries = 2;

        // Create a fresh executor whose cache was constructed with max=2.
        CommandExecutor executor2 = CreateCommandExecutor();
        TrackDatabase(dbname, executor2);

        long evictionsBefore = executor2.PlanCache.Evictions;

        try
        {
            // Each query column access path is a distinct shape (different projection/filter).
            string[] sqls =
            [
                "SELECT id    FROM products WHERE category = \"electronics\"",
                "SELECT price FROM products WHERE category = \"books\"",
                "SELECT id, price FROM products WHERE category = \"electronics\"",
            ];

            foreach (string sql in sqls)
            {
                DatabaseDescriptor db = await executor2.OpenDatabase(dbname);
                KvTransaction txn = await db.Transactions.BeginAsync();
                ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor2.ExecuteSQLQuery(ticket);
                await foreach (QueryResultRow _ in cursor) { }
                await db.Transactions.CommitAsync(txn);
            }

            Assert.GreaterOrEqual(executor2.PlanCache.Evictions, evictionsBefore + 1,
                "At least one eviction expected when three distinct shapes fill a capacity-2 cache.");
        }
        finally
        {
            CamusDBConfig.PlanCacheMaxEntries = originalMax;
        }
    }

    [Test]
    public async Task MultipleHits_CacheHitCountAccumulates()
    {
        // Run the same query N times; Hits should be N-1 (first call is a cold miss).
        const int n = 5;
        (string dbname, _, CommandExecutor executor) = await CreateProductsTable();

        const string sql = "SELECT id FROM products WHERE category = \"electronics\"";

        long hitsBefore = executor.PlanCache.Hits;

        for (int i = 0; i < n; i++)
        {
            DatabaseDescriptor db = await executor.OpenDatabase(dbname);
            KvTransaction txn = await db.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await foreach (QueryResultRow _ in cursor) { }
            await db.Transactions.CommitAsync(txn);
        }

        Assert.AreEqual(hitsBefore + (n - 1), executor.PlanCache.Hits,
            $"After {n} executions, Hits must be {n - 1} (first call is always a cold miss).");
    }

    // ─── Join cache regression test ───────────────────────────────────────────

    [Test]
    public async Task JoinCache_DifferentOnLiterals_ReturnCorrectRows()
    {
        // Regression: when the plan cache hits on a join query, the ON-predicate literals from
        // the CURRENT query must be used — not the literals frozen in the first query's plan.
        //
        // Q1: JOIN … ON a.id = b.aid AND b.flag = 1   → should return flag=1 rows only
        // Q2: JOIN … ON a.id = b.aid AND b.flag = 2   → should return flag=2 rows only
        //   (same shape → cache hit on Q2)
        //
        // Before the fix, Q2 silently returned Q1's rows because the cached JoinSource AST
        // contained the b.flag = 1 literal from Q1.

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Table a — the driving side
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "a",
            columns: [new("id", ColumnType.Id)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        // Table b — flag column carries the discriminating literal
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "b",
            columns:
            [
                new("id",   ColumnType.Id),
                new("aid",  ColumnType.Id,       notNull: true),
                new("flag", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",    [new("id",  OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "b_flag", [new("flag", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        const string aId = "aaaaaaaaaaaaaaaaaaaaaaaa";

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "a",
            values: [new() { { "id", new(ColumnType.Id, aId) } }]));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "b",
            values:
            [
                new() { { "id", new(ColumnType.Id, "111111111111111111111111") }, { "aid", new(ColumnType.Id, aId) }, { "flag", new(ColumnType.Integer64, 1L) } },
                new() { { "id", new(ColumnType.Id, "222222222222222222222222") }, { "aid", new(ColumnType.Id, aId) }, { "flag", new(ColumnType.Integer64, 2L) } },
            ]));

        await database.Transactions.CommitAsync(txn);

        async Task<List<long>> ExecuteJoinWithFlag(long flagValue)
        {
            // Using an ON-clause literal to discriminate rows — the shape is identical for both
            // calls (same tables, same structure), but the literal differs.
            string sql = $"SELECT b.flag FROM a JOIN b ON a.id = b.aid AND b.flag = {flagValue}";

            DatabaseDescriptor db = await executor.OpenDatabase(dbname);
            KvTransaction t = await db.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: t, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

            List<long> flags = [];
            await foreach (QueryResultRow row in cursor)
                flags.Add(row.Row["flag"].LongValue);

            await db.Transactions.CommitAsync(t);
            return flags;
        }

        // Q1 — cold miss; seeds the cache with flag=1 ordering
        List<long> rowsQ1 = await ExecuteJoinWithFlag(1);

        // Q2 — cache hit; must use flag=2 from the current query, NOT flag=1 from the cache
        List<long> rowsQ2 = await ExecuteJoinWithFlag(2);

        Assert.AreEqual(new List<long> { 1L }, rowsQ1,
            "Q1 must return only the flag=1 row.");

        // This assertion failed before the fix: the cached ON AST returned flag=1 for Q2.
        Assert.AreEqual(new List<long> { 2L }, rowsQ2,
            "Q2 must return only the flag=2 row even though the plan cache hit on the same shape.");

        Assert.GreaterOrEqual(executor.PlanCache.Hits, 1L,
            "Q2 must have been a cache hit (same shape as Q1).");
    }
}
