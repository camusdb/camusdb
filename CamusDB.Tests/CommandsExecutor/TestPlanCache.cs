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
// Toggles the process-wide static CamusDBConfig.PlanCacheEnabled in SetUp/TearDown. Under the
// assembly's ParallelScope.Fixtures this would race any concurrent fixture that plans a query and
// reads that flag; NonParallelizable isolates the toggle.
[NonParallelizable]
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

    [Test]
    public async Task ChosenIndexPolicy_ForcedIndexCacheHitReplaysWithCurrentPredicate()
    {
        // Chosen-index replay: SingleTableDecision stores only IndexName (no ScanType).
        //
        // Q1 uses FORCE_INDEX with a WHERE predicate.  The first-run planner takes the
        // forced-index early-return path and creates a FullScanFromIndex step — predicates
        // are NOT absorbed by the index step; they go into ExecutionFilter (full scan +
        // post-filter).  The cache stores SingleTableDecision(IndexName="products_cat").
        //
        // Q2 same SQL shape (same FORCE_INDEX, different literal).  Cache hit →
        // BuildScanNodeFromCachedDecision → TrySelectScanForForcedIndex("products_cat") →
        // predicates match → returns RangeScanFromIndex bounded to the current literal.
        // The plan is more selective than Q1's full scan; the result must be Q2's rows, not Q1's.
        //
        // Assertion: Q2 returns only the "books" row (29), not the electronics rows (999, 499).
        // This would fail if the cache honoured a hypothetical stored ScanType=FullScanFromIndex
        // (strict replay) and forgot to re-bind the predicate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateProductsTable();

        long hitsBefore = executor.PlanCache.Hits;

        async Task<List<long>> ExecuteWithForcedIndex(string category)
        {
            string sql = $"SELECT id, price FROM products@{{FORCE_INDEX=products_cat}} WHERE category = \"{category}\"";
            KvTransaction txn = await database.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<long> prices = [];
            await foreach (QueryResultRow row in cursor)
                prices.Add(row.Row["price"].LongValue);
            await database.Transactions.CommitAsync(txn);
            return prices;
        }

        // Q1: cold miss; planner takes forced-index path → FullScanFromIndex step.
        // Cache stores SingleTableDecision(IndexName="products_cat").
        List<long> electronicsRows = await ExecuteWithForcedIndex("electronics");

        // Q2: cache hit; TrySelectScanForForcedIndex re-matches predicate → RangeScanFromIndex.
        List<long> booksRows = await ExecuteWithForcedIndex("books");

        Assert.AreEqual(hitsBefore + 1, executor.PlanCache.Hits,
            "Q2 must be a cache hit (same FORCE_INDEX + same WHERE structure).");

        // Q1 correctness: both electronics rows must be returned.
        Assert.AreEqual(2, electronicsRows.Count, "Q1 must return both electronics rows.");
        CollectionAssert.AreEquivalent(new[] { 999L, 499L }, electronicsRows,
            "Q1 prices must be the two electronics prices.");

        // Q2 result-correctness (chosen-index policy): must return the books row, not electronics.
        Assert.AreEqual(1, booksRows.Count, "Q2 must return exactly one books row.");
        Assert.AreEqual(29L, booksRows[0],
            "Q2 must return the books price (29), proving the cache hit re-bound the current predicate.");
    }

    private async Task<List<QueryResultRow>> RunQueryAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction txn = await db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor) rows.Add(row);
        await db.Transactions.CommitAsync(txn);
        return rows;
    }

    [Test]
    public async Task CreateIndex_InvalidatesCachedDecision()
    {
        // Index DDL deliberately does not bump TableSchema.Version, so the cache's dependency
        // fingerprint must carry the descriptor's IndexSetGeneration — otherwise a shape cached
        // as a full scan would never consider a subsequently created index.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateProductsTable();

        const string sql = "SELECT id FROM products WHERE category = \"electronics\" AND price = 999";

        long missesBefore = executor.PlanCache.Misses;
        long hitsBefore   = executor.PlanCache.Hits;

        await RunQueryAsync(executor, dbname, sql); // cold miss
        await RunQueryAsync(executor, dbname, sql); // warm hit

        Assert.AreEqual(missesBefore + 1, executor.PlanCache.Misses);
        Assert.AreEqual(hitsBefore + 1, executor.PlanCache.Hits);

        // CREATE INDEX on category+price — bumps the index-set generation, not the schema version.
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "products",
            indexName: "products_cat_price",
            columns: [new("category", OrderType.Ascending), new("price", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex));

        List<QueryResultRow> rows = await RunQueryAsync(executor, dbname, sql); // must MISS and replan
        await RunQueryAsync(executor, dbname, sql);                             // fresh entry → hit

        Assert.AreEqual(missesBefore + 2, executor.PlanCache.Misses,
            "CREATE INDEX must invalidate the cached decision (index-set generation changed).");
        Assert.AreEqual(hitsBefore + 2, executor.PlanCache.Hits,
            "The replanned decision must be re-cached and hit on the next execution.");
        Assert.AreEqual(1, rows.Count, "Replanned query must return the matching row.");
    }

    [Test]
    public async Task DropIndex_InvalidatesAndReplacesCachedEntry()
    {
        // DROP INDEX must invalidate the cached decision naming the dead index; the fresh
        // replan must REPLACE the entry (not leave an immortal stale one that replays and
        // falls back to a full replan on every execution forever).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateProductsTable();

        const string sql = "SELECT id, price FROM products WHERE category = \"electronics\"";

        long missesBefore = executor.PlanCache.Misses;
        long hitsBefore   = executor.PlanCache.Hits;

        await RunQueryAsync(executor, dbname, sql); // miss — caches decision on products_cat
        await RunQueryAsync(executor, dbname, sql); // hit

        Assert.AreEqual(missesBefore + 1, executor.PlanCache.Misses);
        Assert.AreEqual(hitsBefore + 1, executor.PlanCache.Hits);

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "products",
            indexName: "products_cat",
            columns: [],
            operation: AlterIndexOperation.DropIndex));

        List<QueryResultRow> rows = await RunQueryAsync(executor, dbname, sql); // miss → replan → re-Put
        await RunQueryAsync(executor, dbname, sql);                             // hit on the fresh entry

        Assert.AreEqual(missesBefore + 2, executor.PlanCache.Misses,
            "DROP INDEX must invalidate the cached decision.");
        Assert.AreEqual(hitsBefore + 2, executor.PlanCache.Hits,
            "The fresh full-scan decision must replace the stale entry and hit next time.");
        Assert.AreEqual(2, rows.Count, "Post-drop replan must still return both electronics rows.");
    }

    [Test]
    public async Task Analyze_InvalidatesCachedDecision()
    {
        // ANALYZE publishes a new histogram/NDV generation; access-path decisions cached
        // against the old statistics must be invalidated, not frozen at first execution.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateProductsTable();

        const string sql = "SELECT id FROM products WHERE category = \"books\"";

        long missesBefore = executor.PlanCache.Misses;
        long hitsBefore   = executor.PlanCache.Hits;

        await RunQueryAsync(executor, dbname, sql); // cold miss
        await RunQueryAsync(executor, dbname, sql); // hit

        Assert.AreEqual(missesBefore + 1, executor.PlanCache.Misses);
        Assert.AreEqual(hitsBefore + 1, executor.PlanCache.Hits);

        await RunQueryAsync(executor, dbname, "ANALYZE products");

        await RunQueryAsync(executor, dbname, sql); // must MISS: analyze generation changed
        await RunQueryAsync(executor, dbname, sql); // fresh entry → hit

        Assert.AreEqual(missesBefore + 2, executor.PlanCache.Misses,
            "ANALYZE must invalidate cached access-path decisions.");
        Assert.AreEqual(hitsBefore + 2, executor.PlanCache.Hits,
            "The re-planned decision must be re-cached and hit afterwards.");
    }
}
