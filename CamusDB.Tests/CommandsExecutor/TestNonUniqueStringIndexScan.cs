
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that equality predicates on non-unique String/Id indexes use an
/// index-range-scan rather than falling back to a full table scan.
///
/// Prior to the fix, <c>SupportsExactEqualityPrefixUpperBound</c> returned
/// false for String/Id columns, causing the planner to emit a FullScanFromTableIndex.
/// These tests prove the fix: (1) plan shape changes from full-scan to range-scan,
/// and (2) query results are identical to the full-scan baseline.
/// </summary>
public class TestNonUniqueStringIndexScan : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupCatalogTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "products",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
                new("name", ColumnType.String, notNull: true),
                new("price", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        // Non-unique index on a String column.
        AlterIndexTicket addCategoryIdx = new(
            databaseName: dbname,
            tableName: "products",
            indexName: "category_idx",
            columns: new ColumnIndexInfo[] { new("category", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex);
        await executor.AlterIndex(addCategoryIdx);

        // Composite (String, Int64) index for Case-2 half-open range tests.
        AlterIndexTicket addCategoryPriceIdx = new(
            databaseName: dbname,
            tableName: "products",
            indexName: "category_price_idx",
            columns: new ColumnIndexInfo[]
            {
                new("category", OrderType.Ascending),
                new("price", OrderType.Ascending),
            },
            operation: AlterIndexOperation.AddIndex);
        await executor.AlterIndex(addCategoryPriceIdx);

        return (dbname, database, executor);
    }

    private static async Task InsertProducts(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        IEnumerable<(string category, string name, long price)> products)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        foreach ((string category, string name, long price) in products)
        {
            InsertTicket ticket = new(
                txnState: txn,
                databaseName: dbname,
                tableName: "products",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "category", new(ColumnType.String, category) },
                        { "name", new(ColumnType.String, name) },
                        { "price", new(ColumnType.Integer64, price) },
                    }
                });
            await executor.Insert(ticket);
        }

        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    [Test]
    public async Task StringEqualityReturnsOnlyMatchingRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop",   999L),
            ("electronics", "phone",    599L),
            ("furniture",   "chair",    199L),
            ("furniture",   "desk",     349L),
            ("clothing",    "shirt",     29L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT category, name FROM products WHERE category = 'electronics'");

        Assert.AreEqual(2, rows.Count,
            "Expected exactly 2 electronics rows; got " + rows.Count);

        HashSet<string> names = rows.Select(r => r.Row["name"].StrValue!).ToHashSet();
        Assert.That(names, Does.Contain("laptop"), "laptop must be in the result");
        Assert.That(names, Does.Contain("phone"),  "phone must be in the result");

        foreach (QueryResultRow row in rows)
            Assert.AreEqual("electronics", row.Row["category"].StrValue,
                "Every returned row must have category = 'electronics'");
    }

    [Test]
    public async Task StringEqualityExcludesNonMatchingRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop",   999L),
            ("furniture",   "chair",    199L),
            ("clothing",    "shirt",     29L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'furniture'");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("chair", rows[0].Row["name"].StrValue);
    }

    [Test]
    public async Task StringEqualityOnEmptyMatchReturnsNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop", 999L),
            ("furniture",   "chair",  199L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'toys'");

        Assert.AreEqual(0, rows.Count, "No rows should match a category that doesn't exist");
    }

    [Test]
    public async Task StringEqualityResultsMatchFullScanBaseline()
    {
        // This is the result-equivalence proof: the index scan must return the same rows
        // as a full table scan with the same predicate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop",     999L),
            ("electronics", "tablet",     499L),
            ("electronics", "headphones", 149L),
            ("furniture",   "chair",      199L),
            ("furniture",   "desk",       349L),
            ("clothing",    "shirt",       29L),
        });

        // Index scan path (uses category_idx).
        List<QueryResultRow> indexRows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics' ORDER BY name");

        // Full scan baseline: force it via a non-indexed form (using a comparison the planner
        // cannot drive via category_idx — simulated by querying with an OR that blocks index use).
        List<QueryResultRow> fullScanRows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics' OR category = 'electronics' ORDER BY name");

        Assert.AreEqual(fullScanRows.Count, indexRows.Count,
            "Index scan and full-scan baseline must return the same number of rows");

        for (int i = 0; i < fullScanRows.Count; i++)
            Assert.AreEqual(fullScanRows[i].Row["name"].StrValue, indexRows[i].Row["name"].StrValue,
                $"Row {i} must match between index scan and full-scan baseline");
    }

    [Test]
    public async Task ExplainShowsIndexRangeScanForStringEquality()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: "EXPLAIN SELECT * FROM products WHERE category = 'electronics'",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        await database.Transactions.CommitAsync(txn);

        // EXPLAIN rows have columns: stage, node, detail, estimated_rows, estimated_cost.
        string nodeText = string.Join(" ", rows.Select(r =>
        {
            string node   = r.Row.TryGetValue("node",   out ColumnValue? nv) ? nv?.StrValue ?? "" : "";
            string detail = r.Row.TryGetValue("detail", out ColumnValue? dv) ? dv?.StrValue ?? "" : "";
            return node + " " + detail;
        }));

        Assert.That(nodeText, Does.Contain("index-range-scan").IgnoreCase,
            $"EXPLAIN must show index-range-scan for String equality on a non-unique index.\nGot:\n{nodeText}");
        Assert.That(nodeText, Does.Contain("category_idx"),
            "EXPLAIN must name the category_idx index");
    }

    [Test]
    public async Task MultipleMatchingRowsSameCategory_AllReturned()
    {
        // Stress: many rows share the same category value.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        var products = Enumerable.Range(0, 20)
            .Select(i => ("electronics", "device-" + i, (long)(100 + i)))
            .Concat(Enumerable.Range(0, 5)
                .Select(i => ("furniture", "item-" + i, (long)(200 + i))));

        await InsertProducts(executor, database, dbname, products);

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics'");

        Assert.AreEqual(20, rows.Count,
            "All 20 electronics rows must be returned by the index range scan");

        foreach (QueryResultRow row in rows)
            Assert.IsTrue(row.Row["name"].StrValue!.StartsWith("device-"),
                "All returned rows must be electronics products");
    }

    // ── Case 2: String equality prefix + half-open trailing range ─────────────

    [Test]
    public async Task StringPrefixPlusOpenUpperRange_ReturnsOnlyMatchingRows()
    {
        // category_price_idx covers (category String, price Int64).
        // WHERE category='electronics' AND price > 300 — open upper side.
        // Before the fix the scan would have run to the end of the index (furniture, clothing…).
        // After fix, toBound is capped at [electronics] inclusive, so only electronics rows
        // can appear; the residual filter on category guards any off-prefix candidates.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop",  999L),
            ("electronics", "phone",   599L),
            ("electronics", "cable",    19L),
            ("furniture",   "chair",   199L),
            ("furniture",   "desk",    349L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name, price FROM products WHERE category = 'electronics' AND price > 300");

        Assert.AreEqual(2, rows.Count,
            "Only electronics rows with price > 300 should be returned");

        HashSet<string> names = rows.Select(r => r.Row["name"].StrValue!).ToHashSet();
        Assert.That(names, Does.Contain("laptop"));
        Assert.That(names, Does.Contain("phone"));

        foreach (QueryResultRow row in rows)
            Assert.Greater(row.Row["price"].LongValue, 300L,
                "All returned rows must satisfy price > 300");
    }

    [Test]
    public async Task StringPrefixPlusOpenLowerRange_ReturnsOnlyMatchingRows()
    {
        // WHERE category='electronics' AND price < 200 — open lower side.
        // Before the fix fromBound was null → scan from start of index (includes
        // any category that sorts before 'electronics', e.g. 'clothing').
        // After fix, fromBound is floored at [electronics] inclusive.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("clothing",    "shirt",    29L),
            ("electronics", "cable",    19L),
            ("electronics", "phone",   599L),
            ("furniture",   "chair",   199L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics' AND price < 200");

        Assert.AreEqual(1, rows.Count,
            "Only electronics rows with price < 200 should be returned");
        Assert.AreEqual("cable", rows[0].Row["name"].StrValue);
    }

    // ── Path 2 — both sides bounded ──────────────────────────────────────────

    [Test]
    public async Task StringPrefixPlusBothBoundedRange_ReturnsOnlyMatchingRows()
    {
        // WHERE category='electronics' AND price > 100 AND price < 500 — both sides bounded.
        // The scan is already confined (fromBound=[electronics,100], toBound=[electronics,500])
        // so neither open-side sentinel fix applies. Verify correctness: no off-category
        // and no out-of-range rows must appear.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCatalogTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("clothing",    "shirt",     29L),
            ("electronics", "cable",     19L),
            ("electronics", "phone",    599L),
            ("electronics", "tablet",   349L),
            ("furniture",   "chair",    199L),
            ("furniture",   "desk",     349L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name, price FROM products WHERE category = 'electronics' AND price > 100 AND price < 500");

        Assert.AreEqual(1, rows.Count,
            "Only electronics rows with 100 < price < 500 should be returned");
        Assert.AreEqual("tablet", rows[0].Row["name"].StrValue);
        Assert.Greater(rows[0].Row["price"].LongValue, 100L);
        Assert.Less(rows[0].Row["price"].LongValue, 500L);
    }

    // ── Path 3 — String equality prefix on composite index (no range col) ──────

    /// <summary>
    /// Sets up a products table whose ONLY index is the composite (category, price) index.
    /// Forces the planner to use Path 3 (partial-equality prefix) rather than a single-col
    /// index (Path 1) which would otherwise win on score.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupCompositeOnlyTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "products",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
                new("name", ColumnType.String, notNull: true),
                new("price", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        // Composite index ONLY — no single-col category_idx.  The planner must use Path 3
        // (partial equality prefix) for WHERE category = '...'.
        AlterIndexTicket addIdx = new(
            databaseName: dbname,
            tableName: "products",
            indexName: "category_price_idx",
            columns: new ColumnIndexInfo[]
            {
                new("category", OrderType.Ascending),
                new("price", OrderType.Ascending),
            },
            operation: AlterIndexOperation.AddIndex);
        await executor.AlterIndex(addIdx);

        return (dbname, database, executor);
    }

    [Test]
    public async Task CompositeOnlyIndex_StringPrefixOnly_ReturnsAllMatchingRows()
    {
        // Path 3 correctness: WHERE category='electronics' with only category_price_idx available.
        // The planner emits a RangeScanFromIndex with inclusive [electronics, electronics] prefix
        // bounds.  All electronics rows must be returned regardless of price.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCompositeOnlyTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop",     999L),
            ("electronics", "cable",       19L),
            ("electronics", "headphones", 149L),
            ("furniture",   "chair",      199L),
            ("clothing",    "shirt",       29L),
        });

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics'");

        Assert.AreEqual(3, rows.Count,
            "All 3 electronics rows must be returned via composite-index Path-3 scan");

        HashSet<string> names = rows.Select(r => r.Row["name"].StrValue!).ToHashSet();
        Assert.That(names, Does.Contain("laptop"));
        Assert.That(names, Does.Contain("cable"));
        Assert.That(names, Does.Contain("headphones"));
    }

    [Test]
    public async Task CompositeOnlyIndex_StringPrefixOnly_ExcludesOtherCategories()
    {
        // Result-equivalence proof for Path 3: index scan result must match a full-scan baseline.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCompositeOnlyTable();

        await InsertProducts(executor, database, dbname, new[]
        {
            ("electronics", "laptop",  999L),
            ("electronics", "phone",   599L),
            ("furniture",   "chair",   199L),
            ("clothing",    "shirt",    29L),
        });

        // Index-scan path (Path 3 via composite-only index).
        List<QueryResultRow> indexRows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics' ORDER BY name");

        // Full-scan baseline: OR trick forces the planner off the index.
        List<QueryResultRow> fullScanRows = await RunSql(executor, database, dbname,
            "SELECT name FROM products WHERE category = 'electronics' OR category = 'electronics' ORDER BY name");

        Assert.AreEqual(fullScanRows.Count, indexRows.Count,
            "Path-3 index scan and full-scan baseline must return the same row count");

        for (int i = 0; i < fullScanRows.Count; i++)
            Assert.AreEqual(fullScanRows[i].Row["name"].StrValue, indexRows[i].Row["name"].StrValue,
                $"Row {i} must match between Path-3 index scan and full-scan baseline");
    }
}
