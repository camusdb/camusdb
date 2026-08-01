/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies spillable derived-table materialization and streaming subquery execution.
///
/// <para>
/// Derived-table (FROM subquery) materializations use <see cref="SpillableRowList"/>, which
/// buffers rows in memory until <see cref="CamusDBConfig.SpillEffectiveThreshold"/> and then
/// overflows to a spill file. Enumeration is re-entrant so the derived table can be scanned
/// by multiple join arms. Spill files are deleted when the join cursor is fully consumed.
/// </para>
///
/// <para>
/// IN / scalar / EXISTS subquery execution streams rows to the caller, which extracts only
/// what is needed (e.g. one <see cref="ColumnValue"/> per row for IN, just the first row for
/// scalar subqueries). This eliminates the unbounded buffering without requiring spill files
/// for these sites.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSubqueryMaterializationSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;
    private int _savedFanIn;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_sq_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _savedSpillEnabled   = CamusDBConfig.SpillEnabled;
        _savedThreshold      = CamusDBConfig.SpillThresholdRows;
        _savedForceThreshold = CamusDBConfig.ForceSpillThresholdRows;
        _savedFanIn          = CamusDBConfig.SpillMergeFanIn;

        CamusDBConfig.DataDirectory = _dataDir;
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        CamusDBConfig.SpillEnabled            = _savedSpillEnabled;
        CamusDBConfig.SpillThresholdRows      = _savedThreshold;
        CamusDBConfig.ForceSpillThresholdRows = _savedForceThreshold;
        CamusDBConfig.SpillMergeFanIn         = _savedFanIn;
        CamusDBConfig.DataDirectory           = null!;

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private sealed record Fixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// Creates a <c>products</c> table (id, name, price) with <paramref name="count"/> rows
    /// and a <c>categories</c> table (id, product_id, label) with one row per product.
    /// </summary>
    private async Task<Fixture> SetupProductsAndCategories(int count = 10)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",    ColumnType.Id),
                new("name",  ColumnType.String, notNull: true),
                new("price", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "categories",
            columns:
            [
                new("id",         ColumnType.Id),
                new("product_id", ColumnType.Id, notNull: true),
                new("label",      ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> products = new();
        List<Dictionary<string, ColumnValue>> cats = new();

        for (int i = 0; i < count; i++)
        {
            string pid = ObjectIdGenerator.Generate().ToString();
            products.Add(new()
            {
                { "id",    new(ColumnType.Id,        pid) },
                { "name",  new(ColumnType.String,    "Product" + i) },
                { "price", new(ColumnType.Integer64, (long)(i + 1) * 10) },
            });

            cats.Add(new()
            {
                { "id",         new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                { "product_id", new(ColumnType.Id,     pid) },
                { "label",      new(ColumnType.String, "Cat" + i) },
            });
        }

        await executor.Insert(new InsertTicket(txn, dbname, "products", values: products));
        await executor.Insert(new InsertTicket(txn, dbname, "categories", values: cats));
        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    private static List<string> SortedNames(List<QueryResultRow> rows, string col = "name") =>
        rows.Select(r => r.Row.TryGetValue(col, out var v) ? v.StrValue ?? "" : "")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

    // ── Derived-table spill tests ─────────────────────────────────────────────

    /// <summary>
    /// Verifies that a JOIN against a derived table returns the same rows when the derived
    /// table materialization spills to disk as when it stays in memory.
    /// The spill path is proven exercised (threshold set to 3, 10 products → overflow).
    /// </summary>
    [Test]
    public async Task DerivedTableSpill_MatchesInMemoryPath()
    {
        const string sql = "SELECT d.name FROM (SELECT id, name FROM products) d";

        CamusDBConfig.SpillEnabled = false;
        Fixture fRef = await SetupProductsAndCategories(10);
        List<QueryResultRow> reference = await Run(fRef, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        CamusDBConfig.SpillMergeFanIn         = 4;
        Fixture fSpill = await SetupProductsAndCategories(10);
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        Assert.AreEqual(reference.Count, spillResult.Count,
            "Spill derived-table must return the same number of rows as the in-memory path.");
        CollectionAssert.AreEqual(SortedNames(reference), SortedNames(spillResult),
            "The set of names must be identical between the spill and in-memory paths.");
    }

    /// <summary>
    /// Verifies that spill files created during derived-table materialization are deleted
    /// after the outer query cursor is fully consumed.
    /// </summary>
    [Test]
    public async Task DerivedTableSpill_NoSpillFilesRemainAfterCompletion()
    {
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        CamusDBConfig.SpillMergeFanIn         = 4;

        Fixture f = await SetupProductsAndCategories(10);
        List<QueryResultRow> rows = await Run(f, "SELECT d.name FROM (SELECT id, name FROM products) d");

        Assert.That(rows.Count, Is.GreaterThan(0));

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] remaining = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(remaining,
                "All derived-table spill files must be deleted after the query completes.");
        }
    }

    /// <summary>
    /// Verifies that with SpillEnabled=false the derived-table path stays in memory and
    /// produces no spill files.
    /// </summary>
    [Test]
    public async Task DerivedTableSpill_FlagOff_NoSpillFiles()
    {
        CamusDBConfig.SpillEnabled = false;

        Fixture f = await SetupProductsAndCategories(10);
        List<QueryResultRow> rows = await Run(f, "SELECT d.name FROM (SELECT id, name FROM products) d");

        Assert.That(rows.Count, Is.EqualTo(10));

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] spills = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(spills, "Flag-off path must not create any spill files.");
        }
    }

    /// <summary>
    /// Verifies flag-on vs flag-off result equivalence for a derived table with projection
    /// and a filter in the outer query.
    /// </summary>
    [Test]
    public async Task DerivedTableSpill_FlagOnVsOff_IdenticalResults()
    {
        const string sql = "SELECT d.name FROM (SELECT id, name, price FROM products) d WHERE d.price > 50";

        CamusDBConfig.SpillEnabled = false;
        Fixture fOff = await SetupProductsAndCategories(15);
        List<QueryResultRow> offRows = await Run(fOff, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 4;
        CamusDBConfig.SpillMergeFanIn         = 4;
        Fixture fOn = await SetupProductsAndCategories(15);
        List<QueryResultRow> onRows = await Run(fOn, sql);

        CollectionAssert.AreEqual(SortedNames(offRows), SortedNames(onRows),
            "Spill-on and spill-off derived-table scans must return the same filtered rows.");
    }

    /// <summary>
    /// Verifies that a <see cref="SpillableRowList"/> which has overflowed to disk is
    /// re-enumerable: each <c>EnumerateAsync</c> call opens a fresh reader and yields the full
    /// row set in insertion order. This is the behavior a nested-loop join's inner-side re-scan
    /// depends on (the materialized derived table is enumerated once per outer row), which the
    /// end-to-end derived-table tests — all single-scan — do not exercise.
    /// </summary>
    [Test]
    public async Task SpillableRowList_ReEnumeratesAfterOverflow()
    {
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;

        await using SpillableRowList list = new(new QueryExecutionContext(CamusDBConfig.Ambient));

        for (int i = 0; i < 10; i++)
        {
            await list.AddAsync(new QueryResultRow(default(ObjectIdValue), new Dictionary<string, ColumnValue>
            {
                { "id", new(ColumnType.Integer64, (long)i) },
            }));
        }

        await list.SealAsync();

        Assert.That(list.Count, Is.EqualTo(10), "All added rows must be counted.");

        List<long> first = new();
        await foreach (QueryResultRow row in list.EnumerateAsync())
            first.Add(row.Row["id"].LongValue);

        List<long> second = new();
        await foreach (QueryResultRow row in list.EnumerateAsync())
            second.Add(row.Row["id"].LongValue);

        CollectionAssert.AreEqual(Enumerable.Range(0, 10).Select(i => (long)i).ToList(), first,
            "First enumeration of a spilled list must yield every row in insertion order.");
        CollectionAssert.AreEqual(first, second,
            "Re-enumeration of a spilled list must yield the identical full row set.");
    }

    // ── Subquery streaming tests (IN / EXISTS / scalar) ───────────────────────

    /// <summary>
    /// Verifies that an IN subquery returns correct results regardless of the SpillEnabled
    /// flag. The subquery side now streams rows without buffering a list, so no spill files
    /// are created even with SpillEnabled=true.
    /// </summary>
    [Test]
    public async Task InSubqueryStreaming_CorrectResults()
    {
        Fixture f = await SetupProductsAndCategories(10);

        // Insert extra products that have no category row — these must not appear in the result.
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "products", values:
        [
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Orphan1") }, { "price", new(ColumnType.Integer64, 1L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Orphan2") }, { "price", new(ColumnType.Integer64, 2L) } },
        ]));
        await f.Database.Transactions.CommitAsync(txn);

        // Flag-off baseline
        CamusDBConfig.SpillEnabled = false;
        List<QueryResultRow> offRows = await Run(f,
            "SELECT name FROM products WHERE id IN (SELECT product_id FROM categories) ORDER BY name");

        // Flag-on (streaming path is the same regardless; flag just gates DerivedTable spill)
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        List<QueryResultRow> onRows = await Run(f,
            "SELECT name FROM products WHERE id IN (SELECT product_id FROM categories) ORDER BY name");

        Assert.AreEqual(offRows.Count, onRows.Count,
            "IN subquery must return the same row count on both flag-on and flag-off paths.");
        CollectionAssert.AreEqual(SortedNames(offRows), SortedNames(onRows),
            "IN subquery results must be identical between flag-on and flag-off paths.");

        // Orphans must not appear
        Assert.IsFalse(onRows.Any(r => r.Row.TryGetValue("name", out var v) && v.StrValue == "Orphan1"),
            "Products without a matching category row must not appear in the IN subquery result.");
    }

    /// <summary>
    /// Verifies that a NOT IN subquery using the streaming path excludes the correct rows.
    /// </summary>
    [Test]
    public async Task NotInSubqueryStreaming_CorrectResults()
    {
        Fixture f = await SetupProductsAndCategories(8);

        // Add a product that is explicitly in the categories table — it must be excluded.
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        string pid = ObjectIdGenerator.Generate().ToString();
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "products", values:
        [
            new() { { "id", new(ColumnType.Id, pid) }, { "name", new(ColumnType.String, "InCat") }, { "price", new(ColumnType.Integer64, 99L) } },
        ]));
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "categories", values:
        [
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "product_id", new(ColumnType.Id, pid) }, { "label", new(ColumnType.String, "Extra") } },
        ]));
        await f.Database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await Run(f,
            "SELECT name FROM products WHERE id NOT IN (SELECT product_id FROM categories) ORDER BY name");

        // No product that is in categories should appear
        Assert.IsFalse(rows.Any(r => r.Row.TryGetValue("name", out var v) && v.StrValue == "InCat"),
            "Products present in the IN subquery must be excluded from the NOT IN result.");
    }

    /// <summary>
    /// Verifies scalar subquery streaming: SELECT (SELECT MAX(price) FROM products) returns
    /// the correct max value without buffering a list.
    /// </summary>
    [Test]
    public async Task ScalarSubqueryStreaming_ReturnsCorrectValue()
    {
        Fixture f = await SetupProductsAndCategories(5);

        // prices: 10, 20, 30, 40, 50 → max = 50, which is Product4
        List<QueryResultRow> rows = await Run(f,
            "SELECT name, price FROM products WHERE price = (SELECT MAX(price) FROM products)");

        Assert.That(rows.Count, Is.EqualTo(1),
            "Scalar subquery filtering by MAX must return exactly one row.");
        Assert.That(rows[0].Row["price"].LongValue, Is.EqualTo(50L),
            "The returned row must have the maximum price.");
    }
}
