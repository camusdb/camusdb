
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
/// Acceptance tests for the hash join executor.
///
/// Unindexed equi-joins now use HashJoinNode (planner wires it when the right key
/// has no index and the ON predicate contains at least one equi conjunct). These tests
/// confirm that hash join produces results identical to nested-loop / index-nested-loop
/// across all corner cases, and that the build-row cap falls back to nested-loop correctly.
///
/// Covered:
///   1. One-to-one inner join.
///   2. One-to-many inner join (fan-out on the build side).
///   3. No-match case (probe key absent from build table).
///   4. NULL join keys — never match.
///   5. Multi-column equi-join (ON a.x = b.x AND a.y = b.y).
///   6. Residual non-equi conjunct (ON a.x = b.x AND a.z &gt; b.z).
///   7. Build-row cap fallback (tiny cap → nested-loop, still correct).
/// </summary>
// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
public sealed class TestHashJoinExecutor : SharedNodeBaseTest
{
    // ── Fixtures ──────────────────────────────────────────────────────────────

    private sealed record HJFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// orders / line_items schema (same shape as TestJoinParityOracle).
    /// No index on line_items.order_id → planner picks HashJoinNode.
    /// </summary>
    private async Task<HJFixture> SetupOrdersItems(bool includeNullKey = false, CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "line_items",
            columns:
            [
                new("id",       ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("product",  ColumnType.String, notNull: true),
                new("qty",      ColumnType.Integer64),
            ],
            // No index on order_id → hash join
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        string oaId = ObjectIdGenerator.Generate().ToString();
        string obId = ObjectIdGenerator.Generate().ToString();
        string ocId = ObjectIdGenerator.Generate().ToString();
        string odId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                new() { { "id", new(ColumnType.Id, oaId) }, { "name", new(ColumnType.String, "Order-A") }, { "score", new(ColumnType.Integer64, 10L) } },
                new() { { "id", new(ColumnType.Id, obId) }, { "name", new(ColumnType.String, "Order-B") }, { "score", new(ColumnType.Integer64, 20L) } },
                new() { { "id", new(ColumnType.Id, ocId) }, { "name", new(ColumnType.String, "Order-C") }, { "score", new(ColumnType.Integer64, 30L) } },
                new() { { "id", new(ColumnType.Id, odId) }, { "name", new(ColumnType.String, "Order-D") }, { "score", new(ColumnType.Integer64, 40L) } },
            ]));

        List<Dictionary<string, ColumnValue>> items =
        [
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, oaId) }, { "product", new(ColumnType.String, "Widget")    }, { "qty", new(ColumnType.Integer64, 5L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Gadget")    }, { "qty", new(ColumnType.Integer64, 3L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Doohickey") }, { "qty", new(ColumnType.Integer64, 7L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, odId) }, { "product", new(ColumnType.String, "Sprocket")  }, { "qty", new(ColumnType.Integer64, 2L) } },
        ];

        if (includeNullKey)
            items.Add(new()
            {
                { "id",       new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                { "order_id", new(ColumnType.Null,   0) },
                { "product",  new(ColumnType.String, "GhostPart") },
                { "qty",      new(ColumnType.Integer64, 99L) },
            });

        await executor.Insert(new InsertTicket(txn, dbname, "line_items", values: items));
        await database.Transactions.CommitAsync(txn);

        return new HJFixture(dbname, database, executor);
    }

    /// <summary>
    /// Two-column join key: items keyed on (category_id, subcategory_id).
    /// No index on either FK column → hash join uses a two-component composite key.
    /// </summary>
    private async Task<HJFixture> SetupMultiKeyFixture()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "categories",
            columns:
            [
                new("cat_id",  ColumnType.Integer64),
                new("sub_id",  ColumnType.Integer64),
                new("label",   ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("cat_id", OrderType.Ascending), new("sub_id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",      ColumnType.Id),
                new("cat_id",  ColumnType.Integer64),
                new("sub_id",  ColumnType.Integer64),
                new("name",    ColumnType.String, notNull: true),
            ],
            // No index on (cat_id, sub_id) → hash join
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.Insert(new InsertTicket(txn, dbname, "categories",
            values:
            [
                new() { { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "Electronics/Audio")  } },
                new() { { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 20L) }, { "label", new(ColumnType.String, "Electronics/Video")  } },
                new() { { "cat_id", new(ColumnType.Integer64, 2L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "Clothing/Tops")      } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "products",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "Headphones") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 20L) }, { "name", new(ColumnType.String, "Monitor")    } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 2L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "T-Shirt")    } },
                // cat_id=9 does not exist in categories → no match
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 9L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "Orphan")     } },
            ]));

        await database.Transactions.CommitAsync(txn);
        return new HJFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(HJFixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task HashJoin_OneToOne_ReturnsCorrectPairs()
    {
        HJFixture f = await SetupOrdersItems();

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE o.name IN (\"Order-A\", \"Order-C\", \"Order-D\") " +
            "ORDER BY o.name, li.product");

        // Order-A has one item; Order-C has none; Order-D has one.
        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual("Order-A", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Widget",  rows[0].Row["product"].StrValue);
        Assert.AreEqual("Order-D",  rows[1].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", rows[1].Row["product"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_OneToMany_ReturnsBothRows()
    {
        HJFixture f = await SetupOrdersItems();

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE o.name = \"Order-B\" " +
            "ORDER BY li.product");

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual("Order-B",   rows[0].Row["name"].StrValue);
        Assert.AreEqual("Doohickey", rows[0].Row["product"].StrValue);
        Assert.AreEqual("Order-B", rows[1].Row["name"].StrValue);
        Assert.AreEqual("Gadget",  rows[1].Row["product"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_NoMatch_ReturnsEmptyResult()
    {
        HJFixture f = await SetupOrdersItems();

        // Order-C has no items.
        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE o.name = \"Order-C\"");

        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_NullBuildKey_RowExcluded()
    {
        HJFixture f = await SetupOrdersItems(includeNullKey: true);

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product");

        Assert.AreEqual(4, rows.Count, "NULL-keyed item must not appear");

        foreach (QueryResultRow row in rows)
            Assert.AreNotEqual("GhostPart", row.Row["product"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_NullBuildKey_MatchesNonNullRun()
    {
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        HJFixture withNull    = await SetupOrdersItems(includeNullKey: true);
        HJFixture withoutNull = await SetupOrdersItems(includeNullKey: false);

        List<QueryResultRow> withNullRows    = await Run(withNull,    sql);
        List<QueryResultRow> withoutNullRows = await Run(withoutNull, sql);

        Assert.AreEqual(withoutNullRows.Count, withNullRows.Count);

        for (int i = 0; i < withoutNullRows.Count; i++)
        {
            Assert.AreEqual(withoutNullRows[i].Row["name"].StrValue,    withNullRows[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(withoutNullRows[i].Row["product"].StrValue, withNullRows[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_MultiColumnKey_ReturnsCorrectRows()
    {
        HJFixture f = await SetupMultiKeyFixture();

        List<QueryResultRow> rows = await Run(f,
            "SELECT c.label, p.name " +
            "FROM categories c JOIN products p ON p.cat_id = c.cat_id AND p.sub_id = c.sub_id " +
            "ORDER BY c.label, p.name");

        // Only 3 products match (Orphan's cat_id=9 doesn't exist in categories).
        // ORDER BY c.label, p.name — "Clothing/..." sorts before "Electronics/..."
        Assert.AreEqual(3, rows.Count);

        Assert.AreEqual("Clothing/Tops", rows[0].Row["label"].StrValue);
        Assert.AreEqual("T-Shirt",       rows[0].Row["name"].StrValue);

        Assert.AreEqual("Electronics/Audio", rows[1].Row["label"].StrValue);
        Assert.AreEqual("Headphones",        rows[1].Row["name"].StrValue);

        Assert.AreEqual("Electronics/Video", rows[2].Row["label"].StrValue);
        Assert.AreEqual("Monitor",           rows[2].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_ResidualNonEquiConjunct_AppliedCorrectly()
    {
        // ON li.order_id = o.id AND li.qty > 4
        // "Widget" (qty=5, order-A) and "Doohickey" (qty=7, order-B) pass; others don't.
        HJFixture f = await SetupOrdersItems();

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product, li.qty " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id AND li.qty > 4 " +
            "ORDER BY o.name, li.product");

        Assert.AreEqual(2, rows.Count);

        Assert.AreEqual("Order-A", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Widget",  rows[0].Row["product"].StrValue);
        Assert.AreEqual(5L, rows[0].Row["qty"].LongValue);

        Assert.AreEqual("Order-B",   rows[1].Row["name"].StrValue);
        Assert.AreEqual("Doohickey", rows[1].Row["product"].StrValue);
        Assert.AreEqual(7L, rows[1].Row["qty"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_BuildCapFallback_StillCorrect()
    {
        // A cap of 1 means the build phase is exceeded immediately — falls back to nested-loop.
        HJFixture f = await SetupOrdersItems(options: Options with { HashJoinMaxBuildRows = 1 });

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product");

        // Same result as the normal run — correctness preserved through fallback.
        Assert.AreEqual(4, rows.Count);

        Assert.AreEqual("Order-A", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Widget",  rows[0].Row["product"].StrValue);
        Assert.AreEqual("Order-B",   rows[1].Row["name"].StrValue);
        Assert.AreEqual("Doohickey", rows[1].Row["product"].StrValue);
        Assert.AreEqual("Order-B", rows[2].Row["name"].StrValue);
        Assert.AreEqual("Gadget",  rows[2].Row["product"].StrValue);
        Assert.AreEqual("Order-D",  rows[3].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", rows[3].Row["product"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task HashJoin_MatchesIndexedNestedLoop()
    {
        // Re-run the standard join with an index on line_items.order_id to confirm
        // hash join and index-nested-loop produce identical rows.
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        HJFixture hashFixture = await SetupOrdersItems();
        List<QueryResultRow> hashRows = await Run(hashFixture, sql);

        // Build the same fixture but with an index → index-nested-loop path.
        (string dbname2, DatabaseDescriptor database2, CommandExecutor executor2) = await CreateDatabase();
        KvTransaction txn2 = await database2.Transactions.BeginAsync();

        await executor2.CreateTable(new CreateTableTicket(
            databaseName: dbname2, tableName: "orders",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor2.CreateTable(new CreateTableTicket(
            databaseName: dbname2, tableName: "line_items",
            columns:
            [
                new("id",       ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("product",  ColumnType.String, notNull: true),
                new("qty",      ColumnType.Integer64),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_id_idx", [new("order_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        string oaId = ObjectIdGenerator.Generate().ToString();
        string obId = ObjectIdGenerator.Generate().ToString();
        string ocId = ObjectIdGenerator.Generate().ToString();
        string odId = ObjectIdGenerator.Generate().ToString();

        await executor2.Insert(new InsertTicket(txn2, dbname2, "orders",
            values:
            [
                new() { { "id", new(ColumnType.Id, oaId) }, { "name", new(ColumnType.String, "Order-A") }, { "score", new(ColumnType.Integer64, 10L) } },
                new() { { "id", new(ColumnType.Id, obId) }, { "name", new(ColumnType.String, "Order-B") }, { "score", new(ColumnType.Integer64, 20L) } },
                new() { { "id", new(ColumnType.Id, ocId) }, { "name", new(ColumnType.String, "Order-C") }, { "score", new(ColumnType.Integer64, 30L) } },
                new() { { "id", new(ColumnType.Id, odId) }, { "name", new(ColumnType.String, "Order-D") }, { "score", new(ColumnType.Integer64, 40L) } },
            ]));

        await executor2.Insert(new InsertTicket(txn2, dbname2, "line_items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, oaId) }, { "product", new(ColumnType.String, "Widget")    }, { "qty", new(ColumnType.Integer64, 5L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Gadget")    }, { "qty", new(ColumnType.Integer64, 3L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Doohickey") }, { "qty", new(ColumnType.Integer64, 7L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, odId) }, { "product", new(ColumnType.String, "Sprocket")  }, { "qty", new(ColumnType.Integer64, 2L) } },
            ]));

        await database2.Transactions.CommitAsync(txn2);

        HJFixture indexedFixture = new(dbname2, database2, executor2);
        List<QueryResultRow> ixRows = await Run(indexedFixture, sql);

        Assert.AreEqual(4, hashRows.Count,  "hash-join row count");
        Assert.AreEqual(4, ixRows.Count, "index-nested-loop row count");

        for (int i = 0; i < hashRows.Count; i++)
        {
            Assert.AreEqual(hashRows[i].Row["name"].StrValue,    ixRows[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(hashRows[i].Row["product"].StrValue, ixRows[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task BuildLeft_ProducesIdenticalRowsToDefaultBuildRight()
    {
        // Parity test: inject stats so the planner chooses build=left (orders) for one
        // run and build=right (line_items) for another; both must return identical rows.
        //
        // Both variants use the same shared fixture — the executor and database are the same;
        // only the StatisticsManager's seeded row counts differ between the two JoinQueryPlanner
        // instances constructed inside ExecuteSQLQuery.  We verify the final cursor rows match.
        HJFixture f = await SetupOrdersItems();

        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        // Default (no stats injection) → build=right.  Run first to capture the baseline.
        List<QueryResultRow> baseline = await Run(f, sql);

        // Inject stats to force build=left: make "orders" appear very small (1 row) and
        // "line_items" appear large (100 000 rows).  The planner compares left (TableScanNode
        // → orders estimate) vs right (line_items estimate) and picks left as build.
        TableDescriptor ordersTable    = await f.Database.TableDescriptors["orders"];
        TableDescriptor lineItemsTable = await f.Database.TableDescriptors["line_items"];
        f.Executor.Statistics.SeedRowCountForTesting(f.Database, ordersTable,    1);
        f.Executor.Statistics.SeedRowCountForTesting(f.Database, lineItemsTable, 100_000);

        List<QueryResultRow> buildLeft = await Run(f, sql);

        Assert.AreEqual(baseline.Count, buildLeft.Count, "row count must be identical regardless of build side");

        for (int i = 0; i < baseline.Count; i++)
        {
            Assert.AreEqual(baseline[i].Row["name"].StrValue,    buildLeft[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(baseline[i].Row["product"].StrValue, buildLeft[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task MixedEquiKey_ArrayConjunctExcluded_HashJoinUsesIdKeyOnly()
    {
        // When the ON clause contains BOTH an id equi-key and an array equi-key
        // (ON a.id = b.id AND a.tags = b.tags), TryExtractEquiKeys should exclude the
        // array conjunct and yield only the id pair. The planner picks HashJoinNode keyed
        // on id; the array predicate is handled as a residual inside OnPredicate.
        //
        // Without the IsArrayColumn exclusion the array conjunct would enter the hash table
        // with type-only hashing (no element payload), collapsing every distinct array into
        // one bucket and degrading the probe to O(n).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "left_tbl",
            columns:
            [
                new("id",   ColumnType.Id),
                new("tags", ColumnType.Array, arrayElementType: ColumnType.String),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "right_tbl",
            columns:
            [
                new("id",     ColumnType.Id),               // independent PK
                new("ref_id", ColumnType.Id),               // foreign-key-like join column (no index)
                new("tags",   ColumnType.Array, arrayElementType: ColumnType.String),
                new("val",    ColumnType.String, notNull: true),
            ],
            // No index on ref_id → hash join chosen when equi-keys are available
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        string sharedId = ObjectIdGenerator.Generate().ToString();
        string otherId  = ObjectIdGenerator.Generate().ToString();

        ColumnValue tagsXY = ColumnValue.FromArray(ColumnType.String,
            [new(ColumnType.String, "x"), new(ColumnType.String, "y")]);
        ColumnValue tagsZ = ColumnValue.FromArray(ColumnType.String,
            [new(ColumnType.String, "z")]);

        await executor.Insert(new InsertTicket(txn, dbname, "left_tbl",
            values:
            [
                new() { { "id", new(ColumnType.Id, sharedId) }, { "tags", tagsXY }, { "name", new(ColumnType.String, "L-Match") } },
                new() { { "id", new(ColumnType.Id, otherId)  }, { "tags", tagsZ  }, { "name", new(ColumnType.String, "L-NoMatch") } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "right_tbl",
            values:
            [
                // ref_id=sharedId, same tags → should match L-Match
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "ref_id", new(ColumnType.Id, sharedId) }, { "tags", tagsXY }, { "val", new(ColumnType.String, "R-A") } },
                // ref_id=sharedId, different tags → ref_id equi-key matches, residual array predicate filters this out
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "ref_id", new(ColumnType.Id, sharedId) }, { "tags", tagsZ  }, { "val", new(ColumnType.String, "R-B") } },
            ]));

        await database.Transactions.CommitAsync(txn);

        // ON l.id = r.ref_id AND l.tags = r.tags
        // TryExtractEquiKeys extracts the `ref_id` equi-key (non-array) and ignores the
        // `tags` conjunct (array column excluded via IsArrayColumn). Hash join uses ref_id only.
        // The full OnPredicate (including l.tags = r.tags) is applied as residual, so only the
        // row where BOTH ref_id AND tags match appears (R-B has ref_id match but tags mismatch).
        KvTransaction txn2 = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn2, database: dbname,
            sql: "SELECT l.name, r.val FROM left_tbl l JOIN right_tbl r ON l.id = r.ref_id AND l.tags = r.tags ORDER BY l.name, r.val",
            parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        // Only the row where both ref_id AND tags match should appear (R-B filtered by residual).
        Assert.AreEqual(1, rows.Count, "residual array predicate must filter R-B");
        Assert.AreEqual("L-Match", rows[0].Row["name"].StrValue);
        Assert.AreEqual("R-A",     rows[0].Row["val"].StrValue);
    }

    // ── Cost-based selection for indexed joins ─────────────────────────

    /// <summary>
    /// When the outer (left) side has far more estimated rows than the indexed right side,
    /// When stats indicate a large outer side, the planner selects hash join even though an index match exists on the right side.
    /// This execution test verifies that the hash join path still produces the same rows
    /// as a direct nested-loop / INLJ baseline.
    ///
    /// Schema: users (app_users) ← posts (indexed on user_id).
    /// Stats seeded: users = 100 000, posts = 10 → hash join wins over INLJ.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LargeOuterIndexedRight_HashJoinProducesCorrectRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "app_users",
            columns:
            [
                new("id",    ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "posts",
            columns:
            [
                new("id",      ColumnType.Id),
                new("user_id", ColumnType.Id, notNull: true),
                new("title",   ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "posts_user_id_idx", [new("user_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        string u1 = ObjectIdGenerator.Generate().ToString();
        string u2 = ObjectIdGenerator.Generate().ToString();
        string u3 = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "app_users",
            values:
            [
                new() { { "id", new(ColumnType.Id, u1) }, { "email", new(ColumnType.String, "alice@x.com") } },
                new() { { "id", new(ColumnType.Id, u2) }, { "email", new(ColumnType.String, "bob@x.com")   } },
                new() { { "id", new(ColumnType.Id, u3) }, { "email", new(ColumnType.String, "carol@x.com") } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "posts",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "user_id", new(ColumnType.Id, u1) }, { "title", new(ColumnType.String, "Alice-Post-1") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "user_id", new(ColumnType.Id, u1) }, { "title", new(ColumnType.String, "Alice-Post-2") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "user_id", new(ColumnType.Id, u2) }, { "title", new(ColumnType.String, "Bob-Post-1")   } },
                // u3 (carol) has no posts — she must not appear in the inner join result.
            ]));

        await database.Transactions.CommitAsync(txn);

        // Seed stats: users >> posts → hash join chosen over INLJ.
        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, 100_000);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, 10);

        KvTransaction txn2 = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(
            txnState: txn2, database: dbname,
            sql: "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id ORDER BY u.email, p.title",
            parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        // carol has no posts → excluded from inner join result
        Assert.AreEqual(3, rows.Count, "expect 3 rows: 2 for alice + 1 for bob");
        Assert.AreEqual("alice@x.com",  rows[0].Row["email"].StrValue);
        Assert.AreEqual("Alice-Post-1", rows[0].Row["title"].StrValue);
        Assert.AreEqual("alice@x.com",  rows[1].Row["email"].StrValue);
        Assert.AreEqual("Alice-Post-2", rows[1].Row["title"].StrValue);
        Assert.AreEqual("bob@x.com",    rows[2].Row["email"].StrValue);
        Assert.AreEqual("Bob-Post-1",   rows[2].Row["title"].StrValue);
    }
}
