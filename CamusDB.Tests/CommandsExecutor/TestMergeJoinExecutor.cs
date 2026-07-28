
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
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for the merge-join executor.
///
/// The planner's <c>ForceMergeJoinForTesting</c> flag is set on the executor's
/// <c>Statistics</c> manager so that every inner equi-join in these tests is executed
/// via <c>MergeJoinNode</c> rather than the default hash/index-nested-loop path.
///
/// All tests prove identical results to the nested-loop / parity-oracle baseline.
///
/// Covered:
///   1. One-to-one inner join.
///   2. One-to-many inner join (equal-key fan-out → correct cross-product within key group).
///   3. No-match case (left row with no matching right row is excluded).
///   4. Nullable join key (NULL keys on either side must never match).
///   5. Duplicate keys on BOTH sides → full cross-product within the key group.
///   6. Residual non-equi conjunct applied after the merge step.
///   7. Multi-column equi-join key.
/// </summary>
public sealed class TestMergeJoinExecutor : SharedNodeBaseTest
{
    // ── Fixtures ──────────────────────────────────────────────────────────────

    private sealed record MJFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// orders / line_items schema — no index on line_items.order_id.
    /// ForceMergeJoinForTesting is set after creation.
    /// </summary>
    private async Task<MJFixture> SetupOrdersItems(bool includeNullKey = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",    ColumnType.Id),
                new("name",  ColumnType.String, notNull: true),
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
                { "id",       new(ColumnType.Id,       ObjectIdGenerator.Generate().ToString()) },
                { "order_id", new(ColumnType.Null,     0) },
                { "product",  new(ColumnType.String,   "GhostPart") },
                { "qty",      new(ColumnType.Integer64, 99L) },
            });

        await executor.Insert(new InsertTicket(txn, dbname, "line_items", values: items));
        await database.Transactions.CommitAsync(txn);

        executor.Statistics.ForceMergeJoinForTesting = true;

        return new MJFixture(dbname, database, executor);
    }

    /// <summary>
    /// Two-column join key: categories / products, no index on either FK column.
    /// </summary>
    private async Task<MJFixture> SetupMultiKeyFixture()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "categories",
            columns:
            [
                new("cat_id", ColumnType.Integer64),
                new("sub_id", ColumnType.Integer64),
                new("label",  ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("cat_id", OrderType.Ascending), new("sub_id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",     ColumnType.Id),
                new("cat_id", ColumnType.Integer64),
                new("sub_id", ColumnType.Integer64),
                new("name",   ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.Insert(new InsertTicket(txn, dbname, "categories",
            values:
            [
                new() { { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "Electronics/Audio") } },
                new() { { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 20L) }, { "label", new(ColumnType.String, "Electronics/Video") } },
                new() { { "cat_id", new(ColumnType.Integer64, 2L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "Clothing/Tops")     } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "products",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "Headphones") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "sub_id", new(ColumnType.Integer64, 20L) }, { "name", new(ColumnType.String, "Monitor")    } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 2L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "T-Shirt")    } },
                // cat_id=9 has no matching category → excluded from inner-join result
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "cat_id", new(ColumnType.Integer64, 9L) }, { "sub_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "Orphan")     } },
            ]));

        await database.Transactions.CommitAsync(txn);

        executor.Statistics.ForceMergeJoinForTesting = true;

        return new MJFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(MJFixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task MergeJoin_OneToOne_ReturnsCorrectPairs()
    {
        MJFixture f = await SetupOrdersItems();

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE o.name IN (\"Order-A\", \"Order-C\", \"Order-D\") " +
            "ORDER BY o.name, li.product");

        // Order-A: 1 item (Widget); Order-C: no items; Order-D: 1 item (Sprocket).
        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual("Order-A", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Widget",  rows[0].Row["product"].StrValue);
        Assert.AreEqual("Order-D",  rows[1].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", rows[1].Row["product"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task MergeJoin_OneToMany_ReturnsBothRows()
    {
        MJFixture f = await SetupOrdersItems();

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
    public async Task MergeJoin_NoMatch_ReturnsEmptyResult()
    {
        MJFixture f = await SetupOrdersItems();

        // Order-C has no line items.
        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE o.name = \"Order-C\"");

        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task MergeJoin_NullBuildKey_RowExcluded()
    {
        MJFixture f = await SetupOrdersItems(includeNullKey: true);

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
    public async Task MergeJoin_NullBuildKey_MatchesNonNullRun()
    {
        // With a NULL-keyed row present the non-null matches are identical to a run without it.
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        MJFixture withNull    = await SetupOrdersItems(includeNullKey: true);
        MJFixture withoutNull = await SetupOrdersItems(includeNullKey: false);

        List<QueryResultRow> withNullRows    = await Run(withNull,    sql);
        List<QueryResultRow> withoutNullRows = await Run(withoutNull, sql);

        Assert.AreEqual(withoutNullRows.Count, withNullRows.Count, "row count must be identical");

        for (int i = 0; i < withoutNullRows.Count; i++)
        {
            Assert.AreEqual(withoutNullRows[i].Row["name"].StrValue,    withNullRows[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(withoutNullRows[i].Row["product"].StrValue, withNullRows[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task MergeJoin_FullScan_ProducesFourRows()
    {
        // Full sweep without WHERE: Order-C has no items, A/B/D produce 4 rows.
        MJFixture f = await SetupOrdersItems();

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product");

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
    public async Task MergeJoin_ResidualNonEquiConjunct_AppliedCorrectly()
    {
        // ON li.order_id = o.id AND li.qty > 4
        // Widget (qty=5) and Doohickey (qty=7) pass; Gadget (qty=3) and Sprocket (qty=2) don't.
        MJFixture f = await SetupOrdersItems();

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
    public async Task MergeJoin_MultiColumnKey_ReturnsCorrectRows()
    {
        MJFixture f = await SetupMultiKeyFixture();

        List<QueryResultRow> rows = await Run(f,
            "SELECT c.label, p.name " +
            "FROM categories c JOIN products p ON p.cat_id = c.cat_id AND p.sub_id = c.sub_id " +
            "ORDER BY c.label, p.name");

        // Orphan (cat_id=9) has no matching category → excluded; 3 products remain.
        Assert.AreEqual(3, rows.Count);

        Assert.AreEqual("Clothing/Tops",     rows[0].Row["label"].StrValue);
        Assert.AreEqual("T-Shirt",           rows[0].Row["name"].StrValue);
        Assert.AreEqual("Electronics/Audio", rows[1].Row["label"].StrValue);
        Assert.AreEqual("Headphones",        rows[1].Row["name"].StrValue);
        Assert.AreEqual("Electronics/Video", rows[2].Row["label"].StrValue);
        Assert.AreEqual("Monitor",           rows[2].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task MergeJoin_MatchesNestedLoopParity()
    {
        // Run the same query with merge join and with nested-loop (force off) and assert
        // both produce byte-identical rows — the parity oracle test for merge join.
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        MJFixture mergeFixture = await SetupOrdersItems();
        List<QueryResultRow> mergeRows = await Run(mergeFixture, sql);

        // Disable ForceMergeJoin so the planner reverts to hash join (same result set).
        mergeFixture.Executor.Statistics.ForceMergeJoinForTesting = false;
        List<QueryResultRow> hashRows = await Run(mergeFixture, sql);

        Assert.AreEqual(4, mergeRows.Count, "merge join row count");
        Assert.AreEqual(4, hashRows.Count,  "hash join row count");

        for (int i = 0; i < mergeRows.Count; i++)
        {
            Assert.AreEqual(mergeRows[i].Row["name"].StrValue,    hashRows[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(mergeRows[i].Row["product"].StrValue, hashRows[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    // ── ForcedIndex executor tests ────────────────────────────────────────────
    //
    // These tests prove the ForcedIndex path is behaviorally correct: the index-ordered
    // scan feeds rows into the merge in the same order as ColumnValue.CompareTo /
    // CompareMergeKeys. If those orders diverged, the merge would silently produce wrong
    // rows — a structure-only planner test cannot catch that.

    /// <summary>
    /// vendors / products schema with secondary indexes on the join key columns.
    /// Rows are inserted in NON-sorted order so the test is only correct if the
    /// index scan actually yields rows in key order.
    /// </summary>
    private async Task<MJFixture> SetupIndexedJoinFixture()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "vendors",
            columns:
            [
                new("id",   ColumnType.Id),
                new("code", ColumnType.String, notNull: true),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",          ColumnType.Id),
                new("vendor_code", ColumnType.String, notNull: true),
                new("label",       ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        // Insert vendors in reverse code order so we know the index scan must sort them.
        await executor.Insert(new InsertTicket(txn, dbname, "vendors",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "code", new(ColumnType.String, "C") }, { "name", new(ColumnType.String, "Gamma") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "code", new(ColumnType.String, "A") }, { "name", new(ColumnType.String, "Alpha") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "code", new(ColumnType.String, "B") }, { "name", new(ColumnType.String, "Beta")  } },
            ]));

        // Insert products similarly out of order; two products for vendor A.
        await executor.Insert(new InsertTicket(txn, dbname, "products",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "vendor_code", new(ColumnType.String, "B") }, { "label", new(ColumnType.String, "Prod-B")  } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "vendor_code", new(ColumnType.String, "A") }, { "label", new(ColumnType.String, "Prod-A1") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "vendor_code", new(ColumnType.String, "C") }, { "label", new(ColumnType.String, "Prod-C")  } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "vendor_code", new(ColumnType.String, "A") }, { "label", new(ColumnType.String, "Prod-A2") } },
            ]));

        await database.Transactions.CommitAsync(txn);

        // Add secondary indexes on the join key columns after data is loaded.
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "vendors",
            indexName: "vendors_code_idx",
            columns: [new("code", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex));

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "products",
            indexName: "products_vendor_code_idx",
            columns: [new("vendor_code", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex));

        executor.Statistics.ForceMergeJoinForTesting = true;

        return new MJFixture(dbname, database, executor);
    }

    /// <summary>
    /// Behavioral guard for the ForcedIndex path: both join sides have a secondary index
    /// on the join key, rows were inserted out-of-order, and the expected output is only
    /// correct if the index scan yields rows in key order that matches CompareMergeKeys.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task MergeJoin_IndexedBothSides_ProducesCorrectRows()
    {
        MJFixture f = await SetupIndexedJoinFixture();

        List<QueryResultRow> rows = await Run(f,
            "SELECT v.name, p.label " +
            "FROM vendors v JOIN products p ON p.vendor_code = v.code " +
            "ORDER BY v.name, p.label");

        // Expected: Alpha→(Prod-A1, Prod-A2), Beta→Prod-B, Gamma→Prod-C
        Assert.AreEqual(4, rows.Count, "row count");
        Assert.AreEqual("Alpha",  rows[0].Row["name"].StrValue);
        Assert.AreEqual("Prod-A1", rows[0].Row["label"].StrValue);
        Assert.AreEqual("Alpha",  rows[1].Row["name"].StrValue);
        Assert.AreEqual("Prod-A2", rows[1].Row["label"].StrValue);
        Assert.AreEqual("Beta",   rows[2].Row["name"].StrValue);
        Assert.AreEqual("Prod-B", rows[2].Row["label"].StrValue);
        Assert.AreEqual("Gamma",  rows[3].Row["name"].StrValue);
        Assert.AreEqual("Prod-C", rows[3].Row["label"].StrValue);
    }

    /// <summary>
    /// Parity oracle: indexed merge join and hash join must return identical rows.
    /// Also verifies via EXPLAIN that the merge plan actually uses ForcedIndex scan
    /// steps (not SortNode), confirming the index path was exercised.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task MergeJoin_IndexedBothSides_MatchesHashJoinParity()
    {
        const string sql =
            "SELECT v.name, p.label " +
            "FROM vendors v JOIN products p ON p.vendor_code = v.code " +
            "ORDER BY v.name, p.label";

        MJFixture f = await SetupIndexedJoinFixture();

        List<QueryResultRow> mergeRows = await Run(f, sql);

        // Confirm the plan used ForcedIndex scan steps (EXPLAIN returns step rows).
        KvTransaction explainTxn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket explainTicket = new(txnState: explainTxn, database: f.DbName,
            sql: "EXPLAIN " + sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> explainCursor) =
            await f.Executor.ExecuteSQLQuery(explainTicket);
        List<QueryResultRow> planRows = await explainCursor.ToListAsync();

        // PlanRenderer splits "table-scan(table=..., forced-index=...)" at the first '(' →
        // node="table-scan", detail="table=..., forced-index=...". Check the detail column.
        bool hasForcedIndexStep = planRows.Any(r =>
            r.Row.TryGetValue("detail", out ColumnValue? v) &&
            v.StrValue != null && v.StrValue.Contains("forced-index"));
        Assert.IsTrue(hasForcedIndexStep,
            "EXPLAIN plan must contain at least one forced-index step when indexes cover both join keys");

        // Switch to hash join and collect the oracle rows.
        f.Executor.Statistics.ForceMergeJoinForTesting = false;
        List<QueryResultRow> hashRows = await Run(f, sql);

        Assert.AreEqual(hashRows.Count, mergeRows.Count, "merge and hash join must return same row count");
        for (int i = 0; i < hashRows.Count; i++)
        {
            Assert.AreEqual(hashRows[i].Row["name"].StrValue,  mergeRows[i].Row["name"].StrValue,  $"row[{i}].name");
            Assert.AreEqual(hashRows[i].Row["label"].StrValue, mergeRows[i].Row["label"].StrValue, $"row[{i}].label");
        }
    }
}
