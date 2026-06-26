
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
/// Parity oracle for the hash-join / merge-join implementation.
///
/// Each test asserts exact result rows against the current nested-loop /
/// index-nested-loop operators. Later hash-join and merge-join phases must produce
/// byte-for-byte identical output on the same data.
///
/// Covered cases:
///   1. One-to-one inner join  (each left row matches exactly one right row).
///   2. One-to-many inner join (one left row matches multiple right rows).
///   3. Unindexed right key    (forces nested-loop, not index-nested-loop).
///   4. Nullable join key      (NULL keys on either side must never match).
/// </summary>
[NonParallelizable]
public sealed class TestJoinParityOracle : SharedNodeBaseTest
{
    // ── Fixture ───────────────────────────────────────────────────────────────

    private sealed record JoinFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// Creates an orders / line_items schema.
    ///
    /// orders   : id (pk), name string NOT NULL
    /// line_items: id (pk), order_id Id (nullable, optionally indexed), product string NOT NULL
    ///
    /// Default data (indexed = false, includeNullKey = false):
    ///   order-A  → item "Widget"   (one-to-one)
    ///   order-B  → items "Gadget", "Doohickey" (one-to-many)
    ///   order-C  → (no items — left-side row with no match, should not appear)
    ///   order-D  → item "Sprocket" (one-to-one)
    ///
    /// With includeNullKey = true an extra item row with order_id = NULL is added;
    /// it must never appear in an inner-join result.
    /// </summary>
    private async Task<JoinFixture> Setup(bool indexOrderId = false, bool includeNullKey = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        ConstraintInfo[] itemConstraints = indexOrderId
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_id_idx", [new("order_id", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "line_items",
            columns:
            [
                new("id",       ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("product",  ColumnType.String, notNull: true),
            ],
            constraints: itemConstraints,
            ifNotExists: false));

        string orderAId = ObjectIdGenerator.Generate().ToString();
        string orderBId = ObjectIdGenerator.Generate().ToString();
        string orderCId = ObjectIdGenerator.Generate().ToString();
        string orderDId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                new() { { "id", new(ColumnType.Id, orderAId) }, { "name", new(ColumnType.String, "Order-A") } },
                new() { { "id", new(ColumnType.Id, orderBId) }, { "name", new(ColumnType.String, "Order-B") } },
                new() { { "id", new(ColumnType.Id, orderCId) }, { "name", new(ColumnType.String, "Order-C") } },
                new() { { "id", new(ColumnType.Id, orderDId) }, { "name", new(ColumnType.String, "Order-D") } },
            ]));

        List<Dictionary<string, ColumnValue>> items =
        [
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderAId) }, { "product", new(ColumnType.String, "Widget") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderBId) }, { "product", new(ColumnType.String, "Gadget") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderBId) }, { "product", new(ColumnType.String, "Doohickey") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderDId) }, { "product", new(ColumnType.String, "Sprocket") } },
        ];

        if (includeNullKey)
            items.Add(new()
            {
                { "id",       new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                { "order_id", new(ColumnType.Null,   0) },
                { "product",  new(ColumnType.String, "GhostPart") },
            });

        await executor.Insert(new InsertTicket(txn, dbname, "line_items", values: items));

        await database.Transactions.CommitAsync(txn);

        return new JoinFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(JoinFixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task OneToOne_InnerJoin_ReturnsMatchingPairs()
    {
        // orders A and D each have exactly one item → two one-to-one pairs.
        // order C has no items → must not appear.
        JoinFixture f = await Setup();

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE o.name IN (\"Order-A\", \"Order-C\", \"Order-D\") " +
            "ORDER BY o.name, li.product");

        Assert.AreEqual(2, rows.Count);

        Assert.AreEqual("Order-A", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Widget",  rows[0].Row["product"].StrValue);

        Assert.AreEqual("Order-D",  rows[1].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", rows[1].Row["product"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task OneToMany_InnerJoin_ReturnsAllMatchingRows()
    {
        // Order-B has two items; both must appear in the result.
        JoinFixture f = await Setup();

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
    public async Task FullJoin_InnerJoin_ReturnsAllFourMatchingRows()
    {
        // Full sweep: order-C has no items; orders A, B, D produce 4 rows total.
        JoinFixture f = await Setup();

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
    public async Task UnindexedRightKey_NestedLoop_MatchesIndexedResult()
    {
        // When line_items.order_id has no index the planner must use nested-loop.
        // This test proves the nested-loop result matches the index-nested-loop result.
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        JoinFixture nestedLoop = await Setup(indexOrderId: false);
        List<QueryResultRow> nlRows = await Run(nestedLoop, sql);

        JoinFixture indexed = await Setup(indexOrderId: true);
        List<QueryResultRow> ixRows = await Run(indexed, sql);

        Assert.AreEqual(4, nlRows.Count, "nested-loop row count");
        Assert.AreEqual(4, ixRows.Count, "index-nested-loop row count");

        for (int i = 0; i < nlRows.Count; i++)
        {
            Assert.AreEqual(nlRows[i].Row["name"].StrValue,    ixRows[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(nlRows[i].Row["product"].StrValue, ixRows[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task NullableJoinKey_NullKeysNeverMatch()
    {
        // The extra "GhostPart" row has order_id = NULL.
        // NULL = NULL is unknown in SQL inner-join semantics → it must not appear.
        //
        // WHY the current nested-loop excludes it: the ON predicate `li.order_id = o.id`
        // is evaluated as `ExprEquals` → `ColumnValue.CompareTo(...) == 0`, and CompareTo
        // returns NON-zero for any NULL operand (other-is-NULL → 1, self-is-NULL → -1), so
        // the equality is false and the NULL-keyed row never matches. This test is the guard
        // for that behavior: if `ColumnValue.CompareTo` is ever "normalized" so that
        // NULL.CompareTo(NULL) returns 0, inner joins on a nullable key would silently start
        // matching NULL = NULL — this assertion must catch it. The hash/merge operators must
        // reach the same outcome by excluding NULL keys from build/probe (MATCH SIMPLE).
        JoinFixture f = await Setup(includeNullKey: true);

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product");

        Assert.AreEqual(4, rows.Count, "NULL-keyed row must not appear in result");

        foreach (QueryResultRow row in rows)
            Assert.AreNotEqual("GhostPart", row.Row["product"].StrValue, "NULL-keyed row leaked into result");
    }

    [Test]
    [NonParallelizable]
    public async Task NullableJoinKey_NonNullRowsUnaffected()
    {
        // Even with a NULL-keyed row present the non-null matches are intact and identical
        // to the run without the null row.
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product";

        JoinFixture withNull    = await Setup(includeNullKey: true);
        JoinFixture withoutNull = await Setup(includeNullKey: false);

        List<QueryResultRow> withNullRows    = await Run(withNull,    sql);
        List<QueryResultRow> withoutNullRows = await Run(withoutNull, sql);

        Assert.AreEqual(withoutNullRows.Count, withNullRows.Count, "row count must be identical");

        for (int i = 0; i < withoutNullRows.Count; i++)
        {
            Assert.AreEqual(withoutNullRows[i].Row["name"].StrValue,    withNullRows[i].Row["name"].StrValue,    $"row[{i}].name");
            Assert.AreEqual(withoutNullRows[i].Row["product"].StrValue, withNullRows[i].Row["product"].StrValue, $"row[{i}].product");
        }
    }

    // ── Three-way parity sweep (NLJ vs Hash vs Merge) ─────────────────────────
    //
    // Uses force flags on StatisticsManager to override algorithm selection so each
    // scenario can be exercised under all three algorithms regardless of index
    // availability or row-count thresholds.

    private sealed record ParityFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// Fixture with orders (score column), line_items (null-key row), shipments, and a
    /// multi-key table pair. No secondary indexes — algorithms are forced via flags.
    /// </summary>
    private async Task<ParityFixture> SetupThreeWayFixture()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",    ColumnType.Id),
                new("name",  ColumnType.String,    notNull: true),
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

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "shipments",
            columns:
            [
                new("id",       ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("carrier",  ColumnType.String, notNull: true),
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

        // A→Widget, B→Gadget, B→Doohickey, D→Sprocket, NULL→GhostPart
        await executor.Insert(new InsertTicket(txn, dbname, "line_items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, oaId) }, { "product", new(ColumnType.String, "Widget")    }, { "qty", new(ColumnType.Integer64, 5L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Gadget")    }, { "qty", new(ColumnType.Integer64, 3L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Doohickey") }, { "qty", new(ColumnType.Integer64, 7L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, odId) }, { "product", new(ColumnType.String, "Sprocket")  }, { "qty", new(ColumnType.Integer64, 2L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Null, 0)  }, { "product", new(ColumnType.String, "GhostPart") }, { "qty", new(ColumnType.Integer64, 99L) } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "shipments",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, oaId) }, { "carrier", new(ColumnType.String, "FedEx") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "carrier", new(ColumnType.String, "UPS")   } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, ocId) }, { "carrier", new(ColumnType.String, "DHL")   } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, odId) }, { "carrier", new(ColumnType.String, "USPS")  } },
            ]));

        await database.Transactions.CommitAsync(txn);
        return new ParityFixture(dbname, database, executor);
    }

    private async Task<ParityFixture> SetupMultiKeyFixture()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "categories",
            columns:
            [
                new("id",        ColumnType.Integer64),
                new("region_id", ColumnType.Integer64),
                new("name",      ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "items",
            columns:
            [
                new("id",        ColumnType.Integer64),
                new("cat_id",    ColumnType.Integer64),
                new("region_id", ColumnType.Integer64),
                new("label",     ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.Insert(new InsertTicket(txn, dbname, "categories",
            values:
            [
                new() { { "id", new(ColumnType.Integer64, 1L) }, { "region_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "Alpha") } },
                new() { { "id", new(ColumnType.Integer64, 2L) }, { "region_id", new(ColumnType.Integer64, 20L) }, { "name", new(ColumnType.String, "Beta")  } },
                new() { { "id", new(ColumnType.Integer64, 3L) }, { "region_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "Gamma") } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "items",
            values:
            [
                new() { { "id", new(ColumnType.Integer64, 1L) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "region_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "A1") } },
                new() { { "id", new(ColumnType.Integer64, 2L) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "region_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "A2") } },
                new() { { "id", new(ColumnType.Integer64, 3L) }, { "cat_id", new(ColumnType.Integer64, 2L) }, { "region_id", new(ColumnType.Integer64, 20L) }, { "label", new(ColumnType.String, "B1") } },
                new() { { "id", new(ColumnType.Integer64, 4L) }, { "cat_id", new(ColumnType.Integer64, 3L) }, { "region_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "G1") } },
                // region mismatch → no match (cat_id matches but region_id does not)
                new() { { "id", new(ColumnType.Integer64, 5L) }, { "cat_id", new(ColumnType.Integer64, 1L) }, { "region_id", new(ColumnType.Integer64, 99L) }, { "label", new(ColumnType.String, "X1") } },
                // cat_id with no matching category
                new() { { "id", new(ColumnType.Integer64, 6L) }, { "cat_id", new(ColumnType.Integer64, 9L) }, { "region_id", new(ColumnType.Integer64, 10L) }, { "label", new(ColumnType.String, "Z1") } },
            ]));

        await database.Transactions.CommitAsync(txn);
        return new ParityFixture(dbname, database, executor);
    }

    private enum JoinAlgorithm { NestedLoop, Hash, Merge }

    private static async Task<List<string>> RunSorted(ParityFixture f, string sql, JoinAlgorithm algo)
    {
        f.Executor.Statistics.ForceNestedLoopForTesting = false;
        f.Executor.Statistics.ForceHashJoinForTesting   = false;
        f.Executor.Statistics.ForceMergeJoinForTesting  = false;

        switch (algo)
        {
            case JoinAlgorithm.NestedLoop: f.Executor.Statistics.ForceNestedLoopForTesting = true; break;
            case JoinAlgorithm.Hash:       f.Executor.Statistics.ForceHashJoinForTesting   = true; break;
            case JoinAlgorithm.Merge:      f.Executor.Statistics.ForceMergeJoinForTesting  = true; break;
        }

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        return rows.Select(r =>
                string.Join("|", r.Row.OrderBy(kv => kv.Key).Select(kv => kv.Value.ToString() ?? "null")))
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
    }

    private static void AssertParityAll(List<string> nlj, List<string> hash, List<string> merge, string scenario)
    {
        Assert.AreEqual(nlj.Count, hash.Count,  $"{scenario}: NLJ={nlj.Count} vs Hash={hash.Count}");
        Assert.AreEqual(nlj.Count, merge.Count, $"{scenario}: NLJ={nlj.Count} vs Merge={merge.Count}");
        for (int i = 0; i < nlj.Count; i++)
        {
            Assert.AreEqual(nlj[i], hash[i],  $"{scenario} row[{i}]: NLJ vs Hash");
            Assert.AreEqual(nlj[i], merge[i], $"{scenario} row[{i}]: NLJ vs Merge");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_OneToOne_AllAlgorithmsAgree()
    {
        ParityFixture f = await SetupThreeWayFixture();
        const string sql = "SELECT o.name, s.carrier FROM orders o JOIN shipments s ON s.order_id = o.id";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        Assert.AreEqual(4, nlj.Count, "expected 4 matched pairs (one per order)");
        AssertParityAll(nlj, hash, merge, "one-to-one");
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_OneToMany_AllAlgorithmsAgree()
    {
        ParityFixture f = await SetupThreeWayFixture();
        const string sql =
            "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        // A→Widget, B→Gadget, B→Doohickey, D→Sprocket (C has no items; GhostPart has null key)
        Assert.AreEqual(4, nlj.Count, "expected 4 matched pairs");
        AssertParityAll(nlj, hash, merge, "one-to-many");
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_NullJoinKey_ExcludedByAllAlgorithms()
    {
        ParityFixture f = await SetupThreeWayFixture();
        const string sql =
            "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        Assert.IsFalse(nlj.Any(r   => r.Contains("GhostPart")), "NLJ must exclude null-key row");
        Assert.IsFalse(hash.Any(r  => r.Contains("GhostPart")), "Hash must exclude null-key row");
        Assert.IsFalse(merge.Any(r => r.Contains("GhostPart")), "Merge must exclude null-key row");
        AssertParityAll(nlj, hash, merge, "null-key exclusion");
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_ResidualNonEquiConjunct_AllAlgorithmsAgree()
    {
        ParityFixture f = await SetupThreeWayFixture();
        // score > 15 → only B(20), C(30), D(40). C has no line_items → B×2 + D×1 = 3 rows.
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id AND o.score > 15";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        Assert.AreEqual(3, nlj.Count, "NLJ: B×2 + D×1 = 3 rows after residual filter");
        AssertParityAll(nlj, hash, merge, "residual non-equi conjunct");
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_MultiColumnEquiJoin_AllAlgorithmsAgree()
    {
        ParityFixture f = await SetupMultiKeyFixture();
        const string sql =
            "SELECT c.name, i.label " +
            "FROM categories c JOIN items i ON i.cat_id = c.id AND i.region_id = c.region_id";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        // (1,10)→A1, (1,10)→A2, (2,20)→B1, (3,10)→G1; X1 and Z1 have no match
        Assert.AreEqual(4, nlj.Count, "NLJ: 4 matching (cat,region) pairs");
        AssertParityAll(nlj, hash, merge, "multi-column equi-join");
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_ThreeTableChain_AllAlgorithmsAgree()
    {
        ParityFixture f = await SetupThreeWayFixture();
        const string sql =
            "SELECT o.name, li.product, s.carrier " +
            "FROM orders o " +
            "JOIN line_items li ON li.order_id = o.id " +
            "JOIN shipments s ON s.order_id = o.id";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        // A→Widget×FedEx, B→Gadget×UPS, B→Doohickey×UPS, D→Sprocket×USPS
        Assert.AreEqual(4, nlj.Count, "NLJ: 4 matched triples");
        AssertParityAll(nlj, hash, merge, "three-table chain");
    }

    [Test]
    [NonParallelizable]
    public async Task Parity_NoMatchingRows_AllAlgorithmsReturnEmpty()
    {
        ParityFixture f = await SetupThreeWayFixture();
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id AND o.score < 0";

        List<string> nlj   = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        List<string> hash  = await RunSorted(f, sql, JoinAlgorithm.Hash);
        List<string> merge = await RunSorted(f, sql, JoinAlgorithm.Merge);

        Assert.AreEqual(0, nlj.Count,   "NLJ: score < 0 matches nothing");
        Assert.AreEqual(0, hash.Count,  "Hash: score < 0 matches nothing");
        Assert.AreEqual(0, merge.Count, "Merge: score < 0 matches nothing");
    }

    // ── Benchmarks (gated — run on demand only) ───────────────────────────────

    [Test]
    [NonParallelizable]
    [Explicit("Benchmark: hash join vs nested-loop on 1 000-row unindexed tables. Not suitable for CI.")]
    public async Task Benchmark_HashVsNlj_LargeUnindexed()
    {
        const int rows = 1_000;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "left_tbl",
            columns: [new("id", ColumnType.Integer64), new("val", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "right_tbl",
            columns:
            [
                new("id",    ColumnType.Integer64),
                new("fk",    ColumnType.Integer64),
                new("label", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> leftBatch = [];
        List<Dictionary<string, ColumnValue>> rightBatch = [];
        for (int i = 0; i < rows; i++)
        {
            leftBatch.Add(new() { { "id", new(ColumnType.Integer64, (long)i) }, { "val", new(ColumnType.String, $"L{i}") } });
            rightBatch.Add(new() { { "id", new(ColumnType.Integer64, (long)i) }, { "fk", new(ColumnType.Integer64, (long)(i % rows)) }, { "label", new(ColumnType.String, $"R{i}") } });
        }
        await executor.Insert(new InsertTicket(txn, dbname, "left_tbl",  values: leftBatch));
        await executor.Insert(new InsertTicket(txn, dbname, "right_tbl", values: rightBatch));
        await database.Transactions.CommitAsync(txn);

        const string sql = "SELECT l.val, r.label FROM left_tbl l JOIN right_tbl r ON r.fk = l.id";
        ParityFixture f  = new(dbname, database, executor);

        var sw = Stopwatch.StartNew();
        List<string> hashRows = await RunSorted(f, sql, JoinAlgorithm.Hash);
        long hashMs = sw.ElapsedMilliseconds;

        sw.Restart();
        List<string> nljRows = await RunSorted(f, sql, JoinAlgorithm.NestedLoop);
        long nljMs = sw.ElapsedMilliseconds;

        Console.WriteLine($"[Benchmark] Hash={hashMs}ms  NLJ={nljMs}ms  rows={hashRows.Count}");

        Assert.AreEqual(hashRows.Count, nljRows.Count, "hash and NLJ must return the same row count");
        Assert.Less(hashMs, nljMs, "hash join must be faster than nested-loop on large unindexed tables");
    }

    [Test]
    [NonParallelizable]
    [Explicit("Benchmark: merge join vs hash join on pre-sorted (indexed) inputs. Not suitable for CI.")]
    public async Task Benchmark_MergeVsHash_PreSortedInputs()
    {
        const int rows = 1_000;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "left_tbl",
            columns: [new("id", ColumnType.Integer64), new("val", ColumnType.String, notNull: true)],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",   [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "l_idx", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "right_tbl",
            columns:
            [
                new("id",    ColumnType.Integer64),
                new("fk",    ColumnType.Integer64),
                new("label", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",   [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "r_idx", [new("fk", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> leftBatch = [];
        List<Dictionary<string, ColumnValue>> rightBatch = [];
        for (int i = 0; i < rows; i++)
        {
            leftBatch.Add(new() { { "id", new(ColumnType.Integer64, (long)i) }, { "val", new(ColumnType.String, $"L{i}") } });
            rightBatch.Add(new() { { "id", new(ColumnType.Integer64, (long)i) }, { "fk", new(ColumnType.Integer64, (long)i) }, { "label", new(ColumnType.String, $"R{i}") } });
        }
        await executor.Insert(new InsertTicket(txn, dbname, "left_tbl",  values: leftBatch));
        await executor.Insert(new InsertTicket(txn, dbname, "right_tbl", values: rightBatch));
        await database.Transactions.CommitAsync(txn);

        // Seed high row counts so merge is cost-selected over INLJ (>MergeJoinPreferenceThreshold=100).
        TableDescriptor leftTable  = await database.TableDescriptors["left_tbl"];
        TableDescriptor rightTable = await database.TableDescriptors["right_tbl"];
        executor.Statistics.SeedRowCountForTesting(database, leftTable,  rows);
        executor.Statistics.SeedRowCountForTesting(database, rightTable, rows);

        const string sql = "SELECT l.val, r.label FROM left_tbl l JOIN right_tbl r ON r.fk = l.id";
        ParityFixture f  = new(dbname, database, executor);

        var sw = Stopwatch.StartNew();
        List<string> mergeRows = await RunSorted(f, sql, JoinAlgorithm.Merge);
        long mergeMs = sw.ElapsedMilliseconds;

        sw.Restart();
        List<string> hashRows = await RunSorted(f, sql, JoinAlgorithm.Hash);
        long hashMs = sw.ElapsedMilliseconds;

        Console.WriteLine($"[Benchmark] Merge={mergeMs}ms  Hash={hashMs}ms  rows={mergeRows.Count}");

        Assert.AreEqual(mergeRows.Count, hashRows.Count, "merge and hash must return the same row count");
    }
}
