
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
}
