/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A join predicate written with bare column names plans as the same predicate with qualified
/// names. Every column name of the test tables carries the prefix of its table, as in the TPC-H
/// schema, so a statement can name each column with no qualifier.
///
/// <para>Each plan test compares the EXPLAIN rows of the bare statement with the rows of its
/// qualified twin: the operators, the keys, the filters, the estimates and the shape id. A row comparison alone
/// proves nothing here, because a nested loop returns the same rows as a hash join. The row counts
/// come from the data model below, not from a second run of the engine.</para>
///
/// <para>Data: 30 customers; 120 orders, 4 for each customer; 400 line items; 5 nations; 3 regions.
/// <c>customer</c> and <c>orders</c> both have a column <c>note</c>, and <c>customer</c> and
/// <c>region</c> both have a column <c>region_id</c>, for the names that two sources have.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestJoinBareColumnNames : SharedNodeBaseTest
{
    private const int Customers = 30;

    private const int Orders = 120;

    private const int LineItems = 400;

    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    // ─── The data model, for expected rows ───────────────────────────────────

    private static bool IsBuilding(int customer) => customer % 3 == 0;

    private static int CustomerOfOrder(int order) => order % Customers + 1;

    private static int OrderOfLineItem(int item) => item % Orders + 1;

    private static int ShipDate(int item) => item % 200;

    // ─── FROM list with the join conjuncts in WHERE ──────────────────────────

    private const string BareCommaJoin =
        "SELECT l_orderkey, sum(l_price) AS revenue, o_orderdate FROM customer, orders, lineitem " +
        "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey " +
        "AND o_orderdate < 100 AND l_shipdate > 50 GROUP BY l_orderkey, o_orderdate ORDER BY revenue DESC, o_orderdate";

    private const string QualifiedCommaJoin =
        "SELECT l_orderkey, sum(l_price) AS revenue, o_orderdate FROM customer, orders, lineitem " +
        "WHERE customer.c_mktsegment = 'BUILDING' AND customer.c_custkey = orders.o_custkey AND lineitem.l_orderkey = orders.o_orderkey " +
        "AND orders.o_orderdate < 100 AND lineitem.l_shipdate > 50 GROUP BY l_orderkey, o_orderdate ORDER BY revenue DESC, o_orderdate";

    [Test]
    public async Task CommaJoin_BareNames_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        List<string> plan = await Plan(db, BareCommaJoin);

        AssertNoNestedLoop(plan);
        CollectionAssert.AreEqual(await Plan(db, QualifiedCommaJoin), plan);
        StringAssert.Contains("join-order=cost-based", await PlanInfo(db, BareCommaJoin));

        // One output row for each order of a BUILDING customer before day 100 that has a line item
        // shipped after day 50.
        int expected = Enumerable.Range(1, Orders).Count(order =>
            order < 100
            && IsBuilding(CustomerOfOrder(order))
            && Enumerable.Range(1, LineItems).Any(item => OrderOfLineItem(item) == order && ShipDate(item) > 50));

        List<QueryResultRow> rows = await Run(db, BareCommaJoin);

        Assert.AreEqual(expected, rows.Count);
        CollectionAssert.AreEqual(Render(await Run(db, QualifiedCommaJoin)), Render(rows));
    }

    [Test]
    public async Task CommaJoin_OneBareJoinConjunct_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        // Query creation hoists a qualified join conjunct and cannot hoist a bare one. Both must
        // reach their joins in WHERE order, as in the statement with qualified names only.
        const string mixed =
            "SELECT c_custkey, l_id FROM customer, orders, lineitem " +
            "WHERE l_orderkey = o_orderkey AND customer.c_custkey = orders.o_custkey AND c_custkey < 4";

        const string qualified =
            "SELECT c_custkey, l_id FROM customer, orders, lineitem " +
            "WHERE lineitem.l_orderkey = orders.o_orderkey AND customer.c_custkey = orders.o_custkey AND customer.c_custkey < 4";

        List<string> plan = await Plan(db, mixed);

        AssertNoNestedLoop(plan);
        CollectionAssert.AreEqual(await Plan(db, qualified), plan);

        int expected = Enumerable.Range(1, LineItems).Count(item => CustomerOfOrder(OrderOfLineItem(item)) < 4);
        Assert.AreEqual(expected, (await Run(db, mixed)).Count);
    }

    [Test]
    public async Task CommaJoin_SelfJoinAliasesWithBareNames_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        // Only the table that the statement joins two times needs an alias.
        const string bare =
            "SELECT n1.n_name AS supp, n2.n_name AS cust, l_id FROM lineitem, orders, customer, nation n1, nation n2 " +
            "WHERE o_orderkey = l_orderkey AND c_custkey = o_custkey AND c_nationkey = n1.n_nationkey " +
            "AND l_shipdate = n2.n_nationkey AND ((n1.n_name = 'C' AND n2.n_name = 'A') OR (n1.n_name = 'D' AND n2.n_name = 'B'))";

        const string qualified =
            "SELECT n1.n_name AS supp, n2.n_name AS cust, l_id FROM lineitem, orders, customer, nation n1, nation n2 " +
            "WHERE orders.o_orderkey = lineitem.l_orderkey AND customer.c_custkey = orders.o_custkey AND customer.c_nationkey = n1.n_nationkey " +
            "AND lineitem.l_shipdate = n2.n_nationkey AND ((n1.n_name = 'C' AND n2.n_name = 'A') OR (n1.n_name = 'D' AND n2.n_name = 'B'))";

        List<string> plan = await Plan(db, bare);

        CollectionAssert.AreEqual(await Plan(db, qualified), plan);
        StringAssert.Contains("join-order=cost-based", await PlanInfo(db, bare));

        // The OR conjunct joins the two nations with no key. Every join that has an equality uses it.
        Assert.AreEqual(1, plan.Count(row => row.StartsWith("nested-loop-join", StringComparison.Ordinal)), string.Join("\n", plan));

        // The nations 0 to 3 are 'A' to 'D'.
        int expected = Enumerable.Range(1, LineItems).Count(item =>
        {
            int customerNation = CustomerOfOrder(OrderOfLineItem(item)) % 5;
            int shipNation = ShipDate(item);
            return (customerNation == 2 && shipNation == 0) || (customerNation == 3 && shipNation == 1);
        });

        Assert.Greater(expected, 0);
        Assert.AreEqual(expected, (await Run(db, bare)).Count);
    }

    // ─── JOIN ... ON ─────────────────────────────────────────────────────────

    [Test]
    public async Task ExplicitJoin_BareNamesInOn_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        const string bare =
            "SELECT l_id, o_orderdate FROM customer JOIN orders ON c_custkey = o_custkey " +
            "JOIN lineitem ON l_orderkey = o_orderkey WHERE c_mktsegment = 'BUILDING'";

        const string qualified =
            "SELECT l_id, o_orderdate FROM customer JOIN orders ON customer.c_custkey = orders.o_custkey " +
            "JOIN lineitem ON lineitem.l_orderkey = orders.o_orderkey WHERE customer.c_mktsegment = 'BUILDING'";

        List<string> plan = await Plan(db, bare);

        AssertNoNestedLoop(plan);
        CollectionAssert.AreEqual(await Plan(db, qualified), plan);
        StringAssert.Contains("join-order=cost-based", await PlanInfo(db, bare));

        int expected = Enumerable.Range(1, LineItems).Count(item => IsBuilding(CustomerOfOrder(OrderOfLineItem(item))));
        Assert.AreEqual(expected, (await Run(db, bare)).Count);
    }

    [Test]
    public async Task LeftJoin_BareNamesInOn_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        // The second conjunct reads the right side only, so it stays in the ON: a customer with no
        // order on that day keeps its row, with a NULL order.
        const string bare =
            "SELECT c_custkey, o_orderkey FROM customer LEFT JOIN orders ON c_custkey = o_custkey AND o_orderdate = 7";

        const string qualified =
            "SELECT c_custkey, o_orderkey FROM customer LEFT JOIN orders ON customer.c_custkey = orders.o_custkey AND orders.o_orderdate = 7";

        List<string> plan = await Plan(db, bare);

        AssertNoNestedLoop(plan);
        CollectionAssert.AreEqual(await Plan(db, qualified), plan);
        Assert.IsTrue(plan.Any(row => row.Contains("kind=left-outer", StringComparison.Ordinal)), string.Join("\n", plan));

        List<QueryResultRow> rows = await Run(db, bare);

        Assert.AreEqual(Customers, rows.Count);
        Assert.AreEqual(1, rows.Count(row => row.Row["o_orderkey"].Type != ColumnType.Null));
        Assert.AreEqual(CustomerOfOrder(7), rows.Single(row => row.Row["o_orderkey"].Type != ColumnType.Null).Row["c_custkey"].LongValue);
    }

    [Test]
    public async Task ExplicitJoin_ExpressionKeyWithBareNames_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        const string bare = "SELECT c_custkey, o_orderkey FROM customer JOIN orders ON o_custkey + 0 = c_custkey";
        const string qualified = "SELECT c_custkey, o_orderkey FROM customer JOIN orders ON orders.o_custkey + 0 = customer.c_custkey";

        List<string> plan = await Plan(db, bare);

        AssertNoNestedLoop(plan);
        CollectionAssert.AreEqual(await Plan(db, qualified), plan);
        Assert.AreEqual(Orders, (await Run(db, bare)).Count);
    }

    [Test]
    public async Task ExplicitJoin_RangeOnBareNames_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        const string bare =
            "SELECT o_orderkey, l_id FROM orders JOIN lineitem ON l_shipdate BETWEEN o_orderdate AND o_orderdate + 1 WHERE o_orderkey < 4";

        const string qualified =
            "SELECT o_orderkey, l_id FROM orders JOIN lineitem ON lineitem.l_shipdate BETWEEN orders.o_orderdate AND orders.o_orderdate + 1 WHERE orders.o_orderkey < 4";

        List<string> plan = await Plan(db, bare);

        CollectionAssert.AreEqual(await Plan(db, qualified), plan);
        Assert.IsTrue(plan.Any(row => row.Contains("range-seek=ix_lineitem_shipdate", StringComparison.Ordinal)), string.Join("\n", plan));

        int expected = Enumerable.Range(1, 3).Sum(order =>
            Enumerable.Range(1, LineItems).Count(item => ShipDate(item) >= order && ShipDate(item) <= order + 1));

        Assert.AreEqual(expected, (await Run(db, bare)).Count);
    }

    [Test]
    public async Task On_BareNameThatALaterTableAlsoHas_ReadsTheSourceOfItsJoin()
    {
        Db db = await Setup();

        // The first ON reads customer and orders only, so region_id there is the column of
        // customer, although region has a column of the same name.
        const string sql =
            "SELECT c_custkey, r_name FROM customer JOIN orders ON c_custkey = o_custkey AND region_id = 1 " +
            "JOIN region ON customer.region_id = region.region_id";

        List<QueryResultRow> rows = await Run(db, sql);

        Assert.AreEqual(Enumerable.Range(1, Orders).Count(order => CustomerOfOrder(order) % 3 == 1), rows.Count);
        Assert.IsTrue(rows.All(row => row.Row["r_name"].StrValue == "region 1"));
        AssertNoNestedLoop(await Plan(db, sql));
    }

    // ─── Other sources and other clauses ─────────────────────────────────────

    [Test]
    public async Task DerivedTable_BareNames_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        const string bare =
            "SELECT c_custkey, n FROM customer, (SELECT o_custkey AS ck, count(*) AS n FROM orders GROUP BY o_custkey) t WHERE c_custkey = ck";

        const string qualified =
            "SELECT c_custkey, n FROM customer, (SELECT o_custkey AS ck, count(*) AS n FROM orders GROUP BY o_custkey) t WHERE customer.c_custkey = t.ck";

        List<string> plan = await Plan(db, bare);

        AssertNoNestedLoop(plan);
        CollectionAssert.AreEqual(await Plan(db, qualified), plan);

        List<QueryResultRow> rows = await Run(db, bare);

        Assert.AreEqual(Customers, rows.Count);
        Assert.IsTrue(rows.All(row => row.Row["n"].LongValue == Orders / Customers));
    }

    [Test]
    public async Task LateralList_BareNames_PlansAsTheQualifiedForm()
    {
        Db db = await Setup();

        // A LATERAL item keeps the join order as written, so only the hoist of the join conjunct
        // into the join of orders keeps this statement from a cross product.
        const string bare =
            "SELECT c_custkey, o_orderkey, x.next FROM customer, orders, LATERAL (SELECT o_orderkey + 1 AS next) x WHERE c_custkey = o_custkey";

        const string qualified =
            "SELECT c_custkey, o_orderkey, x.next FROM customer, orders, LATERAL (SELECT o_orderkey + 1 AS next) x WHERE customer.c_custkey = orders.o_custkey";

        List<string> plan = await Plan(db, bare);

        CollectionAssert.AreEqual(await Plan(db, qualified), plan);
        Assert.IsTrue(plan.Any(row => row.StartsWith("hash-join", StringComparison.Ordinal)
            || row.StartsWith("index-nested-loop-join", StringComparison.Ordinal)), string.Join("\n", plan));

        List<QueryResultRow> rows = await Run(db, bare);

        Assert.AreEqual(Orders, rows.Count);
        Assert.IsTrue(rows.All(row => row.Row["next"].LongValue == row.Row["o_orderkey"].LongValue + 1));
    }

    [TestCase("EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey AND l_shipdate > 150)")]
    [TestCase("o_orderkey IN (SELECT l_orderkey FROM lineitem WHERE l_shipdate > 150)")]
    [TestCase("o_orderdate > (SELECT avg(l_shipdate) FROM lineitem WHERE l_orderkey = o_orderkey)")]
    public async Task Subquery_BesideBareJoinConjunct_KeepsItsRows(string condition)
    {
        Db db = await Setup();

        string bare = "SELECT c_custkey, o_orderkey FROM customer, orders WHERE c_custkey = o_custkey AND " + condition;
        string qualified = "SELECT c_custkey, o_orderkey FROM customer, orders WHERE customer.c_custkey = orders.o_custkey AND " + condition;

        AssertNoNestedLoop(await Plan(db, bare));

        List<string> rows = Render(await Run(db, bare));

        Assert.Greater(rows.Count, 0);
        Assert.Less(rows.Count, Orders);
        CollectionAssert.AreEqual(Render(await Run(db, qualified)), rows);
    }

    [Test]
    public async Task PlanCache_BareStatement_ReplaysTheOrderOfItsQualifiedTwin()
    {
        Db db = await Setup(Options with { PlanCacheEnabled = true });
        PlanCache cache = db.Executor.PlanCache;

        List<string> qualified = Render(await Run(db, QualifiedCommaJoin));

        long hits = cache.Hits;
        long failures = cache.ReplayFailures;

        // The two statements bind to one query, so they have one shape and one cache entry.
        List<string> bare = Render(await Run(db, BareCommaJoin));

        Assert.AreEqual(hits + 1, cache.Hits);
        Assert.AreEqual(failures, cache.ReplayFailures);
        CollectionAssert.AreEqual(qualified, bare);
    }

    // ─── Names that the binder refuses ───────────────────────────────────────

    [TestCase("SELECT c_custkey FROM customer, orders WHERE c_custkey = o_custkey AND note = 'x'", CamusDBErrorCodes.InvalidInput, "Ambiguous column: note")]
    [TestCase("SELECT c_custkey FROM customer JOIN orders ON c_custkey = o_custkey AND note = 'x'", CamusDBErrorCodes.InvalidInput, "Ambiguous column: note")]
    [TestCase("SELECT c_custkey FROM customer, orders WHERE c_custkey = o_custkey AND missing = 1", CamusDBErrorCodes.UnknownColumn, "Unknown column: missing")]
    [TestCase("SELECT c_custkey FROM customer JOIN orders ON c_custkey = missing", CamusDBErrorCodes.UnknownColumn, "Unknown column: missing")]
    [TestCase("SELECT c_custkey FROM customer JOIN orders ON c_custkey = n_nationkey JOIN nation ON c_nationkey = n_nationkey", CamusDBErrorCodes.UnknownColumn, "Unknown column: n_nationkey")]
    public async Task BareName_WithNoOwnerOrTwoOwners_FailsAsBefore(string sql, string code, string message)
    {
        Db db = await Setup();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await Run(db, sql));

        Assert.AreEqual(code, error!.Code);
        StringAssert.Contains(message, error.Message);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static void AssertNoNestedLoop(List<string> plan)
    {
        string rendered = string.Join("\n", plan);

        Assert.IsFalse(plan.Any(row => row.StartsWith("nested-loop-join", StringComparison.Ordinal)), rendered);
        Assert.IsTrue(plan.Any(row => row.StartsWith("hash-join", StringComparison.Ordinal)
            || row.StartsWith("index-nested-loop-join", StringComparison.Ordinal)
            || row.StartsWith("merge-join", StringComparison.Ordinal)), rendered);
    }

    /// <summary>
    /// The rows of plain EXPLAIN as "node | detail | estimated rows | estimated cost". The plan-info
    /// row holds the shape id of the bound query, and the estimates show that a bare name reaches
    /// the row estimate of its join as a key.
    /// </summary>
    private static async Task<List<string>> Plan(Db db, string sql)
    {
        List<QueryResultRow> rows = await Run(db, "EXPLAIN " + sql);

        return rows.Select(row => string.Join(" | ",
            row.Row["node"].StrValue,
            row.Row["detail"].StrValue,
            row.Row["estimated_rows"],
            row.Row["estimated_cost"])).ToList();
    }

    /// <summary>The detail of the plan-info row of EXPLAIN (ANALYZE), which names how the join order was chosen.</summary>
    private static async Task<string> PlanInfo(Db db, string sql)
    {
        List<QueryResultRow> rows = await Run(db, "EXPLAIN (ANALYZE) " + sql);
        return rows.Single(row => row.Row["node"].StrValue == "plan-info").Row["detail"].StrValue!;
    }

    private static List<string> Render(List<QueryResultRow> rows) =>
        rows.Select(row => string.Join(", ", row.Row.Select(cell => cell.Key + "=" + cell.Value))).ToList();

    private static async Task<List<QueryResultRow>> Run(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            await db.Database.Transactions.CommitAsync(txn);
            return rows;
        }
        catch
        {
            await db.Database.Transactions.RollbackAsync(txn);
            throw;
        }
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private async Task<Db> Setup(CamusDBOptions? options = null)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE customer (c_custkey int64 primary key, c_mktsegment string, c_nationkey int64, region_id int64, note string)");
        await Ddl(db, "CREATE TABLE orders (o_orderkey int64 primary key, o_custkey int64, o_orderdate int64, note string)");
        await Ddl(db, "CREATE TABLE lineitem (l_id int64 primary key, l_orderkey int64, l_shipdate int64, l_price int64)");
        await Ddl(db, "CREATE TABLE nation (n_nationkey int64 primary key, n_name string)");
        await Ddl(db, "CREATE TABLE region (region_id int64 primary key, r_name string)");
        await Ddl(db, "CREATE INDEX ix_lineitem_shipdate ON lineitem (l_shipdate)");

        await Exec(db, "INSERT INTO customer (c_custkey, c_mktsegment, c_nationkey, region_id, note) VALUES " + Values(Customers,
            i => $"({i}, '{(IsBuilding(i) ? "BUILDING" : "OTHER")}', {i % 5}, {i % 3}, 'c')"));

        await Exec(db, "INSERT INTO orders (o_orderkey, o_custkey, o_orderdate, note) VALUES " + Values(Orders,
            i => $"({i}, {CustomerOfOrder(i)}, {i}, 'o')"));

        await Exec(db, "INSERT INTO lineitem (l_id, l_orderkey, l_shipdate, l_price) VALUES " + Values(LineItems,
            i => $"({i}, {OrderOfLineItem(i)}, {ShipDate(i)}, {i})"));

        await Exec(db, "INSERT INTO nation (n_nationkey, n_name) VALUES (0, 'A'), (1, 'B'), (2, 'C'), (3, 'D'), (4, 'E')");
        await Exec(db, "INSERT INTO region (region_id, r_name) VALUES (0, 'region 0'), (1, 'region 1'), (2, 'region 2')");

        // The cost-based join order needs the row count of every table.
        foreach (string table in new[] { "customer", "orders", "lineitem", "nation", "region" })
            await Run(db, "ANALYZE " + table);

        return db;
    }

    private static string Values(int count, Func<int, string> row)
    {
        StringBuilder values = new();

        for (int i = 1; i <= count; i++)
            values.Append(i == 1 ? "" : ", ").Append(row(i));

        return values.ToString();
    }
}
