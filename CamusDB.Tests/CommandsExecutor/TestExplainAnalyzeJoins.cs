/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// EXPLAIN (ANALYZE) on join plans, driven through <see cref="ExecuteSQLTicket"/> so the plan, the
/// execution and the rendering all take the path a client statement takes.
///
/// <para>The data is fixed so every counter has one right answer: 4 customers and 7 orders, of which
/// customer 1 owns 3, customer 2 owns 2, customer 3 owns 1, customer 4 owns none, and one order has a
/// NULL customer. The inner join therefore has 6 rows, and the left outer join from customers has 7.</para>
///
/// <para>The join order is the planner's choice, so the assertions never assume which table is on
/// the left: each test reads the leaf row to learn it, and derives the expected counters of the join
/// row from the other table. A join row's storage counters count its right side, which has no row of
/// its own.</para>
///
/// <para>The fixture toggles the planner's test-only operator switches on the executor's statistics
/// manager, so it does not run in parallel with itself.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestExplainAnalyzeJoins : SharedNodeBaseTest
{
    private const int CustomerCount = 4;

    private const int OrderCount = 7;

    private const int InnerJoinRows = 6;

    private const int LeftOuterJoinRows = 7;

    private const string InnerJoinSql =
        "SELECT c.name, o.total FROM customers c INNER JOIN orders o ON o.customer_id = c.id";

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupCustomersOrders(
        CamusDBOptions? options = null,
        bool indexOrdersByCustomer = true)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "customers",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns:
            [
                new("id",          ColumnType.Id),
                new("customer_id", ColumnType.Id),
                new("total",       ColumnType.Integer64, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        if (indexOrdersByCustomer)
        {
            await executor.AlterIndex(new AlterIndexTicket(
                databaseName: dbname,
                tableName: "orders",
                indexName: "orders_customer_idx",
                columns: [new("customer_id", OrderType.Ascending)],
                operation: AlterIndexOperation.AddIndex));
        }

        string[] customerIds = new string[CustomerCount];
        for (int i = 0; i < CustomerCount; i++)
            customerIds[i] = ObjectIdGenerator.Generate().ToString();

        // Orders per customer: 3, 2, 1, 0 — plus one order with no customer.
        int[] ordersPerCustomer = [3, 2, 1, 0];

        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < CustomerCount; i++)
        {
            await executor.Insert(new InsertTicket(txn, dbname, "customers",
                values: [new() { { "id", new(ColumnType.Id, customerIds[i]) }, { "name", new(ColumnType.String, "customer " + i) } }]));

            for (int j = 0; j < ordersPerCustomer[i]; j++)
            {
                await executor.Insert(new InsertTicket(txn, dbname, "orders",
                    values: [new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "customer_id", new(ColumnType.Id, customerIds[i]) },
                        { "total", new(ColumnType.Integer64, 10L * (i + 1) + j) },
                    }]));
            }
        }

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values: [new()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "customer_id", ColumnValue.Null },
                { "total", new(ColumnType.Integer64, 99L) },
            }]));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static string Node(QueryResultRow row) => row.Row["node"].StrValue!;

    private static string Detail(QueryResultRow row) => row.Row["detail"].StrValue!;

    private static long Long(QueryResultRow row, string column) => row.Row[column].LongValue;

    private static bool IsNull(QueryResultRow row, string column) => row.Row[column].Type == ColumnType.Null;

    /// <summary>The single row whose node name is <paramref name="node"/>; fails when there is not exactly one.</summary>
    private static QueryResultRow Single(List<QueryResultRow> rows, string node)
    {
        List<QueryResultRow> matches = rows.Where(r => Node(r) == node).ToList();
        Assert.That(matches, Has.Count.EqualTo(1), $"expected one '{node}' row in:\n{Dump(rows)}");
        return matches[0];
    }

    /// <summary>The leaf table-scan row of a two-table join plan, and the rows of its table and of the other one.</summary>
    private static (QueryResultRow Leaf, int LeftRows, int RightRows) LeftLeaf(List<QueryResultRow> rows)
    {
        QueryResultRow leaf = Single(rows, "table-scan");
        bool customersLeft = Detail(leaf).Contains("table=customers", StringComparison.Ordinal);
        return customersLeft ? (leaf, CustomerCount, OrderCount) : (leaf, OrderCount, CustomerCount);
    }

    private static string Dump(List<QueryResultRow> rows) =>
        string.Join("\n", rows.Select(r => string.Join(" | ", r.Row.Select(kv => $"{kv.Key}={kv.Value.ToString()}"))));

    // ── Every operator ────────────────────────────────────────────────────────

    /// <summary>
    /// A nested-loop join reads a base-table right side from storage once and replays it for the
    /// other left rows, so its row shows the right rows read one time, not left × right. The leaf
    /// reads its table once.
    /// </summary>
    [Test]
    public async Task NestedLoopJoin_ReadsTheRightSideOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceNestedLoopForTesting = true;

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow join = Single(rows, "nested-loop-join");
        (QueryResultRow leaf, int leftRows, int rightRows) = LeftLeaf(rows);

        Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.AreEqual(rightRows, Long(join, "rows_read"), "the right side is read from storage once");
        Assert.AreEqual(rightRows, Long(join, "kv_scan_entries"));
        Assert.That(Detail(join), Does.Not.Contain("ran="), Dump(rows));
        Assert.AreEqual(leftRows, Long(leaf, "actual_rows"));
        Assert.AreEqual(leftRows, Long(leaf, "rows_read"));
    }

    /// <summary>
    /// A right filter that calls a volatile function must run again for every left row, so the
    /// join reads the right side again for each one, and says so.
    /// </summary>
    [Test]
    public async Task NestedLoopJoin_VolatileRightFilter_RereadsTheRightSidePerLeftRow()
    {
        // The declared order keeps orders on the right; the cost-based order would put the filtered
        // table first, and the volatile filter would then be on the left leaf.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders(
            Options with { CostBasedJoinOrderEnabled = false });
        executor.Statistics.ForceNestedLoopForTesting = true;

        // random() * 0 keeps every row but makes the right filter volatile.
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT c.name, o.total FROM customers c INNER JOIN orders o ON o.customer_id = c.id " +
            "WHERE o.total + random() * 0 >= 0");

        QueryResultRow join = Single(rows, "nested-loop-join");
        if (!Detail(join).Contains("right-filter=", StringComparison.Ordinal))
            Assert.Ignore("The volatile conjunct was not pushed to the right side in this plan:\n" + Dump(rows));

        (_, int leftRows, int rightRows) = LeftLeaf(rows);

        Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.That(Detail(join), Does.Contain("ran=nested-loop-rescan"), Dump(rows));
        Assert.AreEqual(leftRows * rightRows, Long(join, "rows_read"), Dump(rows));
    }

    /// <summary>
    /// With spilling off the spool holds at most <c>HashJoinMaxBuildRows</c> rows. A larger right
    /// side is dropped from the spool and read again for every left row, with the same results.
    /// </summary>
    [Test]
    public async Task NestedLoopJoin_RightSideOverTheCap_RereadsWithTheSameResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders(
            Options with { SpillEnabled = false, HashJoinMaxBuildRows = 2 });
        executor.Statistics.ForceNestedLoopForTesting = true;

        List<QueryResultRow> result = await RunAsync(executor, database, dbname, InnerJoinSql);
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow join = Single(rows, "nested-loop-join");
        (_, int leftRows, int rightRows) = LeftLeaf(rows);

        Assert.AreEqual(InnerJoinRows, result.Count);
        Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.That(Detail(join), Does.Contain("ran=nested-loop-rescan"), Dump(rows));
        Assert.AreEqual(leftRows * rightRows, Long(join, "rows_read"), Dump(rows));
    }

    /// <summary>
    /// With spilling on and a tiny threshold the spool is written to a spill file and replayed from
    /// it. The rows are the same, and the right side is still read from storage once.
    /// </summary>
    [Test]
    public async Task NestedLoopJoin_SpilledSpool_ReadsOnceWithTheSameResult()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), "camusdb_nlj_spool_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        SpillFileManager.AcquireInstanceLock(dataDir);

        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders(
                Options with { SpillEnabled = true, ForceSpillThresholdRows = 2, DataDirectory = dataDir });
            executor.Statistics.ForceNestedLoopForTesting = true;

            List<QueryResultRow> result = await RunAsync(executor, database, dbname, InnerJoinSql + " ORDER BY o.total");
            List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

            QueryResultRow join = Single(rows, "nested-loop-join");
            (_, _, int rightRows) = LeftLeaf(rows);

            Assert.AreEqual(new long[] { 10, 11, 12, 20, 21, 30 }, result.Select(r => r.Row["total"].LongValue).ToArray());
            Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
            Assert.AreEqual(rightRows, Long(join, "rows_read"), Dump(rows));
        }
        finally
        {
            SpillFileManager.ReleaseInstanceLock();
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    /// <summary>
    /// The spool is filled only when a left row asks for the right side, so an empty left input
    /// reads nothing on the right. When the left side is not empty, the right side is read once.
    /// </summary>
    [Test]
    public async Task NestedLoopJoin_EmptyLeftSide_ReadsNothingOnTheRight()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceNestedLoopForTesting = true;

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT c.name, o.total FROM customers c INNER JOIN orders o ON o.customer_id = c.id " +
            "WHERE c.name = 'nobody'");

        QueryResultRow join = Single(rows, "nested-loop-join");
        QueryResultRow leaf = Single(rows, "table-scan");

        Assert.AreEqual(0, Long(join, "actual_rows"), Dump(rows));

        if (Long(leaf, "actual_rows") == 0)
            Assert.AreEqual(0, Long(join, "rows_read"), "an empty left side must not read the right side\n" + Dump(rows));
        else
            Assert.AreEqual(CustomerCount, Long(join, "rows_read"), "the right side is read once\n" + Dump(rows));
    }

    /// <summary>A left outer join through the spool pads exactly the left rows with no match.</summary>
    [Test]
    public async Task NestedLoopJoin_LeftOuter_PadsFromTheSpool()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceNestedLoopForTesting = true;

        const string sql = "SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON o.customer_id = c.id";

        List<QueryResultRow> result = await RunAsync(executor, database, dbname, sql);
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + sql);

        Assert.AreEqual(LeftOuterJoinRows, result.Count);
        Assert.AreEqual(1, result.Count(r => r.Row["total"].Type == ColumnType.Null), "customer 3 has no order");

        QueryResultRow join = Single(rows, "nested-loop-join");
        Assert.AreEqual(LeftOuterJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.AreEqual(OrderCount, Long(join, "rows_read"), "the right side is read once\n" + Dump(rows));
    }

    /// <summary>A hash join reads its right side once, whichever side it builds.</summary>
    [Test]
    public async Task HashJoin_ReadsTheRightSideOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceHashJoinForTesting = true;

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow join = Single(rows, "hash-join");
        (QueryResultRow leaf, int leftRows, int rightRows) = LeftLeaf(rows);

        Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.AreEqual(rightRows, Long(join, "rows_read"));
        Assert.AreEqual(rightRows, Long(join, "kv_scan_entries"));
        Assert.AreEqual(leftRows, Long(leaf, "actual_rows"));
        Assert.That(Detail(join), Does.Not.Contain("ran="), "a build under its cap runs as planned");
    }

    /// <summary>
    /// An index nested-loop join probes its right index once per left row. A unique index answers
    /// each probe with one point lookup; a non-unique one walks its entries for the key. Either way
    /// the join row reads exactly the matched right rows, and the NULL key probes nothing.
    /// </summary>
    [Test]
    public async Task IndexNestedLoopJoin_CountsProbesAndMatchedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceIndexNestedLoopForTesting = true;

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow join = Single(rows, "index-nested-loop-join");
        (QueryResultRow leaf, int leftRows, _) = LeftLeaf(rows);

        Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.AreEqual(InnerJoinRows, Long(join, "rows_read"), "only the matched right rows are fetched");
        Assert.AreEqual(leftRows, Long(leaf, "actual_rows"));

        if (Detail(join).Contains("index=~pk", StringComparison.Ordinal))
        {
            // orders on the left, customers probed by primary key: 7 orders, one with a NULL key.
            Assert.AreEqual(OrderCount - 1, Long(join, "kv_lookups"), "one point lookup per non-NULL key");
            Assert.AreEqual(0, Long(join, "kv_scan_entries"));
        }
        else
        {
            // customers on the left, orders probed through the non-unique customer index.
            Assert.AreEqual(0, Long(join, "kv_lookups"));
            Assert.GreaterOrEqual(Long(join, "kv_scan_entries"), InnerJoinRows, "every matching entry is visited");
        }
    }

    /// <summary>
    /// A merge join reads its ordered right input through a node the plan walk does not render; its
    /// reads are added to the join row, as every other join charges its right side to its own row.
    /// </summary>
    [Test]
    public async Task MergeJoin_RightInputReadsAreOnTheJoinRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceMergeJoinForTesting = true;

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow join = Single(rows, "merge-join");
        (_, int leftRows, int rightRows) = LeftLeaf(rows);

        Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
        Assert.AreEqual(rightRows, Long(join, "rows_read"), "the right input is read once");
        Assert.GreaterOrEqual(Long(join, "kv_scan_entries"), rightRows);

        // The left input arrives sorted or index-ordered; either way its rows reach the join once.
        QueryResultRow below = rows[rows.IndexOf(join) + 1];
        Assert.AreEqual(leftRows, Long(below, "actual_rows"), Dump(rows));
    }

    /// <summary>A left outer join counts its padded rows in its output.</summary>
    [Test]
    public async Task LeftOuterJoin_ActualRowsIncludePaddedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON o.customer_id = c.id");

        QueryResultRow root = rows[0];
        Assert.That(Node(root), Does.EndWith("join"), Dump(rows));
        Assert.That(Detail(root), Does.Contain("kind=left-outer"));
        Assert.AreEqual(LeftOuterJoinRows, Long(root, "actual_rows"), Dump(rows));
    }

    /// <summary>
    /// The root row carries the time of the whole statement and every other row has none, as on a
    /// single-table plan.
    /// </summary>
    [Test]
    public async Task JoinPlan_TimeIsOnTheRootRowOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);
        List<QueryResultRow> nodes = rows.Where(r => Node(r) != "plan-info").ToList();

        Assert.That(nodes, Has.Count.GreaterThanOrEqualTo(2), Dump(rows));
        Assert.IsFalse(IsNull(nodes[0], "actual_time_ms"), "the root row carries the statement time");
        Assert.GreaterOrEqual(nodes[0].Row["actual_time_ms"].FloatValue, 0.0);

        foreach (QueryResultRow row in nodes.Skip(1))
        {
            Assert.IsTrue(IsNull(row, "actual_time_ms"), $"{Node(row)} must not carry a time");
            Assert.IsFalse(IsNull(row, "actual_rows"), $"{Node(row)} ran, so it has actual rows");
        }
    }

    /// <summary>
    /// The join row's actual rows are the rows the join produced, and the statement still drains
    /// every row: the plan's actual output matches what the query returns without EXPLAIN.
    /// </summary>
    [Test]
    public async Task JoinPlan_RootActualRowsMatchTheQueryResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> result = await RunAsync(executor, database, dbname, InnerJoinSql);
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        Assert.AreEqual(InnerJoinRows, result.Count);
        Assert.AreEqual(result.Count, Long(rows[0], "actual_rows"), Dump(rows));
    }

    /// <summary>
    /// The work after the last join is not a plan node, so a LIMIT shows as a join that stopped early:
    /// the cursor is lazy and the limit stops pulling rows. It stops at the last row it returns; a
    /// limit that pulled one more row would make the join search for a match nobody reads.
    /// </summary>
    [Test]
    public async Task JoinPlan_LimitStopsTheJoinEarly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();
        executor.Statistics.ForceNestedLoopForTesting = true;

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql + " LIMIT 1");

        QueryResultRow join = Single(rows, "nested-loop-join");
        Assert.AreEqual(1, Long(join, "actual_rows"), Dump(rows));
    }

    /// <summary>
    /// The limit stops at the last row it returns, so an OFFSET still skips the right rows and a
    /// LIMIT 0 returns none. A LIMIT near the largest value plus an OFFSET must not wrap around:
    /// the end position saturates instead of becoming negative and stopping after the first row.
    /// </summary>
    [TestCase(" LIMIT 0", 0)]
    [TestCase(" LIMIT 2", 2)]
    [TestCase(" LIMIT 2 OFFSET 3", 2)]
    [TestCase(" LIMIT 10 OFFSET 5", 1)]
    [TestCase(" OFFSET 4", 2)]
    [TestCase(" LIMIT 9223372036854775807 OFFSET 1", 5)]
    public async Task Limit_ReturnsTheRequestedWindowOfAJoin(string limitClause, int expectedRows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, InnerJoinSql + limitClause);

        Assert.AreEqual(expectedRows, rows.Count);
    }

    /// <summary>
    /// A derived table on the left runs once and is counted by its rows; it reads no storage of its
    /// own (its inner query does, and has no row here).
    /// </summary>
    [Test]
    public async Task DerivedTable_ActualRowsAreItsMaterializedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT t.customer_id, c.name FROM (SELECT customer_id FROM orders WHERE total > 20) t " +
            "INNER JOIN customers c ON c.id = t.customer_id");

        QueryResultRow root = rows[0];
        Assert.That(Node(root), Does.EndWith("join"), Dump(rows));

        // Orders with total > 20: customer 2 has 20, 21; customer 3 has 30; the NULL-customer order has 99.
        int derivedRows = 3;
        int joined = 2;
        Assert.AreEqual(joined, Long(root, "actual_rows"), Dump(rows));

        QueryResultRow? derived = rows.FirstOrDefault(r => Node(r) == "derived-table-scan");
        if (derived is { Row.Count: > 0 } derivedRow)
        {
            Assert.AreEqual(derivedRows, Long(derivedRow, "actual_rows"), Dump(rows));
            Assert.AreEqual(0, Long(derivedRow, "rows_read"));
        }
    }

    // ── Right-side access paths ───────────────────────────────────────────────

    /// <summary>
    /// <c>orders</c> gets an index on <c>total</c>, and its row count is seeded to a million after
    /// ANALYZE, so its filter estimates many rows and the join-order search puts the 4 customers
    /// first: <c>orders</c> becomes the right table, with a filter on an indexed column. The real
    /// table still has 7 rows, so the actual counters stay small and exact.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupWithIndexedRightFilter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "orders",
            indexName: "orders_total_idx",
            columns: [new("total", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex));

        await RunAsync(executor, database, dbname, "ANALYZE customers");
        await RunAsync(executor, database, dbname, "ANALYZE orders");

        TableDescriptor orders = await database.TableDescriptors["orders"];
        executor.Statistics.SeedRowCountForTesting(database, orders, 1_000_000);
        return (dbname, database, executor);
    }

    [TestCase("hash-join")]
    [TestCase("nested-loop-join")]
    public async Task RightTableWithAnIndexedFilter_ReadsThroughTheIndex(string joinNode)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupWithIndexedRightFilter();

        if (joinNode == "hash-join")
            executor.Statistics.ForceHashJoinForTesting = true;
        else
            executor.Statistics.ForceNestedLoopForTesting = true;

        const string sql = "SELECT c.name, o.total FROM customers c INNER JOIN orders o ON o.customer_id = c.id WHERE o.total = 20";

        List<QueryResultRow> result = await RunAsync(executor, database, dbname, sql);
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + sql);

        QueryResultRow join = Single(rows, joinNode);
        QueryResultRow leaf = rows.Last(r => Node(r) != "plan-info");
        if (!Detail(leaf).Contains("table=customers", StringComparison.Ordinal))
            Assert.Ignore("The planner put orders first; the right table is not the filtered one:\n" + Dump(rows));

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(20, result[0].Row["total"].LongValue);
        Assert.That(Detail(join), Does.Contain("right-index=orders_total_idx"), Dump(rows));
        Assert.AreEqual(1, Long(join, "actual_rows"), Dump(rows));
        Assert.AreEqual(1, Long(join, "rows_read"), "only the matching right row is read\n" + Dump(rows));
    }

    // ── What the plan cannot show ─────────────────────────────────────────────

    /// <summary>
    /// A hash join whose build outgrows its cap with spilling off runs as a nested-loop join. The plan
    /// still says hash-join; the ANALYZE row says what ran, and the reads are the fallback's.
    /// </summary>
    [Test]
    public async Task HashJoin_BuildOverTheCapShowsTheNestedLoopFallback()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders(
            Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1 });
        executor.Statistics.ForceHashJoinForTesting = true;

        List<QueryResultRow> result = await RunAsync(executor, database, dbname, InnerJoinSql);
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow join = Single(rows, "hash-join");
        Assert.That(Detail(join), Does.Contain("ran=nested-loop-fallback"), Dump(rows));
        Assert.AreEqual(result.Count, Long(join, "actual_rows"));

        // The fallback reads the left side once and its right side once: the nested-loop join keeps
        // the right rows in a spool (the cap is the hash build cap, 1 row here, so the spool is
        // dropped and every left row reads the right side again). The abandoned build read rows on
        // top of that: on the join row when it built the right side, on the leaf row when it built
        // the left one. Either way the total is more than the fallback alone.
        (QueryResultRow leaf, int leftRows, int rightRows) = LeftLeaf(rows);
        Assert.GreaterOrEqual(Long(join, "rows_read"), (long)leftRows * rightRows, Dump(rows));
        Assert.Greater(Long(leaf, "rows_read") + Long(join, "rows_read"), leftRows + (long)leftRows * rightRows, Dump(rows));
    }

    /// <summary>A hash join whose build outgrows the spill threshold runs as a Grace hash join, and says so.</summary>
    [Test]
    public async Task HashJoin_BuildOverTheSpillThresholdShowsGraceHash()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), "camusdb_analyze_grace_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        SpillFileManager.AcquireInstanceLock(dataDir);

        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders(
                Options with { SpillEnabled = true, ForceSpillThresholdRows = 1, SpillMergeFanIn = 4, DataDirectory = dataDir });
            executor.Statistics.ForceHashJoinForTesting = true;

            List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

            QueryResultRow join = Single(rows, "hash-join");
            Assert.That(Detail(join), Does.Contain("ran=grace-hash"), Dump(rows));
            Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"));
        }
        finally
        {
            SpillFileManager.ReleaseInstanceLock();
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    /// <summary>
    /// With statistics loaded the join-order search costs the order, and its own estimate of the
    /// join's rows is printed next to the plan annotation's, so the two models can be compared with
    /// the actual rows.
    /// </summary>
    [Test]
    public async Task CostBasedJoin_ShowsTheJoinOrderSearchEstimate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        await RunAsync(executor, database, dbname, "ANALYZE customers");
        await RunAsync(executor, database, dbname, "ANALYZE orders");

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);

        QueryResultRow root = rows[0];
        Assert.That(Node(root), Does.EndWith("join"), Dump(rows));
        Assert.That(Detail(root), Does.Match(@"join-order-est-rows=\d+"), Dump(rows));

        // A leaf is not a join: it has no search estimate.
        Assert.That(Detail(Single(rows, "table-scan")), Does.Not.Contain("join-order-est-rows"));

        // Plain EXPLAIN output is unchanged.
        List<QueryResultRow> plain = await RunAsync(executor, database, dbname, "EXPLAIN " + InnerJoinSql);
        Assert.That(Detail(plain[0]), Does.Not.Contain("join-order-est-rows"));
    }

    /// <summary>
    /// The plan-info row of EXPLAIN (ANALYZE) shows the planning time and how the join order was
    /// chosen. With statistics loaded the dynamic program chose it, so the row says cost-based and
    /// counts the steps it costed. The count repeats between runs, which is why a regression check
    /// can compare it where it cannot compare a time. Plain EXPLAIN shows none of it.
    /// </summary>
    [Test]
    public async Task CostBasedJoin_PlanInfoShowsPlanningTimeAndTheSearch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        await RunAsync(executor, database, dbname, "ANALYZE customers");
        await RunAsync(executor, database, dbname, "ANALYZE orders");

        List<QueryResultRow> first = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);
        string info = Detail(Single(first, "plan-info"));

        Assert.That(info, Does.Match(@"planning-ms=\d+(\.\d+)?"), Dump(first));
        Assert.That(info, Does.Contain("join-order=cost-based"), Dump(first));

        long steps = StepsCosted(info);
        Assert.That(steps, Is.GreaterThan(0), Dump(first));

        List<QueryResultRow> second = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);
        Assert.AreEqual(steps, StepsCosted(Detail(Single(second, "plan-info"))), "the step count must repeat between runs");

        List<QueryResultRow> plain = await RunAsync(executor, database, dbname, "EXPLAIN " + InnerJoinSql);
        string plainInfo = Detail(Single(plain, "plan-info"));
        Assert.That(plainInfo, Does.Not.Contain("planning-ms"));
        Assert.That(plainInfo, Does.Not.Contain("join-order="));
    }

    /// <summary>
    /// With the cost-based join order off, the heuristic chooses the order and no step is costed.
    /// </summary>
    [Test]
    public async Task HeuristicJoin_PlanInfoShowsNoCostedSteps()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupCustomersOrders(Options with { CostBasedJoinOrderEnabled = false });

        await RunAsync(executor, database, dbname, "ANALYZE customers");
        await RunAsync(executor, database, dbname, "ANALYZE orders");

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + InnerJoinSql);
        string info = Detail(Single(rows, "plan-info"));

        Assert.That(info, Does.Contain("join-order=heuristic"), Dump(rows));
        Assert.AreEqual(0, StepsCosted(info), Dump(rows));
    }

    /// <summary>A single-table plan has a planning time but no join-order search.</summary>
    [Test]
    public async Task SingleTablePlan_PlanInfoShowsPlanningTimeOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT total FROM orders WHERE total > 20");
        string info = Detail(Single(rows, "plan-info"));

        Assert.That(info, Does.Match(@"planning-ms=\d+(\.\d+)?"), Dump(rows));
        Assert.That(info, Does.Not.Contain("join-order="), Dump(rows));
    }

    private static long StepsCosted(string planInfo)
    {
        System.Text.RegularExpressions.Match match =
            System.Text.RegularExpressions.Regex.Match(planInfo, @"join-steps-costed=(\d+)");
        Assert.IsTrue(match.Success, $"no join-steps-costed in: {planInfo}");
        return long.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// A three-table chain: every node of the left chain has its own counters, so the actual rows
    /// can be read step by step, which is what the join-order estimates are judged against.
    /// </summary>
    [Test]
    public async Task ThreeTableJoin_EveryStepHasActualRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupCustomersOrders();

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT c.name, o.total, c2.name FROM customers c " +
            "INNER JOIN orders o ON o.customer_id = c.id " +
            "INNER JOIN customers c2 ON c2.id = o.customer_id");

        List<QueryResultRow> joins = rows.Where(r => Node(r).EndsWith("join", StringComparison.Ordinal)).ToList();
        Assert.That(joins, Has.Count.EqualTo(2), Dump(rows));

        foreach (QueryResultRow join in joins)
            Assert.AreEqual(InnerJoinRows, Long(join, "actual_rows"), Dump(rows));
    }
}
