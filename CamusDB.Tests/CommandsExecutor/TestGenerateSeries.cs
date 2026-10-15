/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>generate_series</c> as a relation in <c>FROM</c>. Every case goes through SQL, so the parser,
/// the query creator, the binder, the join planner and the streaming leaf scan are all on the path.
/// The expected results are PostgreSQL's.
///
/// <para>This fixture runs the engine in cluster mode (DDL through the replicated schema log, as
/// <see cref="SharedNodeBaseTest"/> builds it); <see cref="TestGenerateSeriesStandalone"/> runs every
/// test again in standalone mode.</para>
/// </summary>
[NonParallelizable]
public class TestGenerateSeries : SharedNodeBaseTest
{
    private static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<long> ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        long affected = (await executor.ExecuteNonSQLQuery(ticket)).ModifiedRows;
        await database.Transactions.CommitAsync(tx);
        return affected;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<List<long>> Longs(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        string column,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql, parameters);
        return rows.Select(r =>
        {
            ColumnValue value = r.Row[column];
            Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
            return value.LongValue;
        }).ToList();
    }

    private static async Task<CamusDBException> AssertFails(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string code, string messagePart)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(code, ex!.Code, sql + ": " + ex.Message);
        Assert.That(ex.Message, Does.Contain(messagePart), sql);
        return ex;
    }

    // ── PostgreSQL edge cases ──────────────────────────────────────────────────

    [Test]
    public async Task Series_EdgeCases_MatchPostgres()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4, 5 },
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, 5)", "generate_series"));

        CollectionAssert.AreEqual(new long[] { 5, 3, 1 },
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(5, 1, -2)", "generate_series"));

        CollectionAssert.AreEqual(new long[] { 1, 4, 7, 10 },
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, 10, 3)", "generate_series"));

        CollectionAssert.AreEqual(new long[] { 1, 3 },
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, 4, 2)", "generate_series"));

        CollectionAssert.IsEmpty(
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, 0)", "generate_series"));

        CollectionAssert.IsEmpty(
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, 5, -1)", "generate_series"));

        CollectionAssert.AreEqual(new long[] { 7 },
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(7, 7)", "generate_series"));

        CollectionAssert.IsEmpty(
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, NULL)", "generate_series"));

        CollectionAssert.IsEmpty(
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(NULL, 5)", "generate_series"));

        // A NULL argument wins over a zero step: the function is strict, so it is never called.
        CollectionAssert.IsEmpty(
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, NULL, 0)", "generate_series"));

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1, 5, 0)", CamusDBErrorCodes.InvalidInput, "step size cannot equal zero");
    }

    [Test]
    public async Task Series_Overflow_EndsTheSeriesInsteadOfWrapping()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new[] { long.MaxValue - 1, long.MaxValue },
            await Longs(executor, database, dbname,
                "SELECT * FROM generate_series(9223372036854775806, 9223372036854775807)", "generate_series"));

        CollectionAssert.AreEqual(new[] { long.MaxValue - 1 },
            await Longs(executor, database, dbname,
                "SELECT * FROM generate_series(9223372036854775806, 9223372036854775807, 5)", "generate_series"));

        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@start", new ColumnValue(ColumnType.Integer64, long.MinValue + 1) },
            { "@stop", new ColumnValue(ColumnType.Integer64, long.MinValue) },
            { "@step", new ColumnValue(ColumnType.Integer64, -1) },
        };

        CollectionAssert.AreEqual(new[] { long.MinValue + 1, long.MinValue },
            await Longs(executor, database, dbname,
                "SELECT * FROM generate_series(@start, @stop, @step)", "generate_series", parameters));
    }

    // ── Alias rule ─────────────────────────────────────────────────────────────

    [Test]
    public async Task Alias_NamesBothTheRelationAndTheColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 },
            await Longs(executor, database, dbname, "SELECT i FROM generate_series(1, 3) AS i", "i"));

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 },
            await Longs(executor, database, dbname, "SELECT i FROM generate_series(1, 3) i", "i"));

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 },
            await Longs(executor, database, dbname, "SELECT i.i FROM generate_series(1, 3) AS i", "i"));

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 },
            await Longs(executor, database, dbname, "SELECT * FROM generate_series(1, 3) AS i", "i"));

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 },
            await Longs(executor, database, dbname, "SELECT generate_series FROM generate_series(1, 3)", "generate_series"));

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 },
            await Longs(executor, database, dbname,
                "SELECT generate_series.generate_series FROM generate_series(1, 3)", "generate_series"));
    }

    [Test]
    public async Task ColumnAliasList_NamesTheColumnApartFromTheRelation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long[] { 1, 2 },
            await Longs(executor, database, dbname, "SELECT n FROM generate_series(1, 2) AS t(n)", "n"));

        CollectionAssert.AreEqual(new long[] { 1, 2 },
            await Longs(executor, database, dbname, "SELECT t.n FROM generate_series(1, 2) t(n)", "n"));

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1, 2) AS t(a, b)", CamusDBErrorCodes.InvalidInput, "returns 1 column");

        await AssertFails(executor, database, dbname,
            "SELECT t FROM generate_series(1, 2) AS t(n)", CamusDBErrorCodes.UnknownColumn, "t");
    }

    // ── Clauses over a series ──────────────────────────────────────────────────

    [Test]
    public async Task Clauses_WhereGroupByOrderByLimit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long[] { 2, 4, 6, 8, 10 },
            await Longs(executor, database, dbname,
                "SELECT i FROM generate_series(1, 10) i WHERE i % 2 = 0", "i"));

        CollectionAssert.AreEqual(new long[] { 10, 9, 8 },
            await Longs(executor, database, dbname,
                "SELECT i FROM generate_series(1, 10) i ORDER BY i DESC LIMIT 3", "i"));

        CollectionAssert.AreEqual(new long[] { 4, 5 },
            await Longs(executor, database, dbname,
                "SELECT i FROM generate_series(1, 10) i LIMIT 2 OFFSET 3", "i"));

        List<QueryResultRow> grouped = await ExecQuery(executor, database, dbname,
            "SELECT i % 3 AS r, COUNT(*) AS c, SUM(i) AS s FROM generate_series(1, 10) i GROUP BY i % 3 ORDER BY r");

        CollectionAssert.AreEqual(
            new[] { (0L, 3L, 18L), (1L, 4L, 22L), (2L, 3L, 15L) },
            grouped.Select(r => (r.Row["r"].LongValue, r.Row["c"].LongValue, r.Row["s"].LongValue)).ToArray());

        List<QueryResultRow> count = await ExecQuery(executor, database, dbname,
            "SELECT COUNT(*) AS c FROM generate_series(1, 100000)");
        Assert.AreEqual(100000L, count[0].Row["c"].LongValue);
    }

    /// <summary>
    /// A million-row series streams: the live heap after a full collection stays near what it was
    /// before the query, both early and late in the read. A series materialized before its first row,
    /// or one that accumulates as it goes, would hold every row (a row object, its value array and its
    /// value), well over 100 MB for a million rows.
    /// </summary>
    [Test]
    public async Task MillionRowSeries_StreamsWithBoundedMemory()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        long heapBefore = GC.GetTotalMemory(forceFullCollection: true);

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname,
            sql: "SELECT i FROM generate_series(1, 1000000) i WHERE i % 7 <> 3", parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        long rows = 0;
        long sum = 0;
        long heapNearStart = 0;
        long heapNearEnd = 0;

        await foreach (QueryResultRow row in cursor)
        {
            rows++;
            sum += row.Row["i"].LongValue;

            if (rows == 50_000)
                heapNearStart = GC.GetTotalMemory(forceFullCollection: true);
            else if (rows == 800_000)
                heapNearEnd = GC.GetTotalMemory(forceFullCollection: true);
        }

        await database.Transactions.CommitAsync(tx);

        // 1..1000000 minus the 142857 values with i % 7 = 3 (3, 10, ..., 999995).
        Assert.AreEqual(1_000_000L - 142_857L, rows);
        Assert.AreEqual(500_000_500_000L - (3L + 999_995L) * 142_857L / 2, sum);

        long growth = Math.Max(heapNearStart, heapNearEnd) - heapBefore;
        TestContext.Progress.WriteLine(
            $"live heap before {heapBefore / 1024 / 1024} MB, near start {heapNearStart / 1024 / 1024} MB, near end {heapNearEnd / 1024 / 1024} MB");
        Assert.Less(growth, 32L * 1024 * 1024,
            $"the live heap grew by {growth / 1024 / 1024} MB during the read; the series must not be materialized");
    }

    [Test]
    public async Task Limit_OnAHugeSeries_ReturnsAtOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();

        CollectionAssert.AreEqual(Enumerable.Range(1, 10).Select(i => (long)i).ToArray(),
            await Longs(executor, database, dbname,
                "SELECT i FROM generate_series(1, 1000000000) i LIMIT 10", "i"));

        Assert.Less(watch.Elapsed, TimeSpan.FromSeconds(10), "LIMIT must stop the stream, not drain a billion rows");
    }

    // ── Joins with a table ─────────────────────────────────────────────────────

    private static async Task SeedItems(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ExecDdl(executor, database, dbname,
            "CREATE TABLE items (id int64 PRIMARY KEY, name string(32), qty int64)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO items (id, name, qty) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    }

    [Test]
    public async Task Join_CommaAndOn_WithARealTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedItems(executor, database, dbname);

        List<QueryResultRow> cross = await ExecQuery(executor, database, dbname,
            "SELECT t.id, g FROM items t, generate_series(1, 2) g ORDER BY t.id, g");
        CollectionAssert.AreEqual(
            new[] { (1L, 1L), (1L, 2L), (2L, 1L), (2L, 2L), (3L, 1L), (3L, 2L) },
            cross.Select(r => (r.Row["id"].LongValue, r.Row["g"].LongValue)).ToArray());

        // The series on the left of the comma, the table on the right.
        List<QueryResultRow> reversed = await ExecQuery(executor, database, dbname,
            "SELECT g, t.name FROM generate_series(2, 3) g, items t WHERE t.id = g ORDER BY g");
        CollectionAssert.AreEqual(new[] { (2L, "b"), (3L, "c") },
            reversed.Select(r => (r.Row["g"].LongValue, r.Row["name"].StrValue)).ToArray());

        List<QueryResultRow> on = await ExecQuery(executor, database, dbname,
            "SELECT s.n, t.qty FROM generate_series(0, 4) AS s(n) JOIN items t ON t.id = s.n ORDER BY s.n");
        CollectionAssert.AreEqual(new[] { (1L, 10L), (2L, 20L), (3L, 30L) },
            on.Select(r => (r.Row["n"].LongValue, r.Row["qty"].LongValue)).ToArray());

        List<QueryResultRow> left = await ExecQuery(executor, database, dbname,
            "SELECT g, t.qty FROM generate_series(2, 5) g LEFT JOIN items t ON t.id = g ORDER BY g");
        Assert.AreEqual(4, left.Count);
        CollectionAssert.AreEqual(new long[] { 2, 3, 4, 5 }, left.Select(r => r.Row["g"].LongValue).ToArray());
        Assert.AreEqual(20L, left[0].Row["qty"].LongValue);
        Assert.AreEqual(ColumnType.Null, left[2].Row["qty"].Type);

        List<QueryResultRow> crossJoin = await ExecQuery(executor, database, dbname,
            "SELECT COUNT(*) AS c FROM items CROSS JOIN generate_series(1, 4) g");
        Assert.AreEqual(12L, crossJoin[0].Row["c"].LongValue);

        // Two series joined to each other.
        List<QueryResultRow> twoSeries = await ExecQuery(executor, database, dbname,
            "SELECT a, b FROM generate_series(1, 3) a JOIN generate_series(2, 4) b ON a = b ORDER BY a");
        CollectionAssert.AreEqual(new[] { (2L, 2L), (3L, 3L) },
            twoSeries.Select(r => (r.Row["a"].LongValue, r.Row["b"].LongValue)).ToArray());
    }

    [Test]
    public async Task Join_AColumnNameOnBothSides_IsAmbiguous()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedItems(executor, database, dbname);

        // The series column is named id, as is the table's key, so a bare id names two columns.
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname,
            sql: "SELECT id FROM items, generate_series(1, 2) id", parameters: null);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        });
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        Assert.That(ex!.Message, Does.Contain("mbiguous").IgnoreCase);

        // Qualified, both resolve.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT items.id AS item, id.id AS n FROM items, generate_series(1, 2) id WHERE items.id = 3 ORDER BY n");
        CollectionAssert.AreEqual(new[] { (3L, 1L), (3L, 2L) },
            rows.Select(r => (r.Row["item"].LongValue, r.Row["n"].LongValue)).ToArray());
    }

    // ── Errors ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Errors_NameTheProblem()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedItems(executor, database, dbname);

        await AssertFails(executor, database, dbname,
            "SELECT * FROM no_such_function(1, 2)", CamusDBErrorCodes.InvalidInput, "no_such_function");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM upper('a')", CamusDBErrorCodes.InvalidInput, "'upper' is a scalar function");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM unnest(1)", CamusDBErrorCodes.FeatureNotSupported, "unnest");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1)", CamusDBErrorCodes.InvalidInput, "expects 2 or 3 arguments, got 1");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1, 2, 3, 4)", CamusDBErrorCodes.InvalidInput, "expects 2 or 3 arguments, got 4");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series()", CamusDBErrorCodes.InvalidInput, "got 0");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1, 'x')", CamusDBErrorCodes.InvalidInput, "argument 2 must be an integer");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1.5, 3)", CamusDBErrorCodes.InvalidInput, "argument 1 must be an integer");

        // An argument that reads a column of another relation in the same FROM is LATERAL.
        await AssertFails(executor, database, dbname,
            "SELECT * FROM items t, generate_series(1, t.qty) g", CamusDBErrorCodes.FeatureNotSupported, "LATERAL");

        await AssertFails(executor, database, dbname,
            "SELECT * FROM generate_series(1, (SELECT MAX(qty) FROM items)) g",
            CamusDBErrorCodes.FeatureNotSupported, "subquery");

        // WITH ORDINALITY is not part of the grammar.
        KvTransaction ordinalityTx = await database.Transactions.BeginAsync();
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: ordinalityTx, database: dbname,
                sql: "SELECT * FROM generate_series(1, 3) WITH ORDINALITY", parameters: null));
            await cursor.ToListAsync();
        });
        await database.Transactions.RollbackIfNotCompletedAsync(ordinalityTx);

        // The function is a relation only in FROM.
        await AssertFails(executor, database, dbname,
            "SELECT generate_series(1, 3)", CamusDBErrorCodes.FeatureNotSupported, "supported only in FROM");
    }

    [Test]
    public async Task Arguments_ExpressionsAndParameters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long[] { 2, 4, 6 },
            await Longs(executor, database, dbname,
                "SELECT * FROM generate_series(1 + 1, 2 * 3, abs(-2))", "generate_series"));

        CollectionAssert.AreEqual(new long[] { 3, 4 },
            await Longs(executor, database, dbname,
                "SELECT * FROM generate_series(CAST('3' AS int64), 4)", "generate_series"));

        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@from", new ColumnValue(ColumnType.Integer64, 10) },
            { "@to", new ColumnValue(ColumnType.Integer64, 12) },
        };

        CollectionAssert.AreEqual(new long[] { 10, 11, 12 },
            await Longs(executor, database, dbname,
                "SELECT n FROM generate_series(@from, @to) n", "n", parameters));
    }

    // ── Derived tables, views, INSERT ... SELECT, CTAS ─────────────────────────

    [Test]
    public async Task DerivedTable_OverASeries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long[] { 2, 4, 6 },
            await Longs(executor, database, dbname,
                "SELECT x FROM (SELECT i * 2 AS x FROM generate_series(1, 3) i) d ORDER BY x", "x"));

        CollectionAssert.AreEqual(new long[] { 3 },
            await Longs(executor, database, dbname,
                "SELECT d.x FROM (SELECT i AS x FROM generate_series(1, 5) AS i WHERE i > 2) AS d WHERE d.x < 4", "x"));
    }

    [Test]
    public async Task View_OverASeries_SurvivesReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedItems(executor, database, dbname);

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW evens AS SELECT i FROM generate_series(1, 10) AS i WHERE i % 2 = 0");

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW item_slots AS SELECT t.name, s.n FROM items t, generate_series(1, 2) AS s(n)");

        CollectionAssert.AreEqual(new long[] { 2, 4, 6, 8, 10 },
            await Longs(executor, database, dbname, "SELECT i FROM evens ORDER BY i", "i"));

        Assert.AreEqual(6, (await ExecQuery(executor, database, dbname, "SELECT name, n FROM item_slots")).Count);

        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE VIEW evens");
        StringAssert.Contains("generate_series(1, 10)", shown[0].Row["create view"].StrValue!);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        CollectionAssert.AreEqual(new long[] { 2, 4, 6, 8, 10 },
            await Longs(executor, reopened, dbname, "SELECT i FROM evens ORDER BY i", "i"));

        Assert.AreEqual(6, (await ExecQuery(executor, reopened, dbname, "SELECT name, n FROM item_slots")).Count);
    }

    /// <summary>
    /// A view's stored body records each table column by id, so a rename cannot break it. A function
    /// alias inside a subquery can shadow an outer table alias; its column must not be recorded as
    /// that table's column, or the rename below would rewrite the subquery to a column the function
    /// does not have.
    /// </summary>
    [Test]
    public async Task View_FunctionAliasShadowingATableAlias_SurvivesAColumnRename()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedItems(executor, database, dbname);

        // Inside the derived table, s is the function; outside it, s is the table.
        await ExecDdl(executor, database, dbname,
            "CREATE VIEW shadowed AS SELECT s.id FROM items s, " +
            "(SELECT s.qty AS n FROM generate_series(10, 20, 10) AS s(qty)) d WHERE s.qty = d.n");

        CollectionAssert.AreEqual(new long[] { 1, 2 },
            await Longs(executor, database, dbname, "SELECT id FROM shadowed ORDER BY id", "id"));

        await ExecDdl(executor, database, dbname, "ALTER TABLE items RENAME COLUMN qty TO quantity");

        CollectionAssert.AreEqual(new long[] { 1, 2 },
            await Longs(executor, database, dbname, "SELECT id FROM shadowed ORDER BY id", "id"));

        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE VIEW shadowed");
        string body = shown[0].Row["create view"].StrValue!;
        StringAssert.Contains("s.quantity = d.n", body);
        StringAssert.Contains("SELECT s.qty AS n FROM generate_series(10, 20, 10) AS s(qty)", body);
    }

    /// <summary>
    /// A view's body pins the table columns it reads, so they cannot be dropped under it. The function
    /// column s.qty below is not the table's qty, so it must not pin that column.
    /// </summary>
    [Test]
    public async Task View_FunctionColumn_DoesNotPinATableColumnOfTheSameName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedItems(executor, database, dbname);

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW slots AS SELECT s.id, d.n FROM items s, " +
            "(SELECT s.qty AS n FROM generate_series(1, 2) AS s(qty)) d");

        await ExecDdl(executor, database, dbname, "ALTER TABLE items DROP COLUMN qty");

        Assert.AreEqual(6, (await ExecQuery(executor, database, dbname, "SELECT id, n FROM slots")).Count);
    }

    [Test]
    public async Task InsertSelectAndCreateTableAs_FromASeries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE numbers (id int64 PRIMARY KEY, half int64)");

        long inserted = await ExecNonQuery(executor, database, dbname,
            "INSERT INTO numbers SELECT i, i / 2 FROM generate_series(1, 1000) AS i");
        Assert.AreEqual(1000L, inserted);

        List<QueryResultRow> count = await ExecQuery(executor, database, dbname,
            "SELECT COUNT(*) AS c, SUM(half) AS s FROM numbers");
        Assert.AreEqual(1000L, count[0].Row["c"].LongValue);
        Assert.AreEqual(250000L, count[0].Row["s"].LongValue);

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE squares AS SELECT n, n * n AS sq FROM generate_series(1, 5) AS t(n)");

        List<QueryResultRow> squares = await ExecQuery(executor, database, dbname,
            "SELECT n, sq FROM squares ORDER BY n");
        CollectionAssert.AreEqual(new[] { (1L, 1L), (2L, 4L), (3L, 9L), (4L, 16L), (5L, 25L) },
            squares.Select(r => (r.Row["n"].LongValue, r.Row["sq"].LongValue)).ToArray());
    }

    // ── EXPLAIN ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Explain_ShowsTheFunctionAndItsExactRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> plan = await ExecQuery(executor, database, dbname,
            "EXPLAIN SELECT i FROM generate_series(1, 100, 3) AS i");

        QueryResultRow scan = plan.Single(r => r.Row["node"].StrValue == "function-scan");
        StringAssert.Contains("generate_series(1, 100, 3)", scan.Row["detail"].StrValue!);
        StringAssert.Contains("alias=i", scan.Row["detail"].StrValue!);
        Assert.AreEqual(34L, scan.Row["estimated_rows"].LongValue);

        List<QueryResultRow> analyzed = await ExecQuery(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT i FROM generate_series(1, 100, 3) AS i");
        QueryResultRow measured = analyzed.Single(r => r.Row["node"].StrValue == "function-scan");
        Assert.AreEqual(34L, measured.Row["actual_rows"].LongValue);

        // The rows are computed: nothing is read from storage.
        Assert.AreEqual(0L, measured.Row["rows_read"].LongValue);
        Assert.AreEqual(0L, measured.Row["kv_lookups"].LongValue);
        Assert.AreEqual(0L, measured.Row["kv_scan_entries"].LongValue);
    }
}

/// <summary>
/// Every <see cref="TestGenerateSeries"/> case again, with the engine in standalone mode, where DDL
/// persists inside the caller's transaction instead of going through the replicated schema log.
/// </summary>
[NonParallelizable]
public sealed class TestGenerateSeriesStandalone : TestGenerateSeries
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
