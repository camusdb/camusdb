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

/// <summary>The helpers and the seed data the set-operation fixtures share.</summary>
public abstract class SetOperationsTestBase : SharedNodeBaseTest
{
    protected static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    protected static async Task<long> ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        long affected = (await executor.ExecuteNonSQLQuery(ticket)).ModifiedRows;
        await database.Transactions.CommitAsync(tx);
        return affected;
    }

    protected static async Task<List<QueryResultRow>> ExecQuery(
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

    /// <summary>The one column of each row, in result order; null for a NULL cell.</summary>
    protected static async Task<List<long?>> Longs(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string column)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql);
        return rows.Select(r =>
        {
            ColumnValue value = r.Row[column];
            if (value.Type == ColumnType.Null)
                return (long?)null;

            Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
            return value.LongValue;
        }).ToList();
    }

    /// <summary>The same values sorted, NULL first, for a result whose order is not defined.</summary>
    protected static async Task<List<long?>> SortedLongs(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string column)
    {
        List<long?> values = await Longs(executor, database, dbname, sql, column);
        values.Sort((x, y) => Nullable.Compare(x, y));
        return values;
    }

    protected static async Task<CamusDBException> AssertFails(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string code, string? messagePart = null)
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
        if (messagePart is not null)
            Assert.That(ex.Message, Does.Contain(messagePart), sql);
        return ex;
    }

    /// <summary>
    /// Two tables with duplicates and a NULL on both sides: a holds 1, 1, 1, 2, 3, NULL and b holds
    /// 1, 1, 2, 2, 4, NULL. The column names differ, so a match must be by position.
    /// </summary>
    protected static async Task SeedTwoTables(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ExecDdl(executor, database, dbname, "CREATE TABLE a (id int64 PRIMARY KEY, x int64 NULL)");
        await ExecDdl(executor, database, dbname, "CREATE TABLE b (id int64 PRIMARY KEY, y int64 NULL)");

        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO a (id, x) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 3), (6, NULL)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO b (id, y) VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 4), (6, NULL)");
    }
}

/// <summary>
/// <c>UNION</c>, <c>INTERSECT</c> and <c>EXCEPT</c>, with and without <c>ALL</c>. Every case goes
/// through SQL, so the parser, the query creator, the binder, the join planner and the set-operation
/// executor are all on the path. The expected results are PostgreSQL's. A result without ORDER BY has
/// no defined order, so those cases compare sorted values.
///
/// <para>This fixture runs the engine in cluster mode; <see cref="TestSetOperationsStandalone"/> runs
/// every test again in standalone mode.</para>
/// </summary>
[NonParallelizable]
public class TestSetOperations : SetOperationsTestBase
{
    // ── The six forms ──────────────────────────────────────────────────────────

    [Test]
    public async Task EachForm_ReturnsExactCounts_WithDuplicatesAndNulls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        CollectionAssert.AreEqual(new long?[] { null, null, 1, 1, 1, 1, 1, 2, 2, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a UNION ALL SELECT y FROM b", "x"));

        CollectionAssert.AreEqual(new long?[] { null, 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a UNION SELECT y FROM b", "x"));

        CollectionAssert.AreEqual(new long?[] { null, 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a UNION DISTINCT SELECT y FROM b", "x"));

        CollectionAssert.AreEqual(new long?[] { null, 1, 2 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a INTERSECT SELECT y FROM b", "x"));

        // min(3, 2) ones, min(1, 2) twos, min(1, 1) NULLs.
        CollectionAssert.AreEqual(new long?[] { null, 1, 1, 2 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a INTERSECT ALL SELECT y FROM b", "x"));

        CollectionAssert.AreEqual(new long?[] { 3 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a EXCEPT SELECT y FROM b", "x"));

        // max(3 - 2, 0) ones, max(1 - 2, 0) twos, one three, max(1 - 1, 0) NULLs.
        CollectionAssert.AreEqual(new long?[] { 1, 3 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM a EXCEPT ALL SELECT y FROM b", "x"));

        // The other direction: b's rows that a does not hold.
        CollectionAssert.AreEqual(new long?[] { 2, 4 },
            await SortedLongs(executor, database, dbname, "SELECT y FROM b EXCEPT ALL SELECT x FROM a", "y"));
    }

    [Test]
    public async Task OutputNames_ComeFromTheLeftOperand()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT x AS first_name, id FROM a WHERE id = 1 UNION ALL SELECT y, id FROM b WHERE id = 5");

        Assert.AreEqual(2, rows.Count);
        foreach (QueryResultRow row in rows)
        {
            Assert.IsTrue(row.Row.ContainsKey("first_name"));
            Assert.IsTrue(row.Row.ContainsKey("id"));
            Assert.IsFalse(row.Row.ContainsKey("y"));
        }
    }

    [Test]
    public async Task NullRows_AreEqualToEachOther()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> union = await ExecQuery(executor, database, dbname, "SELECT NULL AS n UNION SELECT NULL");
        Assert.AreEqual(1, union.Count);
        Assert.AreEqual(ColumnType.Null, union[0].Row["n"].Type);

        Assert.AreEqual(2, (await ExecQuery(executor, database, dbname, "SELECT NULL AS n UNION ALL SELECT NULL")).Count);
        Assert.AreEqual(1, (await ExecQuery(executor, database, dbname, "SELECT NULL AS n INTERSECT SELECT NULL")).Count);
        Assert.AreEqual(0, (await ExecQuery(executor, database, dbname, "SELECT NULL AS n EXCEPT SELECT NULL")).Count);
    }

    [Test]
    public async Task FromlessOperands_Work()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CollectionAssert.AreEqual(new long?[] { 1, 2 },
            await SortedLongs(executor, database, dbname, "SELECT 1 AS n UNION ALL SELECT 2", "n"));

        CollectionAssert.AreEqual(new long?[] { 1 },
            await SortedLongs(executor, database, dbname, "SELECT 1 AS n UNION SELECT 1", "n"));

        CollectionAssert.AreEqual(new long?[] { 1, 2, 3 },
            await Longs(executor, database, dbname, "SELECT 3 AS n UNION SELECT 1 UNION SELECT 2 ORDER BY n", "n"));

        await AssertFails(executor, database, dbname, "SELECT * UNION SELECT 1", CamusDBErrorCodes.InvalidInput, "FROM clause");
    }

    // ── Type rules ─────────────────────────────────────────────────────────────

    [Test]
    public async Task TypeRules_WidenToTheResultType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> intFloat = await ExecQuery(executor, database, dbname, "SELECT 1 AS v UNION SELECT 1.0");
        Assert.AreEqual(1, intFloat.Count, "an int64 1 and a float64 1.0 are the same row once both are float64");
        Assert.AreEqual(ColumnType.Float64, intFloat[0].Row["v"].Type);

        List<QueryResultRow> intNumeric = await ExecQuery(executor, database, dbname, "SELECT 1 AS v UNION SELECT NUMERIC '1'");
        Assert.AreEqual(1, intNumeric.Count);
        Assert.AreEqual(ColumnType.Numeric, intNumeric[0].Row["v"].Type);

        List<QueryResultRow> dateDatetime = await ExecQuery(executor, database, dbname,
            "SELECT CAST('2024-01-02' AS DATE) AS v UNION SELECT CAST('2024-01-02T00:00:00Z' AS DATETIME)");
        Assert.AreEqual(1, dateDatetime.Count, "a date equals the datetime at its midnight");
        Assert.AreEqual(ColumnType.DateTime, dateDatetime[0].Row["v"].Type);

        List<QueryResultRow> nullInt = await ExecQuery(executor, database, dbname,
            "SELECT NULL AS v UNION ALL SELECT 7");
        Assert.AreEqual(2, nullInt.Count, "an untyped NULL takes the type of the other side");

        List<QueryResultRow> intNull = await ExecQuery(executor, database, dbname,
            "SELECT 'x' AS v UNION ALL SELECT NULL");
        Assert.AreEqual(2, intNull.Count);
    }

    [Test]
    public async Task TypeRules_RefuseIncompatibleTypesAndColumnCounts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await AssertFails(executor, database, dbname, "SELECT 'a' UNION SELECT 1", CamusDBErrorCodes.InvalidInput, "Column 1 of UNION");
        await AssertFails(executor, database, dbname, "SELECT x FROM a INTERSECT SELECT 'z' FROM b", CamusDBErrorCodes.InvalidInput, "INTERSECT");
        await AssertFails(executor, database, dbname, "SELECT x, id FROM a UNION SELECT y FROM b", CamusDBErrorCodes.InvalidInput, "same number of columns");
        await AssertFails(executor, database, dbname, "SELECT 1, 2 EXCEPT SELECT 1", CamusDBErrorCodes.InvalidInput, "same number of columns");
    }

    // ── Precedence ─────────────────────────────────────────────────────────────

    [Test]
    public async Task Precedence_IntersectBindsTighter_ExceptGroupsLeft()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // {1} UNION ({2, 3} INTERSECT {3}) = {1, 3}; ({1} UNION {2, 3}) INTERSECT {3} would be {3}.
        string implicitGroup = "SELECT 1 AS n UNION (SELECT 2 UNION ALL SELECT 3) INTERSECT SELECT 3";
        string explicitGroup = "SELECT 1 AS n UNION ((SELECT 2 UNION ALL SELECT 3) INTERSECT SELECT 3)";

        CollectionAssert.AreEqual(new long?[] { 1, 3 }, await SortedLongs(executor, database, dbname, implicitGroup, "n"));
        CollectionAssert.AreEqual(new long?[] { 1, 3 }, await SortedLongs(executor, database, dbname, explicitGroup, "n"));
        CollectionAssert.AreEqual(new long?[] { 3 },
            await SortedLongs(executor, database, dbname, "(SELECT 1 AS n UNION (SELECT 2 UNION ALL SELECT 3)) INTERSECT SELECT 3", "n"));

        // ({1, 2, 3} EXCEPT {1}) EXCEPT {2} = {3}; {1, 2, 3} EXCEPT ({1} EXCEPT {2}) would be {2, 3}.
        string triple = "(SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)";
        CollectionAssert.AreEqual(new long?[] { 3 },
            await SortedLongs(executor, database, dbname, $"{triple} EXCEPT SELECT 1 EXCEPT SELECT 2", "n"));
        CollectionAssert.AreEqual(new long?[] { 2, 3 },
            await SortedLongs(executor, database, dbname, $"{triple} EXCEPT (SELECT 1 EXCEPT SELECT 2)", "n"));

        // UNION and EXCEPT share a level: ({1} UNION {2}) EXCEPT {1} = {2}.
        CollectionAssert.AreEqual(new long?[] { 2 },
            await SortedLongs(executor, database, dbname, "SELECT 1 AS n UNION SELECT 2 EXCEPT SELECT 1", "n"));
    }

    // ── ORDER BY, LIMIT, OFFSET ────────────────────────────────────────────────

    [Test]
    public async Task OrderLimitOffset_ApplyToTheCombinedResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        CollectionAssert.AreEqual(new long?[] { 4, 3, 2, 1 },
            await Longs(executor, database, dbname, "SELECT x FROM a WHERE x IS NOT NULL UNION SELECT y FROM b WHERE y IS NOT NULL ORDER BY x DESC", "x"));

        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4 },
            await Longs(executor, database, dbname, "SELECT x FROM a WHERE x IS NOT NULL UNION SELECT y FROM b WHERE y IS NOT NULL ORDER BY 1", "x"));

        CollectionAssert.AreEqual(new long?[] { 2, 3 },
            await Longs(executor, database, dbname, "SELECT x FROM a WHERE x IS NOT NULL UNION SELECT y FROM b WHERE y IS NOT NULL ORDER BY 1 LIMIT 2 OFFSET 1", "x"));

        CollectionAssert.AreEqual(new long?[] { 3, 4 },
            await Longs(executor, database, dbname, "SELECT x FROM a WHERE x IS NOT NULL UNION SELECT y FROM b WHERE y IS NOT NULL ORDER BY x OFFSET 2", "x"));

        // ORDER BY and LIMIT inside a parenthesized operand belong to that operand.
        CollectionAssert.AreEqual(new long?[] { 1, 1, 4 },
            await Longs(executor, database, dbname,
                "(SELECT x FROM a WHERE x IS NOT NULL ORDER BY x LIMIT 2) UNION ALL (SELECT y FROM b ORDER BY y DESC LIMIT 1) ORDER BY 1", "x"));

        await AssertFails(executor, database, dbname, "SELECT x FROM a UNION SELECT y FROM b ORDER BY x + 1", CamusDBErrorCodes.InvalidInput, "output column");
        await AssertFails(executor, database, dbname, "SELECT x FROM a UNION SELECT y FROM b ORDER BY a.x", CamusDBErrorCodes.InvalidInput, "output column");
        await AssertFails(executor, database, dbname, "SELECT x FROM a UNION SELECT y FROM b ORDER BY 2", CamusDBErrorCodes.InvalidInput, "out of range");
        await AssertFails(executor, database, dbname, "SELECT x FROM a UNION SELECT y FROM b ORDER BY y", CamusDBErrorCodes.UnknownColumn);
    }

    [Test]
    public async Task OrderByInAnOperand_WithoutParentheses_IsASyntaxError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, dbname, "SELECT x FROM a ORDER BY x UNION SELECT y FROM b", null);
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        });
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
    }

    // ── Positions ──────────────────────────────────────────────────────────────

    [Test]
    public async Task SetOperation_InADerivedTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4 },
            await Longs(executor, database, dbname,
                "SELECT u.x FROM (SELECT x FROM a UNION SELECT y FROM b) AS u WHERE u.x IS NOT NULL ORDER BY u.x", "x"));

        List<QueryResultRow> counted = await ExecQuery(executor, database, dbname,
            "SELECT count(*) AS c FROM (SELECT x FROM a INTERSECT ALL SELECT y FROM b) AS i");
        Assert.AreEqual(4, counted[0].Row["c"].LongValue);

        // Joined to a table: the derived side is read more than once by a nested loop.
        List<QueryResultRow> joined = await ExecQuery(executor, database, dbname,
            "SELECT a.id FROM a JOIN (SELECT y FROM b EXCEPT SELECT x FROM a) AS e ON a.id = e.y");
        CollectionAssert.AreEquivalent(new long[] { 4 }, joined.Select(r => r.Row["id"].LongValue));
    }

    [Test]
    public async Task SetOperation_InSubqueries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        CollectionAssert.AreEqual(new long?[] { 1, 3, 4 },
            await Longs(executor, database, dbname,
                "SELECT id FROM a WHERE id IN (SELECT 1 UNION SELECT 3 UNION SELECT 4) ORDER BY id", "id"));

        CollectionAssert.AreEqual(new long?[] { 2, 5, 6 },
            await Longs(executor, database, dbname,
                "SELECT id FROM a WHERE id NOT IN (SELECT 1 UNION SELECT 3 UNION SELECT 4) ORDER BY id", "id"));

        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4, 5, 6 },
            await Longs(executor, database, dbname,
                "SELECT id FROM a WHERE EXISTS (SELECT x FROM a INTERSECT SELECT y FROM b) ORDER BY id", "id"));

        CollectionAssert.IsEmpty(
            await Longs(executor, database, dbname,
                "SELECT id FROM a WHERE EXISTS (SELECT 1 EXCEPT SELECT 1) ORDER BY id", "id"));

        CollectionAssert.AreEqual(new long?[] { 3 },
            await Longs(executor, database, dbname,
                "SELECT id FROM a WHERE id = (SELECT x FROM a EXCEPT SELECT y FROM b)", "id"));

        await AssertFails(executor, database, dbname,
            "SELECT id FROM a WHERE id = (SELECT 1 UNION SELECT 2)", CamusDBErrorCodes.InvalidInput, "more than one row");

        await AssertFails(executor, database, dbname,
            "SELECT id FROM a WHERE id IN (SELECT 1, 2 UNION SELECT 3, 4)", CamusDBErrorCodes.InvalidInput, "exactly one column");
    }

    [Test]
    public async Task SetOperation_WithASubqueryInAnOperand()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        CollectionAssert.AreEqual(new long?[] { 1, 2, 4 },
            await Longs(executor, database, dbname,
                "SELECT id FROM a WHERE id IN (SELECT y FROM b) UNION SELECT id FROM b WHERE id = (SELECT max(x) FROM a) + 1 ORDER BY 1", "id"));
    }

    [Test]
    public async Task SetOperation_AsTheSourceOfInsertSelectAndCtas()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "CREATE TABLE c (v int64 PRIMARY KEY)");

        long inserted = await ExecNonQuery(executor, database, dbname,
            "INSERT INTO c SELECT x FROM a WHERE x IS NOT NULL UNION SELECT y FROM b WHERE y IS NOT NULL");
        Assert.AreEqual(4, inserted);

        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4 },
            await Longs(executor, database, dbname, "SELECT v FROM c ORDER BY v", "v"));

        // A source that starts with a parenthesis is a query, not a column list.
        await ExecDdl(executor, database, dbname, "CREATE TABLE e (v int64 PRIMARY KEY)");
        Assert.AreEqual(2, await ExecNonQuery(executor, database, dbname,
            "INSERT INTO e (SELECT 10 UNION SELECT 20) UNION ALL SELECT 30 EXCEPT SELECT 30"));
        Assert.AreEqual(1, await ExecNonQuery(executor, database, dbname,
            "INSERT INTO e (v) (SELECT 40)"));
        CollectionAssert.AreEqual(new long?[] { 10, 20, 40 },
            await Longs(executor, database, dbname, "SELECT v FROM e ORDER BY v", "v"));

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE d AS SELECT x AS v FROM a EXCEPT ALL SELECT y FROM b");

        CollectionAssert.AreEqual(new long?[] { 1, 3 },
            await SortedLongs(executor, database, dbname, "SELECT v FROM d", "v"));

        TableDescriptor d = await executor.OpenTable(new OpenTableTicket(dbname, "d"));
        Assert.AreEqual(ColumnType.Integer64, d.Schema.Columns!.First(c => c.Name == "v").Type);
    }

    // ── Views ──────────────────────────────────────────────────────────────────

    [Test]
    public async Task AViewAsAnOperand_Expands()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "CREATE VIEW big_b AS SELECT y FROM b WHERE y >= 2");

        CollectionAssert.AreEqual(new long?[] { 2, 3, 4 },
            await Longs(executor, database, dbname, "SELECT x FROM a WHERE x >= 2 UNION SELECT y FROM big_b ORDER BY 1", "x"));

        CollectionAssert.AreEqual(new long?[] { 2 },
            await Longs(executor, database, dbname, "SELECT y FROM big_b INTERSECT SELECT x FROM a", "y"));
    }

    // ── Parser ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task QuantifiedAll_StillParses()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        CollectionAssert.AreEqual(new long?[] { 5, 6 },
            await Longs(executor, database, dbname, "SELECT id FROM a WHERE id > ALL (SELECT y FROM b WHERE y IS NOT NULL) ORDER BY id", "id"));

        CollectionAssert.AreEqual(new long?[] { 5, 6 },
            await Longs(executor, database, dbname, "SELECT id FROM a WHERE id > all (SELECT 1 UNION SELECT 4) ORDER BY id", "id"));
    }
}

/// <summary>EXPLAIN, the plan cache, cancellation and the parser limits of the set operations.</summary>
[NonParallelizable]
public class TestSetOperationsPlans : SetOperationsTestBase
{
    private static List<string> Lines(List<QueryResultRow> rows) =>
        rows.Select(r => r.Row["node"].StrValue + "(" + r.Row["detail"].StrValue + ")").ToList();

    [Test]
    public async Task Explain_RendersTheOperatorAndBothOperands()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        (string Sql, string Node)[] forms =
        [
            ("SELECT x FROM a UNION ALL SELECT y FROM b", "union(all)"),
            ("SELECT x FROM a UNION SELECT y FROM b", "union(distinct: hash)"),
            ("SELECT x FROM a INTERSECT SELECT y FROM b", "intersect(hash)"),
            ("SELECT x FROM a INTERSECT ALL SELECT y FROM b", "intersect(all, hash)"),
            ("SELECT x FROM a EXCEPT SELECT y FROM b", "except(hash)"),
            ("SELECT x FROM a EXCEPT ALL SELECT y FROM b", "except(all, hash)"),
        ];

        foreach ((string sql, string node) in forms)
        {
            List<string> lines = Lines(await ExecQuery(executor, database, dbname, "EXPLAIN " + sql));
            string all = string.Join(" | ", lines);

            int op = lines.IndexOf(node);
            int left = lines.IndexOf("operand(left)");
            int right = lines.IndexOf("operand(right)");

            Assert.That(op, Is.GreaterThanOrEqualTo(0), all);
            Assert.That(left, Is.GreaterThan(op), all);
            Assert.That(right, Is.GreaterThan(left), all);

            // Each operand's own scan renders under its operand row.
            Assert.IsTrue(lines.Skip(left + 1).Take(right - left - 1).Any(l => l.Contains("a")), all);
            Assert.IsTrue(lines.Skip(right + 1).Any(l => l.Contains("b")), all);
        }
    }

    [Test]
    public async Task Explain_RendersANestedSetOperation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        List<string> lines = Lines(await ExecQuery(executor, database, dbname,
            "EXPLAIN SELECT x FROM a UNION ALL SELECT y FROM b EXCEPT SELECT 1"));
        string all = string.Join(" | ", lines);

        Assert.AreEqual(1, lines.Count(l => l == "except(hash)"), all);
        Assert.AreEqual(1, lines.Count(l => l == "union(all)"), all);
        Assert.AreEqual(2, lines.Count(l => l == "operand(left)"), all);
        Assert.That(lines.IndexOf("except(hash)"), Is.LessThan(lines.IndexOf("union(all)")), all);
    }

    [Test]
    public async Task ExplainAnalyze_ReportsTheRowsOfEachOperand()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT x FROM a WHERE id <= 4 INTERSECT ALL SELECT y FROM b");
        string all = string.Join(" | ", Lines(rows));

        long Actual(string node) => rows.First(r => r.Row["node"].StrValue == node).Row["actual_rows"].LongValue;

        Assert.AreEqual(4, Actual("operand"), all);
        Assert.AreEqual(6, rows.Where(r => r.Row["node"].StrValue == "operand").Skip(1).First().Row["actual_rows"].LongValue, all);

        // 1, 1, 1, 2 against 1, 1, 2, 2, 4, NULL: two ones and one two.
        Assert.AreEqual(3, Actual("intersect"), all);
    }

    [Test]
    public async Task PlanCache_KeepsSetOperationsWithDifferentOperandsApart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        string first = "SELECT x FROM a UNION ALL SELECT y FROM b WHERE id = 1";
        string second = "SELECT y FROM b UNION ALL SELECT x FROM a WHERE id = 1";

        for (int round = 0; round < 2; round++)
        {
            CollectionAssert.AreEqual(new long?[] { null, 1, 1, 1, 1, 2, 3 },
                await SortedLongs(executor, database, dbname, first, "x"), $"round {round}");
            CollectionAssert.AreEqual(new long?[] { null, 1, 1, 1, 2, 2, 4 },
                await SortedLongs(executor, database, dbname, second, "y"), $"round {round}");
        }

        Assert.AreNotEqual(Shape(first), Shape(second));
        Assert.AreNotEqual(Shape("SELECT x FROM a UNION SELECT y FROM b"), Shape("SELECT x FROM a INTERSECT SELECT y FROM b"));
        Assert.AreNotEqual(Shape("SELECT x FROM a UNION SELECT y FROM b"), Shape("SELECT x FROM a UNION ALL SELECT y FROM b"));
        Assert.AreNotEqual(Shape("SELECT x FROM a UNION SELECT y FROM b ORDER BY 1"), Shape("SELECT x, id FROM a UNION SELECT y, id FROM b ORDER BY 2"));

        // A literal is still not part of the shape.
        Assert.AreEqual(Shape(first), Shape("SELECT x FROM a UNION ALL SELECT y FROM b WHERE id = 2"));
    }

    private static string Shape(string sql) =>
        CamusDB.Core.CommandsExecutor.Controllers.Queries.QueryShapeComputer.Compute(
            new CamusDB.Core.CommandsExecutor.Controllers.DML.SelectQueryCreator().CreateSelectQuery(
                CamusDB.Core.SQLParser.SQLParserProcessor.Parse(sql)));

    [Test]
    public async Task Cancellation_StopsALongUnionAll()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        using System.Threading.CancellationTokenSource cts = new();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, dbname,
            "SELECT i FROM generate_series(1, 100000000) AS i UNION ALL SELECT j FROM generate_series(1, 100000000) AS j",
            null, cancellationToken: cts.Token);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        long read = 0;
        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (QueryResultRow _ in cursor)
            {
                if (++read == 1000)
                    cts.Cancel();
            }
        });

        Assert.That(read, Is.LessThan(1_000_000), "the scan stops soon after the cancel");
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
    }

    /// <summary>
    /// Generated SQL writes long chains of one operator. The parser rebalances them, so a chain of a
    /// thousand operands runs, returns every row in source order, and does not overflow the stack.
    /// </summary>
    [Test]
    public async Task ALongChainOfOneOperator_Runs()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string unionAll = "SELECT 0 AS n" + string.Concat(Enumerable.Range(1, 1000).Select(i => $" UNION ALL SELECT {i}"));
        CollectionAssert.AreEqual(Enumerable.Range(0, 1001).Select(i => (long?)i).ToList(),
            await Longs(executor, database, dbname, unionAll, "n"), "UNION ALL keeps the source order");

        string union = "SELECT 0 AS n" + string.Concat(Enumerable.Range(1, 1000).Select(i => $" UNION SELECT {i % 10}"));
        CollectionAssert.AreEqual(Enumerable.Range(0, 10).Select(i => (long?)i).ToList(),
            await SortedLongs(executor, database, dbname, union, "n"));

        string intersect = "SELECT 1 AS n" + string.Concat(Enumerable.Repeat(" INTERSECT SELECT 1", 200));
        CollectionAssert.AreEqual(new long?[] { 1 }, await Longs(executor, database, dbname, intersect, "n"));
    }

    /// <summary>
    /// A rebalanced chain types its columns as the chain as written does: a column that is NULL in
    /// every operand but one has that one's type, wherever the rebalance put the typed operand.
    /// </summary>
    [Test]
    public async Task ARebalancedChain_KeepsTheTypeOfAColumnThatIsMostlyNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string chain = "SELECT 1 AS n, 5 AS v" + string.Concat(Enumerable.Range(2, 40).Select(i => $" UNION ALL SELECT {i}, NULL"));
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, chain);

        Assert.AreEqual(41, rows.Count);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["v"].Type);
        Assert.AreEqual(40, rows.Count(r => r.Row["v"].Type == ColumnType.Null));

        // Without the rebalance, as PostgreSQL types it: NULL with NULL first, then the integer.
        List<QueryResultRow> nested = await ExecQuery(executor, database, dbname,
            "SELECT 1 AS v UNION ALL (SELECT NULL UNION ALL SELECT NULL)");
        Assert.AreEqual(3, nested.Count);
    }

    [Test]
    public void Parser_DeeplyNestedSetOperationsAreRefused()
    {
        // EXCEPT is not associative, so its chain keeps one level per operator.
        string tooDeep = "SELECT 1" + string.Concat(Enumerable.Repeat(" EXCEPT SELECT 2", 65));
        CamusDBException ex = Assert.Throws<CamusDBException>(() => CamusDB.Core.SQLParser.SQLParserProcessor.Parse(tooDeep))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTooDeeplyNested, ex.Code);
        Assert.That(ex.Message, Does.Contain("UNION, INTERSECT or EXCEPT"));

        string deepest = "SELECT 1" + string.Concat(Enumerable.Repeat(" EXCEPT SELECT 2", 64));
        Assert.DoesNotThrow(() => CamusDB.Core.SQLParser.SQLParserProcessor.Parse(deepest));
    }

    [Test]
    public async Task TheDeepestAllowedExceptChain_Runs()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string deepest = "SELECT 1 AS n" + string.Concat(Enumerable.Repeat(" EXCEPT ALL SELECT 2", 64));
        CollectionAssert.AreEqual(new long?[] { 1 }, await Longs(executor, database, dbname, deepest, "n"));
    }

    [Test]
    public void Parser_SetOperationRoundTripsThroughTheWireCodec()
    {
        CamusDB.Core.SQLParser.NodeAst parsed = CamusDB.Core.SQLParser.SQLParserProcessor.Parse(
            "(SELECT a FROM t ORDER BY a LIMIT 5) UNION ALL SELECT b FROM u INTERSECT SELECT c FROM v ORDER BY 1 LIMIT 10 OFFSET 2");

        Assert.AreEqual(CamusDB.Core.SQLParser.NodeType.SetOperationReference, parsed.rightAst!.nodeType);
        Assert.AreEqual("UNION ALL", parsed.rightAst.yytext);
        Assert.AreEqual("INTERSECT", parsed.rightAst.rightAst!.rightAst!.yytext, "INTERSECT binds tighter than UNION");

        string json = CamusDB.Core.SQLParser.NodeAstWireCodec.Serialize(parsed);
        CamusDB.Core.SQLParser.NodeAst restored = CamusDB.Core.SQLParser.NodeAstWireCodec.Deserialize(json);

        Assert.AreEqual(json, CamusDB.Core.SQLParser.NodeAstWireCodec.Serialize(restored));
        Assert.AreEqual(CamusDB.Core.SQLParser.NodeType.SetOperationReference, restored.rightAst!.nodeType);
    }

    [Test]
    public void Parser_AParenthesizedQueryTakesOneSetOfTrailingClauses()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            CamusDB.Core.SQLParser.SQLParserProcessor.Parse("(SELECT a FROM t LIMIT 5) ORDER BY a"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);

        // One set of clauses on a parenthesized query is fine.
        Assert.DoesNotThrow(() => CamusDB.Core.SQLParser.SQLParserProcessor.Parse("(SELECT a FROM t) ORDER BY a LIMIT 1"));
    }

    [Test]
    public async Task AsOfSystemTime_OnAnOperand_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await AssertFails(executor, database, dbname,
            "SELECT x FROM a AS OF SYSTEM TIME '-1s' UNION SELECT y FROM b", CamusDBErrorCodes.InvalidAsOfSystemTime);
    }
}

/// <summary>Every <see cref="TestSetOperations"/> case in standalone mode.</summary>
[NonParallelizable]
public sealed class TestSetOperationsStandalone : TestSetOperations
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
