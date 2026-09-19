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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Two related rules of the arithmetic operators, driven through SQL:
/// <list type="bullet">
///   <item><c>+ - * /</c> with a NULL operand give NULL, as <c>%</c> / <c>mod</c> already did. Before,
///     a NULL operand raised "No matching signature", so a query over a nullable column failed as
///     soon as one NULL row arrived.</item>
///   <item>Unary minus works over any expression (<c>-col</c>, <c>-(a + b)</c>), not only in front of
///     a numeric literal, and a spaced <c>- 5</c> folds to the same literal as <c>-5</c>.</item>
/// </list>
/// </summary>
public sealed class TestNullArithmeticAndUnaryMinus : SharedNodeBaseTest
{
    private static async Task Exec(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        if (ddl)
            await executor.ExecuteDDLSQL(ticket);
        else
            await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<ColumnValue> Scalar(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await Query(database, executor, dbname, sql);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row["x"];
    }

    private static async Task<List<long>> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        (await Query(database, executor, dbname, sql)).Select(r => r.Row["id"].LongValue).ToList();

    /// <summary>nums: 1 (10, 1.5), 2 (NULL, NULL), 3 (-4, 2.5), 4 (0, NULL).</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE nums (id int64 primary key, n int64, f float64)", ddl: true);
        await Exec(database, executor, dbname, "CREATE INDEX nums_n_idx ON nums (n)", ddl: true);
        await Exec(database, executor, dbname,
            "INSERT INTO nums (id, n, f) VALUES (1, 10, 1.5), (2, NULL, NULL), (3, -4, 2.5), (4, 0, NULL)", ddl: false);

        return (dbname, database, executor);
    }

    // ── NULL propagation through + - * / ────────────────────────────────────

    [Test]
    public async Task NullOperand_OnEitherSide_GivesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string sql in new[]
        {
            "SELECT NULL + 1 AS x", "SELECT 1 + NULL AS x", "SELECT NULL + NULL AS x",
            "SELECT NULL - 1 AS x", "SELECT 1 - NULL AS x",
            "SELECT NULL * 2 AS x", "SELECT 2.5 * NULL AS x",
            "SELECT NULL / 2 AS x", "SELECT 2 / NULL AS x", "SELECT NULL / 2.0 AS x",
            // The NULL check runs before the zero check, as in PostgreSQL.
            "SELECT NULL / 0 AS x",
        })
        {
            ColumnValue value = await Scalar(database, executor, dbname, sql);
            Assert.AreEqual(ColumnType.Null, value.Type, sql);
        }

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(database, executor, dbname, "SELECT 1 / 0 AS x"))!;
        Assert.AreEqual("Division by zero", ex.Message);
    }

    [Test]
    public async Task NullColumn_InProjection_GivesNullForThatRowOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // Ordered by id: row 2, the second result row, is NULL in both columns.
        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT id, n * 2 AS a, n + 1 AS b, f - 1.0 AS c, n / 2 AS d FROM nums ORDER BY id");

        Assert.AreEqual(4, rows.Count);
        Assert.AreEqual(20L, rows[0].Row["a"].LongValue);
        Assert.AreEqual(0.5, rows[0].Row["c"].FloatValue, 1e-12);

        foreach (string column in new[] { "a", "b", "c", "d" })
            Assert.AreEqual(ColumnType.Null, rows[1].Row[column].Type, column);

        Assert.AreEqual(-8L, rows[2].Row["a"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[3].Row["c"].Type);
    }

    [Test]
    public async Task NullFirstRow_InResultSet_DoesNotBreakLaterRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT id, n * 3 AS x FROM nums WHERE id >= 2 ORDER BY id");

        Assert.AreEqual(ColumnType.Null, rows[0].Row["x"].Type);
        Assert.AreEqual(ColumnType.Integer64, rows[1].Row["x"].Type);
        Assert.AreEqual(-12L, rows[1].Row["x"].LongValue);
    }

    [Test]
    public async Task ArithmeticInWhere_DropsNullRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        CollectionAssert.AreEqual(new[] { 1L }, await Ids(database, executor, dbname, "SELECT id FROM nums WHERE n * 2 > 1 ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 1L, 2L }, await Ids(database, executor, dbname, "SELECT id FROM nums WHERE n * 2 > 1 OR id = 2 ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 3L, 4L }, await Ids(database, executor, dbname, "SELECT id FROM nums WHERE NOT (n + 1 > 5) ORDER BY id"));
    }

    [Test]
    public async Task OrderByArithmetic_PlacesNullsLikeTheBareColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> byColumn = await Ids(database, executor, dbname, "SELECT id FROM nums ORDER BY n, id");
        List<long> byExpression = await Ids(database, executor, dbname, "SELECT id FROM nums ORDER BY n * 2, id");

        CollectionAssert.AreEqual(byColumn, byExpression);
    }

    [Test]
    public async Task CorrelatedSubquery_OverNullableOuterArithmetic()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // Outer row 2 has n NULL: i.n > NULL * 1 is UNKNOWN for every inner row, so EXISTS is false.
        CollectionAssert.AreEqual(new[] { 3L, 4L }, await Ids(database, executor, dbname,
            "SELECT o.id FROM nums o WHERE EXISTS (SELECT 1 FROM nums i WHERE i.n > o.n * 1) ORDER BY o.id"));
    }

    [Test]
    public async Task CheckConstraintOverArithmetic_NullOperandPasses()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE lines (id int64 primary key, a int64, b int64)", ddl: true);
        await Exec(database, executor, dbname, "ALTER TABLE lines ADD CONSTRAINT chk_sum CHECK (a + b > 0)", ddl: true);

        // a + b is NULL, the comparison is UNKNOWN, and UNKNOWN passes a CHECK.
        await Exec(database, executor, dbname, "INSERT INTO lines (id, a, b) VALUES (1, 1, 1), (2, NULL, 5)", ddl: false);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(database, executor, dbname, "INSERT INTO lines (id, a, b) VALUES (3, -5, 1)", ddl: false));

        CollectionAssert.AreEqual(new[] { 1L, 2L }, await Ids(database, executor, dbname, "SELECT id FROM lines ORDER BY id"));
    }

    // ── Unary minus ─────────────────────────────────────────────────────────

    [Test]
    public async Task UnaryMinus_OverColumnsAndExpressions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT id, -n AS a, -(n + 1) AS b, -f AS c, -n * 2 AS d FROM nums ORDER BY id");

        Assert.AreEqual(-10L, rows[0].Row["a"].LongValue);
        Assert.AreEqual(-11L, rows[0].Row["b"].LongValue);
        Assert.AreEqual(ColumnType.Float64, rows[0].Row["c"].Type);
        Assert.AreEqual(-1.5, rows[0].Row["c"].FloatValue, 1e-12);
        Assert.AreEqual(-20L, rows[0].Row["d"].LongValue);

        foreach (string column in new[] { "a", "b", "c", "d" })
            Assert.AreEqual(ColumnType.Null, rows[1].Row[column].Type, column);

        Assert.AreEqual(4L, rows[2].Row["a"].LongValue);
    }

    [Test]
    public async Task UnaryMinus_Literals()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(-5L, (await Scalar(database, executor, dbname, "SELECT - 5 AS x")).LongValue);
        Assert.AreEqual(5L, (await Scalar(database, executor, dbname, "SELECT - -5 AS x")).LongValue);
        Assert.AreEqual(-2.5, (await Scalar(database, executor, dbname, "SELECT - 2.5 AS x")).FloatValue, 1e-12);
        Assert.AreEqual(-7L, (await Scalar(database, executor, dbname, "SELECT -(3 + 4) AS x")).LongValue);
        Assert.AreEqual(7L, (await Scalar(database, executor, dbname, "SELECT 3 - -4 AS x")).LongValue);
        Assert.AreEqual(ColumnType.Null, (await Scalar(database, executor, dbname, "SELECT -NULL AS x")).Type);
    }

    [Test]
    public async Task UnaryMinus_Errors()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException typeError = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(database, executor, dbname, "SELECT -'abc' AS x"))!;
        StringAssert.StartsWith("No matching signature for unary operator -", typeError.Message);

        CamusDBException overflow = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(database, executor, dbname, "SELECT -(-9223372036854775807 - 1) AS x"))!;
        StringAssert.Contains("overflow", overflow.Message);
    }

    [Test]
    public async Task UnaryMinus_InWhereOrderByGroupByAndAggregates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        CollectionAssert.AreEqual(new[] { 3L }, await Ids(database, executor, dbname, "SELECT id FROM nums WHERE -n > 0"));
        CollectionAssert.AreEqual(new[] { 1L, 4L, 3L }, await Ids(database, executor, dbname, "SELECT id FROM nums WHERE n IS NOT NULL ORDER BY -n, id"));

        List<QueryResultRow> sum = await Query(database, executor, dbname, "SELECT SUM(-n) AS s FROM nums");
        Assert.AreEqual(-6L, sum[0].Row["s"].LongValue);

        List<QueryResultRow> negatedSum = await Query(database, executor, dbname, "SELECT -SUM(n) AS s FROM nums");
        Assert.AreEqual(-6L, negatedSum[0].Row["s"].LongValue);

        List<QueryResultRow> grouped = await Query(database, executor, dbname,
            "SELECT -n AS k, COUNT(*) AS c FROM nums WHERE n IS NOT NULL GROUP BY -n ORDER BY k");
        CollectionAssert.AreEqual(new[] { -10L, 0L, 4L }, grouped.Select(r => r.Row["k"].LongValue).ToList());

        List<QueryResultRow> having = await Query(database, executor, dbname,
            "SELECT id FROM nums WHERE n IS NOT NULL GROUP BY id HAVING -SUM(n) > 0");
        CollectionAssert.AreEqual(new[] { 3L }, having.Select(r => r.Row["id"].LongValue).ToList());
    }

    [Test]
    public async Task SpacedNegativeLiteral_StillUsesIndexRange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        CollectionAssert.AreEqual(new[] { 3L }, await Ids(database, executor, dbname, "SELECT id FROM nums WHERE n = - 4"));

        List<QueryResultRow> plan = await Query(database, executor, dbname, "EXPLAIN SELECT id FROM nums WHERE n = - 4");
        string text = string.Join(" ", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));
        StringAssert.Contains("nums_n_idx", text);
    }

    [Test]
    public async Task UnaryMinus_InViewBodyAndCheckConstraint_RoundTrips()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await Exec(database, executor, dbname, "CREATE VIEW negs AS SELECT id, -n AS m, -(n * 2) AS d FROM nums", ddl: true);

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT id, m, d FROM negs WHERE m > 0 ORDER BY id");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(3L, rows[0].Row["id"].LongValue);
        Assert.AreEqual(4L, rows[0].Row["m"].LongValue);
        Assert.AreEqual(8L, rows[0].Row["d"].LongValue);

        string shown = (await Query(database, executor, dbname, "SHOW CREATE VIEW negs"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("-n", shown);

        await Exec(database, executor, dbname, "CREATE TABLE temps (id int64 primary key, t int64)", ddl: true);
        await Exec(database, executor, dbname, "ALTER TABLE temps ADD CONSTRAINT chk_t CHECK (-t < 100)", ddl: true);
        await Exec(database, executor, dbname, "INSERT INTO temps (id, t) VALUES (1, -50)", ddl: false);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(database, executor, dbname, "INSERT INTO temps (id, t) VALUES (2, -150)", ddl: false));
    }
}
