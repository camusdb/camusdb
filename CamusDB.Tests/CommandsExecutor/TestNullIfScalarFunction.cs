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
/// End-to-end coverage for <c>NULLIF(a, b)</c>: NULL when <c>a = b</c> is TRUE, otherwise <c>a</c>.
/// The comparison must agree with the <c>=</c> operator for mixed numeric types, a NULL operand
/// must keep <c>a</c> (UNKNOWN is not TRUE), and the call must work where a stored expression is
/// parsed again on every write (a CHECK constraint).
/// </summary>
[NonParallelizable]
public sealed class TestNullIfScalarFunction : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        QuerySchemaHolder? schemaOut = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, null, schemaOut);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<ColumnValue> ExecScalar(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, sql);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row["0"];
    }

    private static async Task SeedCounts(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE counts (id int64 PRIMARY KEY, total int64, cnt int64, name string)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO counts (id, total, cnt, name) VALUES (1, 10, 2, 'alpha'), (2, 10, 0, ''), (3, 10, NULL, NULL)");
    }

    [TestCase("SELECT NULLIF(1, 1)")]
    [TestCase("SELECT nullif(1, 1)")]
    [TestCase("SELECT NULLIF('a', 'a')")]
    [TestCase("SELECT NULLIF(NULL, 1)")]
    [TestCase("SELECT NULLIF(NULL, NULL)")]
    [TestCase("SELECT NULLIF(1, 1.0)")]
    [TestCase("SELECT NULLIF(2.0, 2)")]
    [TestCase("SELECT NULLIF(NUMERIC '2', 2)")]
    [TestCase("SELECT NULLIF(2, NUMERIC '2.00')")]
    [TestCase("SELECT NULLIF(true, true)")]
    public async Task NullIf_EqualOrNullFirstArgument_ReturnsNull(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, sql);
        Assert.AreEqual(ColumnType.Null, value.Type, sql);
    }

    [Test]
    public async Task NullIf_NotEqual_ReturnsFirstArgumentUnchanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue integer = await ExecScalar(database, executor, dbname, "SELECT NULLIF(1, 2)");
        Assert.AreEqual(ColumnType.Integer64, integer.Type);
        Assert.AreEqual(1L, integer.LongValue);

        ColumnValue text = await ExecScalar(database, executor, dbname, "SELECT NULLIF('a', '')");
        Assert.AreEqual(ColumnType.String, text.Type);
        Assert.AreEqual("a", text.StrValue);

        // The first argument keeps its own type: an INT64 beside a FLOAT64 is not widened.
        ColumnValue mixed = await ExecScalar(database, executor, dbname, "SELECT NULLIF(3, 2.5)");
        Assert.AreEqual(ColumnType.Integer64, mixed.Type);
        Assert.AreEqual(3L, mixed.LongValue);

        ColumnValue numeric = await ExecScalar(database, executor, dbname, "SELECT NULLIF(NUMERIC '1.50', 2)");
        Assert.AreEqual(ColumnType.Numeric, numeric.Type);
        Assert.AreEqual("1.5", numeric.NumericValue);
    }

    [Test]
    public async Task NullIf_NullSecondArgument_ComparisonIsUnknown_ReturnsFirstArgument()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, "SELECT NULLIF(5, NULL)");
        Assert.AreEqual(ColumnType.Integer64, value.Type);
        Assert.AreEqual(5L, value.LongValue);
    }

    [Test]
    public async Task NullIf_PairWithNoComparisonRule_IsNotEqual_LikeTheEqualsOperator()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue equals = await ExecScalar(database, executor, dbname, "SELECT 1 = 'a'");
        Assert.AreEqual(ColumnType.Bool, equals.Type);
        Assert.IsFalse(equals.BoolValue);

        ColumnValue value = await ExecScalar(database, executor, dbname, "SELECT NULLIF(1, 'a')");
        Assert.AreEqual(ColumnType.Integer64, value.Type);
        Assert.AreEqual(1L, value.LongValue);
    }

    [Test]
    public async Task NullIf_GuardsDivisionByZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedCounts(database, executor, dbname);

        // Without the guard the zero row fails the whole statement.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT total / cnt AS ratio FROM counts WHERE id = 2"))!;
        StringAssert.Contains("Division by zero", ex.Message);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, total / NULLIF(cnt, 0) AS ratio FROM counts ORDER BY id");

        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual(5L, rows[0].Row["ratio"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[1].Row["ratio"].Type);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["ratio"].Type);
    }

    [Test]
    public async Task NullIf_InProjection_DeclaresTheFirstArgumentType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedCounts(database, executor, dbname);

        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, NULLIF(name, '') AS label, NULLIF(cnt, 0) AS n FROM counts ORDER BY id", schema);

        Assert.AreEqual(ColumnType.String, schema.Schema.Single(c => c.Name == "label").Type);
        Assert.AreEqual(ColumnType.Integer64, schema.Schema.Single(c => c.Name == "n").Type);

        Assert.AreEqual("alpha", rows[0].Row["label"].StrValue);
        Assert.AreEqual(ColumnType.Null, rows[1].Row["label"].Type);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["label"].Type);

        Assert.AreEqual(2L, rows[0].Row["n"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[1].Row["n"].Type);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["n"].Type);
    }

    [Test]
    public async Task NullIf_InWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedCounts(database, executor, dbname);

        List<QueryResultRow> blank = await ExecQuery(database, executor, dbname,
            "SELECT id FROM counts WHERE NULLIF(name, '') IS NULL ORDER BY id");
        CollectionAssert.AreEqual(new[] { 2L, 3L }, blank.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> nonZero = await ExecQuery(database, executor, dbname,
            "SELECT id FROM counts WHERE NULLIF(cnt, 0) > 1");
        CollectionAssert.AreEqual(new[] { 1L }, nonZero.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task NullIf_InCheckConstraint_RejectsTheSentinelValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE named (id int64 PRIMARY KEY, name string CHECK (NULLIF(name, '') IS NOT NULL))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO named (id, name) VALUES (1, 'x')");
        Assert.AreEqual(1, (await ExecQuery(database, executor, dbname, "SELECT id FROM named")).Count);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO named (id, name) VALUES (2, '')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    [TestCase("SELECT NULLIF(1)")]
    [TestCase("SELECT NULLIF(1, 2, 3)")]
    public async Task NullIf_WrongArgumentCount_IsInvalidInput(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, sql);
    }
}
