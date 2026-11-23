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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An <c>ARRAY[…]</c> literal has one element type: the common type of its elements, by the rule
/// CASE, COALESCE and UNION use. It is decided from the element expressions, so every row of one
/// statement gets the same element type, also a row where a NULL hides a column's type. Before, the
/// first non-NULL element of each row decided it, and every other element had to match it exactly.
/// </summary>
[NonParallelizable]
public sealed class TestArrayLiteralCommonType : SharedNodeBaseTest
{
    // ── The PostgreSQL table ────────────────────────────────────────────────

    [Test]
    public async Task AnIntegerAndAFloat_GiveAFloat64Array()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue array = await ExecScalar(database, executor, dbname, "SELECT ARRAY[1, 2.5]");

        AssertElements(array, ColumnType.Float64, 1.0, 2.5);
    }

    [Test]
    public async Task AQuotedLiteral_DoesNotTakeANumericType_AsInCase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertQueryThrows(database, executor, dbname, "SELECT ARRAY[1, '2']");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.StartsWith("No common type for the elements of ARRAY: Integer64 and String", ex.Message);
    }

    [Test]
    public async Task ACastColumnAndAStringLiteral_GiveAStringArray()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedNumbers(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT ARRAY[i::string, 'hello'] AS a FROM nums WHERE id = 1");

        ColumnValue array = rows.Single().Row["a"];
        Assert.AreEqual(ColumnType.String, array.ArrayElementType);
        CollectionAssert.AreEqual(new[] { "1", "hello" }, array.ArrayValues!.Select(v => v.StrValue).ToArray());
    }

    [Test]
    public async Task AnIntegerAndABool_HaveNoCommonType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertQueryThrows(database, executor, dbname, "SELECT ARRAY[1, true]");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.StartsWith("No common type for the elements of ARRAY: Integer64 and Bool", ex.Message);
    }

    // ── Widening ────────────────────────────────────────────────────────────

    [TestCase("SELECT ARRAY[1, CAST(2.5 AS float32)]", ColumnType.Float32)]
    [TestCase("SELECT ARRAY[CAST(2.5 AS float32), 1]", ColumnType.Float32)]
    [TestCase("SELECT ARRAY[1, NUMERIC '2.5']", ColumnType.Numeric)]
    [TestCase("SELECT ARRAY[NUMERIC '2.5', 1.5]", ColumnType.Float64)]
    [TestCase("SELECT ARRAY[NULL, 1, NULL, 2.5]", ColumnType.Float64)]
    [TestCase("SELECT ARRAY[CAST('2026-01-01' AS DATE), CAST('2026-01-02T03:04:05Z' AS DATETIME)]", ColumnType.DateTime)]
    [TestCase("SELECT ARRAY[CAST('2026-01-01' AS DATE), '2026-01-02']", ColumnType.Date)]
    [TestCase("SELECT ARRAY['2026-01-02', CAST('2026-01-01' AS DATE)]", ColumnType.Date)]
    public async Task EveryElement_IsConvertedToTheCommonType(string sql, ColumnType expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue array = await ExecScalar(database, executor, dbname, sql);

        Assert.AreEqual(expected, array.ArrayElementType, sql);
        Assert.IsTrue(array.ArrayValues!.All(v => v.Type == expected || v.Type == ColumnType.Null), sql);
    }

    [Test]
    public async Task NullAndEmptyArrays_KeepAnUnknownElementType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, "SELECT ARRAY[NULL, NULL]")).ArrayElementType);
        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, "SELECT ARRAY[]")).ArrayElementType);
    }

    // ── The type comes from the statement, not from one row ──────────────────

    /// <summary>
    /// Row 2 has a NULL float and row 3 has two NULLs. The array is still an
    /// <c>ARRAY&lt;Float64&gt;</c> in each of them, at the top level, in a derived table and in a view
    /// body, and the column metadata says so before any row is read.
    /// </summary>
    [Test]
    public async Task ANullElement_DoesNotChangeTheElementTypeOfItsRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedNumbers(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW num_pairs AS SELECT id, ARRAY[i, f] AS v FROM nums");

        string[] statements =
        [
            "SELECT id, ARRAY[i, f] AS v FROM nums ORDER BY id",
            "SELECT id, v FROM (SELECT id, ARRAY[i, f] AS v FROM nums) d ORDER BY id",
            "SELECT id, v FROM num_pairs ORDER BY id",
        ];

        foreach (string sql in statements)
        {
            QuerySchemaHolder schema = new();
            List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, sql, schemaOut: schema);

            Assert.AreEqual(3, rows.Count, sql);
            AssertElements(rows[0].Row["v"], ColumnType.Float64, 1.0, 2.5);
            AssertElements(rows[1].Row["v"], ColumnType.Float64, 2.0, null);
            AssertElements(rows[2].Row["v"], ColumnType.Float64, null, null);

            DerivedColumnSchema column = schema.Schema.Single(c => c.Name == "v");
            Assert.AreEqual(ColumnType.Array, column.Type, sql);
            Assert.AreEqual(ColumnType.Float64, column.ElementType, sql);
        }
    }

    [Test]
    public async Task ASubscriptOfTheArray_HasTheCommonType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT (ARRAY[1, 2.5])[1] AS x", schemaOut: schema);

        Assert.AreEqual(ColumnType.Float64, schema.Schema.Single(c => c.Name == "x").Type);
        Assert.AreEqual(ColumnType.Float64, rows.Single().Row["x"].Type);
        Assert.AreEqual(1.0, rows.Single().Row["x"].FloatValue);
    }

    [Test]
    public async Task ElementsWithNoCommonType_FailBeforeAnyRowIsRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE empty_t (id int64 PRIMARY KEY, i int64 NULL, b bool NULL, d date NULL, tags array(int64) NULL)");

        StringAssert.StartsWith("No common type for the elements of ARRAY: Integer64 and Bool",
            AssertQueryThrows(database, executor, dbname, "SELECT ARRAY[i, b] FROM empty_t").Message);

        StringAssert.Contains("Cannot convert 'nope' to a date",
            AssertQueryThrows(database, executor, dbname, "SELECT ARRAY[d, 'nope'] FROM empty_t").Message);

        StringAssert.Contains("Nested arrays",
            AssertQueryThrows(database, executor, dbname, "SELECT ARRAY[tags] FROM empty_t").Message);
    }

    [Test]
    public async Task ADateColumn_AndAStringLiteral_GiveADateArray()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE days (id int64 PRIMARY KEY, d date NULL)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO days (id, d) VALUES (1, '2026-01-01'), (2, NULL)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT ARRAY[d, '2026-02-01'] AS v FROM days ORDER BY id");

        foreach (QueryResultRow row in rows)
        {
            Assert.AreEqual(ColumnType.Date, row.Row["v"].ArrayElementType);
            Assert.AreEqual(ColumnType.Date, row.Row["v"].ArrayValues![1].Type);
        }
    }

    // ── Parameters ──────────────────────────────────────────────────────────

    [Test]
    public async Task AParameter_IsCombinedWhenTheArrayIsEvaluated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue widened = await ExecScalar(database, executor, dbname, "SELECT ARRAY[1, @p]",
            new() { { "@p", new ColumnValue(ColumnType.Float64, 2.5) } });
        AssertElements(widened, ColumnType.Float64, 1.0, 2.5);

        ColumnValue date = await ExecScalar(database, executor, dbname, "SELECT ARRAY['2026-01-01', @p]",
            new() { { "@p", new ColumnValue(ColumnType.Date, new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc).Ticks) } });
        Assert.AreEqual(ColumnType.Date, date.ArrayElementType);
        Assert.IsTrue(date.ArrayValues!.All(v => v.Type == ColumnType.Date));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT ARRAY[1, @p]",
                new() { { "@p", new ColumnValue(ColumnType.Bool, true) } }))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.StartsWith("No common type for the elements of ARRAY", ex.Message);
    }

    // ── Writes, CHECK constraints ───────────────────────────────────────────

    [Test]
    public async Task InsertValues_StoresTheCommonType_AndAnInteger64ColumnRefusesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE vals (id int64 PRIMARY KEY, fs array(float64) NULL, ns array(int64) NULL)");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO vals (id, fs) VALUES (1, ARRAY[1, 2.5])");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT fs FROM vals WHERE id = 1");
        AssertElements(rows.Single().Row["fs"], ColumnType.Float64, 1.0, 2.5);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO vals (id, ns) VALUES (2, ARRAY[1, 2.5])"));

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO vals (id, fs) VALUES (3, ARRAY[1, true])"));
    }

    /// <summary>
    /// <c>ARRAY[i, f]</c> is compared with an <c>ARRAY&lt;Float64&gt;</c>. Two arrays of different
    /// element types do not compare, so a row whose NULL float made the array an
    /// <c>ARRAY&lt;Integer64&gt;</c> failed with a type error. The condition takes its element type
    /// from the column types, on insert, on update and when a constraint is added over existing rows.
    /// </summary>
    [Test]
    public async Task ACheckConstraint_TypesTheArrayFromTheColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE checked (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL, " +
            "CHECK (ARRAY[i, f] <> ARRAY[0.0, 0.0]))");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO checked (id, i, f) VALUES (1, 5, NULL)");
        await ExecNonQuery(database, executor, dbname, "UPDATE checked SET i = 6 WHERE id = 1");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO checked (id, i, f) VALUES (2, 0, 0.0)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);

        await ExecDdl(database, executor, dbname, "CREATE TABLE later (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO later (id, i, f) VALUES (1, 5, NULL)");
        await ExecDdl(database, executor, dbname, "ALTER TABLE later ADD CONSTRAINT later_pair CHECK (ARRAY[i, f] <> ARRAY[0.0, 0.0])");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO later (id, i, f) VALUES (2, 7, NULL)");
        Assert.AreEqual(2, (await ExecQuery(database, executor, dbname, "SELECT id FROM later")).Count);
    }

    [Test]
    public async Task ACheckConstraint_WithElementsOfNoCommonType_IsRefusedWhenCreated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname,
                "CREATE TABLE bad_check (id int64 PRIMARY KEY, i int64 NULL, b bool NULL, CHECK (cardinality(ARRAY[i, b]) = 2))"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.StartsWith("No common type for the elements of ARRAY", ex.Message);
    }

    [Test]
    public async Task AnUpdate_StoresTheColumnElementType_InEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedNumbers(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "UPDATE nums SET pair = ARRAY[i, f] WHERE TRUE");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT pair FROM nums ORDER BY id");

        AssertElements(rows[0].Row["pair"], ColumnType.Float64, 1.0, 2.5);
        AssertElements(rows[1].Row["pair"], ColumnType.Float64, 2.0, null);
        AssertElements(rows[2].Row["pair"], ColumnType.Float64, null, null);
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>nums (id, i int64, f float64, pair array(float64))</c> with rows (1, 1, 2.5), (2, 2, NULL)
    /// and (3, NULL, NULL).
    /// </summary>
    private static async Task SeedNumbers(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE nums (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL, pair array(float64) NULL)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO nums (id, i, f) VALUES (1, 1, 2.5), (2, 2, NULL), (3, NULL, NULL)");
    }

    private static void AssertElements(ColumnValue array, ColumnType elementType, params double?[] expected)
    {
        Assert.AreEqual(ColumnType.Array, array.Type);
        Assert.AreEqual(elementType, array.ArrayElementType);
        Assert.AreEqual(expected.Length, array.ArrayValues!.Count);

        for (int i = 0; i < expected.Length; i++)
        {
            ColumnValue element = array.ArrayValues[i];

            if (expected[i] is not { } value)
            {
                Assert.AreEqual(ColumnType.Null, element.Type, $"element {i}");
                continue;
            }

            Assert.AreEqual(elementType, element.Type, $"element {i}");
            Assert.AreEqual(value, element.FloatValue, 1e-9, $"element {i}");
        }
    }

    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null,
        QuerySchemaHolder? schemaOut = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, null, schemaOut);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<ColumnValue> ExecScalar(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, sql, parameters);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row["0"];
    }

    private static CamusDBException AssertQueryThrows(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        return Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, sql), sql)!;
    }
}
