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
/// End-to-end coverage for <c>cardinality</c> and <c>array_contains</c>: the NULL rules, element
/// equality (which must match <c>IN</c>), and the places a CHECK constraint needs them — a length
/// limit and a banned value — where the stored condition text is parsed again on every write.
/// </summary>
[NonParallelizable]
public sealed class TestArrayScalarFunctions : SharedNodeBaseTest
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

    private static async Task SeedTags(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE tagged (id int64 PRIMARY KEY, tags array(string))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO tagged (id, tags) VALUES " +
            "(1, ARRAY['red', 'green', 'blue']), (2, ARRAY['blue']), (3, ARRAY[]), (4, NULL), (5, ARRAY['red', NULL])");
    }

    // ── cardinality ─────────────────────────────────────────────────────────

    [TestCase("SELECT cardinality(ARRAY[1, 2, 3])", 3L)]
    [TestCase("SELECT cardinality(ARRAY['a'])", 1L)]
    [TestCase("SELECT cardinality(ARRAY[])", 0L)]
    [TestCase("SELECT cardinality(ARRAY[NULL, 1, NULL])", 3L)]
    public async Task Cardinality_CountsEveryElementIncludingNulls(string sql, long expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, sql);
        Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
        Assert.AreEqual(expected, value.LongValue, sql);
    }

    [Test]
    public async Task Cardinality_OfNullIsNull_AndOfANonArrayIsAnError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, "SELECT cardinality(NULL)")).Type);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT cardinality('abc')"))!;
        StringAssert.Contains("to be an array", ex.Message);
    }

    [Test]
    public async Task Cardinality_OnAColumn_InProjectionAndWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, cardinality(tags) AS n FROM tagged ORDER BY id", schema);

        Assert.AreEqual(ColumnType.Integer64, schema.Schema.Single(c => c.Name == "n").Type);
        Assert.AreEqual(3L, rows[0].Row["n"].LongValue);
        Assert.AreEqual(1L, rows[1].Row["n"].LongValue);
        Assert.AreEqual(0L, rows[2].Row["n"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[3].Row["n"].Type);
        Assert.AreEqual(2L, rows[4].Row["n"].LongValue);

        List<QueryResultRow> filtered = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE cardinality(tags) >= 2 ORDER BY id");
        CollectionAssert.AreEqual(new[] { 1L, 5L }, filtered.Select(r => r.Row["id"].LongValue).ToArray());
    }

    // ── array_length ────────────────────────────────────────────────────────

    [TestCase("SELECT array_length(ARRAY[1, 2, 3], 1)", 3L)]
    [TestCase("SELECT array_length(ARRAY['a'], 1)", 1L)]
    [TestCase("SELECT array_length(ARRAY[NULL, NULL], 1)", 2L)]
    public async Task ArrayLength_OfDimensionOneIsTheElementCount(string sql, long expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, sql);
        Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
        Assert.AreEqual(expected, value.LongValue, sql);
    }

    /// <summary>
    /// PostgreSQL gives NULL, not 0, for an empty array: it has no dimensions at all. Any dimension
    /// other than 1 is NULL too, since a CamusDB array has exactly one.
    /// </summary>
    [TestCase("SELECT array_length(ARRAY[], 1)")]
    [TestCase("SELECT array_length(ARRAY[1, 2], 2)")]
    [TestCase("SELECT array_length(ARRAY[1, 2], 0)")]
    [TestCase("SELECT array_length(ARRAY[1, 2], -1)")]
    [TestCase("SELECT array_length(NULL, 1)")]
    [TestCase("SELECT array_length(ARRAY[1, 2], NULL)")]
    public async Task ArrayLength_IsNullForEmptyArraysOtherDimensionsAndNulls(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, sql)).Type, sql);
    }

    [TestCase("SELECT array_length('abc', 1)", "to be an array")]
    [TestCase("SELECT array_length(ARRAY[1], 1.0)", "Integer64")]
    [TestCase("SELECT array_length(ARRAY[1], '1')", "Integer64")]
    [TestCase("SELECT array_length(ARRAY[1])", "expects")]
    public async Task ArrayLength_WrongArgumentsAreErrors(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, sql), sql)!;
        StringAssert.Contains(message, ex.Message, sql);
    }

    [Test]
    public async Task ArrayLength_OnAColumn_DiffersFromCardinalityOnlyForEmptyArrays()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, array_length(tags, 1) AS len, cardinality(tags) AS card FROM tagged ORDER BY id");

        // Rows: 3 elements, 1 element, empty, NULL array, 2 elements (one NULL).
        Assert.AreEqual(3L, rows[0].Row["len"].LongValue);
        Assert.AreEqual(1L, rows[1].Row["len"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["len"].Type);
        Assert.AreEqual(0L, rows[2].Row["card"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[3].Row["len"].Type);
        Assert.AreEqual(2L, rows[4].Row["len"].LongValue);
    }

    /// <summary>
    /// The trap PostgreSQL users hit: <c>CHECK (array_length(tags, 1) &gt;= 1)</c> does not reject an
    /// empty array, because the length is NULL and a CHECK passes on UNKNOWN. <c>cardinality</c>
    /// counts an empty array as 0, so it does reject one.
    /// </summary>
    [Test]
    public async Task Check_ArrayLengthAcceptsAnEmptyArray_CardinalityRejectsIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE by_length (id int64 PRIMARY KEY, tags array(string) CHECK (array_length(tags, 1) >= 1))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO by_length (id, tags) VALUES (1, ARRAY[])");
        Assert.AreEqual(1, (await ExecQuery(database, executor, dbname, "SELECT id FROM by_length")).Count);

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE by_cardinality (id int64 PRIMARY KEY, tags array(string) CHECK (cardinality(tags) >= 1))");
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO by_cardinality (id, tags) VALUES (1, ARRAY[])"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    [Test]
    public async Task ArrayLength_InWhereAndViewBody()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> filtered = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE array_length(tags, 1) > 1 ORDER BY id");
        CollectionAssert.AreEqual(new[] { 1L, 5L }, filtered.Select(r => r.Row["id"].LongValue).ToArray());

        await ExecDdl(database, executor, dbname, "CREATE VIEW tag_lengths AS SELECT id, array_length(tags, 1) AS len FROM tagged");
        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname, "SELECT len FROM tag_lengths WHERE id = 1");
        Assert.AreEqual(3L, viaView.Single().Row["len"].LongValue);

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW tag_lengths"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("array_length(tags, 1)", ddl);
    }

    // ── array_contains ──────────────────────────────────────────────────────

    [TestCase("SELECT array_contains(ARRAY['a', 'b'], 'b')", true)]
    [TestCase("SELECT array_contains(ARRAY['a', 'b'], 'c')", false)]
    [TestCase("SELECT array_contains(ARRAY[1, 2], 2)", true)]
    [TestCase("SELECT array_contains(ARRAY[1, 2], 2.0)", true)]
    [TestCase("SELECT array_contains(ARRAY[1.5, 2.5], 2.5)", true)]
    [TestCase("SELECT array_contains(ARRAY['a', NULL], 'a')", true)]
    [TestCase("SELECT array_contains(ARRAY[1, 2], 'x')", false)]
    [TestCase("SELECT array_contains(ARRAY[], 'a')", false)]
    [TestCase("SELECT array_contains(ARRAY[], NULL)", false)]
    public async Task ArrayContains_ReturnsTrueOrFalse(string sql, bool expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, sql);
        Assert.AreEqual(ColumnType.Bool, value.Type, sql);
        Assert.AreEqual(expected, value.BoolValue, sql);
    }

    [TestCase("SELECT array_contains(NULL, 'a')")]
    [TestCase("SELECT array_contains(ARRAY['a'], NULL)")]
    [TestCase("SELECT array_contains(ARRAY['a', NULL], 'b')")]
    public async Task ArrayContains_IsUnknownWhereInIsUnknown(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, sql)).Type, sql);
    }

    /// <summary>
    /// <c>array_contains(a, x)</c> and <c>x IN (...)</c> over the same elements must give the same
    /// answer in every case, including the NULL ones; they share one equality rule.
    /// </summary>
    [TestCase("'b'", "'a', 'b'")]
    [TestCase("'c'", "'a', 'b'")]
    [TestCase("2.0", "1, 2")]
    [TestCase("'b'", "'a', NULL")]
    [TestCase("'a'", "'a', NULL")]
    [TestCase("NULL", "'a'")]
    public async Task ArrayContains_AgreesWithIn(string value, string elements)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue viaFunction = await ExecScalar(database, executor, dbname, $"SELECT array_contains(ARRAY[{elements}], {value})");
        ColumnValue viaIn = await ExecScalar(database, executor, dbname, $"SELECT {value} IN ({elements})");

        Assert.AreEqual(viaIn.Type, viaFunction.Type, $"{value} in [{elements}]");
        if (viaIn.Type == ColumnType.Bool)
            Assert.AreEqual(viaIn.BoolValue, viaFunction.BoolValue, $"{value} in [{elements}]");
    }

    [Test]
    public async Task ArrayContains_OnANonArrayIsAnError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT array_contains('abc', 'a')"))!;
        StringAssert.Contains("to be an array", ex.Message);
    }

    [Test]
    public async Task ArrayContains_OnAColumn_InWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE array_contains(tags, 'red') ORDER BY id");
        CollectionAssert.AreEqual(new[] { 1L, 5L }, rows.Select(r => r.Row["id"].LongValue).ToArray());

        // Row 5 holds a NULL element and no 'blue': UNKNOWN, so NOT does not select it either.
        List<QueryResultRow> negated = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE NOT array_contains(tags, 'blue') ORDER BY id");
        CollectionAssert.AreEqual(new[] { 3L }, negated.Select(r => r.Row["id"].LongValue).ToArray());
    }

    // ── CHECK constraints and views ─────────────────────────────────────────

    [Test]
    public async Task Check_LimitsLengthAndBansAValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE posts (id int64 PRIMARY KEY, " +
            "tags array(string) CHECK (cardinality(tags) <= 3 AND NOT array_contains(tags, 'banned')))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO posts (id, tags) VALUES (1, ARRAY['a', 'b', 'c']), (2, ARRAY[]), (3, NULL)");

        foreach (string sql in new[]
        {
            "INSERT INTO posts (id, tags) VALUES (4, ARRAY['a', 'b', 'c', 'd'])",
            "INSERT INTO posts (id, tags) VALUES (5, ARRAY['banned'])",
            "UPDATE posts SET tags = ARRAY['x', 'banned'] WHERE id = 1",
        })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, sql), sql)!;
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code, sql);
        }

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM posts ORDER BY id");
        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L }, rows.Select(r => r.Row["id"].LongValue).ToArray());
    }

    /// <summary>
    /// A CHECK passes on UNKNOWN. An array with a NULL element and no banned value makes
    /// <c>array_contains</c> UNKNOWN, so the row is accepted — the same as a CHECK on <c>NOT IN</c>.
    /// </summary>
    [Test]
    public async Task Check_AcceptsARowWhereTheTestIsUnknown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE posts (id int64 PRIMARY KEY, tags array(string) CHECK (NOT array_contains(tags, 'banned')))");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO posts (id, tags) VALUES (1, ARRAY['ok', NULL])");

        Assert.AreEqual(1, (await ExecQuery(database, executor, dbname, "SELECT id FROM posts")).Count);
    }

    [Test]
    public async Task ViewBody_UsesBothFunctions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW tag_stats AS SELECT id, cardinality(tags) AS n, array_contains(tags, 'blue') AS has_blue FROM tagged");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id, n, has_blue FROM tag_stats ORDER BY id");
        Assert.AreEqual(3L, rows[0].Row["n"].LongValue);
        Assert.IsTrue(rows[0].Row["has_blue"].BoolValue);
        Assert.IsFalse(rows[2].Row["has_blue"].BoolValue);

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW tag_stats"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("array_contains(tags, 'blue')", ddl);
    }
}
