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
/// End-to-end coverage for four PostgreSQL expression forms: digit separators in numeric literals
/// (<c>200_000</c>), the <c>%</c> operator, the postfix <c>::</c> cast, and array subscripts
/// (<c>arr[n]</c>). Each is driven through SQL, and each is also exercised where the tree is
/// rendered back to text and parsed again — a CHECK constraint and a view body — because that is
/// where an operator the renderer cannot print fails, long after the statement that created it.
/// </summary>
[NonParallelizable]
public sealed class TestPostgresExpressionSyntax : SharedNodeBaseTest
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
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null,
        QuerySchemaHolder? schemaOut = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, null, schemaOut);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
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

    /// <summary>A table with an array column; row 3 holds a NULL array.</summary>
    private static async Task SeedTags(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE tagged (id int64 PRIMARY KEY, tags array(string), n int64)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO tagged (id, tags, n) VALUES " +
            "(1, ARRAY['red', 'green'], 10), (2, ARRAY['blue'], 11), (3, NULL, 12)");
    }

    // ── digit separators ────────────────────────────────────────────────────

    [Test]
    public async Task DigitSeparators_AreAcceptedInIntegersDecimalsAndExponents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue integer = await ExecScalar(database, executor, dbname, "SELECT 200_000");
        Assert.AreEqual(ColumnType.Integer64, integer.Type);
        Assert.AreEqual(200_000L, integer.LongValue);

        Assert.AreEqual(-1_000_000L, (await ExecScalar(database, executor, dbname, "SELECT -1_000_000")).LongValue);
        Assert.AreEqual(1000.0005, (await ExecScalar(database, executor, dbname, "SELECT 1_000.000_5")).FloatValue, 1e-9);
        Assert.AreEqual(1e10, (await ExecScalar(database, executor, dbname, "SELECT 1e1_0")).FloatValue, 1e-3);
    }

    [Test]
    public async Task DigitSeparators_WorkWhereTheParserReadsTheDigitsItself()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        // LIMIT reads the token text directly, not through literal evaluation.
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged ORDER BY id LIMIT 0_2");
        Assert.AreEqual(2, rows.Count);

        // A declared string length is also read from the token text.
        await ExecDdl(database, executor, dbname, "CREATE TABLE sized (id int64 PRIMARY KEY, s string(1_0))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO sized (id, s) VALUES (1, 'abcdefghij')");
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO sized (id, s) VALUES (2, 'abcdefghijk')"));
    }

    [TestCase("SELECT 200_")]
    [TestCase("SELECT 2__0")]
    [TestCase("SELECT 1_.5")]
    [TestCase("SELECT 1.5_")]
    [TestCase("SELECT 1_e5")]
    [TestCase("SELECT -1_")]
    [TestCase("SELECT 1_000abc")]
    public async Task DigitSeparators_MisplacedUnderscoreIsASyntaxError(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertQueryThrows(database, executor, dbname, sql);
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code, sql);
        StringAssert.Contains("Invalid numeric literal", ex.Message, sql);
    }

    [Test]
    public async Task DigitSeparators_LeaveUnderscoreIdentifiersAlone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE u (id int64 PRIMARY KEY, _000 int64)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO u (id, _000) VALUES (1, 7)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT _000 FROM u");
        Assert.AreEqual(7L, rows.Single().Row["_000"].LongValue);
    }

    // ── % ───────────────────────────────────────────────────────────────────

    [Test]
    public async Task Modulo_MatchesModFunctionForEveryOperandShape()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string op, string fn) in new[]
        {
            ("7 % 3", "mod(7, 3)"),
            ("-7 % 3", "mod(-7, 3)"),
            ("7 % -3", "mod(7, -3)"),
            ("7.5 % 2", "mod(7.5, 2)"),
            ("NULL % 3", "mod(NULL, 3)"),
        })
        {
            ColumnValue viaOperator = await ExecScalar(database, executor, dbname, "SELECT " + op);
            ColumnValue viaFunction = await ExecScalar(database, executor, dbname, "SELECT " + fn);
            Assert.AreEqual(viaFunction.Type, viaOperator.Type, op);
            Assert.AreEqual(0, viaFunction.CompareTo(viaOperator), op);
        }

        Assert.AreEqual(1L, (await ExecScalar(database, executor, dbname, "SELECT 7 % 3")).LongValue);
        Assert.AreEqual(-1L, (await ExecScalar(database, executor, dbname, "SELECT -7 % 3")).LongValue);
        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, "SELECT NULL % 3")).Type);
    }

    [Test]
    public async Task Modulo_HasTheSamePrecedenceAsMultiplication()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // 1 + ((7 % 3) * 2), not (1 + 7) % (3 * 2).
        Assert.AreEqual(3L, (await ExecScalar(database, executor, dbname, "SELECT 1 + 7 % 3 * 2")).LongValue);
        Assert.AreEqual(2L, (await ExecScalar(database, executor, dbname, "SELECT (1 + 7) % 3")).LongValue);

        // Left-associative: (17 % 5) % 3 = 2, whereas 17 % (5 % 3) = 1.
        Assert.AreEqual(2L, (await ExecScalar(database, executor, dbname, "SELECT 17 % 5 % 3")).LongValue);
    }

    [Test]
    public async Task Modulo_ByZeroIsAnError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertQueryThrows(database, executor, dbname, "SELECT 7 % 0");
        StringAssert.Contains("division by zero", ex.Message);
    }

    [Test]
    public async Task Modulo_LeavesPercentInsideLikePatternsAlone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.IsTrue((await ExecScalar(database, executor, dbname, "SELECT 'abc' LIKE 'a%'")).BoolValue);
    }

    [Test]
    public async Task Modulo_WorksInWhereOrderByCheckAndViewBody()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE evens (id int64 PRIMARY KEY, v int64 CHECK (v % 2 = 0))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO evens (id, v) VALUES (1, 4), (2, 10), (3, 6)");

        // The CHECK is stored as rendered text and parsed again, so this proves the round trip.
        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO evens (id, v) VALUES (4, 5)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);

        List<QueryResultRow> filtered = await ExecQuery(database, executor, dbname,
            "SELECT id FROM evens WHERE v % 4 = 2 ORDER BY v % 3, id");
        CollectionAssert.AreEqual(new[] { 3L, 2L }, filtered.Select(r => r.Row["id"].LongValue).ToArray());

        await ExecDdl(database, executor, dbname, "CREATE VIEW evens_mod AS SELECT id, v % 3 AS r FROM evens");
        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname, "SELECT id, r FROM evens_mod ORDER BY id");
        CollectionAssert.AreEqual(new[] { 1L, 1L, 0L }, viaView.Select(r => r.Row["r"].LongValue).ToArray());

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW evens_mod"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("mod(v, 3)", ddl);
    }

    /// <summary>
    /// A call with two or more arguments carries them in an argument-list chain, which the SQL
    /// renderer must flatten. <c>%</c> depends on this, since it becomes <c>mod(a, b)</c>, but the
    /// rule covers every multi-argument function in a CHECK or a view body.
    /// </summary>
    [Test]
    public async Task TwoArgumentCall_SurvivesCheckAndViewRoundTrips()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE pairs (id int64 PRIMARY KEY, a int64, b int64 CHECK (mod(b, 2) = 0))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO pairs (id, a, b) VALUES (1, NULL, 4)");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO pairs (id, a, b) VALUES (2, 1, 3)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);

        await ExecDdl(database, executor, dbname, "CREATE VIEW pair_values AS SELECT id, coalesce(a, b) AS v FROM pairs");
        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname, "SELECT v FROM pair_values");
        Assert.AreEqual(4L, viaView.Single().Row["v"].LongValue);
    }

    [Test]
    public async Task Modulo_RendersAsModCallInExplain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "EXPLAIN SELECT id FROM tagged WHERE n % 2 = 0");
        string plan = string.Join("\n", rows.Select(r => r.Row["detail"].StrValue));
        StringAssert.Contains("mod(n, 2)", plan);
    }

    // ── :: ──────────────────────────────────────────────────────────────────

    [Test]
    public async Task PostfixCast_BehavesLikeCast()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(43L, (await ExecScalar(database, executor, dbname, "SELECT '42'::int64 + 1")).LongValue);
        Assert.AreEqual(43L, (await ExecScalar(database, executor, dbname, "SELECT '42'::int + 1")).LongValue);
        Assert.AreEqual(43L, (await ExecScalar(database, executor, dbname, "SELECT '42'::integer + 1")).LongValue);

        ColumnValue text = await ExecScalar(database, executor, dbname, "SELECT 1::text");
        Assert.AreEqual(ColumnType.String, text.Type);
        Assert.AreEqual("1", text.StrValue);

        Assert.AreEqual(ColumnType.String, (await ExecScalar(database, executor, dbname, "SELECT random()::text")).Type);
        Assert.AreEqual(42L, (await ExecScalar(database, executor, dbname, "SELECT '42'::text::int64")).LongValue);
    }

    [Test]
    public async Task PostfixCast_BindsTighterThanEveryBinaryOperator()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Only '2' is cast. If the cast took the whole sum, adding a string would be the error.
        Assert.AreEqual(3L, (await ExecScalar(database, executor, dbname, "SELECT 1 + '2'::int64")).LongValue);
        Assert.AreEqual(7L, (await ExecScalar(database, executor, dbname, "SELECT 1 + '2'::int64 * 3")).LongValue);
        Assert.IsTrue((await ExecScalar(database, executor, dbname, "SELECT 5 > '4'::int64")).BoolValue);

        // The lexer folds the sign into a numeric literal, so the cast applies to -1.
        Assert.AreEqual("-1", (await ExecScalar(database, executor, dbname, "SELECT -1::text")).StrValue);
    }

    [Test]
    public async Task PostfixCast_AppliesToBoundParameters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, "SELECT @p::int64 + 1",
            new Dictionary<string, ColumnValue> { { "@p", new ColumnValue(ColumnType.String, "41") } });
        Assert.AreEqual(42L, value.LongValue);
    }

    [Test]
    public async Task PostfixCast_WorksInWhereAndSurvivesCheckAndViewRoundTrips()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE codes (id int64 PRIMARY KEY, code string(8) CHECK (code::int64 > 0))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO codes (id, code) VALUES (1, '4'), (2, '12')");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO codes (id, code) VALUES (3, '0')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM codes WHERE code::int64 > 5");
        Assert.AreEqual(2L, rows.Single().Row["id"].LongValue);

        await ExecDdl(database, executor, dbname, "CREATE VIEW code_numbers AS SELECT id, code::int64 AS num FROM codes");
        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname, "SELECT num FROM code_numbers ORDER BY num");
        CollectionAssert.AreEqual(new[] { 4L, 12L }, viaView.Select(r => r.Row["num"].LongValue).ToArray());
    }

    // ── subscripts ──────────────────────────────────────────────────────────

    [Test]
    public async Task Subscript_OnArrayLiteral_CountsFromOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual("a", (await ExecScalar(database, executor, dbname, "SELECT (ARRAY['a', 'b', 'c'])[1]")).StrValue);
        Assert.AreEqual("c", (await ExecScalar(database, executor, dbname, "SELECT ARRAY['a', 'b', 'c'][3]")).StrValue);
        Assert.AreEqual(2L, (await ExecScalar(database, executor, dbname, "SELECT (ARRAY[1, 2, 3])[1 + 7 % 3]")).LongValue);
    }

    [TestCase("SELECT (ARRAY['a', 'b'])[0]")]
    [TestCase("SELECT (ARRAY['a', 'b'])[3]")]
    [TestCase("SELECT (ARRAY['a', 'b'])[-1]")]
    [TestCase("SELECT (ARRAY['a', 'b'])[NULL]")]
    [TestCase("SELECT (ARRAY[])[1]")]
    public async Task Subscript_OutOfRangeOrNullIsNull(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, sql)).Type, sql);
    }

    [TestCase("SELECT 'abc'[1]", "Cannot subscript")]
    [TestCase("SELECT (ARRAY['a'])['1']", "must be an integer")]
    [TestCase("SELECT (ARRAY['a'])[1.0]", "must be an integer")]
    public async Task Subscript_WrongOperandTypesAreErrors(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertQueryThrows(database, executor, dbname, sql);
        StringAssert.Contains(message, ex.Message, sql);
    }

    [Test]
    public async Task Subscript_OnArrayColumn_InProjectionWhereAndOrderBy()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        // tags is referenced only inside the subscript: the scan must still fetch the column.
        QuerySchemaHolder schema = new();
        List<QueryResultRow> projected = await ExecQuery(database, executor, dbname,
            "SELECT tags[2] AS second FROM tagged ORDER BY id", schemaOut: schema);
        Assert.AreEqual("green", projected[0].Row["second"].StrValue);
        Assert.AreEqual(ColumnType.Null, projected[1].Row["second"].Type);
        Assert.AreEqual(ColumnType.Null, projected[2].Row["second"].Type);

        List<QueryResultRow> filtered = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE tags[1] = 'blue'");
        Assert.AreEqual(2L, filtered.Single().Row["id"].LongValue);

        List<QueryResultRow> ordered = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE tags IS NOT NULL ORDER BY tags[1]");
        CollectionAssert.AreEqual(new[] { 2L, 1L }, ordered.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task Subscript_InfersTheElementTypeForTheResultSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        QuerySchemaHolder schema = new();
        await ExecQuery(database, executor, dbname,
            "SELECT * FROM (SELECT tags[1] AS first, (ARRAY[1, 2])[n % 2 + 1] AS pick FROM tagged) AS d",
            schemaOut: schema);

        Assert.AreEqual(ColumnType.String, schema.Schema.Single(c => c.Name == "first").Type);
        Assert.AreEqual(ColumnType.Integer64, schema.Schema.Single(c => c.Name == "pick").Type);
    }

    [Test]
    public async Task Subscript_SurvivesCheckAndViewRoundTrips()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE guarded (id int64 PRIMARY KEY, tags array(string) CHECK (tags[1] <> 'bad'))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO guarded (id, tags) VALUES (1, ARRAY['ok', 'bad'])");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO guarded (id, tags) VALUES (2, ARRAY['bad'])"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW first_tags AS SELECT id, tags[1] AS first, (ARRAY['x', 'y'])[id] AS label FROM guarded");

        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname, "SELECT first, label FROM first_tags");
        Assert.AreEqual("ok", viaView.Single().Row["first"].StrValue);
        Assert.AreEqual("x", viaView.Single().Row["label"].StrValue);

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW first_tags"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("tags[1]", ddl);
        StringAssert.Contains("ARRAY['x', 'y'][", ddl);
    }

    [Test]
    public async Task Subscript_RendersInExplain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "EXPLAIN SELECT id FROM tagged WHERE tags[1] = 'red'");
        string plan = string.Join("\n", rows.Select(r => r.Row["detail"].StrValue));
        StringAssert.Contains("tags[1]", plan);
    }

    /// <summary>
    /// A constant ARRAY[...] is cached on its tree node, and the parser cache shares one tree across
    /// executions of the same SQL text. An array of placeholders must therefore never be cached, or
    /// the second execution would see the first execution's values.
    /// </summary>
    [Test]
    public async Task Subscript_OnPlaceholderArray_IsNotCachedAcrossExecutions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        const string sql = "SELECT (ARRAY[@a, @b])[2]";

        ColumnValue first = await ExecScalar(database, executor, dbname, sql, new()
        {
            { "@a", new ColumnValue(ColumnType.String, "one") },
            { "@b", new ColumnValue(ColumnType.String, "two") },
        });

        ColumnValue second = await ExecScalar(database, executor, dbname, sql, new()
        {
            { "@a", new ColumnValue(ColumnType.String, "three") },
            { "@b", new ColumnValue(ColumnType.String, "four") },
        });

        Assert.AreEqual("two", first.StrValue);
        Assert.AreEqual("four", second.StrValue);
    }

    // ── ARRAY[...] elements are full expressions ────────────────────────────

    [Test]
    public async Task ArrayElements_AcceptCastsCallsAndArithmetic()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT ARRAY[n::string, 'hello'] AS a, ARRAY[n + 1, n * 2, n % 3] AS b, " +
            "ARRAY[upper('x'), CASE WHEN n > 10 THEN 'big' ELSE 'small' END] AS c " +
            "FROM tagged WHERE id = 1");

        QueryResultRow row = rows.Single();
        CollectionAssert.AreEqual(new[] { "10", "hello" }, row.Row["a"].ArrayValues!.Select(v => v.StrValue).ToArray());
        CollectionAssert.AreEqual(new[] { 11L, 20L, 1L }, row.Row["b"].ArrayValues!.Select(v => v.LongValue).ToArray());
        CollectionAssert.AreEqual(new[] { "X", "small" }, row.Row["c"].ArrayValues!.Select(v => v.StrValue).ToArray());
    }

    /// <summary>
    /// The statement that first exposed the element restriction: an element that casts a column,
    /// next to a literal, under a primary-key filter.
    /// </summary>
    [Test]
    public async Task ArrayElements_CastOfAColumnUnderAPointLookup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE wide (id string(16) PRIMARY KEY, n int64)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO wide (id, n) VALUES ('id_0004998', 1), ('id_0004999', 42)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT ARRAY[n::string, 'hello'] FROM wide WHERE id = \"id_0004999\"");

        CollectionAssert.AreEqual(new[] { "42", "hello" }, rows.Single().Row["0"].ArrayValues!.Select(v => v.StrValue).ToArray());
    }

    [Test]
    public async Task ArrayElements_ResolveQualifiedColumnsOfAJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE TABLE owners (id int64 PRIMARY KEY, name string(16))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO owners (id, name) VALUES (1, 'ann'), (2, 'bob')");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT ARRAY[o.name, t.n::string] AS pair FROM tagged t JOIN owners o ON t.id = o.id ORDER BY o.id");

        Assert.AreEqual(2, rows.Count);
        CollectionAssert.AreEqual(new[] { "ann", "10" }, rows[0].Row["pair"].ArrayValues!.Select(v => v.StrValue).ToArray());
        CollectionAssert.AreEqual(new[] { "bob", "11" }, rows[1].Row["pair"].ArrayValues!.Select(v => v.StrValue).ToArray());
    }

    [Test]
    public async Task ArrayElements_AcceptAggregatesAndWhereSubqueries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTags(database, executor, dbname);

        List<QueryResultRow> aggregated = await ExecQuery(database, executor, dbname,
            "SELECT ARRAY[min(n), max(n)] AS bounds FROM tagged");
        CollectionAssert.AreEqual(new[] { 10L, 12L }, aggregated.Single().Row["bounds"].ArrayValues!.Select(v => v.LongValue).ToArray());

        // A scalar subquery is resolved before the scan only in WHERE; a select list with a FROM does
        // not pre-materialize one yet, with or without an array around it.
        List<QueryResultRow> withSubquery = await ExecQuery(database, executor, dbname,
            "SELECT id FROM tagged WHERE n = (ARRAY[(SELECT max(n) FROM tagged), 0])[1]");
        Assert.AreEqual(3L, withSubquery.Single().Row["id"].LongValue);
    }

    [Test]
    public async Task ArrayElements_WorkInInsertUpdateCheckAndViewBody()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE boxes (id int64 PRIMARY KEY, n int64, " +
            "tags array(string) CHECK ((ARRAY[upper(tags[1]), 'OK'])[1] <> 'BAD'))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO boxes (id, n, tags) VALUES (1, 5, ARRAY[concat('a', 'b'), 5::string])");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO boxes (id, n, tags) VALUES (2, 1, ARRAY['bad'])"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);

        await ExecNonQuery(database, executor, dbname,
            "UPDATE boxes SET tags = ARRAY[tags[2], (n * 2)::string] WHERE id = 1");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW box_pairs AS SELECT id, ARRAY[n::string, tags[1]] AS pair FROM boxes");

        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname, "SELECT pair FROM box_pairs");
        CollectionAssert.AreEqual(new[] { "5", "5" }, viaView.Single().Row["pair"].ArrayValues!.Select(v => v.StrValue).ToArray());

        List<QueryResultRow> stored = await ExecQuery(database, executor, dbname, "SELECT tags FROM boxes WHERE id = 1");
        CollectionAssert.AreEqual(new[] { "5", "10" }, stored.Single().Row["tags"].ArrayValues!.Select(v => v.StrValue).ToArray());
    }

    // ── all four together ───────────────────────────────────────────────────

    /// <summary>
    /// The data-generation statement these forms exist for, with a table in place of
    /// <c>generate_series</c> so it runs today.
    /// </summary>
    [Test]
    public async Task DataGenerationStatement_RunsEndToEnd()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE series (i int64 PRIMARY KEY)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO series (i) VALUES (1), (2), (3), (4), (200_000)");

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (order_id INT PRIMARY KEY, order_user_id INT, order_status TEXT, tag TEXT)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders " +
            "SELECT i, 1 + (i % 200_000), (ARRAY['paid', 'shipped', 'delivered'])[1+i%3], substring(random()::text, 1, 3) " +
            "FROM series");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT order_id, order_user_id, order_status, tag FROM orders ORDER BY order_id");

        CollectionAssert.AreEqual(new[] { 2L, 3L, 4L, 5L, 1L }, rows.Select(r => r.Row["order_user_id"].LongValue).ToArray());
        CollectionAssert.AreEqual(
            new[] { "shipped", "delivered", "paid", "shipped", "delivered" },
            rows.Select(r => r.Row["order_status"].StrValue).ToArray());
        Assert.IsTrue(rows.All(r => r.Row["tag"].Type == ColumnType.String && r.Row["tag"].StrValue!.Length == 3));
    }
}
