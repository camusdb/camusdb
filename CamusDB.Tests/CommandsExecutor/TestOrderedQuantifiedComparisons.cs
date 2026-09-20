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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the quantified comparisons that are not membership tests:
/// <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, <c>&gt;=</c> with any quantifier, plus <c>= ALL</c> and
/// <c>&lt;&gt; ANY</c>. The membership forms (<c>= ANY</c>, <c>= SOME</c>, <c>&lt;&gt; ALL</c>) are
/// covered by <see cref="TestQuantifiedComparisons"/>, because the parser gives them a different
/// shape.
///
/// <para>The oracle is the expansion the fold is defined by: <c>x op ANY (a1, a2)</c> is
/// <c>(x op a1) OR (x op a2)</c> and <c>x op ALL (a1, a2)</c> is <c>(x op a1) AND (x op a2)</c>,
/// with SQL three-valued OR and AND. Comparing against that expansion, rather than against an
/// expected literal per case, is what makes every NULL case a real check: the engine already
/// implements three-valued OR and AND, and the fold must not invent its own.</para>
/// </summary>
[NonParallelizable]
public sealed class TestOrderedQuantifiedComparisons : SharedNodeBaseTest
{
    private static readonly string[] Operators = ["<", "<=", ">", ">=", "=", "<>"];

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
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
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

    private static async Task<long[]> Ids(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null) =>
        (await ExecQuery(database, executor, dbname, sql, parameters)).Select(r => r.Row["id"].LongValue).ToArray();

    /// <summary>
    /// <c>vals</c>: x is 1, 2, 3 and NULL. <c>scored</c>: arrays with three elements, one element,
    /// an empty array, a NULL array, and an array holding a NULL element. <c>allowed</c>: the
    /// subquery source. <c>nothing</c>: an empty subquery source. <c>maybe</c>: a source whose only
    /// rows are NULL.
    /// </summary>
    private static async Task Seed(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE vals (id int64 PRIMARY KEY, x int64)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO vals (id, x) VALUES (1, 1), (2, 2), (3, 3), (4, NULL)");

        await ExecDdl(database, executor, dbname, "CREATE TABLE scored (id int64 PRIMARY KEY, scores array(int64))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO scored (id, scores) VALUES " +
            "(1, ARRAY[2, 4, 6]), (2, ARRAY[5]), (3, ARRAY[]), (4, NULL), (5, ARRAY[2, NULL])");

        await ExecDdl(database, executor, dbname, "CREATE TABLE allowed (id int64 PRIMARY KEY, y int64)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO allowed (id, y) VALUES (10, 1), (11, 3)");

        await ExecDdl(database, executor, dbname, "CREATE TABLE nothing (id int64 PRIMARY KEY, y int64)");

        await ExecDdl(database, executor, dbname, "CREATE TABLE maybe (id int64 PRIMARY KEY, y int64)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO maybe (id, y) VALUES (20, NULL), (21, NULL)");
    }

    private static void AssertSameValue(ColumnValue expected, ColumnValue actual, string sql)
    {
        Assert.AreEqual(expected.Type, actual.Type, sql);

        if (expected.Type == ColumnType.Bool)
            Assert.AreEqual(expected.BoolValue, actual.BoolValue, sql);
    }

    private static void AssertBool(bool expected, ColumnValue actual, string sql)
    {
        Assert.AreEqual(ColumnType.Bool, actual.Type, sql);
        Assert.AreEqual(expected, actual.BoolValue, sql);
    }

    private static void AssertUnknown(ColumnValue actual, string sql) =>
        Assert.AreEqual(ColumnType.Null, actual.Type, sql);

    // ── the fold equals the OR / AND expansion it is defined by ─────────────

    /// <summary>
    /// Every operator, both quantifiers, against a two-element array. The cases cover a NULL left
    /// operand, a NULL element with a decisive element beside it, and a NULL element with no
    /// decisive element — the three shapes the three-valued rule turns on.
    /// </summary>
    [TestCase("1", "1", "2")]
    [TestCase("2", "1", "3")]
    [TestCase("9", "1", "3")]
    [TestCase("0", "1", "3")]
    [TestCase("NULL", "1", "3")]
    [TestCase("2", "1", "NULL")]
    [TestCase("2", "3", "NULL")]
    [TestCase("2", "NULL", "NULL")]
    public async Task LiteralArray_EqualsTheOrAndExpansion(string x, string first, string second)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string op in Operators)
        {
            ColumnValue anyExpected = await ExecScalar(database, executor, dbname,
                $"SELECT ({x} {op} {first}) OR ({x} {op} {second})");

            ColumnValue allExpected = await ExecScalar(database, executor, dbname,
                $"SELECT ({x} {op} {first}) AND ({x} {op} {second})");

            foreach (string quantifier in new[] { "ANY", "SOME" })
            {
                string sql = $"SELECT {x} {op} {quantifier} (ARRAY[{first}, {second}])";
                AssertSameValue(anyExpected, await ExecScalar(database, executor, dbname, sql), sql);
            }

            string allSql = $"SELECT {x} {op} ALL (ARRAY[{first}, {second}])";
            AssertSameValue(allExpected, await ExecScalar(database, executor, dbname, allSql), allSql);
        }
    }

    /// <summary>
    /// The quantifier word is not case-sensitive, and <c>!=</c> is <c>&lt;&gt;</c>.
    /// </summary>
    [Test]
    public async Task SpellingVariants_GiveTheSameAnswer()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string sql in new[]
                 {
                     "SELECT 2 > any (ARRAY[1, 5])",
                     "SELECT 2 > Some (ARRAY[1, 5])",
                     "SELECT 2 > ANY (ARRAY[1, 5])",
                 })
        {
            AssertBool(true, await ExecScalar(database, executor, dbname, sql), sql);
        }

        AssertBool(true, await ExecScalar(database, executor, dbname, "SELECT 2 != ANY (ARRAY[1, 2])"),
            "!= ANY is <> ANY");
        AssertBool(false, await ExecScalar(database, executor, dbname, "SELECT 2 = ALL (ARRAY[1, 2])"),
            "= ALL over a non-constant set");
    }

    // ── the empty set and the NULL array ────────────────────────────────────

    /// <summary>
    /// An empty set has nothing to compare, so <c>ANY</c> is FALSE and <c>ALL</c> is TRUE — even for
    /// a NULL left operand, which never reaches a comparison. The rule holds for a written
    /// <c>ARRAY[]</c>, for an empty array column, and for a subquery with no rows.
    /// </summary>
    [Test]
    public async Task EmptySet_IsFalseForAnyAndTrueForAll()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        foreach (string x in new[] { "1", "NULL" })
        {
            foreach (string op in Operators)
            {
                string anySql = $"SELECT {x} {op} ANY (ARRAY[])";
                string allSql = $"SELECT {x} {op} ALL (ARRAY[])";

                AssertBool(false, await ExecScalar(database, executor, dbname, anySql), anySql);
                AssertBool(true, await ExecScalar(database, executor, dbname, allSql), allSql);
            }
        }

        // The empty array column: row 3 of scored.
        AssertBool(false, await ExecScalar(database, executor, dbname,
            "SELECT 1 < ANY (scores) FROM scored WHERE id = 3"), "empty array column, ANY");
        AssertBool(true, await ExecScalar(database, executor, dbname,
            "SELECT 1 < ALL (scores) FROM scored WHERE id = 3"), "empty array column, ALL");

        // The empty subquery.
        AssertBool(false, await ExecScalar(database, executor, dbname,
            "SELECT 1 < ANY (SELECT y FROM nothing)"), "empty subquery, ANY");
        AssertBool(true, await ExecScalar(database, executor, dbname,
            "SELECT 1 < ALL (SELECT y FROM nothing)"), "empty subquery, ALL");
    }

    /// <summary>
    /// A NULL array is not an empty array: nothing is known about the set, so the answer is UNKNOWN
    /// for both quantifiers. Row 4 of <c>scored</c> holds one.
    /// </summary>
    [Test]
    public async Task NullArray_IsUnknown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        foreach (string quantifier in new[] { "ANY", "ALL" })
        {
            string sql = $"SELECT 1 < {quantifier} (scores) FROM scored WHERE id = 4";
            AssertUnknown(await ExecScalar(database, executor, dbname, sql), sql);
        }
    }

    // ── an array column, a parameter, and a WHERE clause ────────────────────

    /// <summary>
    /// The right operand may be an array column. The expected rows are worked out per row of
    /// <c>scored</c>: [2,4,6], [5], [], NULL, [2, NULL].
    /// </summary>
    [Test]
    public async Task ArrayColumn_FiltersRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        // 3 < ANY: [2,4,6] yes; [5] yes; [] no; NULL unknown; [2, NULL] unknown.
        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname, "SELECT id FROM scored WHERE 3 < ANY (scores) ORDER BY id"));

        // 1 < ALL: [2,4,6] yes; [5] yes; [] yes; NULL unknown; [2, NULL] unknown.
        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM scored WHERE 1 < ALL (scores) ORDER BY id"));

        // 2 = ALL: [2,4,6] no; [5] no; [] yes; NULL unknown; [2, NULL] unknown.
        CollectionAssert.AreEqual(new[] { 3L },
            await Ids(database, executor, dbname, "SELECT id FROM scored WHERE 2 = ALL (scores) ORDER BY id"));

        // 2 <> ANY: [2,4,6] yes; [5] yes; [] no; NULL unknown; [2, NULL] unknown (2<>2 is false,
        // and the NULL element leaves the rest unknown).
        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname, "SELECT id FROM scored WHERE 2 <> ANY (scores) ORDER BY id"));
    }

    /// <summary>An array parameter is an ordinary array-valued right operand.</summary>
    [Test]
    public async Task ArrayParameter_IsAcceptedAsTheRightOperand()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@bounds"] = ColumnValue.FromArray(ColumnType.Integer64,
            [
                new ColumnValue(ColumnType.Integer64, 2L),
                new ColumnValue(ColumnType.Integer64, 3L),
            ]),
        };

        CollectionAssert.AreEqual(new[] { 1L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x < ALL (@bounds) ORDER BY id", parameters));

        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x < ANY (@bounds) ORDER BY id", parameters));
    }

    /// <summary>A DELETE takes the same predicate, through the same rewrite and evaluator.</summary>
    [Test]
    public async Task Delete_UsesTheSamePredicate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "DELETE FROM vals WHERE x >= ALL (ARRAY[1, 2])");

        // x is 2 or 3 for the deleted rows; the NULL row is UNKNOWN and stays.
        CollectionAssert.AreEqual(new[] { 1L, 4L },
            await Ids(database, executor, dbname, "SELECT id FROM vals ORDER BY id"));
    }

    /// <summary>
    /// An UPDATE takes the form in its WHERE clause. A SET value is not tested: the grammar builds
    /// a SET value from <c>expr</c>, which holds no comparison at all, so <c>SET hit = n &gt; 1</c>
    /// is already a syntax error and this form inherits that.
    /// </summary>
    [Test]
    public async Task Update_UsesTheFormInWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "UPDATE vals SET x = 0 WHERE x > ALL (ARRAY[1, 2])");

        // Only x = 3 is above every bound; the NULL row is UNKNOWN and is not updated.
        CollectionAssert.AreEqual(new[] { 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = 0 ORDER BY id"));
    }

    /// <summary>
    /// UNKNOWN must stay UNKNOWN under a <c>NOT</c>. A fold that collapsed it to FALSE would make
    /// <c>NOT (…)</c> TRUE and would keep rows that must be dropped.
    /// </summary>
    [Test]
    public async Task Not_OverTheForm_KeepsUnknownUnknown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertUnknown(await ExecScalar(database, executor, dbname, "SELECT NOT (NULL < ANY (ARRAY[1, 2]))"),
            "NOT UNKNOWN is UNKNOWN");
        AssertUnknown(await ExecScalar(database, executor, dbname, "SELECT NOT (5 < ALL (ARRAY[9, NULL]))"),
            "an undecided NULL element keeps the fold UNKNOWN");
        AssertBool(true, await ExecScalar(database, executor, dbname, "SELECT NOT (1 < ANY (ARRAY[0]))"),
            "NOT FALSE is TRUE");
    }

    /// <summary>The form is an ordinary boolean expression: it may be projected and ordered by.</summary>
    [Test]
    public async Task Projection_And_OrderBy_AcceptTheForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, x >= ALL (ARRAY[1, 2]) AS top FROM vals ORDER BY id");

        Assert.AreEqual(4, rows.Count);
        Assert.AreEqual(false, rows[0].Row["top"].BoolValue);
        Assert.AreEqual(true, rows[1].Row["top"].BoolValue);
        Assert.AreEqual(true, rows[2].Row["top"].BoolValue);
        Assert.AreEqual(ColumnType.Null, rows[3].Row["top"].Type);

        CollectionAssert.AreEqual(new[] { 3L, 2L, 1L },
            await Ids(database, executor, dbname,
                "SELECT id FROM vals WHERE x IS NOT NULL ORDER BY x > ANY (ARRAY[2]) DESC, id DESC"));
    }

    /// <summary>
    /// A HAVING clause may hold the form over an aggregate. The aggregate's value comes from the
    /// post-aggregation workspace row, so both operands are resolved there before the fold runs.
    /// </summary>
    [Test]
    public async Task Having_AcceptsTheFormOverAnAggregate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE hits (id int64 PRIMARY KEY, k int64)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO hits (id, k) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 3), (6, 3)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT k FROM hits GROUP BY k HAVING count(*) > ALL (ARRAY[1, 2]) ORDER BY k");

        // Only k = 1 has three rows, which is above every bound.
        CollectionAssert.AreEqual(new[] { 1L }, rows.Select(r => r.Row["k"].LongValue).ToArray());

        List<QueryResultRow> any = await ExecQuery(database, executor, dbname,
            "SELECT k FROM hits GROUP BY k HAVING count(*) > ANY (ARRAY[1, 2]) ORDER BY k");

        // k = 1 has three rows and k = 3 has two, and both are above one of the bounds.
        CollectionAssert.AreEqual(new[] { 1L, 3L }, any.Select(r => r.Row["k"].LongValue).ToArray());
    }

    // ── subquery forms ──────────────────────────────────────────────────────

    /// <summary>
    /// Both spellings of the subquery form — <c>ANY (SELECT …)</c> and <c>ANY ((SELECT …))</c> —
    /// build the same node, and the rewrite gives the fold the rows the subquery returned.
    /// <c>allowed.y</c> is 1 and 3.
    /// </summary>
    [Test]
    public async Task Subquery_FoldsOverTheReturnedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname,
                "SELECT id FROM vals WHERE x < ANY (SELECT y FROM allowed) ORDER BY id"));

        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname,
                "SELECT id FROM vals WHERE x < ANY ((SELECT y FROM allowed)) ORDER BY id"));

        CollectionAssert.AreEqual(new[] { 3L },
            await Ids(database, executor, dbname,
                "SELECT id FROM vals WHERE x >= ALL (SELECT y FROM allowed) ORDER BY id"));
    }

    /// <summary>
    /// A subquery row that is NULL keeps its meaning: it makes an otherwise undecided fold UNKNOWN.
    /// <c>maybe.y</c> is NULL in every row, so no comparison can decide.
    /// </summary>
    [Test]
    public async Task Subquery_WithOnlyNullRows_IsUnknown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        foreach (string quantifier in new[] { "ANY", "ALL" })
        {
            string sql = $"SELECT 1 < {quantifier} (SELECT y FROM maybe)";
            AssertUnknown(await ExecScalar(database, executor, dbname, sql), sql);
        }

        // A NULL row does not hide a decisive one: one row of allowed is 3, so 1 < ANY is TRUE even
        // with the NULL rows of maybe in the set.
        AssertBool(true, await ExecScalar(database, executor, dbname,
            "SELECT 1 < ANY (SELECT y FROM allowed)"), "a decisive row decides");
    }

    /// <summary>
    /// A correlated subquery is rejected, for the reason <c>IN</c> rejects one: the subquery runs
    /// once, before the outer scan, so an outer column has no value to read.
    /// </summary>
    [Test]
    public async Task CorrelatedSubquery_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname,
                "SELECT id FROM vals WHERE x < ANY (SELECT y FROM allowed WHERE y = vals.x)"));
    }

    // ── stored text: a CHECK constraint and a view body ─────────────────────

    /// <summary>
    /// A CHECK is stored as text and enforced against the AST re-parsed from it, so this both
    /// asserts the rendered form and proves it re-parses to the same predicate: the rows that must
    /// be rejected are rejected.
    /// </summary>
    [Test]
    public async Task Check_RendersAndReParsesToTheSamePredicate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE runs (id int64 PRIMARY KEY, scores array(int64) CHECK (0 < ALL (scores)), " +
            "top int64 CHECK (top >= ANY (ARRAY[10, 20])))");

        // Accepted: every score above zero; an empty array (ALL over nothing is TRUE); a NULL array
        // and an array with a NULL element (both UNKNOWN, and a CHECK passes on UNKNOWN).
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO runs (id, scores, top) VALUES (1, ARRAY[1, 2], 20), (2, ARRAY[], 10), " +
            "(3, NULL, 99), (4, ARRAY[1, NULL], 10)");

        foreach (string bad in new[]
                 {
                     "INSERT INTO runs (id, scores, top) VALUES (5, ARRAY[1, 0], 20)",
                     "INSERT INTO runs (id, scores, top) VALUES (6, ARRAY[1], 5)",
                 })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, bad), bad)!;
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code, bad);
        }

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE TABLE runs"))[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("0 < ALL (scores)", ddl);
        StringAssert.Contains("top >= ANY (ARRAY[10, 20])", ddl);
    }

    /// <summary>A CHECK may not hold a subquery, in this form as in every other.</summary>
    [Test]
    public async Task Check_WithASubquery_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname,
                "CREATE TABLE bad (id int64 PRIMARY KEY, n int64 CHECK (n < ALL (SELECT y FROM allowed)))"))!;

        StringAssert.Contains("subqueries", ex.Message);
    }

    /// <summary>
    /// A view body is stored as text too. The array form and the subquery form must both render and
    /// re-parse, and the view must return the rows the direct query returns.
    /// </summary>
    [Test]
    public async Task ViewBody_RendersAndQueries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW v_lit AS SELECT id FROM vals WHERE x >= ALL (ARRAY[1, 2])");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW v_col AS SELECT id FROM scored WHERE 3 < ANY (scores)");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW v_sub AS SELECT id FROM vals WHERE x < ANY (SELECT y FROM allowed)");

        CollectionAssert.AreEqual(new[] { 2L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM v_lit ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname, "SELECT id FROM v_col ORDER BY id"));

        async Task<string> Shown(string view) =>
            (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW " + view))[0].Row["create view"].StrValue!;

        StringAssert.Contains("x >= ALL (ARRAY[1, 2])", await Shown("v_lit"));
        StringAssert.Contains("3 < ANY (scores)", await Shown("v_col"));
        StringAssert.Contains("x < ANY ((SELECT", await Shown("v_sub"));
    }

    /// <summary>EXPLAIN names the form, so a plan reads as the statement was written.</summary>
    [Test]
    public async Task Explain_NamesTheForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        string plan = string.Join("\n", (await ExecQuery(database, executor, dbname,
                "EXPLAIN SELECT id FROM scored WHERE 3 < ANY (scores)"))
            .Select(r => r.Row.TryGetValue("detail", out ColumnValue? d) ? d?.StrValue ?? "" : ""));

        StringAssert.Contains("< ANY", plan);
    }

    // ── type errors keep the meaning of one comparison ──────────────────────

    /// <summary>
    /// The quantifier changes how many comparisons run, never what one comparison means. A mixed
    /// numeric pair widens here as it widens for the scalar operator, and an incomparable pair
    /// fails here in exactly the way the scalar operator fails.
    ///
    /// <para>The failure is asserted as parity with <c>1 &lt; 'a'</c> rather than against a named
    /// exception, because the scalar path lets <c>ColumnValue.CompareTo</c> raise a plain
    /// <c>ArgumentException</c> instead of a domain error. That is pre-existing behavior of the
    /// scalar operator; the point of this test is that the quantified form does not diverge from
    /// it.</para>
    /// </summary>
    [Test]
    public async Task OneElementComparison_BehavesLikeTheScalarOperator()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertBool(true, await ExecScalar(database, executor, dbname, "SELECT 2 > ANY (ARRAY[1.5, 9.5])"),
            "a mixed numeric pair widens");

        Exception scalar = Assert.CatchAsync(async () =>
            await ExecQuery(database, executor, dbname, "SELECT 1 < 'a'"))!;

        Exception quantified = Assert.CatchAsync(async () =>
            await ExecQuery(database, executor, dbname, "SELECT 1 < ALL (ARRAY['a', 'b'])"))!;

        Assert.AreEqual(scalar.GetType(), quantified.GetType());
        Assert.AreEqual(scalar.Message, quantified.Message);
    }

    /// <summary>A right operand that is not an array and not a subquery is a type error.</summary>
    [Test]
    public async Task NonArrayRightOperand_IsATypeError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT 1 < ALL (2)"))!;

        StringAssert.Contains("must be an array or a subquery", ex.Message);
    }
}
