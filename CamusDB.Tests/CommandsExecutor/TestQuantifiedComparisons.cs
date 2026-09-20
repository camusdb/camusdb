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
/// End-to-end coverage for <c>x = ANY (…)</c>, <c>x = SOME (…)</c> and <c>x &lt;&gt; ALL (…)</c>.
/// The parser rewrites each one into <c>IN</c>, <c>NOT IN</c> or <c>array_contains</c>, so the tests
/// check two things: the answers equal those of the equivalent <c>IN</c> form, NULL cases included,
/// and the rendered form re-parses where text is stored (a CHECK constraint and a view body).
/// </summary>
[NonParallelizable]
public sealed class TestQuantifiedComparisons : SharedNodeBaseTest
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
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
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

    private static async Task<long[]> Ids(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null) =>
        (await ExecQuery(database, executor, dbname, sql, parameters)).Select(r => r.Row["id"].LongValue).ToArray();

    /// <summary>
    /// <c>vals</c>: x is 1, 2, 3 and NULL. <c>tagged</c>: 3 element arrays, 1 element, empty, a NULL
    /// array, and an array that holds a NULL element. <c>allowed</c>: the subquery source, with a NULL.
    /// </summary>
    private static async Task Seed(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE vals (id int64 PRIMARY KEY, x int64)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO vals (id, x) VALUES (1, 1), (2, 2), (3, 3), (4, NULL)");

        await ExecDdl(database, executor, dbname, "CREATE TABLE tagged (id int64 PRIMARY KEY, tags array(string))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO tagged (id, tags) VALUES " +
            "(1, ARRAY['red', 'green', 'blue']), (2, ARRAY['blue']), (3, ARRAY[]), (4, NULL), (5, ARRAY['red', NULL])");

        await ExecDdl(database, executor, dbname, "CREATE TABLE allowed (id int64 PRIMARY KEY, y int64)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO allowed (id, y) VALUES (10, 1), (11, 3)");
    }

    private static void AssertSameValue(ColumnValue expected, ColumnValue actual, string sql)
    {
        Assert.AreEqual(expected.Type, actual.Type, sql);
        if (expected.Type == ColumnType.Bool)
            Assert.AreEqual(expected.BoolValue, actual.BoolValue, sql);
    }

    // ── the three forms equal IN / NOT IN, NULL cases included ──────────────

    /// <summary>
    /// A match, no match, a NULL <c>x</c>, a NULL element with no match, and a NULL element with a
    /// match. Each quantified form must give the exact value (TRUE, FALSE or NULL) of its IN form.
    /// </summary>
    [TestCase("1", "1, 2")]
    [TestCase("3", "1, 2")]
    [TestCase("NULL", "1, 2")]
    [TestCase("3", "1, NULL")]
    [TestCase("1", "1, NULL")]
    [TestCase("'b'", "'a', 'b'")]
    [TestCase("2.0", "1, 2")]
    public async Task LiteralArray_GivesTheSameValueAsIn(string x, string items)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue inValue = await ExecScalar(database, executor, dbname, $"SELECT {x} IN ({items})");
        ColumnValue notInValue = await ExecScalar(database, executor, dbname, $"SELECT {x} NOT IN ({items})");

        foreach (string sql in new[] { $"SELECT {x} = ANY (ARRAY[{items}])", $"SELECT {x} = SOME (ARRAY[{items}])", $"SELECT {x} = any(ARRAY[{items}])" })
            AssertSameValue(inValue, await ExecScalar(database, executor, dbname, sql), sql);

        foreach (string sql in new[] { $"SELECT {x} <> ALL (ARRAY[{items}])", $"SELECT {x} != All (ARRAY[{items}])" })
            AssertSameValue(notInValue, await ExecScalar(database, executor, dbname, sql), sql);
    }

    /// <summary>
    /// The same equivalence on the <c>array_contains</c> path: the array comes from a call, so the
    /// rewrite does not turn it into an IN list.
    /// </summary>
    [TestCase("1", "1, 2")]
    [TestCase("3", "1, 2")]
    [TestCase("NULL", "1, 2")]
    [TestCase("3", "1, NULL")]
    [TestCase("1", "1, NULL")]
    public async Task ArrayExpression_GivesTheSameValueAsIn(string x, string items)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue inValue = await ExecScalar(database, executor, dbname, $"SELECT {x} IN ({items})");
        ColumnValue notInValue = await ExecScalar(database, executor, dbname, $"SELECT {x} NOT IN ({items})");

        string anySql = $"SELECT {x} = ANY (coalesce(ARRAY[{items}], ARRAY[]))";
        string allSql = $"SELECT {x} <> ALL (coalesce(ARRAY[{items}], ARRAY[]))";
        AssertSameValue(inValue, await ExecScalar(database, executor, dbname, anySql), anySql);
        AssertSameValue(notInValue, await ExecScalar(database, executor, dbname, allSql), allSql);
    }

    /// <summary>
    /// An empty array has nothing to compare, so <c>= ANY</c> is FALSE and <c>&lt;&gt; ALL</c> is
    /// TRUE, even for a NULL <c>x</c>. This matches PostgreSQL.
    /// </summary>
    [TestCase("SELECT 1 = ANY (ARRAY[])", false)]
    [TestCase("SELECT NULL = ANY (ARRAY[])", false)]
    [TestCase("SELECT 1 <> ALL (ARRAY[])", true)]
    [TestCase("SELECT NULL <> ALL (ARRAY[])", true)]
    public async Task EmptyArray_IsFalseForAnyAndTrueForAll(string sql, bool expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, sql);
        Assert.AreEqual(ColumnType.Bool, value.Type, sql);
        Assert.AreEqual(expected, value.BoolValue, sql);
    }

    // ── WHERE: literal arrays, array columns, parameters, subqueries ────────

    [Test]
    public async Task Where_LiteralArray_MatchesInAndNotIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CollectionAssert.AreEqual(new[] { 1L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = ANY (ARRAY[1, 3]) ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 2L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x <> ALL (ARRAY[1, 3]) ORDER BY id"));

        // A NULL element makes every non-match UNKNOWN, so <> ALL keeps no row.
        CollectionAssert.IsEmpty(
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x <> ALL (ARRAY[1, NULL]) ORDER BY id"));

        // NOT flips TRUE and FALSE but keeps UNKNOWN, so the NULL x stays out.
        CollectionAssert.AreEqual(new[] { 2L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE NOT (x = ANY (ARRAY[1])) ORDER BY id"));
    }

    /// <summary>
    /// An element that reads a column cannot move into an IN list, because an IN list item is
    /// evaluated with no row. Such an array takes the <c>array_contains</c> path and reads each row.
    /// </summary>
    [Test]
    public async Task Where_ArrayWithAColumnElement_IsEvaluatedPerRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = ANY (ARRAY[id, 99]) ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 2L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE 20 = ANY (ARRAY[x, id * 10]) ORDER BY id"));
    }

    [Test]
    public async Task Where_ArrayColumn_AnyAndAll()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CollectionAssert.AreEqual(new[] { 1L, 5L },
            await Ids(database, executor, dbname, "SELECT id FROM tagged WHERE 'red' = ANY (tags) ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 1L, 2L },
            await Ids(database, executor, dbname, "SELECT id FROM tagged WHERE 'blue' = SOME (tags) ORDER BY id"));

        // 2 has no red and 3 is empty (TRUE); 4 is a NULL array and 5 holds red, so both drop out.
        CollectionAssert.AreEqual(new[] { 2L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM tagged WHERE 'red' <> ALL (tags) ORDER BY id"));

        List<QueryResultRow> projected = await ExecQuery(database, executor, dbname,
            "SELECT id, 'green' = ANY (tags) AS has_green FROM tagged ORDER BY id");
        Assert.AreEqual(true, projected[0].Row["has_green"].BoolValue);
        Assert.AreEqual(false, projected[1].Row["has_green"].BoolValue);
        Assert.AreEqual(false, projected[2].Row["has_green"].BoolValue);
        Assert.AreEqual(ColumnType.Null, projected[3].Row["has_green"].Type);
        Assert.AreEqual(ColumnType.Null, projected[4].Row["has_green"].Type);
    }

    [Test]
    public async Task Where_ArrayParameter_AnyAndAll()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@ids", ColumnValue.FromArray(ColumnType.Integer64, new List<ColumnValue> { new(ColumnType.Integer64, 1L), new(ColumnType.Integer64, 3L) }) },
        };

        CollectionAssert.AreEqual(new[] { 1L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE id = ANY (@ids) ORDER BY id", parameters));
        CollectionAssert.AreEqual(new[] { 2L, 4L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE id <> ALL (@ids) ORDER BY id", parameters));
    }

    [Test]
    public async Task Where_Subquery_WithAndWithoutExtraParentheses()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CollectionAssert.AreEqual(new[] { 1L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = ANY (SELECT y FROM allowed) ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 1L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = SOME ((SELECT y FROM allowed)) ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 2L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x <> ALL (SELECT y FROM allowed) ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 2L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x <> ALL ((SELECT y FROM allowed)) ORDER BY id"));

        // An empty subquery: = ANY keeps nothing, <> ALL keeps every row, the NULL x too.
        CollectionAssert.IsEmpty(
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = ANY (SELECT y FROM allowed WHERE y > 100)"));
        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L, 4L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x <> ALL (SELECT y FROM allowed WHERE y > 100) ORDER BY id"));
    }

    [Test]
    public async Task Update_And_Delete_AcceptTheQuantifiedForms()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "UPDATE vals SET x = 7 WHERE id = ANY (ARRAY[1, 2])");
        await ExecNonQuery(database, executor, dbname, "DELETE FROM vals WHERE x <> ALL (ARRAY[7])");

        CollectionAssert.AreEqual(new[] { 1L, 2L, 4L },
            await Ids(database, executor, dbname, "SELECT id FROM vals ORDER BY id"));
    }

    // ── CHECK constraints and view bodies (stored text is parsed again) ─────

    [Test]
    public async Task Check_AllOverAnArrayColumn_RejectsABannedValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE posts (id int64 PRIMARY KEY, tags array(string) CHECK ('banned' <> ALL (tags)))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO posts (id, tags) VALUES (1, ARRAY['ok']), (2, ARRAY[]), (3, NULL), (4, ARRAY['ok', NULL])");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO posts (id, tags) VALUES (5, ARRAY['ok', 'banned'])"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE TABLE posts"))[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("NOT array_contains(tags, 'banned')", ddl);
    }

    [Test]
    public async Task Check_LiteralArrays_RenderAsInLists_AndEmptyArraysStayCalls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE statuses (id int64 PRIMARY KEY, " +
            "s int64 CHECK (s = ANY (ARRAY[1, 2, 3])), " +
            "t int64 CHECK (t <> ALL (ARRAY[0])), " +
            "u int64 CHECK (u <> ALL (ARRAY[])), " +
            "v int64 CHECK (v = ANY (ARRAY[]) OR v IS NULL))");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO statuses (id, s, t, u, v) VALUES (1, 2, 5, 9, NULL)");
        // A NULL s is UNKNOWN, and a CHECK passes on UNKNOWN.
        await ExecNonQuery(database, executor, dbname, "INSERT INTO statuses (id, s, t, u, v) VALUES (2, NULL, NULL, NULL, NULL)");

        foreach (string bad in new[]
                 {
                     "INSERT INTO statuses (id, s, t, u, v) VALUES (3, 4, 5, 9, NULL)",
                     "INSERT INTO statuses (id, s, t, u, v) VALUES (4, 1, 0, 9, NULL)",
                     "INSERT INTO statuses (id, s, t, u, v) VALUES (5, 1, 5, 9, 1)",
                 })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, bad), bad)!;
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code, bad);
        }

        string ddl = (await ExecQuery(database, executor, dbname, "SHOW CREATE TABLE statuses"))[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("s IN (1, 2, 3)", ddl);
        StringAssert.Contains("t NOT IN (0)", ddl);
        StringAssert.Contains("NOT array_contains(ARRAY[], u)", ddl);
        StringAssert.Contains("array_contains(ARRAY[], v)", ddl);
    }

    [Test]
    public async Task ViewBody_EveryForm_QueriesAndRenders()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v_lit AS SELECT id FROM vals WHERE x = ANY (ARRAY[1, 3])");
        await ExecDdl(database, executor, dbname, "CREATE VIEW v_notlit AS SELECT id FROM vals WHERE x <> ALL (ARRAY[1, 3])");
        await ExecDdl(database, executor, dbname, "CREATE VIEW v_col AS SELECT id FROM tagged WHERE 'red' = ANY (tags)");
        await ExecDdl(database, executor, dbname, "CREATE VIEW v_notcol AS SELECT id FROM tagged WHERE 'red' <> ALL (tags)");
        await ExecDdl(database, executor, dbname, "CREATE VIEW v_empty AS SELECT id FROM vals WHERE x <> ALL (ARRAY[]) AND NOT (x = ANY (ARRAY[]))");

        CollectionAssert.AreEqual(new[] { 1L, 3L }, await Ids(database, executor, dbname, "SELECT id FROM v_lit ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 2L }, await Ids(database, executor, dbname, "SELECT id FROM v_notlit ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 1L, 5L }, await Ids(database, executor, dbname, "SELECT id FROM v_col ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 2L, 3L }, await Ids(database, executor, dbname, "SELECT id FROM v_notcol ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L, 4L }, await Ids(database, executor, dbname, "SELECT id FROM v_empty ORDER BY id"));

        async Task<string> Shown(string view) =>
            (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW " + view))[0].Row["create view"].StrValue!;

        StringAssert.Contains("x IN (1, 3)", await Shown("v_lit"));
        StringAssert.Contains("x NOT IN (1, 3)", await Shown("v_notlit"));
        StringAssert.Contains("array_contains(tags, 'red')", await Shown("v_col"));
        StringAssert.Contains("NOT array_contains(tags, 'red')", await Shown("v_notcol"));
        StringAssert.Contains("array_contains(ARRAY[], x)", await Shown("v_empty"));
    }

    /// <summary>
    /// A view body with a subquery form is stored as its IN / NOT IN rewrite. Querying such a view
    /// fails today for plain <c>IN (SELECT …)</c> too (a view-body subquery is not resolved), so this
    /// test checks only the stored text; the query side follows whatever <c>IN (SELECT …)</c> supports.
    /// </summary>
    [Test]
    public async Task ViewBody_SubqueryForms_AreStoredAsInSubqueries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v_sub AS SELECT id FROM vals WHERE x = ANY (SELECT y FROM allowed)");
        await ExecDdl(database, executor, dbname, "CREATE VIEW v_notsub AS SELECT id FROM vals WHERE x <> ALL ((SELECT y FROM allowed))");

        string sub = (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW v_sub"))[0].Row["create view"].StrValue!;
        string notSub = (await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW v_notsub"))[0].Row["create view"].StrValue!;

        StringAssert.Contains("x IN (SELECT", sub);
        StringAssert.Contains("x NOT IN (SELECT", notSub);
    }

    // ── the index path of IN ────────────────────────────────────────────────

    /// <summary>
    /// A literal array becomes the IN node, so the planner picks the same index access for both.
    /// </summary>
    [Test]
    public async Task Explain_AnyOverLiteralArray_UsesTheSameIndexAccessAsIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await ExecDdl(database, executor, dbname, "CREATE INDEX x_idx ON vals (x)");

        async Task<string> Plan(string sql) => string.Join("\n", (await ExecQuery(database, executor, dbname, "EXPLAIN " + sql)).Select(r =>
        {
            string node = r.Row.TryGetValue("node", out ColumnValue? nv) ? nv?.StrValue ?? "" : "";
            string detail = r.Row.TryGetValue("detail", out ColumnValue? dv) ? dv?.StrValue ?? "" : "";
            return node + " " + detail;
        }));

        string viaIn = await Plan("SELECT id FROM vals WHERE x IN (1, 3)");
        string viaAny = await Plan("SELECT id FROM vals WHERE x = ANY (ARRAY[1, 3])");

        StringAssert.Contains("x_idx", viaIn);
        Assert.AreEqual(viaIn, viaAny);

        CollectionAssert.AreEqual(new[] { 1L, 3L },
            await Ids(database, executor, dbname, "SELECT id FROM vals WHERE x = ANY (ARRAY[1, 3]) ORDER BY id"));
    }

    // ── argument-count errors ───────────────────────────────────────────────

    [TestCase("SELECT 1 = ANY (ARRAY[1], ARRAY[2])")]
    [TestCase("SELECT 1 = ANY ()")]
    [TestCase("SELECT 1 <> ALL ()")]
    public async Task WrongArgumentCount_IsASyntaxError(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, sql), sql)!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code, sql);
        StringAssert.Contains("takes exactly one argument", ex.Message, sql);
    }

    /// <summary>
    /// A quantifier the rewrite does not see is still a call to <c>any</c>, <c>some</c> or
    /// <c>all</c>, and the stub functions reject it with a message that says where it is valid.
    /// </summary>
    [TestCase("SELECT any(ARRAY[1])")]
    [TestCase("SELECT some(ARRAY[1], 2)")]
    [TestCase("SELECT ANY (ARRAY[1]) = 1")]
    [TestCase("SELECT 2 = all(ARRAY[1]) + 1")]
    public async Task StrayQuantifierCall_IsRejectedWithAClearMessage(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, sql), sql)!;
        StringAssert.Contains("valid only as the right operand of a comparison", ex.Message, sql);
    }

    // ── the words stay usable as names ──────────────────────────────────────

    [Test]
    public async Task ColumnsNamedAnySomeAll_StillWork()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE perms (id int64 PRIMARY KEY, any int64, some int64, all int64)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO perms (id, any, some, all) VALUES (1, 10, 20, 30), (2, 11, 21, 31)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, any, some, all FROM perms WHERE some = 21 AND any = 11 AND all = 31");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2L, rows[0].Row["id"].LongValue);
        Assert.AreEqual(11L, rows[0].Row["any"].LongValue);
        Assert.AreEqual(21L, rows[0].Row["some"].LongValue);
        Assert.AreEqual(31L, rows[0].Row["all"].LongValue);

        // A column named any on the right of = is a column, not a quantifier.
        CollectionAssert.AreEqual(new[] { 1L },
            await Ids(database, executor, dbname, "SELECT id FROM perms WHERE 10 = any ORDER BY id"));
    }
}
