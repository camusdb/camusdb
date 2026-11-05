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
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>NULL NOT IN (empty set)</c> is TRUE, and <c>NULL NOT IN (a set with a row)</c> is UNKNOWN. So an
/// anti-join must know if the subquery has a row before it can answer for a NULL outer value.
///
/// <para>Over a NOT NULL inner column, that question looked at the inner table and not at the subquery:
/// it read one row and ignored the inner WHERE. So <c>v NOT IN (SELECT n FROM i WHERE FALSE)</c> dropped
/// every outer row with a NULL <c>v</c>, with no error, when the inner table had a row. The same
/// predicate under a CASE takes the general path and gave the correct answer, which is how the NoRec
/// oracle of the SQL fuzzer found it. A non-NULL outer value and the null-aware anti-join were correct,
/// because both evaluate the filter.</para>
///
/// <para>Each case asserts the plan, so a case cannot pass on the general path. The general path is
/// also the reference that each case must agree with.</para>
/// </summary>
[TestFixture]
internal sealed class TestAntiJoinNullOuterValue : BaseTest
{
    private sealed record Fixture(string Name, DatabaseDescriptor Database, CommandExecutor Executor, SqlFuzzReplay Db);

    /// <summary>
    /// Outer rows 1 and 3 have a NULL <c>v</c>. <c>i.n</c> is NOT NULL and has an index, and <c>i.id</c> is
    /// the primary key, so a NOT IN over either column is a plain anti-join, one through a non-unique
    /// probe and one through a unique probe. <c>i.flag</c> is outside the select list, so a filter on
    /// it needs a column that the emptiness check must decode. The rows go in one at a time, so the
    /// scan reads the row with <c>id = 1</c> first.
    /// </summary>
    private async Task<Fixture> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { AutoAnalyzeEnabled = false, TtlEnabled = false, QueryResultCacheEnabled = false });
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl("CREATE TABLE o (id int64 PRIMARY KEY, v int64)");
        await db.Write("INSERT INTO o (id, v) VALUES (1, NULL)");
        await db.Write("INSERT INTO o (id, v) VALUES (2, 10)");
        await db.Write("INSERT INTO o (id, v) VALUES (3, NULL)");
        await db.Write("INSERT INTO o (id, v) VALUES (4, 99)");

        await db.Ddl("CREATE TABLE i (id int64 PRIMARY KEY, n int64 NOT NULL, flag int64)");
        await db.Ddl("CREATE INDEX i_n ON i (n)");
        await db.Write("INSERT INTO i (id, n, flag) VALUES (1, 10, 0)");
        await db.Write("INSERT INTO i (id, n, flag) VALUES (2, 20, 1)");
        await db.Write("INSERT INTO i (id, n, flag) VALUES (3, 30, NULL)");

        await db.Ddl("CREATE TABLE none (id int64 PRIMARY KEY, n int64 NOT NULL)");
        await db.Ddl("CREATE INDEX none_n ON none (n)");

        return new Fixture(dbname, database, executor, db);
    }

    private static List<string> Keys(params long[] keys) =>
        SqlFuzzValues.Multiset(keys.Select(k => (IReadOnlyList<ColumnValue>)new[] { new ColumnValue(ColumnType.Integer64, k) }));

    private static async Task AssertIsAPlainAntiJoin(SqlFuzzReplay db, string sql)
    {
        SqlFuzzRows rows = await db.Query("EXPLAIN " + sql);
        string plan = string.Join("\n", rows.Rows.Select(r => string.Join(" ", r.Select(v => v.StrValue ?? ""))));

        Assert.That(plan, Does.Contain("anti-join").And.Not.Contain("null-aware"), sql);
    }

    /// <summary>The statement, on the anti-join path, with the expected rows and the general path as the reference.</summary>
    private static async Task AssertNotIn(SqlFuzzReplay db, string predicate, long[] expected)
    {
        string optimized = $"SELECT o.id AS k FROM o WHERE ({predicate})";
        string reference = $"SELECT o.id AS k, (CASE WHEN ({predicate}) THEN 1 ELSE 0 END) AS norec_flag FROM o";

        await AssertIsAPlainAntiJoin(db, optimized);
        Assert.That(await db.Rows(optimized), Is.EqualTo(Keys(expected)), optimized);
        await db.AssertNoRecRows(optimized, reference);
    }

    // ── The filter decides, not the table ────────────────────────────────────────────────────────

    // The two lists can differ because `n` holds 10, 20, 30 and `id` holds 1, 2, 3. Only `n` can hold
    // the outer value 10. A NULL outer value (rows 1 and 3) stays only when the filter removes every row.
    [TestCase("FALSE", new long[] { 1, 2, 3, 4 }, new long[] { 1, 2, 3, 4 }, TestName = "RemovesEveryRow_FalseLiteral")]
    [TestCase("s.id < 0", new long[] { 1, 2, 3, 4 }, new long[] { 1, 2, 3, 4 }, TestName = "RemovesEveryRow_SelectedKey")]
    [TestCase("s.flag = 5", new long[] { 1, 2, 3, 4 }, new long[] { 1, 2, 3, 4 }, TestName = "RemovesEveryRow_ColumnOutsideTheSelectList")]
    [TestCase("s.flag = NULL", new long[] { 1, 2, 3, 4 }, new long[] { 1, 2, 3, 4 }, TestName = "RemovesEveryRow_UnknownOnEveryRow")]
    [TestCase("s.id = 1", new long[] { 4 }, new long[] { 2, 4 }, TestName = "KeepsOneRow_TheFirstOfTheScan")]
    [TestCase("s.id = 2", new long[] { 2, 4 }, new long[] { 2, 4 }, TestName = "KeepsOneRow_InTheMiddle")]
    [TestCase("s.id = 3", new long[] { 2, 4 }, new long[] { 2, 4 }, TestName = "KeepsOneRow_TheLastOfTheScan")]
    [TestCase("s.flag = 1", new long[] { 2, 4 }, new long[] { 2, 4 }, TestName = "KeepsOneRow_ColumnOutsideTheSelectList")]
    [TestCase("s.flag IS NULL", new long[] { 2, 4 }, new long[] { 2, 4 }, TestName = "KeepsOneRow_WhereTheFilterColumnIsNull")]
    [TestCase("TRUE", new long[] { 4 }, new long[] { 2, 4 }, TestName = "KeepsEveryRow_TrueLiteral")]
    public async Task NotIn_AnswersFromTheFilteredSet(string filter, long[] overNonUniqueIndex, long[] overPrimaryKey)
    {
        Fixture f = await SetupAsync();

        await AssertNotIn(f.Db, $"o.v NOT IN (SELECT s.n FROM i AS s WHERE {filter})", overNonUniqueIndex);
        await AssertNotIn(f.Db, $"o.v NOT IN (SELECT s.id FROM i AS s WHERE {filter})", overPrimaryKey);
    }

    [Test]
    public async Task NotIn_WithNoFilter_AnswersFromTheTable()
    {
        Fixture f = await SetupAsync();

        await AssertNotIn(f.Db, "o.v NOT IN (SELECT s.n FROM i AS s)", [4]);
    }

    [Test]
    public async Task NotIn_OverAnEmptyInnerTable_KeepsEveryOuterRow()
    {
        Fixture f = await SetupAsync();

        await AssertNotIn(f.Db, "o.v NOT IN (SELECT s.n FROM none AS s)", [1, 2, 3, 4]);
        await AssertNotIn(f.Db, "o.v NOT IN (SELECT s.n FROM none AS s WHERE s.id > 0)", [1, 2, 3, 4]);
    }

    [TestCase(-1L, new long[] { 1, 2, 3, 4 }, TestName = "Parameter_RemovesEveryRow")]
    [TestCase(3L, new long[] { 2, 4 }, TestName = "Parameter_KeepsTheLastRow")]
    public async Task NotIn_ReadsAParameterInTheFilter(long value, long[] expected)
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT o.id AS k FROM o WHERE o.v NOT IN (SELECT s.n FROM i AS s WHERE s.id = @p)";

        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, value) };
        KvTransaction tx = await f.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, f.Name, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(tx);

        Assert.That(rows.Select(r => r.Row["k"].LongValue).Order(), Is.EqualTo(expected));
    }

    [Test]
    public async Task NotIn_SeesTheRowsOfItsOwnTransaction()
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT o.id AS k FROM o WHERE o.v NOT IN (SELECT s.n FROM i AS s WHERE s.flag = 7)";

        await AssertIsAPlainAntiJoin(f.Db, sql);
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(1, 2, 3, 4)), "the filter removes every committed row");

        KvTransaction tx = await f.Database.Transactions.BeginAsync();
        _ = await f.Executor.ExecuteNonSQLQuery(new(txnState: tx, database: f.Name, sql: "INSERT INTO i (id, n, flag) VALUES (4, 40, 7)", parameters: null));

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, f.Name, sql, null));
        List<QueryResultRow> inside = await cursor.ToListAsync();
        Assert.That(inside.Select(r => r.Row["k"].LongValue).Order(), Is.EqualTo(new long[] { 2, 4 }), "the transaction sees its own row, so the set has a row");

        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(1, 2, 3, 4)), "another reader does not see the uncommitted row");

        await f.Database.Transactions.RollbackAsync(tx);
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(1, 2, 3, 4)), "the rollback removes the row");
    }

    [Test]
    public async Task NotIn_AFilterThatFailsOnARow_IsAnError_NotAnEmptySet()
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT o.id AS k FROM o WHERE o.v NOT IN (SELECT s.n FROM i AS s WHERE 1 / s.flag = 1)";

        await AssertIsAPlainAntiJoin(f.Db, sql);

        // The scan reads flag = 0 first, so the filter divides by zero.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => f.Db.Rows(sql))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public async Task NullAwareAntiJoin_KeepsItsOwnAnswer()
    {
        Fixture f = await SetupAsync();

        // A NULL in the inner column makes every outer row UNKNOWN, unless the filter removes that row.
        await f.Db.Ddl("CREATE TABLE nu (id int64 PRIMARY KEY, n int64)");
        await f.Db.Ddl("CREATE INDEX nu_n ON nu (n)");
        await f.Db.Write("INSERT INTO nu (id, n) VALUES (1, 10)");
        await f.Db.Write("INSERT INTO nu (id, n) VALUES (2, NULL)");

        const string keepsTheNull = "SELECT o.id AS k FROM o WHERE o.v NOT IN (SELECT s.n FROM nu AS s)";
        const string removesTheNull = "SELECT o.id AS k FROM o WHERE o.v NOT IN (SELECT s.n FROM nu AS s WHERE s.id = 1)";
        const string removesEveryRow = "SELECT o.id AS k FROM o WHERE o.v NOT IN (SELECT s.n FROM nu AS s WHERE FALSE)";

        SqlFuzzRows plan = await f.Db.Query("EXPLAIN " + removesEveryRow);
        Assert.That(string.Join("\n", plan.Rows.Select(r => string.Join(" ", r.Select(v => v.StrValue ?? "")))), Does.Contain("null-aware-anti-join"));

        Assert.That(await f.Db.Rows(keepsTheNull), Is.Empty);
        Assert.That(await f.Db.Rows(removesTheNull), Is.EqualTo(Keys(4)));
        Assert.That(await f.Db.Rows(removesEveryRow), Is.EqualTo(Keys(1, 2, 3, 4)));
    }

    // ── The two statements the fuzzer found ──────────────────────────────────────────────────────

    [Test]
    public async Task FoundByTheFuzzer_TheInnerTableIsTheOuterTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { AutoAnalyzeEnabled = false, TtlEnabled = false, QueryResultCacheEnabled = false });
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl("CREATE TABLE t1 (c0 int64 PRIMARY KEY, c2 int64, c5 string)");
        await db.Write("INSERT INTO t1 (c0, c2, c5) VALUES (-9, NULL, 'u4')");

        await db.AssertNoRecRows(
            "SELECT a0.c5 AS o2 FROM t1 AS a0 WHERE (a0.c2 NOT IN (SELECT s0.c0 FROM t1 AS s0 WHERE FALSE))",
            "SELECT a0.c5 AS o2, (CASE WHEN (a0.c2 NOT IN (SELECT s0.c0 FROM t1 AS s0 WHERE FALSE)) THEN 1 ELSE 0 END) AS norec_flag FROM t1 AS a0");

        Assert.AreEqual(1, (await db.Query("SELECT a0.c5 AS o2 FROM t1 AS a0 WHERE (a0.c2 NOT IN (SELECT s0.c0 FROM t1 AS s0 WHERE FALSE))")).Rows.Count);
    }

    [Test]
    public async Task FoundByTheFuzzer_ADateColumnWithAUniqueIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { AutoAnalyzeEnabled = false, TtlEnabled = false, QueryResultCacheEnabled = false });
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl("CREATE TABLE t0 (c0 int64 PRIMARY KEY, c2 date, c3 int64)");
        await db.Ddl("CREATE TABLE t2 (c0 int64 PRIMARY KEY, c3 jsonb NOT NULL, c4 date NOT NULL)");
        await db.Ddl("CREATE UNIQUE INDEX t2_i2 ON t2 (c4) INCLUDE (c3)");
        await db.Write("INSERT INTO t0 (c0, c2, c3) VALUES (-40, NULL, 1018)");
        await db.Write("INSERT INTO t2 (c0, c3, c4) VALUES (1, CAST('1.5' AS jsonb), CAST('2100-01-20' AS date))");

        await db.AssertNoRecRows(
            "SELECT a0.c3 AS o0 FROM t0 AS a0 WHERE (a0.c2 NOT IN (SELECT s4.c4 FROM t2 AS s4 WHERE FALSE))",
            "SELECT a0.c3 AS o0, (CASE WHEN (a0.c2 NOT IN (SELECT s4.c4 FROM t2 AS s4 WHERE FALSE)) THEN 1 ELSE 0 END) AS norec_flag FROM t0 AS a0");

        Assert.AreEqual(1, (await db.Query("SELECT a0.c3 AS o0 FROM t0 AS a0 WHERE (a0.c2 NOT IN (SELECT s4.c4 FROM t2 AS s4 WHERE FALSE))")).Rows.Count);
    }
}
