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
/// End-to-end tests for <c>||</c> and the bitwise operators <c>&amp; | # ~ &lt;&lt; &gt;&gt;</c> through
/// SQL: the value of each operator for each accepted type pair, the NULL rule, the refused pairs, and
/// each place an expression can stand — a projection, <c>WHERE</c>, <c>ORDER BY</c>, <c>GROUP BY</c>, a
/// <c>CHECK</c>, a view body that must survive a close and reopen, <c>EXPLAIN</c> and the column types of
/// a <c>CREATE TABLE … AS SELECT</c>.
/// </summary>
[NonParallelizable]
internal sealed class TestConcatAndBitwiseOperators : SharedNodeBaseTest
{
    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql, bool ddl = false)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(tx, db.Name, sql, null);

            if (ddl)
                await executor.ExecuteDDLSQL(ticket);
            else
                await executor.ExecuteNonSQLQuery(ticket);

            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await db.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, DatabaseDescriptor db, string expression,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await Select(executor, db, "SELECT " + expression + " AS v", parameters);
        Assert.AreEqual(1, rows.Count, expression);
        return rows[0].Row["v"];
    }

    private static async Task AssertString(CommandExecutor executor, DatabaseDescriptor db, string expression, string expected,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        ColumnValue value = await Scalar(executor, db, expression, parameters);
        Assert.AreEqual(ColumnType.String, value.Type, expression);
        Assert.AreEqual(expected, value.StrValue, expression);
    }

    private static async Task AssertBytes(CommandExecutor executor, DatabaseDescriptor db, string expression, string expectedHex)
    {
        ColumnValue value = await Scalar(executor, db, expression);
        Assert.AreEqual(ColumnType.Bytes, value.Type, expression);
        Assert.AreEqual(expectedHex, Convert.ToHexStringLower(value.BytesValue ?? []), expression);
    }

    private static async Task AssertLong(CommandExecutor executor, DatabaseDescriptor db, string expression, long expected)
    {
        ColumnValue value = await Scalar(executor, db, expression);
        Assert.AreEqual(ColumnType.Integer64, value.Type, expression);
        Assert.AreEqual(expected, value.LongValue, expression);
    }

    private static async Task AssertNull(CommandExecutor executor, DatabaseDescriptor db, string expression)
        => Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, expression)).Type, expression);

    private static async Task AssertInvalid(CommandExecutor executor, DatabaseDescriptor db, string expression, string messagePart)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, "SELECT " + expression + " AS v", null));
            await cursor.ToListAsync();
        })!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, expression + ": " + ex.Message);
        StringAssert.Contains(messagePart, ex.Message, expression);
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupItems()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await Exec(executor, db,
            "CREATE TABLE items (id INT64 PRIMARY KEY, first_name STRING, last_name STRING, flags INT64, mask BYTES)",
            ddl: true);
        await Exec(executor, db,
            "INSERT INTO items (id, first_name, last_name, flags, mask) VALUES " +
            "(1, 'Ada', 'Lovelace', 5, X'0F'), " +
            "(2, 'Alan', 'Turing', 6, X'F0'), " +
            "(3, 'Grace', NULL, 4, X'FF'), " +
            "(4, 'Edsger', 'Dijkstra', NULL, NULL)");

        return (dbname, db, executor);
    }

    // ── || ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Concat_StringAndBytes()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, db, "'a' || 'b'", "ab");
        await AssertString(executor, db, "'a' || ''", "a");
        await AssertString(executor, db, "'a' || 'b' || 'c'", "abc");
        await AssertBytes(executor, db, "X'01' || X'02'", "0102");
        await AssertBytes(executor, db, "X'' || X'02'", "02");
        await AssertBytes(executor, db, "X'01' || X''", "01");

        ColumnValue equal = await Scalar(executor, db, "X'01' || X'02' = X'0102'");
        Assert.IsTrue(equal.BoolValue);
    }

    [Test]
    public async Task Concat_StringWithOtherScalar_UsesTheSharedTextForm()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, db, "'n=' || 5", "n=5");
        await AssertString(executor, db, "5 || '=n'", "5=n");
        await AssertString(executor, db, "'n=' || -5", "n=-5");
        await AssertString(executor, db, "'f=' || 1.5", "f=1.5");
        await AssertString(executor, db, "'b=' || true", "b=true");
        await AssertString(executor, db, "'v=' || X'01FF'", "v=\\x01ff");
        await AssertString(executor, db, "'d=' || CAST('2026-10-12' AS date)", "d=2026-10-12");
        await AssertString(executor, db, "'n=' || CAST(1.50 AS numeric)", "n=1.5");

        // The same text form the string cast gives.
        ColumnValue same = await Scalar(executor, db, "('x' || 2.25) = ('x' || CAST(2.25 AS string))");
        Assert.IsTrue(same.BoolValue);
    }

    [Test]
    public async Task Concat_NullOperand_GivesNull_UnlikeConcatFunction()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertNull(executor, db, "'a' || NULL");
        await AssertNull(executor, db, "NULL || 'a'");
        await AssertNull(executor, db, "NULL || NULL");
        await AssertNull(executor, db, "X'01' || NULL");
        await AssertNull(executor, db, "5 || NULL");

        // concat() skips NULL; || does not.
        await AssertString(executor, db, "concat('a', NULL)", "a");
    }

    [Test]
    public async Task Concat_RefusedPairs()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertInvalid(executor, db, "1 || 2", "operator || for argument types: Integer64, Integer64");
        await AssertInvalid(executor, db, "X'01' || 2", "operator ||");
        await AssertInvalid(executor, db, "true || false", "operator ||");
        await AssertInvalid(executor, db, "ARRAY[1, 2] || ARRAY[3]", "array operands");
        await AssertInvalid(executor, db, "'a' || ARRAY[3]", "array operands");
    }

    [Test]
    public async Task Concat_WithPlaceholders()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, db, "@a || '-' || @b", "x-7", new()
        {
            ["@a"] = new ColumnValue(ColumnType.String, "x"),
            ["@b"] = new ColumnValue(ColumnType.Integer64, 7),
        });
    }

    // ── bitwise ────────────────────────────────────────────────────────────

    [Test]
    public async Task Bitwise_Int64Values()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "12 & 10", 8);
        await AssertLong(executor, db, "12 | 10", 14);
        await AssertLong(executor, db, "12 # 10", 6);
        await AssertLong(executor, db, "~0", -1);
        await AssertLong(executor, db, "~5", -6);
        await AssertLong(executor, db, "~-1", 0);
        await AssertLong(executor, db, "~~5", 5);
        await AssertLong(executor, db, "-8 & 7", 0);
        await AssertLong(executor, db, "-1 # 1", -2);
        await AssertLong(executor, db, "9223372036854775807 | -9223372036854775808", -1);
    }

    [Test]
    public async Task Shifts_Int64Values()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "1 << 0", 1);
        await AssertLong(executor, db, "1 << 4", 16);
        await AssertLong(executor, db, "1 << 63", long.MinValue);
        await AssertLong(executor, db, "3 << 62", long.MinValue + (1L << 62));
        await AssertLong(executor, db, "256 >> 4", 16);
        await AssertLong(executor, db, "7 >> 1", 3);

        // >> is arithmetic: it keeps the sign of a negative value.
        await AssertLong(executor, db, "-16 >> 2", -4);
        await AssertLong(executor, db, "-1 >> 10", -1);
        await AssertLong(executor, db, "-9223372036854775808 >> 63", -1);
        await AssertLong(executor, db, "-5 >> 1", -3);
        await AssertLong(executor, db, "-1 << 1", -2);

        // A count outside 0-63 is taken modulo 64, as PostgreSQL gives on x86-64 and ARM64.
        await AssertLong(executor, db, "1 << 64", 1);
        await AssertLong(executor, db, "1 << 65", 2);
        await AssertLong(executor, db, "1 << -1", long.MinValue);
        await AssertLong(executor, db, "256 >> 68", 16);
    }

    [Test]
    public async Task Bitwise_EqualLengthBytes()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBytes(executor, db, "X'F0F0' & X'FF00'", "f000");
        await AssertBytes(executor, db, "X'F0F0' | X'0F00'", "fff0");
        await AssertBytes(executor, db, "X'F0F0' # X'FFFF'", "0f0f");
        await AssertBytes(executor, db, "~X'F00F'", "0ff0");
        await AssertBytes(executor, db, "X'' & X''", "");
        await AssertBytes(executor, db, "~X''", "");
    }

    [Test]
    public async Task Bitwise_NullOperand_GivesNull()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        foreach (string op in new[] { "&", "|", "#", "<<", ">>" })
        {
            await AssertNull(executor, db, $"1 {op} NULL");
            await AssertNull(executor, db, $"NULL {op} 1");
        }

        await AssertNull(executor, db, "X'01' & NULL");
        await AssertNull(executor, db, "~NULL");
    }

    [Test]
    public async Task Bitwise_RefusedPairs()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertInvalid(executor, db, "X'01' & X'0102'", "equal length, got 1 and 2 bytes");
        await AssertInvalid(executor, db, "X'01' # X''", "equal length");
        await AssertInvalid(executor, db, "1 & X'01'", "operator & for argument types: Integer64, Bytes");
        await AssertInvalid(executor, db, "1.5 | 1", "operator | for argument types: Float64, Integer64");
        await AssertInvalid(executor, db, "CAST(1 AS numeric) # 1", "operator #");
        await AssertInvalid(executor, db, "true & false", "operator &");
        await AssertInvalid(executor, db, "'a' & 'b'", "operator &");
        await AssertInvalid(executor, db, "X'01' << 1", "operator <<");
        await AssertInvalid(executor, db, "1 >> 1.5", "operator >>");
        await AssertInvalid(executor, db, "~1.5", "unary operator ~ for argument type: Float64");
        await AssertInvalid(executor, db, "~'a'", "unary operator ~");
    }

    [Test]
    public async Task Precedence_IsVisibleInValues()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        // (1 + 2) & 3, not 1 + (2 & 3) = 3.
        await AssertLong(executor, db, "1 + 2 & 3", 3);
        await AssertLong(executor, db, "1 + 2 & 1", 1);

        // (6 | 1) & 3 = 3, not the C reading 6 | (1 & 3) = 7.
        await AssertLong(executor, db, "6 | 1 & 3", 3);

        // (~1) + 1 = -1, not ~(1 + 1) = -3.
        await AssertLong(executor, db, "~1 + 1", -1);

        // 1 << (1 + 1).
        await AssertLong(executor, db, "1 << 1 + 1", 4);

        // ('a' || 'b') = 'ab'.
        ColumnValue concatCompare = await Scalar(executor, db, "'a' || 'b' = 'ab'");
        Assert.IsTrue(concatCompare.BoolValue);
    }

    // ── the places an expression can stand ─────────────────────────────────

    [Test]
    public async Task ProjectionWhereOrderByAndGroupBy()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupItems();

        List<QueryResultRow> names = await Select(executor, db,
            "SELECT id, first_name || ' ' || last_name AS full_name FROM items ORDER BY id");
        CollectionAssert.AreEqual(
            new string?[] { "Ada Lovelace", "Alan Turing", null, "Edsger Dijkstra" },
            names.Select(r => r.Row["full_name"].Type == ColumnType.Null ? null : r.Row["full_name"].StrValue).ToArray());

        List<QueryResultRow> withBit = await Select(executor, db,
            "SELECT id FROM items WHERE flags & 4 = 4 AND flags & 1 = 0 ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 3 }, withBit.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> byName = await Select(executor, db,
            "SELECT id FROM items WHERE first_name || last_name = 'AlanTuring'");
        CollectionAssert.AreEqual(new long[] { 2 }, byName.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> byMask = await Select(executor, db,
            "SELECT id FROM items WHERE mask & X'0F' = X'0F' ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 3 }, byMask.Select(r => r.Row["id"].LongValue).ToArray());

        // ORDER BY flags # 6: 5#6=3, 6#6=0, 4#6=2, NULL last in ascending order.
        List<QueryResultRow> ordered = await Select(executor, db,
            "SELECT id FROM items WHERE flags IS NOT NULL ORDER BY flags # 6");
        CollectionAssert.AreEqual(new long[] { 2, 3, 1 }, ordered.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> descending = await Select(executor, db,
            "SELECT id FROM items ORDER BY first_name || '!' DESC");
        CollectionAssert.AreEqual(new long[] { 3, 4, 2, 1 }, descending.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> grouped = await Select(executor, db,
            "SELECT flags & 4 AS bit, COUNT(*) AS n FROM items WHERE flags IS NOT NULL GROUP BY flags & 4");
        Assert.AreEqual(1, grouped.Count);
        Assert.AreEqual(4, grouped[0].Row["bit"].LongValue);
        Assert.AreEqual(3, grouped[0].Row["n"].LongValue);

        List<QueryResultRow> aggregate = await Select(executor, db,
            "SELECT SUM(flags) | 1 AS s, ~MAX(flags) AS m FROM items");
        Assert.AreEqual(15, aggregate[0].Row["s"].LongValue);
        Assert.AreEqual(-7, aggregate[0].Row["m"].LongValue);

        List<QueryResultRow> having = await Select(executor, db,
            "SELECT first_name FROM items GROUP BY first_name HAVING COUNT(*) << 2 = 4 ORDER BY first_name");
        Assert.AreEqual(4, having.Count);
    }

    [Test]
    public async Task UpdateAndInsert_UseTheOperators()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupItems();

        await Exec(executor, db, "UPDATE items SET flags = flags | 8, first_name = first_name || '!' WHERE id = 1");
        await Exec(executor, db, "INSERT INTO items (id, first_name, flags, mask) VALUES (5, 'x' || 'y', 1 << 10, ~X'00')");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT id, first_name, flags, mask FROM items WHERE id IN (1, 5) ORDER BY id");
        Assert.AreEqual("Ada!", rows[0].Row["first_name"].StrValue);
        Assert.AreEqual(13, rows[0].Row["flags"].LongValue);
        Assert.AreEqual("xy", rows[1].Row["first_name"].StrValue);
        Assert.AreEqual(1024, rows[1].Row["flags"].LongValue);
        CollectionAssert.AreEqual(new byte[] { 0xFF }, rows[1].Row["mask"].BytesValue);
    }

    [Test]
    public async Task CheckConstraint_EnforcesAndRoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await Exec(executor, db,
            "CREATE TABLE perms (id INT64 PRIMARY KEY, flags INT64, code STRING, " +
            "CONSTRAINT chk_flags CHECK (flags & ~7 = 0), CONSTRAINT chk_code CHECK (code || '' <> 'bad'))",
            ddl: true);

        await Exec(executor, db, "INSERT INTO perms (id, flags, code) VALUES (1, 7, 'ok')");

        CamusDBException flagsError = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(executor, db, "INSERT INTO perms (id, flags, code) VALUES (2, 8, 'ok')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, flagsError.Code, flagsError.Message);

        CamusDBException codeError = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(executor, db, "INSERT INTO perms (id, flags, code) VALUES (3, 1, 'bad')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, codeError.Code, codeError.Message);

        // A NULL operand makes the CHECK unknown, and unknown passes.
        await Exec(executor, db, "INSERT INTO perms (id, flags, code) VALUES (4, NULL, NULL)");

        // The stored condition re-parses after a close and reopen and still enforces.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        string ddl = (await Select(executor, reopened, "SHOW CREATE TABLE perms"))[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("&", ddl);
        StringAssert.Contains("~7", ddl);
        StringAssert.Contains("||", ddl);

        CamusDBException again = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(executor, reopened, "INSERT INTO perms (id, flags, code) VALUES (5, 16, 'ok')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, again.Code, again.Message);
    }

    [Test]
    public async Task ViewBody_SurvivesCloseAndReopen()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupItems();

        await Exec(executor, db,
            "CREATE VIEW labels AS SELECT id, first_name || ' ' || last_name AS full_name, flags & 4 AS bit4, " +
            "~flags AS inverted, flags << 2 AS shifted, mask # X'FF' AS flipped FROM items WHERE flags | 1 = 5",
            ddl: true);

        async Task AssertView(DatabaseDescriptor database)
        {
            List<QueryResultRow> rows = await Select(executor, database, "SELECT * FROM labels ORDER BY id");
            Assert.AreEqual(2, rows.Count);
            Assert.AreEqual(1, rows[0].Row["id"].LongValue);
            Assert.AreEqual("Ada Lovelace", rows[0].Row["full_name"].StrValue);
            Assert.AreEqual(4, rows[0].Row["bit4"].LongValue);
            Assert.AreEqual(-6, rows[0].Row["inverted"].LongValue);
            Assert.AreEqual(20, rows[0].Row["shifted"].LongValue);
            CollectionAssert.AreEqual(new byte[] { 0xF0 }, rows[0].Row["flipped"].BytesValue);
            Assert.AreEqual(3, rows[1].Row["id"].LongValue);
            Assert.AreEqual(ColumnType.Null, rows[1].Row["full_name"].Type);
        }

        await AssertView(db);

        string shown = (await Select(executor, db, "SHOW CREATE VIEW labels"))[0].Row["create view"].StrValue!;
        foreach (string op in new[] { "||", " & ", "~", "<<", " # ", " | " })
            StringAssert.Contains(op, shown, shown);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        await AssertView(reopened);
    }

    [Test]
    public async Task Explain_ShowsTheOperators()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupItems();

        // EXPLAIN renders expressions in the filter step and in the sort step. The project step prints
        // no expressions.
        List<QueryResultRow> plan = await Select(executor, db,
            "EXPLAIN SELECT first_name || last_name AS n FROM items " +
            "WHERE flags & 4 = 4 AND ~flags >> 1 < 0 AND first_name || last_name <> 'x' AND (flags # 1) | 2 <> 0 " +
            "ORDER BY flags << 2 DESC, last_name || first_name");
        string text = string.Join("\n", plan.SelectMany(r => r.Row.Values).Select(v => v.StrValue ?? ""));

        StringAssert.Contains("flags & 4 = 4", text);
        StringAssert.Contains("~flags >> 1 < 0", text);
        StringAssert.Contains("first_name || last_name <> 'x'", text);
        StringAssert.Contains("flags # 1 | 2 <> 0", text);
        StringAssert.Contains("flags << 2 DESC, last_name || first_name ASC", text);
        StringAssert.DoesNotContain("ExprShiftLeft", text);
        StringAssert.DoesNotContain("ExprConcat", text);
    }

    [Test]
    public async Task CreateTableAsSelect_TypesTheColumns()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupItems();

        await Exec(executor, db,
            "CREATE TABLE copy AS SELECT id, first_name || last_name AS n, 'id=' || id AS label, flags & 3 AS f, " +
            "~flags AS nf, flags << 1 AS sh, mask | X'01' AS m, mask || X'00' AS mm, ~mask AS nm FROM items",
            ddl: true);

        Dictionary<string, string> types = (await Select(executor, db, "SHOW COLUMNS FROM copy"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);

        Assert.AreEqual("STRING", types["n"]);
        Assert.AreEqual("STRING", types["label"]);
        Assert.AreEqual("INT64", types["f"]);
        Assert.AreEqual("INT64", types["nf"]);
        Assert.AreEqual("INT64", types["sh"]);
        Assert.AreEqual("BYTES", types["m"]);
        Assert.AreEqual("BYTES", types["mm"]);
        Assert.AreEqual("BYTES", types["nm"]);

        List<QueryResultRow> rows = await Select(executor, db, "SELECT f, mm FROM copy WHERE id = 1");
        Assert.AreEqual(1, rows[0].Row["f"].LongValue);
        CollectionAssert.AreEqual(new byte[] { 0x0F, 0x00 }, rows[0].Row["mm"].BytesValue);
    }

    [Test]
    public async Task RegexMatch_StillWorksBesidePrefixTilde()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupItems();

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT id FROM items WHERE first_name ~ '^A' AND ~flags < 0 ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 2 }, rows.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> negated = await Select(executor, db,
            "SELECT id FROM items WHERE NOT first_name ~ '^A' ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 3, 4 }, negated.Select(r => r.Row["id"].LongValue).ToArray());
    }
}
