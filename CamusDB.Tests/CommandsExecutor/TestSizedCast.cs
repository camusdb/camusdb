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
/// <c>CAST(x AS string(N))</c>, <c>CAST(x AS bytes(N))</c> and the <c>::</c> form, through SQL. An
/// explicit cast cuts the value to its first N code points or N bytes, as an explicit cast to
/// <c>varchar(N)</c> does in PostgreSQL. A shorter value and NULL pass through. The size stays in the
/// stored text of a view, a CHECK constraint and an index expression, across a reopen.
/// </summary>
[NonParallelizable]
internal sealed class TestSizedCast : BaseTest
{
    private static async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        catch
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecSelect(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await db.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static async Task<IReadOnlyDictionary<string, ColumnValue>> SelectOne(CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.That(rows, Has.Count.EqualTo(1), sql);
        return rows[0].Row;
    }

    private static async Task<List<string>> Strings(CommandExecutor executor, string dbname, string sql, string column)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        return [.. rows.Select(r => r.Row[column].StrValue ?? "<null>").OrderBy(s => s, System.StringComparer.Ordinal)];
    }

    /// <summary>The scan node of the plan and its detail text.</summary>
    private static async Task<(string Node, string Detail)> ScanOf(CommandExecutor executor, string dbname, string select)
    {
        foreach (QueryResultRow row in await ExecSelect(executor, dbname, "EXPLAIN " + select))
        {
            string node = row.Row["node"].StrValue ?? "";
            if (node is "table-scan" or "index-range-scan" or "index-lookup" or "index-in-list-scan" or "index-in-list")
                return (node, row.Row["detail"].StrValue ?? "");
        }

        return ("(no scan node)", "");
    }

    private async Task<(string dbname, CommandExecutor executor)> CreateBooks()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE books (id oid primary key, title string, author string, year int64)");
        await ExecNonQuery(executor, dbname,
            "INSERT INTO books (id, title, author, year) VALUES " +
            "(gen_id(), 'The Lord of the Rings', 'Tolkien', 1954), " +
            "(gen_id(), 'Dune', 'Herbert', 1965), " +
            "(gen_id(), 'Neuromancer', 'Gibson', 1984)");
        return (dbname, executor);
    }

    [Test]
    public async Task CastToSizedString_CutsTheValue()
    {
        (string dbname, CommandExecutor executor) = await CreateBooks();

        List<QueryResultRow> rows = await ExecSelect(executor, dbname,
            "SELECT CAST(title AS string(5)) AS t, author, year FROM books WHERE year < 1970 ORDER BY year");

        Assert.That(rows.Select(r => r.Row["t"].StrValue), Is.EqualTo(new[] { "The L", "Dune" }));
        Assert.That(rows.Select(r => r.Row["t"].Type), Is.All.EqualTo(ColumnType.String));
        Assert.That(rows.Select(r => r.Row["author"].StrValue), Is.EqualTo(new[] { "Tolkien", "Herbert" }));
    }

    [TestCase("CAST('abcdefgh' AS string(5))", "abcde")]
    [TestCase("CAST('abc' AS string(5))", "abc")]
    [TestCase("CAST('abcde' AS string(5))", "abcde")]
    [TestCase("'abcdefgh'::string(5)", "abcde")]
    [TestCase("'abc'::string(5)", "abc")]
    [TestCase("CAST('abcdefgh' AS varchar(3))", "abc")]
    [TestCase("CAST('abcdefgh' AS char(3))", "abc")]
    [TestCase("'abcdefgh'::text(1)", "a")]
    [TestCase("CAST(12345 AS string(3))", "123")]
    [TestCase("CAST(true AS string(2))", "tr")]
    [TestCase("CAST(CAST('abcdefgh' AS string(6)) AS string(3))", "abc")]
    [TestCase("CAST('abcdefgh' AS string(3)) || 'z'", "abcz")]
    [TestCase("upper(CAST('abcdefgh' AS string(2)))", "AB")]
    public async Task CastToSizedString_FromlessSelect(string expression, string expected)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        IReadOnlyDictionary<string, ColumnValue> row = await SelectOne(executor, dbname, $"SELECT {expression} AS v");

        Assert.That(row["v"].Type, Is.EqualTo(ColumnType.String));
        Assert.That(row["v"].StrValue, Is.EqualTo(expected));
    }

    [TestCase("CAST(NULL AS string(5))")]
    [TestCase("NULL::string(5)")]
    [TestCase("CAST(NULL AS bytes(2))")]
    [TestCase("NULL::bytes(2)")]
    public async Task CastNullToSizedType_IsNull(string expression)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        IReadOnlyDictionary<string, ColumnValue> row = await SelectOne(executor, dbname, $"SELECT {expression} AS v");

        Assert.That(row["v"].Type, Is.EqualTo(ColumnType.Null));
    }

    [TestCase("CAST(X'01020304' AS bytes(2))", new byte[] { 0x01, 0x02 })]
    [TestCase("X'01020304'::bytes(3)", new byte[] { 0x01, 0x02, 0x03 })]
    [TestCase("CAST(X'0102' AS bytes(5))", new byte[] { 0x01, 0x02 })]
    [TestCase("CAST(X'0102' AS bytes(2))", new byte[] { 0x01, 0x02 })]
    [TestCase("CAST('0x0A0B0C' AS bytes(1))", new byte[] { 0x0A })]
    [TestCase("'\\xa0b0c0'::bytes(2)", new byte[] { 0xA0, 0xB0 })]
    [TestCase("CAST(X'01020304' AS blob(1))", new byte[] { 0x01 })]
    public async Task CastToSizedBytes_CutsTheValue(string expression, byte[] expected)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        IReadOnlyDictionary<string, ColumnValue> row = await SelectOne(executor, dbname, $"SELECT {expression} AS v");

        Assert.That(row["v"].Type, Is.EqualTo(ColumnType.Bytes));
        Assert.That(row["v"].BytesValue, Is.EqualTo(expected));
    }

    [Test]
    public async Task CastToSizedString_CutsWhereSubstrCuts()
    {
        // 'x🙂yz' holds a surrogate pair at code point 2; 'áéíóú' is two UTF-8 bytes per character
        // and one UTF-16 unit; '🙂🙂🙂' is three pairs.
        string[] texts = ["x🙂yz", "áéíóú", "🙂🙂🙂", "a🙂", "plain"];

        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE texts (id oid primary key, s string)");

        foreach (string text in texts)
            await ExecNonQuery(executor, dbname, $"INSERT INTO texts (id, s) VALUES (gen_id(), '{text}')");

        for (int n = 1; n <= 6; n++)
        {
            List<QueryResultRow> rows = await ExecSelect(executor, dbname,
                $"SELECT s, CAST(s AS string({n})) AS c, s::string({n}) AS p, substr(s, 1, {n}) AS sub, length(CAST(s AS string({n}))) AS len FROM texts");

            Assert.That(rows, Has.Count.EqualTo(texts.Length));

            foreach (QueryResultRow row in rows)
            {
                string source = row.Row["s"].StrValue!;
                Assert.That(row.Row["c"].StrValue, Is.EqualTo(row.Row["sub"].StrValue), $"CAST('{source}' AS string({n}))");
                Assert.That(row.Row["p"].StrValue, Is.EqualTo(row.Row["sub"].StrValue), $"'{source}'::string({n})");
                Assert.That(row.Row["len"].LongValue, Is.LessThanOrEqualTo(n));
            }
        }

        IReadOnlyDictionary<string, ColumnValue> pair = await SelectOne(executor, dbname,
            "SELECT CAST('x🙂yz' AS string(2)) AS two, CAST('a🙂' AS string(1)) AS one, CAST('🙂🙂🙂' AS string(2)) AS emojis");
        Assert.That(pair["two"].StrValue, Is.EqualTo("x🙂"));
        Assert.That(pair["one"].StrValue, Is.EqualTo("a"));
        Assert.That(pair["emojis"].StrValue, Is.EqualTo("🙂🙂"));
    }

    [TestCase("CAST(title AS string(0))")]
    [TestCase("title::string(0)")]
    [TestCase("CAST(title AS bytes(0))")]
    [TestCase("CAST(title AS string(-1))")]
    [TestCase("CAST(title AS string(99999999999))")]
    public async Task InvalidSize_FailsBeforeAnyRowIsRead(string expression)
    {
        (string dbname, CommandExecutor executor) = await CreateBooks();

        // WHERE FALSE reads no row, so only the static check can refuse the statement.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecSelect(executor, dbname, $"SELECT {expression} AS v FROM books WHERE FALSE"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput), ex.Message);
        Assert.That(ex.Message, Does.Contain("size"));
    }

    [Test]
    public async Task WriteToSizedColumn_IsRefused_ButExplicitCastCuts()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE names (id oid primary key, name string(3), raw bytes(2))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO names (id, name) VALUES (gen_id(), 'abcd')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.ValueTooLong), ex.Message);

        ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO names (id, raw) VALUES (gen_id(), X'010203')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.ValueTooLong), ex.Message);

        await ExecNonQuery(executor, dbname,
            "INSERT INTO names (id, name, raw) VALUES (gen_id(), CAST('abcd' AS string(3)), X'010203'::bytes(2))");

        IReadOnlyDictionary<string, ColumnValue> row = await SelectOne(executor, dbname, "SELECT name, raw FROM names");
        Assert.That(row["name"].StrValue, Is.EqualTo("abc"));
        Assert.That(row["raw"].BytesValue, Is.EqualTo(new byte[] { 0x01, 0x02 }));
    }

    [Test]
    public async Task Explain_ShowsTheSizeOfTheCastTarget()
    {
        (string dbname, CommandExecutor executor) = await CreateBooks();

        List<QueryResultRow> plan = await ExecSelect(executor, dbname,
            "EXPLAIN SELECT title FROM books WHERE CAST(title AS string(4)) = 'Dune' AND CAST(author AS string) = 'Herbert'");
        string details = string.Join("\n", plan.Select(r => r.Row["detail"].StrValue ?? ""));

        Assert.That(details, Does.Contain("AS string(4))"), details);
        Assert.That(details, Does.Contain("AS string)"), details);
    }

    [Test]
    public async Task View_KeepsTheSize_AcrossReopen()
    {
        (string dbname, CommandExecutor executor) = await CreateBooks();

        await ExecDDL(executor, dbname,
            "CREATE VIEW short_titles AS SELECT CAST(title AS string(5)) AS t, title::string(2) AS t2, year FROM books");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        string body = database.Schema.Views["short_titles"].Definition!.Sql;
        Assert.That(body, Does.Contain("string(5)"), body);
        Assert.That(body, Does.Contain("string(2)"), body);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.That(reopened.Schema.Views["short_titles"].Definition!.Sql, Is.EqualTo(body));

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT t, t2 FROM short_titles ORDER BY year");
        Assert.That(rows.Select(r => r.Row["t"].StrValue), Is.EqualTo(new[] { "The L", "Dune", "Neuro" }));
        Assert.That(rows.Select(r => r.Row["t2"].StrValue), Is.EqualTo(new[] { "Th", "Du", "Ne" }));
    }

    [Test]
    public async Task CheckConstraint_KeepsTheSize_AcrossReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE codes (id oid primary key, code string, CONSTRAINT chk_prefix CHECK (CAST(code AS string(2)) = 'ab'))");

        await ExecNonQuery(executor, dbname, "INSERT INTO codes (id, code) VALUES (gen_id(), 'abc')");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO codes (id, code) VALUES (gen_id(), 'xbc')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.CheckConstraintViolation), ex.Message);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        CheckConstraintSchema check = reopened.Schema.Tables["codes"].CheckConstraints!.Single(c => c.Name == "chk_prefix");
        Assert.That(check.Expression, Does.Contain("string(2)"), check.Expression);

        await ExecNonQuery(executor, dbname, "INSERT INTO codes (id, code) VALUES (gen_id(), 'abzzz')");

        ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO codes (id, code) VALUES (gen_id(), 'a')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.CheckConstraintViolation), ex.Message);

        Assert.That(await Strings(executor, dbname, "SELECT code FROM codes", "code"), Is.EqualTo(new[] { "abc", "abzzz" }));
    }

    [Test]
    public async Task ExpressionIndex_OnASizedCast_IsUsedOnlyByTheSameSize_AcrossReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE items (id oid primary key, title string)");
        await ExecDDL(executor, dbname, "CREATE INDEX items_prefix ON items ((CAST(title AS string(5))))");

        foreach (string title in new[] { "abcdefgh", "abcdeXYZ", "abcde", "abc", "🙂🙂🙂🙂🙂🙂", "zzzzzz" })
            await ExecNonQuery(executor, dbname, $"INSERT INTO items (id, title) VALUES (gen_id(), '{title}')");

        await AssertIndexBehavior(executor, dbname);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableIndexSchema persisted = reopened.Schema.Tables["items"].Indexes!.First(ix => ix.Name == "items_prefix");
        Assert.That(persisted.KeyExpressions![0].Sql, Does.Contain("string(5)"), persisted.KeyExpressions[0].Sql);
        Assert.That(persisted.KeyExpressions[0].Type, Is.EqualTo(ColumnType.String));

        await AssertIndexBehavior(executor, dbname);

        // A write after the reopen keys the new row by the cut value.
        await ExecNonQuery(executor, dbname, "INSERT INTO items (id, title) VALUES (gen_id(), 'abcde-new')");
        Assert.That(await Strings(executor, dbname, "SELECT title FROM items WHERE CAST(title AS string(5)) = 'abcde'", "title"),
            Is.EqualTo(new[] { "abcde", "abcde-new", "abcdeXYZ", "abcdefgh" }));
    }

    private static async Task AssertIndexBehavior(CommandExecutor executor, string dbname)
    {
        const string sized = "SELECT title FROM items WHERE CAST(title AS string(5)) = 'abcde'";
        Assert.That(await Strings(executor, dbname, sized, "title"), Is.EqualTo(new[] { "abcde", "abcdeXYZ", "abcdefgh" }));

        (string node, string detail) = await ScanOf(executor, dbname, sized);
        Assert.That(node, Is.Not.EqualTo("table-scan"), detail);
        Assert.That(detail, Does.Contain("items_prefix"), detail);
        Assert.That(detail, Does.Contain("string(5)"), detail);

        Assert.That(await Strings(executor, dbname, "SELECT title FROM items WHERE CAST(title AS string(5)) = '🙂🙂🙂🙂🙂'", "title"),
            Is.EqualTo(new[] { "🙂🙂🙂🙂🙂🙂" }));

        // An unsized cast and another size are other expressions, so they must not read the index.
        const string unsized = "SELECT title FROM items WHERE CAST(title AS string) = 'abcde'";
        Assert.That(await Strings(executor, dbname, unsized, "title"), Is.EqualTo(new[] { "abcde" }));
        (_, string unsizedDetail) = await ScanOf(executor, dbname, unsized);
        Assert.That(unsizedDetail, Does.Not.Contain("items_prefix"), unsizedDetail);

        const string otherSize = "SELECT title FROM items WHERE CAST(title AS string(4)) = 'abcd'";
        Assert.That(await Strings(executor, dbname, otherSize, "title"), Is.EqualTo(new[] { "abcde", "abcdeXYZ", "abcdefgh" }));
        (_, string otherDetail) = await ScanOf(executor, dbname, otherSize);
        Assert.That(otherDetail, Does.Not.Contain("items_prefix"), otherDetail);
    }
}
