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
/// Text functions and a <c>CHAR(n)</c> limit count code points, so a character outside the Basic
/// Multilingual Plane counts as one and a slice cannot cut it in half. Half of a pair has no UTF-8 form, and
/// a write once stored U+FFFD for it with no error, so the stored value differed from the same expression
/// in a SELECT. Now no function returns half of a pair, and a write refuses one. Each expected result
/// was measured on PostgreSQL 16.11, except the refusals, which cannot occur there: a PostgreSQL string
/// cannot hold half of a pair.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestStringCodePoints : BaseTest
{
    private const string Smile = "\U0001F642";

    private const string Grin = "\U0001F600";

    /// <summary>A pair with the same second unit as <see cref="Smile"/> and a different first unit.</summary>
    private const string SameLowUnit = "\U0001F242";

    private const string HighHalf = "\uD83D";

    private const string LowHalf = "\uDE42";

    private static async Task Commit(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));

            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.Bool => value.BoolValue.ToString(),
        ColumnType.Null => "NULL",
        ColumnType.Bytes => Convert.ToHexString(value.BytesValue!),
        ColumnType.Array => string.Join(",", value.ArrayValues!.Select(Render)),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task<List<string>> Rows(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return rows.Select(r => string.Join("|", r.Row.Values.Select(Render))).ToList();
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private async Task<string> Scalar(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<string> rows = await Rows(database, executor, dbname, sql);
        Assert.That(rows, Has.Count.EqualTo(1), sql);
        return rows[0];
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE ix (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE INDEX ix_s ON ix (s)");

        return (dbname, database, executor);
    }

    private static Dictionary<string, ColumnValue> Param(string value) => new() { ["@p"] = new ColumnValue(ColumnType.String, value) };

    // ── The functions, as PostgreSQL gives them ──────────────────────────────────────────────────

    [TestCase("SELECT length('" + Smile + "')", "1", TestName = "Length_APair")]
    [TestCase("SELECT length('a" + Smile + "b')", "3", TestName = "Length_APairAmongOthers")]
    [TestCase("SELECT length('" + Smile + Grin + "')", "2", TestName = "Length_TwoPairs")]
    [TestCase("SELECT length('')", "0", TestName = "Length_Empty")]
    [TestCase("SELECT length('abc')", "3", TestName = "Length_NoPair")]
    [TestCase("SELECT octet_length('" + Smile + "')", "4", TestName = "OctetLength_StaysBytes")]
    [TestCase("SELECT length(convert_to('" + Smile + "', 'UTF8'))", "4", TestName = "Length_OfBytes_StaysBytes")]
    [TestCase("SELECT substring('" + Smile + "x', 1, 1)", Smile, TestName = "Substring_FirstIsThePair")]
    [TestCase("SELECT substring('" + Smile + "x', 2, 1)", "x", TestName = "Substring_SecondIsAfterThePair")]
    [TestCase("SELECT substring('" + Smile + "x', 2)", "x", TestName = "Substring_NoLength_AfterThePair")]
    [TestCase("SELECT substring('x" + Smile + "y', 2, 1)", Smile, TestName = "Substring_ThePairInTheMiddle")]
    [TestCase("SELECT substring('x" + Smile + "y', 2)", Smile + "y", TestName = "Substring_NoLength_FromThePair")]
    [TestCase("SELECT substring('" + Smile + Smile + "', 2, 5)", Smile, TestName = "Substring_LengthPastTheEnd")]
    [TestCase("SELECT substring('" + Smile + "', 2)", "", TestName = "Substring_StartJustPastTheEnd")]
    [TestCase("SELECT substring('" + Smile + "', 3)", "", TestName = "Substring_StartPastTheEnd")]
    [TestCase("SELECT substring('" + Smile + "', 1, 0)", "", TestName = "Substring_ZeroLength")]
    [TestCase("SELECT substring('abcde', 2, 2)", "bc", TestName = "Substring_NoPair")]
    [TestCase("SELECT substring('" + Smile + "', 9223372036854775807)", "", TestName = "Substring_LargestStart")]
    [TestCase("SELECT substring('" + Smile + "x', 1, 9223372036854775807)", Smile + "x", TestName = "Substring_LargestLength")]
    [TestCase("SELECT substring('abc', 9223372036854775807)", "", TestName = "Substring_LargestStart_NoPair")]
    [TestCase("SELECT hex(substring(X'F09F9982', 2, 1))", "9F", TestName = "Substring_OfBytes_StaysBytes")]
    [TestCase("SELECT strpos('x" + Smile + "y', 'y')", "3", TestName = "Strpos_AfterAPair")]
    [TestCase("SELECT strpos('" + Smile + Smile + "', '" + Smile + "')", "1", TestName = "Strpos_APairFirst")]
    [TestCase("SELECT strpos('x" + Smile + "', '" + Smile + "')", "2", TestName = "Strpos_APairSecond")]
    [TestCase("SELECT strpos('" + Smile + "', 'z')", "0", TestName = "Strpos_NotFound")]
    [TestCase("SELECT strpos('" + Smile + "', '')", "1", TestName = "Strpos_EmptyNeedle")]
    [TestCase("SELECT strpos(X'F09F9982', X'82')", "4", TestName = "Strpos_OfBytes_StaysBytes")]
    [TestCase("SELECT POSITION('y' IN 'x" + Smile + "y')", "3", TestName = "Position_AfterAPair")]
    [TestCase("SELECT ltrim('" + Smile + "x', '" + Grin + "')", Smile + "x", TestName = "Ltrim_ASetWithAPairThatSharesTheFirstUnit")]
    [TestCase("SELECT ltrim('" + Smile + "x', '" + Smile + "')", "x", TestName = "Ltrim_TheSamePair")]
    [TestCase("SELECT rtrim('x" + Smile + "', '" + Grin + "')", "x" + Smile, TestName = "Rtrim_ASetWithAPairThatSharesTheFirstUnit")]
    [TestCase("SELECT rtrim('x" + Smile + "', '" + Smile + "')", "x", TestName = "Rtrim_TheSamePair")]
    [TestCase("SELECT trim('" + Smile + "x" + Smile + "', '" + Smile + "')", "x", TestName = "Trim_TheSamePair")]
    [TestCase("SELECT trim('" + Smile + "x" + Smile + "', '" + Grin + "')", Smile + "x" + Smile, TestName = "Trim_ADifferentPair")]
    [TestCase("SELECT rtrim('x" + Smile + "', '" + SameLowUnit + "')", "x" + Smile, TestName = "Rtrim_ASetWithAPairThatSharesTheSecondUnit")]
    [TestCase("SELECT ltrim('" + SameLowUnit + "x', '" + Smile + "')", SameLowUnit + "x", TestName = "Ltrim_ASetWithAPairThatSharesTheSecondUnit")]
    [TestCase("SELECT trim('" + Smile + "', '" + SameLowUnit + "')", Smile, TestName = "Trim_ASetWithAPairThatSharesTheSecondUnit")]
    [TestCase("SELECT ltrim('" + Smile + Grin + "x', '" + Grin + Smile + "')", "x", TestName = "Ltrim_ASetOfTwoPairs")]
    [TestCase("SELECT trim('" + Smile + Smile + "', '" + Smile + "')", "", TestName = "Trim_RemovesEverything")]
    [TestCase("SELECT ltrim('a" + Smile + "', 'a')", Smile, TestName = "Ltrim_ASetWithNoPair_KeepsAPair")]
    [TestCase("SELECT rtrim('" + Smile + "a', 'a')", Smile, TestName = "Rtrim_ASetWithNoPair_KeepsAPair")]
    [TestCase("SELECT replace('" + Smile + "x', '" + Smile + "', 'y')", "yx", TestName = "Replace_APair")]
    [TestCase("SELECT upper('" + Smile + "a')", Smile + "A", TestName = "Upper_KeepsAPair")]
    public async Task CountsCodePoints(string sql, string expected) =>
        Assert.That(await Scalar(sql), Is.EqualTo(expected));

    [TestCase("SELECT regexp_instr('" + Smile + "x', 'x')", "2", TestName = "RegexpInstr_AfterAPair")]
    [TestCase("SELECT regexp_instr('" + Smile + "x', 'x', 1, 1, 1)", "3", TestName = "RegexpInstr_EndOption")]
    [TestCase("SELECT regexp_instr('" + Smile + "xy', 'y', 3)", "3", TestName = "RegexpInstr_StartCountsCodePoints")]
    [TestCase("SELECT regexp_count('x" + Smile + "x', 'x', 2)", "1", TestName = "RegexpCount_StartAtThePair")]
    [TestCase("SELECT regexp_count('x" + Smile + "x', 'x', 3)", "1", TestName = "RegexpCount_StartAfterThePair")]
    [TestCase("SELECT regexp_count('" + Smile + "xx', 'x', 3)", "1", TestName = "RegexpCount_StartCountsCodePoints")]
    [TestCase("SELECT regexp_instr('" + Smile + "xx', 'x', 3)", "3", TestName = "RegexpInstr_StartPastAPair")]
    [TestCase("SELECT regexp_substr('" + Smile + "xx', 'x', 3)", "x", TestName = "RegexpSubstr_StartPastAPair")]
    [TestCase("SELECT regexp_substr('" + Smile + "xy', '[a-z]', 2)", "x", TestName = "RegexpSubstr_StartAfterThePair")]
    [TestCase("SELECT regexp_substr('" + Smile + "xy', '[a-z]', 3)", "y", TestName = "RegexpSubstr_StartCountsCodePoints")]
    [TestCase("SELECT regexp_substr('" + Smile + "x', 'x')", "x", TestName = "RegexpSubstr_NamedCharacter")]
    [TestCase("SELECT regexp_substr('a" + Smile + "b', '" + Smile + "')", Smile, TestName = "RegexpSubstr_NamedPair")]
    [TestCase("SELECT regexp_split_to_array('a" + Smile + "', '')", "a," + Smile, TestName = "RegexpSplit_EmptyPattern_APairIsOnePart")]
    [TestCase("SELECT regexp_split_to_array('a," + Smile + "', ',')", "a," + Smile, TestName = "RegexpSplit_APairInAPart")]
    [TestCase("SELECT regexp_match('" + Smile + "x', 'x')", "x", TestName = "RegexpMatch_NamedCharacter")]
    [TestCase("SELECT regexp_replace('a" + Smile + "', '" + Smile + "', 'b')", "ab", TestName = "RegexpReplace_NamedPair")]
    public async Task RegexPositionsCountCodePoints(string sql, string expected) =>
        Assert.That(await Scalar(sql), Is.EqualTo(expected));

    /// <summary>The engine matches UTF-16 units, so these results would hold half of a pair. They are on the list of decided differences.</summary>
    [TestCase("SELECT regexp_substr('" + Smile + "x', '.')", TestName = "RegexRefused_Substr")]
    [TestCase("SELECT regexp_substr('" + Smile + "x', '(.)', 1, 1, '', 1)", TestName = "RegexRefused_SubstrGroup")]
    [TestCase("SELECT regexp_replace('" + Smile + "', '.', 'x')", TestName = "RegexRefused_Replace")]
    [TestCase("SELECT regexp_match('" + Smile + "x', '.')", TestName = "RegexRefused_Match")]
    [TestCase("SELECT regexp_match('" + Smile + "x', '(.)')", TestName = "RegexRefused_MatchGroup")]
    [TestCase("SELECT regexp_instr('" + Smile + "x', '.', 1, 2)", TestName = "RegexRefused_PositionInsideAPair")]
    [TestCase("SELECT regexp_instr('" + Smile + "x', '.', 1, 1, 1)", TestName = "RegexRefused_EndInsideAPair")]
    [TestCase("SELECT regexp_split_to_array('" + Smile + "x', '[\\uDE42]')", TestName = "RegexRefused_SplitKeepsHalf")]
    public void RegexResultWithHalfAPair_IsRefused(string sql)
    {
        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Scalar(sql));
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    [Test]
    public async Task ARegexThatMatchesNoHalf_OverAPair_StillRuns()
    {
        // The form that the docs give for any one character.
        Assert.That(await Scalar("SELECT regexp_substr('" + Smile + "x', '(?:[\\uD800-\\uDBFF][\\uDC00-\\uDFFF]|.)')"), Is.EqualTo(Smile));
        Assert.That(await Scalar("SELECT regexp_count('" + Smile + "x', '(?:[\\uD800-\\uDBFF][\\uDC00-\\uDFFF]|.)')"), Is.EqualTo("2"));
        Assert.That(await Scalar("SELECT regexp_count('" + Smile + "', '" + Smile + "')"), Is.EqualTo("1"));
        Assert.That(await Scalar("SELECT regexp_like('" + Smile + "', '^" + Smile + "$')"), Is.EqualTo("True"));
    }

    [Test]
    public void AJsonFunction_WithAnEscapedHalf_FailsAsASqlError()
    {
        // The read of such a string threw InvalidOperationException, which is not a SQL error.
        CamusDBException? value = Assert.ThrowsAsync<CamusDBException>(() => Scalar("SELECT json_value('{\"a\":\"\\ud83d\"}', '$.a')"));
        Assert.That(value!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidJsonText));

        CamusDBException? contains = Assert.ThrowsAsync<CamusDBException>(() => Scalar("SELECT json_contains('{\"a\":\"\\ud83d\"}', '{\"a\":\"\\ud83d\"}')"));
        Assert.That(contains!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidJsonText));
    }

    // ── A write stores the value of the same expression in a SELECT ──────────────────────────────

    [TestCase("t", TestName = "Write_TheSubstringOfAPair_IsStored_Scan")]
    [TestCase("ix", TestName = "Write_TheSubstringOfAPair_IsStored_Index")]
    public async Task TheSubstringOfAPair_IsStoredAsTheSelectGivesIt(string table)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, $"INSERT INTO {table} (id, s) VALUES (1, substring('{Smile}x', 1, 1))");
        await Commit(database, executor, dbname, $"INSERT INTO {table} (id, s) VALUES (2, '{Smile}x')");
        await Commit(database, executor, dbname, $"UPDATE {table} SET s = substring(s, 1, 1) WHERE id = 2");

        Assert.That(await Rows(database, executor, dbname, $"SELECT id, s, length(s) FROM {table} ORDER BY id"),
            Is.EqualTo(new[] { $"1|{Smile}|1", $"2|{Smile}|1" }));
        Assert.That(await Rows(database, executor, dbname, $"SELECT id FROM {table} WHERE s = substring('{Smile}x', 1, 1) ORDER BY id"),
            Is.EqualTo(new[] { "1", "2" }));
        Assert.That(await Rows(database, executor, dbname, $"SELECT id FROM {table} WHERE s = '{Smile}' ORDER BY id"),
            Is.EqualTo(new[] { "1", "2" }));
        Assert.That(await Rows(database, executor, dbname, $"SELECT id FROM {table} WHERE s = E'\\uFFFD'"), Is.Empty);
    }

    [Test]
    public async Task AFilterOverAStoredPair_CountsCodePoints()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, $"INSERT INTO t (id, s) VALUES (1, '{Smile}'), (2, 'ab'), (3, 'a{Smile}')");

        Assert.That(await Rows(database, executor, dbname, "SELECT id FROM t WHERE length(s) = 2 ORDER BY id"), Is.EqualTo(new[] { "2", "3" }));
        Assert.That(await Rows(database, executor, dbname, "SELECT substring(s, 2) FROM t WHERE id = 3"), Is.EqualTo(new[] { Smile }));
        Assert.That(await Rows(database, executor, dbname, "SELECT strpos(s, 'a') FROM t WHERE id = 3"), Is.EqualTo(new[] { "1" }));
    }

    // ── The column limit ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task AColumnLimit_CountsCodePoints()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE c1 (id INT64 PRIMARY KEY, s CHAR(1))");
        await Commit(database, executor, dbname, "CREATE TABLE v2 (id INT64 PRIMARY KEY, s VARCHAR(2))");

        await Commit(database, executor, dbname, $"INSERT INTO c1 (id, s) VALUES (1, '{Smile}')");
        await Commit(database, executor, dbname, $"INSERT INTO v2 (id, s) VALUES (1, '{Smile}{Smile}')");
        await Commit(database, executor, dbname, $"UPDATE v2 SET s = '{Smile}x' WHERE id = 1");

        Assert.That(await Rows(database, executor, dbname, "SELECT s, length(s), octet_length(s) FROM v2"), Is.EqualTo(new[] { $"{Smile}x|2|5" }));
        Assert.That(await Rows(database, executor, dbname, "SELECT s FROM c1"), Is.EqualTo(new[] { Smile }));
    }

    [TestCase("INSERT INTO c1 (id, s) VALUES (2, '" + Smile + "x')", "max 1, got 2", TestName = "Limit_InsertCharOver")]
    [TestCase("INSERT INTO v2 (id, s) VALUES (2, '" + Smile + Smile + Smile + "')", "max 2, got 3", TestName = "Limit_InsertVarcharOver")]
    [TestCase("UPDATE v2 SET s = '" + Smile + "xy' WHERE id = 1", "max 2, got 3", TestName = "Limit_UpdateOver")]
    [TestCase("INSERT INTO v2 (id, s) VALUES (3, 'abc')", "max 2, got 3", TestName = "Limit_NoPairOver")]
    public async Task AValuePastTheLimit_IsRefused_WithItsCodePointCount(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE c1 (id INT64 PRIMARY KEY, s CHAR(1))");
        await Commit(database, executor, dbname, "CREATE TABLE v2 (id INT64 PRIMARY KEY, s VARCHAR(2))");
        await Commit(database, executor, dbname, "INSERT INTO v2 (id, s) VALUES (1, 'a')");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Commit(database, executor, dbname, sql));
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.ValueTooLong));
        Assert.That(error.Message, Does.Contain(message));
    }

    [Test]
    public async Task ADefault_FollowsTheSameLimit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, $"CREATE TABLE d1 (id INT64 PRIMARY KEY, s CHAR(1) DEFAULT ('{Smile}'))");
        await Commit(database, executor, dbname, "INSERT INTO d1 (id) VALUES (1)");
        Assert.That(await Rows(database, executor, dbname, "SELECT s FROM d1"), Is.EqualTo(new[] { Smile }));

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() =>
            Commit(database, executor, dbname, $"CREATE TABLE d2 (id INT64 PRIMARY KEY, s CHAR(1) DEFAULT ('{Smile}x'))"));
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.ValueTooLong));
    }

    // ── Half of a pair is refused at a write ─────────────────────────────────────────────────────

    // These cases come from a source and not from attributes. The compiler writes an attribute string to
    // the metadata as UTF-8, so half of a pair in one reaches the test as replacement characters.
    private static IEnumerable<TestCaseData> ValuesWithHalfAPair()
    {
        yield return new TestCaseData(HighHalf).SetName("HalfRefused_Insert_HighHalf");
        yield return new TestCaseData(LowHalf).SetName("HalfRefused_Insert_LowHalf");
        yield return new TestCaseData("x" + HighHalf).SetName("HalfRefused_Insert_HighHalfAtTheEnd");
        yield return new TestCaseData(LowHalf + HighHalf).SetName("HalfRefused_Insert_TheHalvesInTheWrongOrder");
        yield return new TestCaseData(Smile + LowHalf).SetName("HalfRefused_Insert_AfterAWholePair");
    }

    [TestCaseSource(nameof(ValuesWithHalfAPair))]
    public async Task AnInsertedParameter_WithHalfAPair_IsRefused(string value)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        foreach (string table in new[] { "t", "ix" })
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() =>
                Commit(database, executor, dbname, $"INSERT INTO {table} (id, s) VALUES (1, @p)", Param(value)));

            Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(error.Message, Does.Contain("column 's'"));
            Assert.That(await Rows(database, executor, dbname, $"SELECT id FROM {table}"), Is.Empty);
        }
    }

    [Test]
    public async Task AWholePairInAParameter_IsStored()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "INSERT INTO t (id, s) VALUES (1, @p)", Param(Smile));

        Assert.That(await Rows(database, executor, dbname, "SELECT s FROM t"), Is.EqualTo(new[] { Smile }));
    }

    [TestCase("UPDATE t SET s = @p WHERE id = 1", TestName = "HalfRefused_Update")]
    [TestCase("INSERT INTO t (id, s) VALUES (1, 'z') ON CONFLICT (id) DO UPDATE SET s = @p", TestName = "HalfRefused_OnConflictUpdate")]
    [TestCase("INSERT INTO t (id, s) SELECT 2, @p", TestName = "HalfRefused_InsertSelect")]
    [TestCase("UPDATE t SET s = concat('x', @p) WHERE id = 1", TestName = "HalfRefused_Update_ThroughAFunction")]
    public async Task AWrite_WithHalfAPair_IsRefused_AndKeepsTheRow(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "INSERT INTO t (id, s) VALUES (1, 'old')");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Commit(database, executor, dbname, sql, Param(HighHalf)));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(await Rows(database, executor, dbname, "SELECT id, s FROM t"), Is.EqualTo(new[] { "1|old" }));
    }

    [Test]
    public async Task ALargeValue_WithHalfAPair_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // Past the size where a cell goes out of line, so the value takes the other path of the encoder.
        string large = new string('x', 100_000) + HighHalf;

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() =>
            Commit(database, executor, dbname, "INSERT INTO t (id, s) VALUES (1, @p)", Param(large)));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(await Rows(database, executor, dbname, "SELECT id FROM t"), Is.Empty);
    }

    [Test]
    public async Task ATypedInsert_WithHalfAPair_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: dbname,
            tableName: "t",
            values:
            [
                new Dictionary<string, ColumnValue>
                {
                    ["id"] = new ColumnValue(ColumnType.Integer64, 1),
                    ["s"] = new ColumnValue(ColumnType.String, HighHalf),
                },
            ])));

        await database.Transactions.RollbackAsync(tx);

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(await Rows(database, executor, dbname, "SELECT id FROM t"), Is.Empty);
    }

    [Test]
    public async Task AFilterWithHalfAPair_FindsNoRowAndDoesNotFail()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, $"INSERT INTO t (id, s) VALUES (1, '{Smile}')");
        await Commit(database, executor, dbname, $"INSERT INTO ix (id, s) VALUES (1, '{Smile}')");

        Assert.That(await Rows(database, executor, dbname, "SELECT id FROM t WHERE s = @p", Param(HighHalf)), Is.Empty);
        Assert.That(await Rows(database, executor, dbname, "SELECT id FROM ix WHERE s = @p", Param(HighHalf)), Is.Empty);
    }

    // ── The helper ───────────────────────────────────────────────────────────────────────────────

    [Test]
    public void Count_OfEachForm()
    {
        Assert.That(TextCodePoints.Count(""), Is.EqualTo(0));
        Assert.That(TextCodePoints.Count("abc"), Is.EqualTo(3));
        Assert.That(TextCodePoints.Count(Smile), Is.EqualTo(1));
        Assert.That(TextCodePoints.Count("a" + Smile + "b" + Grin), Is.EqualTo(4));
        Assert.That(TextCodePoints.Count(HighHalf), Is.EqualTo(1));
        Assert.That(TextCodePoints.Count(LowHalf + HighHalf), Is.EqualTo(2));
        Assert.That(TextCodePoints.Count(HighHalf + "x"), Is.EqualTo(2));
    }

    [TestCase("abc", 0L, 0)]
    [TestCase("abc", 2L, 2)]
    [TestCase("abc", 3L, 3)]
    [TestCase("abc", long.MaxValue, 3)]
    [TestCase("a" + Smile + "b", 1L, 1)]
    [TestCase("a" + Smile + "b", 2L, 3)]
    [TestCase("a" + Smile + "b", 3L, 4)]
    [TestCase("a" + Smile + "b", long.MaxValue, 4)]
    public void UnitOffset_OfEachPosition(string text, long codePoints, int expected) =>
        Assert.That(TextCodePoints.UnitOffset(text, codePoints), Is.EqualTo(expected));

    [Test]
    public void HasUnpairedSurrogate_OfEachForm()
    {
        foreach (string whole in new[] { "", "abc", Smile, Smile + Grin })
            Assert.That(TextCodePoints.HasUnpairedSurrogate(whole), Is.False, Convert.ToHexString(System.Text.Encoding.Unicode.GetBytes(whole)));

        foreach (string half in new[] { HighHalf, LowHalf, LowHalf + HighHalf, Smile + LowHalf, HighHalf + Smile })
            Assert.That(TextCodePoints.HasUnpairedSurrogate(half), Is.True, half.Length + " units");
    }

    [Test]
    public void LengthForLimit_CountsOnlyPastTheLimit()
    {
        Assert.That(TextCodePoints.LengthForLimit(Smile, 2), Is.EqualTo(2), "units inside the limit need no count");
        Assert.That(TextCodePoints.LengthForLimit(Smile, 1), Is.EqualTo(1));
        Assert.That(TextCodePoints.LengthForLimit(Smile + "x", 1), Is.EqualTo(2));
    }
}
