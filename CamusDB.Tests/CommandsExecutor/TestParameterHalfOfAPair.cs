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
/// A statement with half of a surrogate pair in a string parameter fails with <c>CADB0400</c> before it
/// runs. Before this rule, each place that encodes text as UTF-8 decided on its own: <c>convert_to</c>,
/// <c>octet_length</c>, <c>hex</c> and the hash functions used U+FFFD with no error, so <c>md5</c> of two
/// different halves was equal and a <c>BYTES</c> column stored the damaged bytes; the jsonb builders threw a
/// raw .NET exception; and a GROUP BY that spilled to disk returned U+FFFD where the same GROUP BY in memory
/// returned the half. A PostgreSQL string cannot hold half of a pair, so the refusal has no PostgreSQL
/// result to compare with. A whole pair keeps the result it had before.
///
/// <para>The values come from code and not from attributes: the compiler writes an attribute string to the
/// metadata as UTF-8, so half of a pair in one reaches the test as U+FFFD.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestParameterHalfOfAPair : BaseTest
{
    private const string Smile = "\U0001F642";

    private const string HighHalf = "\uD83D";

    private const string LowHalf = "\uDE42";

    /// <summary>Each form of half of a pair: alone, between characters, after a pair, and in the wrong order.</summary>
    private static readonly (string Name, string Value)[] Halves =
    [
        ("the first half", HighHalf),
        ("the second half", LowHalf),
        ("a half between characters", "a" + HighHalf + "b"),
        ("a half after a pair", Smile + LowHalf),
        ("the halves in the wrong order", LowHalf + HighHalf),
    ];

    private static Dictionary<string, ColumnValue> Param(string value) => new() { ["@p"] = new ColumnValue(ColumnType.String, value) };

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.Null => "NULL",
        ColumnType.Bytes => "0x" + Convert.ToHexString(value.BytesValue!),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
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

    private static async Task<List<string>> Rows(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
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

    private static async Task Setup(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE ix (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE INDEX ix_s ON ix (s)");
        await Commit(database, executor, dbname, "CREATE TABLE w (id INT64 PRIMARY KEY, s STRING, b BYTES, j JSONB)");
        await Commit(database, executor, dbname, "INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b')");
        await Commit(database, executor, dbname, "INSERT INTO ix (id, s) VALUES (1, 'a'), (2, 'b')");
    }

    private static void AssertRefused(CamusDBException? error, string what)
    {
        Assert.That(error, Is.Not.Null, what);
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput), what);
        Assert.That(error.Message, Does.Contain("Parameter '@p' holds half of a surrogate pair"), what);
    }

    // ── Every statement with half of a pair in a parameter is refused ────────────────────────────

    private static IEnumerable<TestCaseData> Reads()
    {
        (string Name, string Sql)[] reads =
        [
            ("Select_ThePlainParameter", "SELECT @p"),
            ("Length", "SELECT length(@p)"),
            ("Upper", "SELECT upper(@p)"),
            ("Concat", "SELECT concat(@p, 'x')"),
            ("ConcatOperator", "SELECT @p || 'x'"),
            ("Substring", "SELECT substring(@p, 1, 1)"),
            ("Like", "SELECT @p LIKE 'a%'"),
            ("Equality", "SELECT @p = @p"),
            ("ConvertTo", "SELECT convert_to(@p, 'UTF8')"),
            ("OctetLength", "SELECT octet_length(@p)"),
            ("BitLength", "SELECT bit_length(@p)"),
            ("Hex", "SELECT hex(@p)"),
            ("ToBase64", "SELECT to_base64(@p)"),
            ("DecodeEscape", "SELECT decode(@p, 'escape')"),
            ("Md5", "SELECT md5(@p)"),
            ("Sha1", "SELECT sha1(@p)"),
            ("Sha256", "SELECT sha256(@p)"),
            ("Sha512", "SELECT sha512(@p)"),
            ("Digest", "SELECT digest(@p, 'sha256')"),
            ("Hmac_TheData", "SELECT hmac(@p, 'k', 'sha256')"),
            ("Hmac_TheKey", "SELECT hmac('m', @p, 'sha256')"),
            ("Crc32", "SELECT crc32(@p)"),
            ("ToJsonb", "SELECT to_jsonb(@p)"),
            ("JsonbBuildObject_TheKey", "SELECT jsonb_build_object(@p, 1)"),
            ("JsonbBuildObject_TheValue", "SELECT jsonb_build_object('k', @p)"),
            ("JsonbBuildArray", "SELECT jsonb_build_array(@p)"),
            ("JsonbSet_ThePath", "SELECT jsonb_set(CAST('{\"a\":1}' AS JSONB), ARRAY[@p], CAST('1' AS JSONB))"),
            ("JsonbExists", "SELECT jsonb_exists(CAST('{\"a\":1}' AS JSONB), @p)"),
            ("JsonbExistsOperator", "SELECT CAST('{\"a\":1}' AS JSONB) ? @p"),
            ("JsonbField", "SELECT CAST('{\"a\":1}' AS JSONB) -> @p"),
            ("JsonValid", "SELECT json_valid(@p)"),
            ("CastToJsonb", "SELECT CAST(@p AS JSONB)"),
            ("CastToBytes", "SELECT CAST(@p AS BYTES)"),
            ("RegexpReplace", "SELECT regexp_replace(@p, 'a', 'z')"),
            ("Filter_AScan", "SELECT id FROM t WHERE s = @p"),
            ("Filter_AnIndexSeek", "SELECT id FROM ix WHERE s = @p"),
            ("Filter_AnIndexRange", "SELECT id FROM ix WHERE s > @p"),
            ("Filter_AnInList", "SELECT id FROM t WHERE s IN (@p, 'a')"),
            ("Filter_AFalseBranch", "SELECT id FROM t WHERE FALSE AND s = @p"),
            ("Filter_NoRows", "SELECT id FROM t WHERE id = 99 AND s = @p"),
            ("GroupBy", "SELECT @p, COUNT(*) FROM t GROUP BY @p"),
            ("Distinct", "SELECT DISTINCT @p FROM t"),
            ("OrderBy", "SELECT id FROM t ORDER BY concat(s, @p)"),
            ("Limit0", "SELECT md5(@p) FROM t LIMIT 0"),
        ];

        foreach ((string name, string sql) in reads)
            yield return new TestCaseData(sql).SetName("HalfRefused_" + name);
    }

    [TestCaseSource(nameof(Reads))]
    public async Task AStatement_WithHalfOfAPairInAParameter_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Setup(database, executor, dbname);

        foreach ((string name, string value) in Halves)
            AssertRefused(Assert.ThrowsAsync<CamusDBException>(() => Rows(database, executor, dbname, sql, Param(value))), name);
    }

    private static IEnumerable<TestCaseData> Writes()
    {
        (string Name, string Sql)[] writes =
        [
            ("ConvertToIntoBytes", "INSERT INTO w (id, b) VALUES (1, convert_to(@p, 'UTF8'))"),
            ("Md5IntoString", "INSERT INTO w (id, s) VALUES (1, md5(@p))"),
            ("HexIntoString", "INSERT INTO w (id, s) VALUES (1, hex(@p))"),
            ("ToJsonbIntoJsonb", "INSERT INTO w (id, j) VALUES (1, to_jsonb(@p))"),
            ("JsonbBuildObjectIntoJsonb", "INSERT INTO w (id, j) VALUES (1, jsonb_build_object(@p, 1))"),
            ("ThePlainParameter", "INSERT INTO w (id, s) VALUES (1, @p)"),
            ("Update", "UPDATE w SET s = @p WHERE id = 1"),
            ("UpdateThatMatchesNoRow", "UPDATE w SET b = convert_to(@p, 'UTF8') WHERE id = 99"),
            ("Delete", "DELETE FROM w WHERE s = @p"),
            ("InsertSelect", "INSERT INTO w (id, b) SELECT id + 10, convert_to(@p, 'UTF8') FROM t"),
        ];

        foreach ((string name, string sql) in writes)
            yield return new TestCaseData(sql).SetName("HalfRefused_Write_" + name);
    }

    [TestCaseSource(nameof(Writes))]
    public async Task AWrite_WithHalfOfAPairInAParameter_IsRefused_AndChangesNoRow(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Setup(database, executor, dbname);

        foreach ((string name, string value) in Halves)
        {
            AssertRefused(Assert.ThrowsAsync<CamusDBException>(() => Commit(database, executor, dbname, sql, Param(value))), name);
            Assert.That(await Rows(database, executor, dbname, "SELECT id FROM w"), Is.Empty, name);
        }
    }

    [Test]
    public async Task ADdlStatement_WithHalfOfAPairInAParameter_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertRefused(Assert.ThrowsAsync<CamusDBException>(() =>
            Commit(database, executor, dbname, "CREATE TABLE c AS SELECT md5(@p) AS h", Param(HighHalf))), "CREATE TABLE AS");

        Assert.That(await Rows(database, executor, dbname, "SHOW TABLES"), Is.Empty);
    }

    // ── The message names the parameter and the place ────────────────────────────────────────────

    private static IEnumerable<TestCaseData> Positions()
    {
        yield return new TestCaseData(HighHalf, "(U+D83D) at character 1").SetName("Message_TheFirstHalfAlone");
        yield return new TestCaseData("ab" + LowHalf, "(U+DE42) at character 3").SetName("Message_AfterTwoCharacters");
        yield return new TestCaseData(Smile + HighHalf, "(U+D83D) at character 2").SetName("Message_AfterAPair_CountsThePairAsOne");
        yield return new TestCaseData(LowHalf + HighHalf, "(U+DE42) at character 1").SetName("Message_TheHalvesInTheWrongOrder_NamesTheFirst");
    }

    [TestCaseSource(nameof(Positions))]
    public async Task TheMessage_NamesTheParameterAndTheCharacter(string value, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Rows(database, executor, dbname, "SELECT @p", Param(value)));

        Assert.That(error!.Message, Does.Contain("Parameter '@p' holds half of a surrogate pair " + expected));
    }

    [Test]
    public async Task AStringArrayParameter_WithHalfOfAPairInAnElement_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Setup(database, executor, dbname);

        Dictionary<string, ColumnValue> half = new()
        {
            ["@a"] = ColumnValue.FromArray(ColumnType.String, [new ColumnValue(ColumnType.String, "a"), new ColumnValue(ColumnType.String, "x" + HighHalf)]),
        };

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Rows(database, executor, dbname, "SELECT id FROM t WHERE s = ANY(@a)", half));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(error.Message, Does.Contain("Element 2 of parameter '@a' holds half of a surrogate pair (U+D83D) at character 2"));

        Dictionary<string, ColumnValue> whole = new()
        {
            ["@a"] = ColumnValue.FromArray(ColumnType.String, [new ColumnValue(ColumnType.String, "a"), new ColumnValue(ColumnType.String, Smile)]),
        };

        Assert.That(await Rows(database, executor, dbname, "SELECT id FROM t WHERE s = ANY(@a)", whole), Is.EqualTo(new[] { "1" }));
    }

    [Test]
    public async Task AParameterThatTheStatementDoesNotUse_IsStillRefused()
    {
        // The check reads the bound values, not the statement, so it does not depend on which
        // placeholders the statement names.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertRefused(Assert.ThrowsAsync<CamusDBException>(() => Rows(database, executor, dbname, "SELECT 1", Param(HighHalf))), "unused");
    }

    // ── A whole pair keeps its result ────────────────────────────────────────────────────────────

    [TestCase("SELECT @p", Smile, TestName = "Pair_ThePlainParameter")]
    [TestCase("SELECT length(@p)", "1", TestName = "Pair_Length")]
    [TestCase("SELECT convert_to(@p, 'UTF8')", "0xF09F9982", TestName = "Pair_ConvertTo")]
    [TestCase("SELECT octet_length(@p)", "4", TestName = "Pair_OctetLength")]
    [TestCase("SELECT bit_length(@p)", "32", TestName = "Pair_BitLength")]
    [TestCase("SELECT hex(@p)", "F09F9982", TestName = "Pair_Hex")]
    [TestCase("SELECT to_base64(@p)", "8J+Zgg==", TestName = "Pair_ToBase64")]
    [TestCase("SELECT decode(@p, 'escape')", "0xF09F9982", TestName = "Pair_DecodeEscape")]
    [TestCase("SELECT md5(@p)", "5c8d6d302301d0e25c0e051418dff305", TestName = "Pair_Md5")]
    [TestCase("SELECT sha1(@p)", "0402582bd3af1f752930098a7807f72f362184f6", TestName = "Pair_Sha1")]
    [TestCase("SELECT sha256(@p)", "d06f1525f791397809f9bc98682b5c13318eca4c3123433467fd4dffda44fd14", TestName = "Pair_Sha256")]
    [TestCase("SELECT hmac(@p, 'k', 'sha256')", "0xFDC5CB91E38F8835038658CA627615F7705C37FAD25F16F162FF90C019E24790", TestName = "Pair_Hmac_TheData")]
    [TestCase("SELECT hmac('m', @p, 'sha256')", "0x26454551EDD19858851091861D461A761D80C99FB1956977B7FD28D9EC8EAF1F", TestName = "Pair_Hmac_TheKey")]
    [TestCase("SELECT crc32(@p)", "4065912105", TestName = "Pair_Crc32")]
    [TestCase("SELECT CAST(to_jsonb(@p) AS STRING)", "\"" + Smile + "\"", TestName = "Pair_ToJsonb")]
    [TestCase("SELECT CAST(jsonb_build_object(@p, 1) AS STRING)", "{\"" + Smile + "\": 1}", TestName = "Pair_JsonbBuildObject")]
    [TestCase("SELECT CAST('{\"a\":1}' AS JSONB) ? @p", "f", TestName = "Pair_JsonbExists")]
    public async Task AWholePair_KeepsItsResult(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.That(await Rows(database, executor, dbname, sql, Param(Smile)), Is.EqualTo(new[] { expected }));
    }

    [Test]
    public async Task AWholePair_IsStoredByEachEncoder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Setup(database, executor, dbname);

        await Commit(database, executor, dbname, "INSERT INTO w (id, s, b, j) VALUES (1, @p, convert_to(@p, 'UTF8'), to_jsonb(@p))", Param(Smile));

        Assert.That(await Rows(database, executor, dbname, "SELECT s, b, CAST(j AS STRING) FROM w"),
            Is.EqualTo(new[] { Smile + "|0xF09F9982|\"" + Smile + "\"" }));
    }

    // ── The answer does not depend on the plan ───────────────────────────────────────────────────

    private const string GroupedSql = "SELECT concat(s, @p) AS x, COUNT(*) FROM t GROUP BY concat(s, @p) ORDER BY x";

    [Test]
    public async Task AGroupByThatSpills_GivesTheSameAnswerAsInMemory()
    {
        // A spilled aggregate writes its group keys as UTF-8. Before the parameter check, half of a
        // pair came back as U+FFFD from disk and as the half from memory.
        (string memoryName, DatabaseDescriptor memory, CommandExecutor memoryExecutor) = await CreateDatabase();
        (string spillName, DatabaseDescriptor spill, CommandExecutor spillExecutor) =
            await CreateDatabase(Options with { SpillEnabled = true, SpillThresholdRows = 2 });

        foreach ((string name, DatabaseDescriptor database, CommandExecutor executor) in new[] { (memoryName, memory, memoryExecutor), (spillName, spill, spillExecutor) })
        {
            await Commit(database, executor, name, "CREATE TABLE t (id INT64 PRIMARY KEY, s STRING)");
            await Commit(database, executor, name, "INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
        }

        List<string> expected = ["a" + Smile + "|1", "b" + Smile + "|1", "c" + Smile + "|1", "d" + Smile + "|1", "e" + Smile + "|1"];

        Assert.That(await Rows(memory, memoryExecutor, memoryName, GroupedSql, Param(Smile)), Is.EqualTo(expected), "in memory");
        Assert.That(await Rows(spill, spillExecutor, spillName, GroupedSql, Param(Smile)), Is.EqualTo(expected), "spilled");

        AssertRefused(Assert.ThrowsAsync<CamusDBException>(() => Rows(memory, memoryExecutor, memoryName, GroupedSql, Param(HighHalf))), "in memory");
        AssertRefused(Assert.ThrowsAsync<CamusDBException>(() => Rows(spill, spillExecutor, spillName, GroupedSql, Param(HighHalf))), "spilled");
    }

    // ── The helper ───────────────────────────────────────────────────────────────────────────────

    [Test]
    public void IndexOfUnpairedSurrogate_OfEachForm()
    {
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(""), Is.EqualTo(-1));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate("abc"), Is.EqualTo(-1));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(Smile + Smile), Is.EqualTo(-1));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(HighHalf), Is.EqualTo(0));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate("ab" + LowHalf), Is.EqualTo(2));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(Smile + HighHalf), Is.EqualTo(2));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(HighHalf + "x"), Is.EqualTo(0));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(LowHalf + HighHalf), Is.EqualTo(0));
        Assert.That(TextCodePoints.IndexOfUnpairedSurrogate(Smile + "x" + LowHalf + Smile), Is.EqualTo(3));
    }
}
