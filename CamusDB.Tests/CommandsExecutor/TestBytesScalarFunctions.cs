/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for the <c>bytes</c> functions through SQL: the text forms (<c>encode</c>,
/// <c>decode</c>, <c>hex</c>, base64, <c>convert_to</c> / <c>convert_from</c>), the <c>bytes</c> forms of
/// the text functions, <c>strpos</c> and <c>POSITION(x IN y)</c>, byte and bit access, the binary digests,
/// <c>crc32</c> and <c>gen_random_bytes</c>. Each function is called with empty input, one byte, NULL and
/// a bad argument. One test reads an out-of-line column, and one stores a result in a <c>bytes(N)</c>
/// column.
/// </summary>
[NonParallelizable]
internal sealed class TestBytesScalarFunctions : SharedNodeBaseTest
{
    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<IReadOnlyDictionary<string, ColumnValue>> Row(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row;
    }

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, DatabaseDescriptor db, string expression,
        Dictionary<string, ColumnValue>? parameters = null)
        => (await Row(executor, db, "SELECT " + expression + " AS v", parameters))["v"];

    private static async Task<CamusDBException> AssertThrows(CommandExecutor executor, DatabaseDescriptor db, string expression,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, "SELECT " + expression + " AS v", parameters));
            await cursor.ToListAsync();
        })!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task AssertInvalid(CommandExecutor executor, DatabaseDescriptor db, string expression, string? messagePart = null,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        CamusDBException ex = await AssertThrows(executor, db, expression, parameters);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, expression + ": " + ex.Message);

        if (messagePart is not null)
            StringAssert.Contains(messagePart, ex.Message, expression);
    }

    private static async Task AssertBytes(CommandExecutor executor, DatabaseDescriptor db, string expression, string expectedHex,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        ColumnValue value = await Scalar(executor, db, expression, parameters);
        Assert.AreEqual(ColumnType.Bytes, value.Type, expression);
        Assert.AreEqual(expectedHex, Convert.ToHexStringLower(value.BytesValue ?? []), expression);
    }

    private static async Task AssertString(CommandExecutor executor, DatabaseDescriptor db, string expression, string expected,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        ColumnValue value = await Scalar(executor, db, expression, parameters);
        Assert.AreEqual(ColumnType.String, value.Type, expression);
        Assert.AreEqual(expected, value.StrValue, expression);
    }

    private static async Task AssertLong(CommandExecutor executor, DatabaseDescriptor db, string expression, long expected,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        ColumnValue value = await Scalar(executor, db, expression, parameters);
        Assert.AreEqual(ColumnType.Integer64, value.Type, expression);
        Assert.AreEqual(expected, value.LongValue, expression);
    }

    private static async Task AssertBool(CommandExecutor executor, DatabaseDescriptor db, string expression, bool expected)
    {
        ColumnValue value = await Scalar(executor, db, expression);
        Assert.AreEqual(ColumnType.Bool, value.Type, expression);
        Assert.AreEqual(expected, value.BoolValue, expression);
    }

    private static async Task AssertNull(CommandExecutor executor, DatabaseDescriptor db, string expression)
        => Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, expression)).Type, expression);

    private static Dictionary<string, ColumnValue> Param(string name, byte[] value) => new() { ["@" + name] = new ColumnValue(value) };

    private static Dictionary<string, ColumnValue> Param(string name, string value) => new() { ["@" + name] = new ColumnValue(ColumnType.String, value) };

    // ── encode / decode ─────────────────────────────────────────────────────

    [TestCase("hex")]
    [TestCase("base64")]
    [TestCase("escape")]
    public async Task EncodeDecode_RoundTrip(string format)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        byte[] allBytes = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        byte[] random = new byte[3000];
        new Random(7).NextBytes(random);

        foreach (byte[] value in new[] { Array.Empty<byte>(), new byte[] { 0x5C }, new byte[] { 0x00 }, allBytes, random })
        {
            IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db,
                $"SELECT decode(encode(@b, '{format}'), '{format}') AS back, decode(encode(@b, '{format}'), '{format}') = @b AS same",
                Param("b", value));

            Assert.AreEqual(ColumnType.Bytes, row["back"].Type);
            CollectionAssert.AreEqual(value, row["back"].BytesValue, $"{format}, {value.Length} bytes");
            Assert.IsTrue(row["same"].BoolValue);
        }
    }

    [Test]
    public async Task Encode_KnownValues()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, db, "encode(X'DEADBEEF', 'hex')", "deadbeef");
        await AssertString(executor, db, "encode(X'DEADBEEF', 'HEX')", "deadbeef");
        await AssertString(executor, db, "encode(X'', 'hex')", "");
        await AssertString(executor, db, "encode(X'61', 'base64')", "YQ==");
        await AssertString(executor, db, "encode(X'616263', 'base64')", "YWJj");
        await AssertString(executor, db, "encode(X'', 'base64')", "");
        await AssertString(executor, db, "encode(X'00415C80FF', 'escape')", "\\000A\\\\\\200\\377");
        await AssertString(executor, db, "encode(X'', 'escape')", "");
        await AssertNull(executor, db, "encode(CAST(NULL AS bytes), 'hex')");
        await AssertNull(executor, db, "encode(X'01', NULL)");

        // Base64 output has no line breaks, unlike PostgreSQL.
        string long64 = (await Scalar(executor, db, "encode(@b, 'base64')", Param("b", new byte[300]))).StrValue!;
        Assert.AreEqual(400, long64.Length);
        StringAssert.DoesNotContain("\n", long64);

        await AssertInvalid(executor, db, "encode(X'01', 'base32')", "format 'base32'");
        await AssertInvalid(executor, db, "encode('text', 'hex')", "argument 1 of type Bytes");
        await AssertInvalid(executor, db, "encode(X'01', 1)");
    }

    [Test]
    public async Task Decode_AcceptsPostgresInputAndRejectsBadText()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBytes(executor, db, "decode('DEADbeef', 'hex')", "deadbeef");
        await AssertBytes(executor, db, "decode('', 'hex')", "");
        await AssertBytes(executor, db, "decode('0a', 'hex')", "0a");
        await AssertBytes(executor, db, "decode(@t, 'hex')", "deadbeef", Param("t", "de ad\nbe\tef\r\n"));
        await AssertBytes(executor, db, "decode('YQ==', 'base64')", "61");
        await AssertBytes(executor, db, "decode('', 'base64')", "");
        await AssertBytes(executor, db, "decode(@t, 'base64')", "616263616263", Param("t", "YWJj\nYWJj\n"));
        await AssertBytes(executor, db, "decode('A', 'escape')", "41");
        await AssertBytes(executor, db, "decode('', 'escape')", "");
        await AssertBytes(executor, db, "decode(@t, 'escape')", "005c80ff41", Param("t", "\\000\\\\\\200\\377A"));
        await AssertBytes(executor, db, "decode(@t, 'escape')", "c3a9", Param("t", "é"));
        await AssertNull(executor, db, "decode(NULL, 'hex')");

        await AssertInvalid(executor, db, "decode('abc', 'hex')", "even number of hex digits");
        await AssertInvalid(executor, db, "decode('zz', 'hex')", "invalid hex digit 'z'");
        await AssertInvalid(executor, db, "decode(@t, 'hex')", "invalid hex digit", Param("t", "d e"));
        await AssertInvalid(executor, db, "decode('!!!!', 'base64')", "not valid base64");
        await AssertInvalid(executor, db, "decode('YQ=', 'base64')", "not valid base64");
        await AssertInvalid(executor, db, "decode(@t, 'escape')", "invalid escape", Param("t", "\\q"));
        await AssertInvalid(executor, db, "decode(@t, 'escape')", "invalid escape", Param("t", "\\400"));
        await AssertInvalid(executor, db, "decode(@t, 'escape')", "invalid escape", Param("t", "a\\"));
        await AssertInvalid(executor, db, "decode('00', 'base32')", "format 'base32'");
        await AssertInvalid(executor, db, "decode(X'00', 'hex')", "argument 1 of type String");
    }

    // ── hex / unhex / to_base64 / from_base64 ───────────────────────────────

    [Test]
    public async Task HexAndUnhex_FollowMySql()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, db, "hex(X'0aff')", "0AFF");
        await AssertString(executor, db, "hex(X'')", "");
        await AssertString(executor, db, "hex(X'01')", "01");
        await AssertString(executor, db, "hex('abc')", "616263");
        await AssertString(executor, db, "hex('')", "");
        await AssertString(executor, db, "hex(255)", "FF");
        await AssertString(executor, db, "hex(0)", "0");
        await AssertString(executor, db, "hex(-1)", "FFFFFFFFFFFFFFFF");
        await AssertNull(executor, db, "hex(NULL)");
        await AssertInvalid(executor, db, "hex(1.5)", "Bytes, String or Integer64");

        await AssertBytes(executor, db, "unhex('0aFF')", "0aff");
        await AssertBytes(executor, db, "unhex('')", "");
        await AssertBytes(executor, db, "unhex(hex('abc'))", "616263");
        await AssertNull(executor, db, "unhex(NULL)");
        await AssertInvalid(executor, db, "unhex('ABC')", "even number of hex digits");
        await AssertInvalid(executor, db, "unhex('0 A')", "invalid hex digit");
        await AssertInvalid(executor, db, "unhex(X'01')", "argument 1 of type String");
    }

    [Test]
    public async Task Base64Functions_FollowMySql()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertString(executor, db, "to_base64('abc')", "YWJj");
        await AssertString(executor, db, "to_base64(X'616263')", "YWJj");
        await AssertString(executor, db, "to_base64(X'FF')", "/w==");
        await AssertString(executor, db, "to_base64('')", "");
        await AssertNull(executor, db, "to_base64(NULL)");
        await AssertInvalid(executor, db, "to_base64(5)", "Bytes or String");

        await AssertBytes(executor, db, "from_base64('YWJj')", "616263");
        await AssertBytes(executor, db, "from_base64('/w==')", "ff");
        await AssertBytes(executor, db, "from_base64('')", "");
        await AssertNull(executor, db, "from_base64(NULL)");
        await AssertInvalid(executor, db, "from_base64('YW*j')", "not valid base64");
        await AssertInvalid(executor, db, "from_base64(X'01')", "argument 1 of type String");
    }

    // ── convert_to / convert_from ───────────────────────────────────────────

    [Test]
    public async Task ConvertToAndFrom_AreStrictUtf8()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBytes(executor, db, "convert_to(@t, 'UTF8')", "c3a9", Param("t", "é"));
        await AssertBytes(executor, db, "convert_to('a', 'utf-8')", "61");
        await AssertBytes(executor, db, "convert_to('', 'UTF8')", "");
        await AssertString(executor, db, "convert_from(X'C3A9', 'utf8')", "é");
        await AssertString(executor, db, "convert_from(X'61', 'UTF-8')", "a");
        await AssertString(executor, db, "convert_from(X'', 'UTF8')", "");
        await AssertString(executor, db, "convert_from(convert_to(@t, 'UTF8'), 'UTF8')", "añ😀", Param("t", "añ😀"));
        await AssertNull(executor, db, "convert_to(NULL, 'UTF8')");
        await AssertNull(executor, db, "convert_from(CAST(NULL AS bytes), 'UTF8')");

        await AssertInvalid(executor, db, "convert_from(X'FF', 'UTF8')", "not valid UTF-8");
        await AssertInvalid(executor, db, "convert_from(X'C3', 'UTF8')", "not valid UTF-8");
        await AssertInvalid(executor, db, "convert_to('a', 'LATIN1')", "Only UTF8");
        await AssertInvalid(executor, db, "convert_from(X'61', 'LATIN1')", "Only UTF8");
        await AssertInvalid(executor, db, "convert_to(X'61', 'UTF8')", "argument 1 of type String");
        await AssertInvalid(executor, db, "convert_from('a', 'UTF8')", "argument 1 of type Bytes");
    }

    // ── bytes forms of the text functions ───────────────────────────────────

    [Test]
    public async Task LengthAndSubstring_CountBytes()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "length(X'')", 0);
        await AssertLong(executor, db, "length(X'01')", 1);
        await AssertLong(executor, db, "length(convert_to(@t, 'UTF8'))", 2, Param("t", "é"));
        await AssertLong(executor, db, "length(@t)", 1, Param("t", "é"));
        await AssertNull(executor, db, "length(CAST(NULL AS bytes))");

        await AssertBytes(executor, db, "substring(X'0102030405', 2, 3)", "020304");
        await AssertBytes(executor, db, "substring(X'0102030405', 4)", "0405");
        await AssertBytes(executor, db, "substring(X'0102030405', 5, 10)", "05");
        await AssertBytes(executor, db, "substring(X'0102030405', 6)", "");
        await AssertBytes(executor, db, "substring(X'0102030405', 1, 0)", "");
        await AssertBytes(executor, db, "substring(X'', 1)", "");
        await AssertBytes(executor, db, "substring(X'01', 1, 1)", "01");
        await AssertNull(executor, db, "substring(CAST(NULL AS bytes), 1)");
        await AssertNull(executor, db, "substring(X'01', NULL)");
        await AssertInvalid(executor, db, "substring(X'01', 0)", "1-based position");
        await AssertInvalid(executor, db, "substring(X'01', 1, -1)", "non-negative length");
        await AssertInvalid(executor, db, "substring(5, 1)", "String or Bytes");

        // The string forms do not change.
        await AssertString(executor, db, "substring('hello', 2, 3)", "ell");
        await AssertString(executor, db, "substring('hello', 9)", "");
        await AssertLong(executor, db, "length('hello')", 5);
    }

    [Test]
    public async Task StrposAndPosition_FindTheFirstMatch()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "strpos(X'010203', X'0203')", 2);
        await AssertLong(executor, db, "strpos(X'010203', X'04')", 0);
        await AssertLong(executor, db, "strpos(X'010203', X'')", 1);
        await AssertLong(executor, db, "strpos(X'', X'')", 1);
        await AssertLong(executor, db, "strpos(X'', X'01')", 0);
        await AssertLong(executor, db, "strpos(X'01', X'01')", 1);
        await AssertLong(executor, db, "strpos('hello', 'l')", 3);
        await AssertLong(executor, db, "strpos('hello', 'z')", 0);
        await AssertLong(executor, db, "strpos('hello', '')", 1);
        await AssertNull(executor, db, "strpos(NULL, 'a')");
        await AssertNull(executor, db, "strpos(X'01', CAST(NULL AS bytes))");
        await AssertInvalid(executor, db, "strpos(X'01', 'a')", "argument 2 of type Bytes");
        await AssertInvalid(executor, db, "strpos('a', X'01')", "argument 2 of type String");

        await AssertLong(executor, db, "position(X'03' IN X'010203')", 3);
        await AssertLong(executor, db, "POSITION('lo' IN 'hello')", 4);
        await AssertLong(executor, db, "position('x' IN 'hello')", 0);
        await AssertLong(executor, db, "position('' IN '')", 1);
        await AssertLong(executor, db, "position(('a' ) IN lower('BAA'))", 2);
        await AssertLong(executor, db, "position(@n IN @h)", 2,
            new Dictionary<string, ColumnValue> { ["@n"] = new(ColumnType.String, "b"), ["@h"] = new(ColumnType.String, "abc") });
        await AssertNull(executor, db, "position(NULL IN 'a')");

        // IN inside any other call is still an IN list, and IN with an operand is a syntax error.
        await AssertBool(executor, db, "coalesce(2 IN (1, 2))", true);
        CamusDBException wrongName = await AssertThrows(executor, db, "lower('a' IN 'b')");
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, wrongName.Code);
        StringAssert.Contains("POSITION(needle IN haystack)", wrongName.Message);
    }

    [Test]
    public async Task Position_OverColumns_InWhereAndView()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, name string, b bytes)");
        await Exec(executor, db, "INSERT INTO t (id, name, b) VALUES (1, 'alpha', X'0001'), (2, 'beta', X'0100'), (3, 'gamma', X'')");

        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name,
            "SELECT id, position('a' IN name) AS p, position(X'01' IN b) AS pb FROM t WHERE position('a' IN name) > 1 ORDER BY id", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(new long[] { 2, 3 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
        Assert.AreEqual(new long[] { 4, 2 }, rows.Select(r => r.Row["p"].LongValue).ToArray());
        Assert.AreEqual(new long[] { 1, 0 }, rows.Select(r => r.Row["pb"].LongValue).ToArray());

        // A view stores its body as text; the call and the bytes literal must print back in a form that
        // parses again.
        await ExecDDL(executor, db, "CREATE VIEW v AS SELECT id, position('m' IN name) AS p, position(X'01' IN b) AS pb FROM t");

        tx = await db.Transactions.BeginAsync();
        (_, cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, "SELECT id, p, pb FROM v ORDER BY id", null));
        rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(new long[] { 0, 0, 3 }, rows.Select(r => r.Row["p"].LongValue).ToArray());
        Assert.AreEqual(new long[] { 2, 1, 0 }, rows.Select(r => r.Row["pb"].LongValue).ToArray());

        // A bytes literal in HAVING is accepted by the post-aggregate scope check.
        tx = await db.Transactions.BeginAsync();
        (_, cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name,
            "SELECT b, count(*) AS c FROM t GROUP BY b HAVING starts_with(b, X'00')", null));
        rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("0001", Convert.ToHexStringLower(rows[0].Row["b"].BytesValue!));
    }

    [Test]
    public async Task Trims_RemoveZeroBytesOrAGivenSet()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBytes(executor, db, "trim(X'0000410000')", "41");
        await AssertBytes(executor, db, "ltrim(X'0000410000')", "410000");
        await AssertBytes(executor, db, "rtrim(X'0000410000')", "000041");
        await AssertBytes(executor, db, "trim(X'0000')", "");
        await AssertBytes(executor, db, "trim(X'')", "");
        await AssertBytes(executor, db, "trim(X'41')", "41");
        await AssertBytes(executor, db, "trim(X'0102030201', X'0102')", "03");
        await AssertBytes(executor, db, "ltrim(X'0102030201', X'0102')", "030201");
        await AssertBytes(executor, db, "rtrim(X'0102030201', X'0102')", "010203");
        await AssertBytes(executor, db, "trim(X'0001', X'')", "0001");
        await AssertNull(executor, db, "trim(CAST(NULL AS bytes))");
        await AssertNull(executor, db, "trim(X'01', CAST(NULL AS bytes))");
        await AssertInvalid(executor, db, "trim(X'01', 'a')", "argument 2 of type Bytes");
        await AssertInvalid(executor, db, "trim(1)", "String or Bytes");

        // The one-argument string forms do not change; the set form is PostgreSQL's btrim(text, text).
        await AssertString(executor, db, "trim('  a  ')", "a");
        await AssertString(executor, db, "ltrim('  a  ')", "a  ");
        await AssertString(executor, db, "rtrim('  a  ')", "  a");
        await AssertString(executor, db, "trim('xyaxy', 'yx')", "a");
        await AssertString(executor, db, "ltrim('xxa', 'x')", "a");
        await AssertString(executor, db, "rtrim('axx', 'x')", "a");
        await AssertString(executor, db, "trim('  a  ', '')", "  a  ");
        await AssertString(executor, db, "trim('', 'x')", "");
        await AssertInvalid(executor, db, "trim('a', X'01')", "argument 2 of type String");
    }

    [Test]
    public async Task Predicates_MatchBytes()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBool(executor, db, "starts_with(X'010203', X'0102')", true);
        await AssertBool(executor, db, "starts_with(X'010203', X'02')", false);
        await AssertBool(executor, db, "starts_with(X'', X'')", true);
        await AssertBool(executor, db, "starts_with(X'', X'01')", false);
        await AssertBool(executor, db, "ends_with(X'010203', X'0203')", true);
        await AssertBool(executor, db, "ends_with(X'01', X'01')", true);
        await AssertBool(executor, db, "ends_with(X'010203', X'01')", false);
        await AssertBool(executor, db, "contains(X'010203', X'02')", true);
        await AssertBool(executor, db, "contains(X'010203', X'0103')", false);
        await AssertBool(executor, db, "contains(X'01', X'')", true);
        await AssertNull(executor, db, "contains(CAST(NULL AS bytes), X'01')");
        await AssertInvalid(executor, db, "contains(X'01', 'a')", "argument 2 of type Bytes");
        await AssertInvalid(executor, db, "starts_with('a', X'01')", "argument 2 of type String");
        await AssertBool(executor, db, "contains('hello', 'ell')", true);
    }

    // ── byte and bit access ─────────────────────────────────────────────────

    [Test]
    public async Task GetAndSetByte_UseZeroBasedOffsets()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "get_byte(X'0aff', 0)", 10);
        await AssertLong(executor, db, "get_byte(X'0aff', 1)", 255);
        await AssertLong(executor, db, "get_byte(X'7f', 0)", 127);
        await AssertNull(executor, db, "get_byte(CAST(NULL AS bytes), 0)");
        await AssertNull(executor, db, "get_byte(X'01', NULL)");
        await AssertInvalid(executor, db, "get_byte(X'0aff', 2)", "from 0 to 1 but received 2");
        await AssertInvalid(executor, db, "get_byte(X'0aff', -1)", "received -1");
        await AssertInvalid(executor, db, "get_byte(X'', 0)", "empty value");
        await AssertInvalid(executor, db, "get_byte('a', 0)", "argument 1 of type Bytes");

        await AssertBytes(executor, db, "set_byte(X'0000', 0, 255)", "ff00");
        await AssertBytes(executor, db, "set_byte(X'0000', 1, 255)", "00ff");
        await AssertBytes(executor, db, "set_byte(X'01', 0, 0)", "00");
        await AssertNull(executor, db, "set_byte(X'01', 0, NULL)");
        await AssertInvalid(executor, db, "set_byte(X'0000', 2, 1)", "from 0 to 1 but received 2");
        await AssertInvalid(executor, db, "set_byte(X'', 0, 1)", "empty value");
        await AssertInvalid(executor, db, "set_byte(X'00', 0, 256)", "from 0 to 255");
        await AssertInvalid(executor, db, "set_byte(X'00', 0, -1)", "from 0 to 255");
    }

    [Test]
    public async Task GetAndSetBit_CountFromTheLeastSignificantBit()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "get_bit(X'01', 0)", 1);
        await AssertLong(executor, db, "get_bit(X'01', 7)", 0);
        await AssertLong(executor, db, "get_bit(X'0080', 15)", 1);
        await AssertLong(executor, db, "get_bit(X'0080', 8)", 0);
        await AssertNull(executor, db, "get_bit(CAST(NULL AS bytes), 0)");
        await AssertInvalid(executor, db, "get_bit(X'01', 8)", "from 0 to 7 but received 8");
        await AssertInvalid(executor, db, "get_bit(X'01', -1)", "received -1");
        await AssertInvalid(executor, db, "get_bit(X'', 0)", "empty value");

        await AssertBytes(executor, db, "set_bit(X'00', 0, 1)", "01");
        await AssertBytes(executor, db, "set_bit(X'FF', 7, 0)", "7f");
        await AssertBytes(executor, db, "set_bit(X'0000', 15, 1)", "0080");
        await AssertBytes(executor, db, "set_bit(X'01', 0, 1)", "01");
        await AssertNull(executor, db, "set_bit(X'01', NULL, 1)");
        await AssertInvalid(executor, db, "set_bit(X'00', 8, 1)", "from 0 to 7 but received 8");
        await AssertInvalid(executor, db, "set_bit(X'00', 0, 2)", "0 or 1");
        await AssertInvalid(executor, db, "set_bit(X'', 0, 1)", "empty value");
    }

    [Test]
    public async Task SetByteAndSetBit_DoNotChangeTheStoredValue()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, b bytes)");
        await Exec(executor, db, "INSERT INTO t (id, b) VALUES (1, X'0000')");

        IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db,
            "SELECT set_byte(b, 0, 255) AS a, set_bit(b, 15, 1) AS c, b FROM t");

        Assert.AreEqual("ff00", Convert.ToHexStringLower(row["a"].BytesValue!));
        Assert.AreEqual("0080", Convert.ToHexStringLower(row["c"].BytesValue!));
        Assert.AreEqual("0000", Convert.ToHexStringLower(row["b"].BytesValue!));

        row = await Row(executor, db, "SELECT b FROM t");
        Assert.AreEqual("0000", Convert.ToHexStringLower(row["b"].BytesValue!));
    }

    [Test]
    public async Task BitLengthAndBitCount()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "bit_length(X'')", 0);
        await AssertLong(executor, db, "bit_length(X'01')", 8);
        await AssertLong(executor, db, "bit_length('')", 0);
        await AssertLong(executor, db, "bit_length(@t)", 16, Param("t", "é"));
        await AssertNull(executor, db, "bit_length(NULL)");
        await AssertInvalid(executor, db, "bit_length(1)", "Bytes or String");

        await AssertLong(executor, db, "bit_count(X'')", 0);
        await AssertLong(executor, db, "bit_count(X'00')", 0);
        await AssertLong(executor, db, "bit_count(X'FF0F')", 12);
        await AssertLong(executor, db, "bit_count(@b)", 17 * 8, Param("b", Enumerable.Repeat((byte)0xFF, 17).ToArray()));
        await AssertNull(executor, db, "bit_count(NULL)");
        await AssertInvalid(executor, db, "bit_count('a')", "argument 1 of type Bytes");
    }

    // ── digest / hmac / crc32 ───────────────────────────────────────────────

    [TestCase("md5")]
    [TestCase("sha1")]
    [TestCase("sha256")]
    [TestCase("sha512")]
    public async Task Digest_IsTheHexFunctionDecoded(string algorithm)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        foreach (string input in new[] { "''", "'abc'", "X''", "X'00'", "X'616263'" })
        {
            IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db,
                $"SELECT digest({input}, '{algorithm}') AS d, decode({algorithm}({input}), 'hex') AS h, " +
                $"digest({input}, '{algorithm.ToUpperInvariant()}') = decode({algorithm}({input}), 'hex') AS same");

            Assert.AreEqual(ColumnType.Bytes, row["d"].Type);
            CollectionAssert.AreEqual(row["h"].BytesValue, row["d"].BytesValue, input);
            Assert.IsTrue(row["same"].BoolValue, input);
        }
    }

    [Test]
    public async Task Digest_Sha384AndErrors()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBytes(executor, db, "digest('abc', 'sha384')",
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7");
        await AssertNull(executor, db, "digest(NULL, 'sha256')");
        await AssertNull(executor, db, "digest('a', NULL)");
        await AssertInvalid(executor, db, "digest('a', 'sha224')", "algorithm 'sha224'");
        await AssertInvalid(executor, db, "digest(1, 'md5')", "String or Bytes");

        // The hex functions keep their string result.
        Assert.AreEqual(ColumnType.String, (await Scalar(executor, db, "sha256('abc')")).Type);
    }

    /// <summary>
    /// HMAC-SHA-256/384/512 test cases 1, 2 and 6 of RFC 4231 (case 6 has a key longer than the block),
    /// and HMAC-MD5 / HMAC-SHA-1 test cases of RFC 2202.
    /// </summary>
    [TestCase(1, "sha256", "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7")]
    [TestCase(1, "sha384", "afd03944d84895626b0825f4ab46907f15f9dadbe4101ec682aa034c7cebc59cfaea9ea9076ede7f4af152e8b2fa9cb6")]
    [TestCase(1, "sha512", "87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cdedaa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854")]
    [TestCase(2, "sha256", "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")]
    [TestCase(2, "sha384", "af45d2e376484031617f78d2b58a6b1b9c7ef464f5a01b47e42ec3736322445e8e2240ca5e69e2c78b3239ecfab21649")]
    [TestCase(2, "sha512", "164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737")]
    [TestCase(6, "sha256", "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54")]
    [TestCase(6, "sha384", "4ece084485813e9088d2c63a041bc5b44f9ef1012a2b588f3cd11f05033ac4c60c2ef6ab4030fe8296248df163f44952")]
    [TestCase(6, "sha512", "80b24263c7c1a3ebb71493c1dd7be8b49b46d1f41b4aeec1121b013783f8f3526b56d037e05f2598bd0fd2215d6a1e5295e64f73f63f0aec8b915a985d786598")]
    [TestCase(2, "md5", "750c783e6ab0b503eaa86e310a5db738")]
    [TestCase(2, "sha1", "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79")]
    [TestCase(7, "md5", "6b1ab7fe4bd7bf8f0b62e6ce61b9d0cd")]
    public async Task Hmac_MatchesRfcTestVectors(int testCase, string algorithm, string expectedHex)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        (byte[] key, string data) = testCase switch
        {
            1 => (Enumerable.Repeat((byte)0x0b, 20).ToArray(), "Hi There"),
            2 => (Encoding.ASCII.GetBytes("Jefe"), "what do ya want for nothing?"),
            6 => (Enumerable.Repeat((byte)0xaa, 131).ToArray(), "Test Using Larger Than Block-Size Key - Hash Key First"),
            _ => (Enumerable.Repeat((byte)0xaa, 80).ToArray(), "Test Using Larger Than Block-Size Key - Hash Key First"),
        };

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@k"] = new ColumnValue(key),
            ["@d"] = new ColumnValue(ColumnType.String, data),
            ["@db"] = new ColumnValue(Encoding.ASCII.GetBytes(data)),
        };

        IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db,
            $"SELECT hmac(@d, @k, '{algorithm}') AS text_data, hmac(@db, @k, '{algorithm}') AS bytes_data", parameters);

        Assert.AreEqual(ColumnType.Bytes, row["text_data"].Type);
        Assert.AreEqual(expectedHex, Convert.ToHexStringLower(row["text_data"].BytesValue!));
        Assert.AreEqual(expectedHex, Convert.ToHexStringLower(row["bytes_data"].BytesValue!));
    }

    [Test]
    public async Task Hmac_TextKeyEmptyInputAndErrors()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertBytes(executor, db, "hmac('what do ya want for nothing?', 'Jefe', 'sha256')",
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
        // HMAC-SHA-256 of empty data under an empty key.
        await AssertBytes(executor, db, "hmac(X'', X'', 'sha256')",
            "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad");
        await AssertNull(executor, db, "hmac(NULL, 'k', 'sha256')");
        await AssertNull(executor, db, "hmac('d', NULL, 'sha256')");
        await AssertInvalid(executor, db, "hmac('d', 'k', 'crc')", "algorithm 'crc'");
        await AssertInvalid(executor, db, "hmac('d', 1, 'sha256')", "argument 2 of type String or Bytes");
    }

    [Test]
    public async Task Crc32_MatchesMySql()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await AssertLong(executor, db, "crc32('abc')", 891568578);
        await AssertLong(executor, db, "crc32(X'616263')", 891568578);
        await AssertLong(executor, db, "crc32('The quick brown fox jumps over the lazy dog')", 1095738169);
        await AssertLong(executor, db, "crc32('')", 0);
        await AssertLong(executor, db, "crc32(X'')", 0);
        await AssertLong(executor, db, "crc32(X'00')", 3523407757);
        await AssertNull(executor, db, "crc32(NULL)");
        await AssertInvalid(executor, db, "crc32(1)", "String or Bytes");
    }

    // ── gen_random_bytes ────────────────────────────────────────────────────

    [Test]
    public async Task GenRandomBytes_IsVolatileAndBounded()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db,
            "SELECT gen_random_bytes(16) AS a, gen_random_bytes(16) AS b, gen_random_bytes(1) AS c, gen_random_bytes(1024) AS d");

        Assert.AreEqual(16, row["a"].BytesValue!.Length);
        Assert.AreEqual(1, row["c"].BytesValue!.Length);
        Assert.AreEqual(1024, row["d"].BytesValue!.Length);
        CollectionAssert.AreNotEqual(row["a"].BytesValue, row["b"].BytesValue);

        await AssertNull(executor, db, "gen_random_bytes(NULL)");
        await AssertInvalid(executor, db, "gen_random_bytes(0)", "from 1 to 1024 but received 0");
        await AssertInvalid(executor, db, "gen_random_bytes(1025)", "from 1 to 1024 but received 1025");
        await AssertInvalid(executor, db, "gen_random_bytes(-1)", "from 1 to 1024");
        await AssertInvalid(executor, db, "gen_random_bytes('a')", "argument 1 of type Integer64");

        Assert.IsTrue(ScalarFunctionEvaluator.ContainsVolatileFunction(SQLParserProcessor.Parse("SELECT gen_random_bytes(4)")));
        Assert.IsFalse(ScalarFunctionEvaluator.ContainsVolatileFunction(SQLParserProcessor.Parse("SELECT digest('a', 'md5')")));
    }

    [Test]
    public async Task GenRandomBytes_IsRejectedAsAColumnDefault()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        // gen_uuid_v4() is accepted because it takes no argument; the per-row default path cannot pass
        // one, so gen_random_bytes(n) is rejected by the same rule.
        await ExecDDL(executor, db, "CREATE TABLE ok (id int64 PRIMARY KEY, u uuid DEFAULT (gen_uuid_v4()))");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, b bytes DEFAULT (gen_random_bytes(16)))"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("zero-argument function call", ex.Message);
    }

    // ── static types, sizes and out-of-line values ──────────────────────────

    [Test]
    public void InferredTypes_FollowTheFirstArgument()
    {
        Assert.AreEqual(ColumnType.Bytes, ScalarFunctionEvaluator.InferReturnType("substring", [ColumnType.Bytes, ColumnType.Integer64]));
        Assert.AreEqual(ColumnType.String, ScalarFunctionEvaluator.InferReturnType("substring", [ColumnType.String, ColumnType.Integer64]));
        Assert.AreEqual(ColumnType.String, ScalarFunctionEvaluator.InferReturnType("substring", [ColumnType.Null, ColumnType.Integer64]));
        Assert.AreEqual(ColumnType.Bytes, ScalarFunctionEvaluator.InferReturnType("trim", [ColumnType.Bytes]));
        Assert.AreEqual(ColumnType.Bytes, ScalarFunctionEvaluator.InferReturnType("ltrim", [ColumnType.Bytes, ColumnType.Bytes]));
        Assert.AreEqual(ColumnType.String, ScalarFunctionEvaluator.InferReturnType("rtrim", [ColumnType.String]));
        Assert.AreEqual(ColumnType.Integer64, ScalarFunctionEvaluator.InferReturnType("strpos", [ColumnType.Bytes, ColumnType.Bytes]));
        Assert.AreEqual(ColumnType.Bytes, ScalarFunctionEvaluator.InferReturnType("decode", [ColumnType.String, ColumnType.String]));
        Assert.AreEqual(ColumnType.String, ScalarFunctionEvaluator.InferReturnType("encode", [ColumnType.Bytes, ColumnType.String]));
        Assert.AreEqual(ColumnType.Bytes, ScalarFunctionEvaluator.InferReturnType("digest", [ColumnType.String, ColumnType.String]));
        Assert.AreEqual(ColumnType.Bytes, ScalarFunctionEvaluator.InferReturnType("gen_random_bytes", [ColumnType.Integer64]));
        Assert.AreEqual(ColumnType.Integer64, ScalarFunctionEvaluator.InferReturnType("crc32", [ColumnType.String]));
    }

    [Test]
    public async Task InsertOfAResult_EnforcesTheBytesSize()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, b bytes(4))");
        await Exec(executor, db, "INSERT INTO t (id, b) VALUES (1, decode('01020304', 'hex'))");
        await Exec(executor, db, "INSERT INTO t (id, b) VALUES (2, substring(decode('0102030405', 'hex'), 2))");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(executor, db, "INSERT INTO t (id, b) VALUES (3, decode('0102030405', 'hex'))"))!;
        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex.Code);

        ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(executor, db, "UPDATE t SET b = digest(b, 'md5') WHERE id = 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex.Code);

        IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db, "SELECT encode(b, 'hex') AS h FROM t WHERE id = 2");
        Assert.AreEqual("02030405", row["h"].StrValue);
    }

    /// <summary>
    /// A <c>STORAGE EXTERNAL</c> value at or above the threshold lives under a <c>|v</c> key, not in the
    /// row. The functions read it in the projection and in the <c>WHERE</c>, so the decode column set
    /// must resolve it; an unresolved cell would throw CADB0541.
    /// </summary>
    [Test]
    public async Task OutOfLineColumn_IsReadBySubstringAndEncode()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, tag string, payload bytes STORAGE EXTERNAL)");

        byte[] payload = new byte[4096];
        new Random(11).NextBytes(payload);
        payload[0] = 0xCA;
        payload[1] = 0xFE;

        await Exec(executor, db, "INSERT INTO docs (id, tag, payload) VALUES (1, 'big', @p)",
            new Dictionary<string, ColumnValue> { ["@p"] = new ColumnValue(payload) });

        string bucket = $"{db.Id}:{db.Schema.Tables["docs"].EffectiveStorageId}|v";
        int outOfLine = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(bucket + "/", StringComparison.Ordinal))
                outOfLine++;
        }
        Assert.Greater(outOfLine, 0, "the payload must be stored out of line for this test to mean anything");

        IReadOnlyDictionary<string, ColumnValue> row = await Row(executor, db,
            "SELECT encode(substring(payload, 1, 2), 'hex') AS head, encode(substring(payload, 4095), 'hex') AS tail, " +
            "length(payload) AS len, decode(encode(payload, 'base64'), 'base64') = payload AS same, get_byte(payload, 4095) AS last " +
            "FROM docs WHERE starts_with(payload, X'CAFE')");

        Assert.AreEqual("cafe", row["head"].StrValue);
        Assert.AreEqual(Convert.ToHexStringLower(payload.AsSpan(4094)), row["tail"].StrValue);
        Assert.AreEqual(4096, row["len"].LongValue);
        Assert.IsTrue(row["same"].BoolValue);
        Assert.AreEqual(payload[4095], row["last"].LongValue);

        // Only the function reads the column; it is not projected on its own.
        row = await Row(executor, db, "SELECT encode(payload, 'hex') AS h FROM docs WHERE id = 1");
        Assert.AreEqual(Convert.ToHexStringLower(payload), row["h"].StrValue);
    }
}
