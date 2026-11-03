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
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Extensions;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Edge cases of the <c>jsonb</c> type that sit at a trust boundary or a numeric boundary:
/// <list type="bullet">
/// <item>Cell bytes from a client or a peer must be exactly what the encoder writes. The readers
/// trust sorted keys and the nesting limit, so a non-canonical cell would read back wrong.</item>
/// <item>A cast to int64 rounds the exact JSON number one time.</item>
/// <item>The MySQL-style JSON functions accept every valid document, also one deeper than the
/// default text parser limit of 64 levels.</item>
/// <item>Containment of wide objects and arrays gives the same answers on the linear paths.</item>
/// <item><c>jsonb_object_agg</c> keeps one value per distinct key, not one per row.</item>
/// </list>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestJsonbHardening : BaseTest
{
    private static readonly JsonSerializerOptions RequestJson = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null, bool ddl = false)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
            else
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
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
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
            return await cursor.ToListAsync();
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>A jsonb HTTP parameter in its binary form, as a client sends it in a request body.</summary>
    private static string BinaryParameterJson(byte[] cell) =>
        "{\"type\":" + (int)ColumnType.Jsonb + ",\"bytesValue\":\"" + Convert.ToBase64String(cell) + "\"}";

    private static ColumnValue DeserializeParameter(byte[] cell) =>
        JsonSerializer.Deserialize<ColumnValue>(BinaryParameterJson(cell), RequestJson)!;

    private static byte[] Cell(params byte[] item) => [JsonbLimits.FormatVersion, .. item];

    private static byte[] Hex(string hex) => Convert.FromHexString(hex.Replace(" ", ""));

    private static string Nested(int depth) => new string('[', depth) + "0" + new string(']', depth);

    // ---------------------------------------------------------------------------------------------
    // CAST(jsonb AS int64)
    // ---------------------------------------------------------------------------------------------

    [TestCase("0.4999999999", 0L)]
    [TestCase("-0.4999999999", 0L)]
    [TestCase("0.5", 1L)]
    [TestCase("-0.5", -1L)]
    [TestCase("0.50000000001", 1L)]
    [TestCase("-0.50000000001", -1L)]
    [TestCase("1.5", 2L)]
    [TestCase("-2.5", -3L)]
    [TestCase("2.4999999999999999999", 2L)]
    [TestCase("1e2", 100L)]
    [TestCase("9223372036854775807.4999999999", long.MaxValue)]
    [TestCase("-9223372036854775808.4999999999", long.MinValue)]
    [TestCase("9223372036854775806.5", long.MaxValue)]
    public async Task CastToInt64_RoundsTheExactNumberOnce(string number, long expected)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await Select(executor, db, $"SELECT CAST(jsonb '{number}' AS int64) AS n");
        Assert.AreEqual(ColumnType.Integer64, rows.Single().Row["n"].Type);
        Assert.AreEqual(expected, rows.Single().Row["n"].LongValue);
    }

    [TestCase("9223372036854775807.5")]
    [TestCase("-9223372036854775808.5")]
    [TestCase("1e19")]
    [TestCase("-123456789012345678901234567890")]
    public async Task CastToInt64_OutOfRangeIsRefused(string number)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => Select(executor, db, $"SELECT CAST(jsonb '{number}' AS int64) AS n"));
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, ex!.Code);
    }

    // ---------------------------------------------------------------------------------------------
    // Binary cells from a client or a peer
    // ---------------------------------------------------------------------------------------------

    private static readonly string[] CanonicalDocuments =
    [
        "{\"b\": 1, \"a\": [1, 2.50, \"x\", null, true, false, {\"c\": -1}]}",
        "{\"\": 0, \"aa\": 1, \"b\": 2, \"ab\": {\"zz\": [], \"z\": {}}}",
        "123456789012345678901234567890",
        "-123456789012345678901234567890.123",
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "-9223372036854775809",
        "18446744073709551616",
        "1e-5",
        "0.0",
        "\"héllo 世界 😀\"",
        "[]",
        "{}",
        "null",
    ];

    [Test]
    public void BinaryParameter_EveryEncoderOutputIsAccepted()
    {
        foreach (string text in CanonicalDocuments.Append(Nested(JsonbLimits.MaxNestingDepth)))
        {
            byte[] cell = JsonbEncoder.FromText(text);
            ColumnValue value = DeserializeParameter(cell);
            Assert.AreEqual(ColumnType.Jsonb, value.Type, text);
            Assert.AreEqual(cell, value.BytesValue, text);
        }
    }

    /// <summary>Each case is one rule of the canonical form, broken once. All are refused as a 400.</summary>
    [TestCase("A2 61 62 01 61 61 02", TestName = "BinaryParameter_Refuses_UnsortedKeys")]
    [TestCase("A2 61 61 01 61 61 02", TestName = "BinaryParameter_Refuses_DuplicateKeys")]
    [TestCase("A2 62 61 61 01 61 62 02", TestName = "BinaryParameter_Refuses_LongerKeyFirst")]
    [TestCase("A2 61 61 01", TestName = "BinaryParameter_Refuses_TruncatedMap")]
    [TestCase("63 61 62", TestName = "BinaryParameter_Refuses_TruncatedString")]
    [TestCase("01 01", TestName = "BinaryParameter_Refuses_TrailingBytes")]
    [TestCase("C1 00", TestName = "BinaryParameter_Refuses_UnsupportedTag")]
    [TestCase("62 C3 28", TestName = "BinaryParameter_Refuses_InvalidUtf8")]
    [TestCase("63 ED A0 80", TestName = "BinaryParameter_Refuses_EncodedSurrogate")]
    [TestCase("61 00", TestName = "BinaryParameter_Refuses_NulInString")]
    [TestCase("A1 61 00 01", TestName = "BinaryParameter_Refuses_NulInKey")]
    [TestCase("A1 01 02", TestName = "BinaryParameter_Refuses_NonTextKey")]
    [TestCase("18 05", TestName = "BinaryParameter_Refuses_LongIntegerHeader")]
    [TestCase("81 18 05", TestName = "BinaryParameter_Refuses_LongNestedHeader")]
    [TestCase("F9 3C 00", TestName = "BinaryParameter_Refuses_Float")]
    [TestCase("F7", TestName = "BinaryParameter_Refuses_Undefined")]
    [TestCase("F8 14", TestName = "BinaryParameter_Refuses_LongSimpleValue")]
    [TestCase("41 00", TestName = "BinaryParameter_Refuses_ByteString")]
    [TestCase("9F 01 FF", TestName = "BinaryParameter_Refuses_IndefiniteArray")]
    [TestCase("1B 80 00 00 00 00 00 00 00", TestName = "BinaryParameter_Refuses_PlainIntegerPastInt64")]
    [TestCase("C2 41 05", TestName = "BinaryParameter_Refuses_BignumInsideInt64")]
    [TestCase("C2 49 00 80 00 00 00 00 00 00 00", TestName = "BinaryParameter_Refuses_BignumLeadingZero")]
    [TestCase("C4 82 00 05", TestName = "BinaryParameter_Refuses_DecimalFractionScaleZero")]
    [TestCase("C4 82 20 F5", TestName = "BinaryParameter_Refuses_DecimalFractionBadMantissa")]
    [TestCase("C4 83 20 05 05", TestName = "BinaryParameter_Refuses_DecimalFractionWrongArity")]
    [TestCase("C4 82 39 40 00 05", TestName = "BinaryParameter_Refuses_DecimalFractionScaleTooLarge")]
    public void BinaryParameter_RefusesNonCanonicalItem(string hex)
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => DeserializeParameter(Cell(Hex(hex))));
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText, ex!.Code, ex.Message);
    }

    [Test]
    public void BinaryParameter_RefusesEmptyAndUnknownVersion()
    {
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            Assert.Throws<CamusDBException>(() => DeserializeParameter([JsonbLimits.FormatVersion]))!.Code);
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            Assert.Throws<CamusDBException>(() => DeserializeParameter([2, 0xF6]))!.Code);
    }

    [Test]
    public void BinaryParameter_RefusesANumberPastTheDigitLimit()
    {
        // A bignum of 60000 bytes holds about 144000 digits, past the 131072 integer digits allowed.
        byte[] magnitude = new byte[60000];
        magnitude[0] = 1;
        byte[] item = [0xC2, 0x59, (byte)(magnitude.Length >> 8), (byte)magnitude.Length, .. magnitude];

        CamusDBException? ex = Assert.Throws<CamusDBException>(() => DeserializeParameter(Cell(item)));
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText, ex!.Code);
    }

    [Test]
    public void BinaryParameter_DepthLimitIsEnforced()
    {
        byte[] AtDepth(int depth)
        {
            byte[] item = new byte[depth + 1];
            Array.Fill(item, (byte)0x81, 0, depth);
            item[^1] = 0x00;
            return Cell(item);
        }

        Assert.DoesNotThrow(() => DeserializeParameter(AtDepth(JsonbLimits.MaxNestingDepth)));

        foreach (int depth in new[] { JsonbLimits.MaxNestingDepth + 1, 601, 100_000 })
        {
            CamusDBException? ex = Assert.Throws<CamusDBException>(() => DeserializeParameter(AtDepth(depth)));
            Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep, ex!.Code, depth.ToString());
        }
    }

    [Test]
    public void PeerWireCell_RefusesNonCanonicalItem()
    {
        byte[] cell = Cell(Hex("A2 61 62 01 61 61 02"));
        using JsonDocument document = JsonDocument.Parse(
            "{\"t\":" + (int)ColumnType.Jsonb + ",\"j\":\"" + Convert.ToBase64String(cell) + "\"}");

        CamusDBException? ex = Assert.Throws<CamusDBException>(() => ColumnValueWireCodec.Read(document.RootElement));
        Assert.AreEqual(CamusDBErrorCodes.JsonbValueCorrupt, ex!.Code);

        byte[] good = JsonbEncoder.FromText("{\"a\": 2, \"b\": 1}");
        using JsonDocument goodDocument = JsonDocument.Parse(
            "{\"t\":" + (int)ColumnType.Jsonb + ",\"j\":\"" + Convert.ToBase64String(good) + "\"}");
        Assert.AreEqual(good, ColumnValueWireCodec.Read(goodDocument.RootElement).BytesValue);
    }

    /// <summary>
    /// The end-to-end shape of the defect: a binary parameter bound into an INSERT. A canonical cell
    /// stores and reads back; the non-canonical map that once stored an invisible key never reaches
    /// the statement.
    /// </summary>
    [Test]
    public async Task BinaryParameter_InsertRoundTrip()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb)", ddl: true);

        ColumnValue good = DeserializeParameter(JsonbEncoder.FromText("{\"b\": 1, \"a\": 2}"));
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (1, @p)", new() { ["@p"] = good });

        List<QueryResultRow> rows = await Select(executor, db, "SELECT doc ->> 'a' AS a, doc ? 'b' AS present FROM docs");
        Assert.AreEqual("2", rows.Single().Row["a"].StrValue);
        Assert.IsTrue(rows.Single().Row["present"].BoolValue);

        Assert.Throws<CamusDBException>(() => DeserializeParameter(Cell(Hex("A2 61 62 01 61 61 02"))));
    }

    /// <summary>
    /// The recursive path edits have their own depth bound. A trusted cell is never deeper than the
    /// limit, so this builds a damaged one directly, past the validation, and follows it with a path
    /// longer than the limit: the edit must refuse it instead of recursing without a bound.
    /// </summary>
    [Test]
    public void PathEdits_HaveAnIndependentDepthBound()
    {
        const int depth = 2_000;
        byte[] item = new byte[depth + 1];
        Array.Fill(item, (byte)0x81, 0, depth);
        item[^1] = 0x00;

        string[] path = Enumerable.Repeat("0", depth).ToArray();

        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep,
            Assert.Throws<CamusDBException>(() => JsonbBuilder.DeletePath(item, path))!.Code);
        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep,
            Assert.Throws<CamusDBException>(() => JsonbBuilder.Set(item, path, [0x01], createMissing: true))!.Code);
    }

    // ---------------------------------------------------------------------------------------------
    // MySQL-style JSON functions over deep documents
    // ---------------------------------------------------------------------------------------------

    [TestCase(64)]
    [TestCase(65)]
    [TestCase(JsonbLimits.MaxNestingDepth)]
    public async Task LegacyFunctions_AcceptEveryValidJsonbDocument(int depth)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = ColumnValue.FromJsonText(Nested(depth)) };

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT json_valid(@p) AS valid, json_type(@p) AS kind, json_array_length(@p) AS len, " +
            "json_array_length(@p, '$[0][0]') AS inner_len, json_extract(@p, '$[0][0]') AS inner_doc, " +
            "json_contains(@p, @p) AS self_contains",
            parameters);

        IReadOnlyDictionary<string, ColumnValue> row = rows.Single().Row;
        Assert.IsTrue(row["valid"].BoolValue);
        Assert.AreEqual("array", row["kind"].StrValue);
        Assert.AreEqual(1L, row["len"].LongValue);
        Assert.AreEqual(1L, row["inner_len"].LongValue);
        Assert.AreEqual(Nested(depth - 2), row["inner_doc"].StrValue!.Replace(" ", ""));
        Assert.IsTrue(row["self_contains"].BoolValue);
    }

    /// <summary>A STRING argument keeps the default text parser limit: its behavior did not change.</summary>
    [Test]
    public async Task LegacyFunctions_StringArgumentKeepsTheTextLimit()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT json_valid(@s64) AS v64, json_valid(@s65) AS v65",
            new()
            {
                ["@s64"] = new ColumnValue(ColumnType.String, Nested(64)),
                ["@s65"] = new ColumnValue(ColumnType.String, Nested(65)),
            });

        Assert.IsTrue(rows.Single().Row["v64"].BoolValue);
        Assert.IsFalse(rows.Single().Row["v65"].BoolValue);
    }

    // ---------------------------------------------------------------------------------------------
    // Containment
    // ---------------------------------------------------------------------------------------------

    private static ReadOnlySpan<byte> Item(string text) => JsonbEncoder.FromText(text).AsSpan(1);

    private static bool Contains(string left, string right) => JsonbDocument.Contains(Item(left), Item(right));

    private static string WideObject(int keys, Func<int, string> value, Func<int, bool>? include = null) =>
        "{" + string.Join(",", Enumerable.Range(0, keys).Where(i => include?.Invoke(i) ?? true).Select(i => $"\"k{i:D5}\":{value(i)}")) + "}";

    [Test]
    public void ObjectContainment_WideObjects()
    {
        const int keys = 2_000;
        string full = WideObject(keys, i => i.ToString());

        Assert.IsTrue(Contains(full, full), "an object contains itself");
        Assert.IsTrue(Contains(full, WideObject(keys, i => i.ToString(), i => i % 3 == 0)), "every third key");
        Assert.IsTrue(Contains(full, WideObject(keys, i => i + ".0", i => i % 7 == 0)), "a number at another scale is equal");
        Assert.IsFalse(Contains(full, WideObject(keys, i => i == keys - 1 ? "-1" : i.ToString())), "the last value differs");
        Assert.IsFalse(Contains(full, full[..^1] + ",\"k99999\":1}"), "a key past the last one");
        Assert.IsFalse(Contains(full, "{\"k00000\":0,\"k0000\":1}"), "a shorter key that sorts first");
        Assert.IsFalse(Contains(WideObject(keys, i => i.ToString(), i => i != 1000), full), "the left side lacks one key");
        Assert.IsTrue(Contains(full, "{}"));
    }

    [Test]
    public void ObjectContainment_NestedValues()
    {
        const string left = "{\"a\": {\"b\": [1, 2, {\"c\": 3, \"d\": 4}], \"e\": \"x\"}, \"f\": null, \"g\": [[1], [2, 3]]}";

        Assert.IsTrue(Contains(left, "{\"a\": {\"b\": [{\"d\": 4}]}}"));
        Assert.IsTrue(Contains(left, "{\"g\": [[3]], \"f\": null}"));
        Assert.IsTrue(Contains(left, "{\"a\": {\"e\": \"x\"}}"));
        Assert.IsFalse(Contains(left, "{\"a\": {\"b\": [{\"d\": 5}]}}"));
        Assert.IsFalse(Contains(left, "{\"a\": {\"e\": \"x\", \"z\": 1}}"));
        Assert.IsFalse(Contains(left, "{\"a\": \"x\"}"));
    }

    /// <summary>Wide arrays take the hashed scalar lookup; the answers must match the PostgreSQL rule.</summary>
    [Test]
    public void ArrayContainment_WideArrays()
    {
        string left = "[" + string.Join(",", Enumerable.Range(0, 200).Select(i => i % 2 == 0 ? i.ToString() : $"\"s{i}\"")) + ", null, true, {\"o\": 1}, [9]]";

        string evens = "[" + string.Join(",", Enumerable.Range(0, 100).Select(i => (i * 2) + ".00")) + "]";
        Assert.IsTrue(Contains(left, evens), "numbers compare by value");

        string strings = "[" + string.Join(",", Enumerable.Range(0, 50).Select(i => $"\"s{(i * 2) + 1}\"")) + ", \"s1\", \"s1\"]";
        Assert.IsTrue(Contains(left, strings), "repeats on the right do not matter");

        Assert.IsFalse(Contains(left, "[" + string.Join(",", Enumerable.Range(0, 20).Select(i => i * 2)) + ", 1]"), "an odd number is absent");
        Assert.IsFalse(Contains(left, "[" + string.Join(",", Enumerable.Range(0, 20).Select(i => $"\"s{(i * 2) + 1}\"")) + ", \"s2\"]"), "a string that is only a number on the left");
        Assert.IsTrue(Contains(left, "[" + string.Join(",", Enumerable.Range(0, 20).Select(i => i * 2)) + ", null, true, {}, [9], [], {\"o\": 1}]"));
        Assert.IsFalse(Contains(left, "[" + string.Join(",", Enumerable.Range(0, 20).Select(i => i * 2)) + ", false]"));
        Assert.IsFalse(Contains(left, "[" + string.Join(",", Enumerable.Range(0, 20).Select(i => i * 2)) + ", \"0\"]"), "a string never equals a number");
    }

    // ---------------------------------------------------------------------------------------------
    // jsonb_object_agg state
    // ---------------------------------------------------------------------------------------------

    [Test]
    public void ObjectAggregate_RepeatedKeysKeepOneValue()
    {
        ExtensionAggregate aggregate = JsonbAggregates.All().Single(a => a.Name == "jsonb_object_agg");
        object state = aggregate.CreateState(aggregate.Name);
        IMeasuredAggregateState measured = (IMeasuredAggregateState)state;

        for (int i = 0; i < 4_000; i++)
            aggregate.Accumulate(state, aggregate.Name, [new(ColumnType.String, "key"), new(ColumnType.String, new string('x', 8_192) + i)]);

        Assert.Less(measured.RetainedBytes, 64 * 1024, "only the last value of the repeated key is retained");

        aggregate.Accumulate(state, aggregate.Name, [new(ColumnType.String, "key"), new(ColumnType.String, "last")]);
        aggregate.Accumulate(state, aggregate.Name, [new(ColumnType.String, "other"), new(ColumnType.Integer64, 7L)]);
        Assert.Less(measured.RetainedBytes, 1024, "a smaller replacement value gives its bytes back");

        Assert.AreEqual("{\"key\": \"last\", \"other\": 7}", aggregate.Finish(state, aggregate.Name).JsonValue);
    }

    [Test]
    public void ArrayAggregate_ReportsItsGrowth()
    {
        ExtensionAggregate aggregate = JsonbAggregates.All().Single(a => a.Name == "jsonb_agg");
        object state = aggregate.CreateState(aggregate.Name);
        IMeasuredAggregateState measured = (IMeasuredAggregateState)state;

        for (int i = 0; i < 10; i++)
            aggregate.Accumulate(state, aggregate.Name, [new(ColumnType.String, new string('x', 1_000))]);

        Assert.GreaterOrEqual(measured.RetainedBytes, 10 * 1_000);
        Assert.AreEqual(10, JsonbDocument.ArrayLength(JsonbDocument.Item(aggregate.Finish(state, aggregate.Name).BytesValue)));
    }

    [Test]
    public async Task ObjectAggregate_LastValueWinsThroughSql()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE kv (id int64 PRIMARY KEY, k string, v int64)", ddl: true);
        await Exec(executor, db, "INSERT INTO kv (id, k, v) VALUES (1, 'a', 1), (2, 'b', 2), (3, 'a', 3), (4, 'a', 4)");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT jsonb_object_agg(k, v) AS o FROM kv");
        Assert.AreEqual("{\"a\": 4, \"b\": 2}", rows.Single().Row["o"].JsonValue);
    }

    // ---------------------------------------------------------------------------------------------
    // GIN candidate bound
    // ---------------------------------------------------------------------------------------------

    private static async Task<(List<long> Ids, long RowsRead)> GinIds(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            CamusDB.Core.Diagnostics.StatementProbe probe = new();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db.Name, sql: sql, parameters: null, probe: probe));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return (rows.Select(r => r.Row["id"].LongValue).ToList(), probe.RowsRead);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<(CommandExecutor, DatabaseDescriptor)> GinTable(CommandExecutor executor, DatabaseDescriptor db)
    {
        await Exec(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))", ddl: true);

        // Rows 1..12 hold "kind"; rows 13..30 do not. Rows 1..4 also hold "a".
        StringBuilder insert = new("INSERT INTO docs (id, doc) VALUES ");
        for (int i = 1; i <= 30; i++)
        {
            string doc = i <= 4 ? "{\"kind\": " + i + ", \"a\": 1}" : i <= 12 ? "{\"kind\": " + i + "}" : "{\"other\": " + i + "}";
            insert.Append(i == 1 ? "" : ", ").Append('(').Append(i).Append(", '").Append(doc).Append("')");
        }

        await Exec(executor, db, insert.ToString());
        return (executor, db);
    }

    /// <summary>
    /// A GIN read whose candidates pass the operator row cap continues as a table scan with the same
    /// filter. The cap here is 5 candidates. <c>doc ? 'kind'</c> has 12 and falls back, so it reads all
    /// 30 rows. <c>doc ? 'a'</c> has 4 and stays on the index, so it reads 4 rows. Both must return
    /// the rows of an engine with no effective cap.
    /// </summary>
    [Test]
    public async Task GinScan_PastTheCandidateCap_ContinuesAsATableScan()
    {
        (_, DatabaseDescriptor capped, CommandExecutor cappedExecutor) = await CreateDatabase(Options with { SpillThresholdRows = 5 });
        await GinTable(cappedExecutor, capped);

        (_, DatabaseDescriptor reference, CommandExecutor referenceExecutor) = await CreateDatabase();
        await GinTable(referenceExecutor, reference);

        foreach (string predicate in new[] { "doc ? 'kind'", "doc ? 'a'", "doc @> '{\"kind\": 7}'", "doc ? 'kind' AND doc ? 'a'" })
        {
            string sql = $"SELECT id FROM docs@{{FORCE_INDEX=gin_doc}} WHERE {predicate} ORDER BY id";
            (List<long> expected, _) = await GinIds(referenceExecutor, reference, sql);
            (List<long> actual, _) = await GinIds(cappedExecutor, capped, sql);
            Assert.AreEqual(expected, actual, predicate);
        }

        (List<long> broad, long broadRead) = await GinIds(cappedExecutor, capped, "SELECT id FROM docs@{FORCE_INDEX=gin_doc} WHERE doc ? 'kind'");
        Assert.AreEqual(Enumerable.Range(1, 12).Select(i => (long)i), broad.OrderBy(i => i));
        Assert.AreEqual(30, broadRead, "twelve candidates pass the cap of five, so the read scans the table");

        (List<long> narrow, long narrowRead) = await GinIds(cappedExecutor, capped, "SELECT id FROM docs@{FORCE_INDEX=gin_doc} WHERE doc ? 'a'");
        Assert.AreEqual(Enumerable.Range(1, 4).Select(i => (long)i), narrow.OrderBy(i => i));
        Assert.AreEqual(4, narrowRead, "four candidates stay under the cap, so the read uses the index");

        (List<long> limited, _) = await GinIds(cappedExecutor, capped, "SELECT id FROM docs@{FORCE_INDEX=gin_doc} WHERE doc ? 'kind' LIMIT 1");
        Assert.AreEqual(1, limited.Count);
        Assert.That(limited[0], Is.InRange(1L, 12L));
    }

    /// <summary>
    /// After the fallback, a Serializable read-write read is protected by the table scan's row-range
    /// lock: an insert of a new matching row must not commit while the reader lives.
    /// </summary>
    [Test]
    public async Task GinScan_FallbackKeepsTheSerializableLocks()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(Options with { SpillThresholdRows = 5 });
        await GinTable(executor, db);

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable,
            CamusTransactionMode.ReadWrite);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(reader, db.Name, "SELECT id FROM docs@{FORCE_INDEX=gin_doc} WHERE doc ? 'kind'", null));
            Assert.AreEqual(12, (await cursor.ToListAsync()).Count);

            KvTransaction writer = await db.Transactions.BeginAsync();
            bool refused = false;
            try
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, db.Name, "INSERT INTO docs (id, doc) VALUES (99, '{\"kind\": 99}')", null));
                await db.Transactions.CommitAsync(writer);
            }
            catch (CamusDBException)
            {
                refused = true;
            }
            finally
            {
                await db.Transactions.RollbackIfNotCompletedAsync(writer);
            }

            Assert.IsTrue(refused, "a phantom insert into the range a Serializable reader scanned must not commit");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }
}
