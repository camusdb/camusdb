/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for the <c>jsonb</c> column type through SQL: the type words (<c>jsonb</c> and the
/// <c>json</c> alias stay usable as names), the write coercion of text, the typed literal and the casts,
/// the refusals (key columns, invalid text, nesting depth), SHOW COLUMNS and SHOW CREATE TABLE, the
/// storage strategies and out-of-line documents, spilled sorts and groups, the REST form, and the row
/// round trip across a close and reopen. Expected texts are PostgreSQL 16 output for the same input.
/// </summary>
internal sealed class TestJsonbType : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTable(string ddl)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, ddl);
        return (dbname, db, executor);
    }

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
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<CamusDBException> AssertExecThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task<CamusDBException> AssertSelectThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await cursor.ToListAsync();
        })!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task<CamusDBException> AssertDDLThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    /// <summary>The PostgreSQL text of column <paramref name="column"/> per row, ordered by id.</summary>
    private static async Task<string?[]> ReadDocs(CommandExecutor executor, DatabaseDescriptor db, string column = "doc", string table = "t")
    {
        List<QueryResultRow> rows = await Select(executor, db, $"SELECT id, {column} FROM {table} ORDER BY id");
        return rows.Select(r =>
        {
            ColumnValue value = r.Row[column];
            if (value.Type == ColumnType.Null)
                return null;
            Assert.AreEqual(ColumnType.Jsonb, value.Type);
            return value.JsonValue;
        }).ToArray();
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_TextLiteralsTypedLiteralsAndCasts_StoreTheCanonicalDocument()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb)");

        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (1, '{\"b\": 1, \"a\": [1.50, true, null]}')");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (2, JSONB '{\"a\": 1, \"a\": 2}')");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (3, json '  [ 1e3 , \"x\" ]  ')");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (4, CAST('\"text\"' AS jsonb))");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (5, '123456789012345678901234567890.0012'::jsonb)");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (6, NULL)");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (7, 'null')");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (@id, @doc)", new Dictionary<string, ColumnValue>
        {
            ["@id"] = new(ColumnType.Integer64, 8),
            ["@doc"] = new(ColumnType.String, "{\"param\": \"é\"}"),
        });

        CollectionAssert.AreEqual(new[]
        {
            "{\"a\": [1.50, true, null], \"b\": 1}",
            "{\"a\": 2}",
            "[1000, \"x\"]",
            "\"text\"",
            "123456789012345678901234567890.0012",
            null,
            "null",
            "{\"param\": \"é\"}",
        }, await ReadDocs(executor, db));
    }

    [Test]
    [NonParallelizable]
    public async Task Update_AndDefault_WriteDocuments()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb DEFAULT('{\"v\": 0}'))");

        await Exec(executor, db, "INSERT INTO t (id) VALUES (1)");
        await Exec(executor, db, "INSERT INTO t (id, doc) VALUES (2, '[]')");
        await Exec(executor, db, "UPDATE t SET doc = '{\"v\": 2, \"w\": {}}' WHERE id = 2");

        CollectionAssert.AreEqual(new[] { "{\"v\": 0}", "{\"v\": 2, \"w\": {}}" }, await ReadDocs(executor, db));
    }

    [Test]
    [NonParallelizable]
    public async Task InvalidText_AndTooDeepDocuments_AreRefusedOnWrite()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            (await AssertExecThrows(executor, db, "INSERT INTO t (id, doc) VALUES (1, '{\"a\": }')")).Code);
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            (await AssertExecThrows(executor, db, "INSERT INTO t (id, doc) VALUES (1, '\"\\u0000\"')")).Code);
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            (await AssertSelectThrows(executor, db, "SELECT JSONB 'nope'")).Code);

        string deep = new string('[', JsonbLimits.MaxNestingDepth + 1) + new string(']', JsonbLimits.MaxNestingDepth + 1);
        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep,
            (await AssertExecThrows(executor, db, $"INSERT INTO t (id, doc) VALUES (1, '{deep}')")).Code);

        // Nothing was written by the refused statements.
        Assert.AreEqual(0, (await Select(executor, db, "SELECT id FROM t")).Count);

        // A value of another type is not text, so it does not become a document.
        CamusDBException cast = await AssertSelectThrows(executor, db, "SELECT CAST(1 AS jsonb)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, cast.Code);
        StringAssert.Contains("to_jsonb", cast.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TypeWords_StayUsableAsNames()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE json (id int64 PRIMARY KEY, json jsonb, jsonb json NOT NULL)");

        await Exec(executor, db, "INSERT INTO json (id, json, jsonb) VALUES (1, '[1]', '{\"k\": true}')");

        CollectionAssert.AreEqual(new[] { "[1]" }, await ReadDocs(executor, db, "json", "json"));
        CollectionAssert.AreEqual(new[] { "{\"k\": true}" }, await ReadDocs(executor, db, "jsonb", "json"));

        CamusDBException unknown = await AssertDDLThrows(executor, db, "CREATE TABLE u (id int64 PRIMARY KEY, x foobar)");
        StringAssert.Contains("JSONB", unknown.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task KeyColumns_AreRefused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb)");

        CamusDBException primaryKey = await AssertDDLThrows(executor, db, "CREATE TABLE p (doc jsonb PRIMARY KEY)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, primaryKey.Code);
        StringAssert.Contains("USING GIN", primaryKey.Message);

        CamusDBException unique = await AssertDDLThrows(executor, db, "CREATE TABLE u (id int64 PRIMARY KEY, doc jsonb UNIQUE)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, unique.Code);

        CamusDBException index = await AssertDDLThrows(executor, db, "CREATE INDEX t_doc ON t (doc)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, index.Code);
        StringAssert.Contains("jsonb", index.Message);

        CamusDBException composite = await AssertDDLThrows(executor, db, "CREATE UNIQUE INDEX t_id_doc ON t (id, doc)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, composite.Code);

        // An array of documents has no type syntax: a jsonb array holds several documents.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, db, "CREATE TABLE a (id int64 PRIMARY KEY, docs array(jsonb))"));

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported,
            Assert.Throws<CamusDBException>(() => ColumnValue.FromArray(ColumnType.Jsonb, []))!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task AddColumn_WithBothTypeWords_AndADefault()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY)");
        await Exec(executor, db, "INSERT INTO t (id) VALUES (1)");

        await ExecDDL(executor, db, "ALTER TABLE t ADD COLUMN doc jsonb DEFAULT('{\"a\": 1}')");
        await ExecDDL(executor, db, "ALTER TABLE t ADD meta json");

        await Exec(executor, db, "INSERT INTO t (id, meta) VALUES (2, '[true]')");

        CollectionAssert.AreEqual(new[] { "{\"a\": 1}", "{\"a\": 1}" }, await ReadDocs(executor, db));
        CollectionAssert.AreEqual(new[] { null, "[true]" }, await ReadDocs(executor, db, "meta"));
    }

    [Test]
    [NonParallelizable]
    public async Task ShowColumns_AndShowCreateTable_RoundTrip()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb STORAGE EXTERNAL DEFAULT('{\"q\": \"it''s\", \"n\": 1.50}'), raw json)");

        List<QueryResultRow> cols = await Select(executor, db, "SHOW COLUMNS FROM t");
        QueryResultRow doc = cols.Single(r => r.Row["Field"].StrValue == "doc");
        Assert.AreEqual("JSONB", doc.Row["Type"].StrValue);
        Assert.AreEqual("{\"n\": 1.50, \"q\": \"it's\"}", doc.Row["Default"].StrValue);
        Assert.AreEqual("JSONB", cols.Single(r => r.Row["Field"].StrValue == "raw").Row["Type"].StrValue);

        List<QueryResultRow> shown = await Select(executor, db, "SHOW CREATE TABLE t");
        string ddl = shown[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("JSONB", ddl);
        StringAssert.Contains("STORAGE EXTERNAL", ddl);

        // The rendered DDL creates the same table and the same default when it runs.
        (_, DatabaseDescriptor copy, _) = await CreateDatabase();
        await ExecDDL(executor, copy, ddl);
        await Exec(executor, copy, "INSERT INTO t (id) VALUES (1)");
        CollectionAssert.AreEqual(new[] { "{\"n\": 1.50, \"q\": \"it's\"}" }, await ReadDocs(executor, copy));
    }

    [Test]
    [NonParallelizable]
    public async Task Casts_FromDocuments_FollowPostgreSql()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb, n jsonb, b jsonb)");
        await Exec(executor, db, "INSERT INTO t (id, doc, n, b) VALUES (1, '{\"a\": [1]}', '2.5', 'true')");

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT CAST(doc AS string) AS s, doc::string AS postfix, CAST(n AS int64) AS i, CAST(n AS float64) AS f, " +
            "CAST(n AS numeric) AS num, CAST(b AS bool) AS truth FROM t");
        IReadOnlyDictionary<string, ColumnValue> row = rows[0].Row;
        Assert.AreEqual("{\"a\": [1]}", row["s"].StrValue);
        Assert.AreEqual("{\"a\": [1]}", row["postfix"].StrValue);
        Assert.AreEqual(3L, row["i"].LongValue, "numeric to bigint rounds half away from zero");
        Assert.AreEqual(2.5, row["f"].FloatValue);
        Assert.AreEqual("2.5", row["num"].NumericValue);
        Assert.IsTrue(row["truth"].BoolValue);

        CamusDBException wrongKind = await AssertSelectThrows(executor, db, "SELECT CAST(doc AS int64) FROM t");
        StringAssert.Contains("cannot cast jsonb object to type bigint", wrongKind.Message);
        CamusDBException wrongBool = await AssertSelectThrows(executor, db, "SELECT CAST(n AS bool) FROM t");
        StringAssert.Contains("cannot cast jsonb number to type boolean", wrongBool.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task LargeDocuments_RoundTripUnderEveryStorageStrategy()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, title string, doc jsonb, plain jsonb STORAGE PLAIN, main jsonb STORAGE MAIN, ext jsonb STORAGE EXTERNAL)");

        // Repeated text compresses; the size is well past the out-of-line threshold.
        string large = "{\"items\": [" + string.Join(", ", Enumerable.Range(0, 400).Select(i => $"{{\"sku\": \"SKU-{i:D5}\", \"qty\": {i % 7}}}")) + "]}";
        string canonical = JsonbText.ToText(JsonbDocument.Item(JsonbEncoder.FromText(large)));

        await Exec(executor, db, $"INSERT INTO t (id, title, doc, plain, main, ext) VALUES (1, 'big', '{large}', '{large}', '{large}', '{large}')");

        foreach (string column in new[] { "doc", "plain", "main", "ext" })
            CollectionAssert.AreEqual(new[] { canonical }, await ReadDocs(executor, db, column), column);

        // A query that does not name the documents still reads the row.
        List<QueryResultRow> titles = await Select(executor, db, "SELECT title FROM t");
        Assert.AreEqual("big", titles.Single().Row["title"].StrValue);

        // An update of another column keeps the documents.
        await Exec(executor, db, "UPDATE t SET title = 'renamed' WHERE id = 1");
        CollectionAssert.AreEqual(new[] { canonical }, await ReadDocs(executor, db, "ext"));
    }

    /// <summary>
    /// A sort, a DISTINCT and a GROUP BY over documents on an engine forced to spill after two rows.
    /// The order is the PostgreSQL jsonb order, and 1.0 and 1 are one group, as they are one value.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Queries_ThatSpill_CarryDocuments_InPostgreSqlOrder()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) =
            await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await ExecDDL(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb)");
        await Exec(executor, db,
            "INSERT INTO t (id, doc) VALUES (1, '{\"a\": 1}'), (2, '[1, 2]'), (3, '\"s\"'), (4, '1.0'), (5, 'null'), " +
            "(6, '1'), (7, 'true'), (8, NULL), (9, '[]'), (10, '{\"a\": 1}')");

        // NULL first, then the empty top-level array below every scalar, then null < string < number
        // < boolean < array < object. 1 and 1.0 are equal, so their order between them is not fixed.
        List<QueryResultRow> sorted = await Select(executor, db, "SELECT doc FROM t ORDER BY doc");
        string?[] texts = sorted.Select(r => r.Row["doc"].JsonValue).ToArray();
        CollectionAssert.AreEqual(new[] { null, "[]", "null", "\"s\"" }, texts.Take(4).ToArray());
        CollectionAssert.AreEquivalent(new[] { "1", "1.0" }, texts.Skip(4).Take(2).ToArray());
        CollectionAssert.AreEqual(new[] { "true", "[1, 2]", "{\"a\": 1}", "{\"a\": 1}" }, texts.Skip(6).ToArray());

        List<QueryResultRow> distinct = await Select(executor, db, "SELECT DISTINCT doc FROM t");
        Assert.AreEqual(8, distinct.Count, "1 and 1.0 are one value; the two objects are one value");

        List<QueryResultRow> grouped = await Select(executor, db, "SELECT doc, COUNT(*) AS c FROM t GROUP BY doc");
        Assert.AreEqual(8, grouped.Count);
        Assert.AreEqual(2L, grouped.Single(r => r.Row["doc"].JsonValue == "{\"a\": 1}").Row["c"].LongValue);
        Assert.AreEqual(2L, grouped.Single(r => r.Row["doc"].JsonValue is "1" or "1.0").Row["c"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Values_SurviveCloseAndReopen()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, doc jsonb DEFAULT('[\"d\"]'), other jsonb)");

        await Exec(executor, db, "INSERT INTO t (id, doc, other) VALUES (1, '{\"z\": -9223372036854775809, \"y\": 0.001}', 'false')");
        await Exec(executor, db, "INSERT INTO t (id) VALUES (2)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        CollectionAssert.AreEqual(new[] { "{\"y\": 0.001, \"z\": -9223372036854775809}", "[\"d\"]" }, await ReadDocs(executor, reopened));
        CollectionAssert.AreEqual(new[] { "false", null }, await ReadDocs(executor, reopened, "other"));
    }

    [Test]
    public void RestForm_IsTheDocumentItself_WithExactNumbers()
    {
        ColumnValue value = ColumnValue.FromJsonText("{\"n\": 12345678901234567890.50, \"s\": \"é\", \"a\": [null, true]}");

        object? encoded = CompactRowEncoder.EncodeValue(value);
        Assert.IsInstanceOf<JsonElement>(encoded);
        string json = JsonSerializer.Serialize(encoded);
        Assert.AreEqual("{\"a\":[null,true],\"n\":12345678901234567890.50,\"s\":\"\\u00E9\"}", json);
    }

    [Test]
    public void HttpParameter_ParsesText_AndCellBytesRoundTrip()
    {
        JsonSerializerOptions web = new(JsonSerializerDefaults.Web);
        ColumnValue fromText = JsonSerializer.Deserialize<ColumnValue>("{\"type\": 13, \"strValue\": \"{\\\"b\\\": 2, \\\"a\\\": 1}\"}", web)!;
        Assert.AreEqual(ColumnType.Jsonb, fromText.Type);
        Assert.AreEqual("{\"a\": 1, \"b\": 2}", fromText.JsonValue);

        // A persisted schema default round-trips through its cell bytes.
        string persisted = JsonSerializer.Serialize(fromText, web);
        ColumnValue reloaded = JsonSerializer.Deserialize<ColumnValue>(persisted, web)!;
        Assert.AreEqual(0, fromText.CompareTo(reloaded));

        CamusDBException invalid = Assert.Throws<CamusDBException>(() =>
            JsonSerializer.Deserialize<ColumnValue>("{\"type\": 13, \"strValue\": \"{\"}", web))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText, invalid.Code);
    }
}
