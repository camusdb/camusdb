/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using ProtoColType = CamusDB.Grpc.ColumnType;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// jsonb over the client transports. REST: a parameter <c>{"type": 13, "strValue": "…"}</c> binds a
/// document, a plain string parameter is parsed by the write coercion, and a row carries the document
/// itself as native JSON (its numbers keep every digit). gRPC: <c>json_value</c> in both directions,
/// with the jsonb column type in the schema. Invalid text is refused on both transports with
/// <see cref="CamusDBErrorCodes.InvalidJsonText"/>, and nothing is written.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestJsonbTransports : BaseTest
{
    private const string Document = "{\"n\": 12345678901234567890.50, \"s\": \"é\", \"tags\": [\"vip\", null]}";

    private const string Canonical = "{\"n\": 12345678901234567890.50, \"s\": \"é\", \"tags\": [\"vip\", null]}";

    private ILogger<ICamusDB> Logger => logger;

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    [SetUp]
    public void SetUpTransports()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new(Options);
        grpc = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownTransports()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, Logger, Options) { ControllerContext = Context(body) };

    private static object RestJsonb(string text) => new { type = (int)CoreColumnType.Jsonb, strValue = text };

    private async Task<string> CreateDatabaseWithEventsAsync()
    {
        string db = "jt" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db,
            "CREATE TABLE events (id oid NOT NULL DEFAULT(gen_id()), name string, doc jsonb, PRIMARY KEY (id))", null));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        return db;
    }

    private async Task<List<string?>> ReadDocsAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT name, doc FROM events ORDER BY name", null));
            List<string?> values = [];
            await foreach (QueryResultRow row in cursor)
                values.Add(row.Row["doc"].JsonValue);
            return values;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Rest_JsonbParameters_Write_AndRowsCarryTheDocumentAsNativeJson()
    {
        string db = await CreateDatabaseWithEventsAsync();

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO events (name, doc) VALUES ('a', @a), ('b', @b), ('c', NULL)",
            parameters = new Dictionary<string, object>
            {
                ["@a"] = RestJsonb(Document),
                ["@b"] = new { type = (int)CoreColumnType.String, strValue = "[1, {\"k\": true}]" },
            },
        }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse inserted = (ExecuteNonSQLQueryResponse)insert.Value!;
        Assert.AreEqual("ok", inserted.Status, inserted.Message);
        Assert.AreEqual(3, inserted.Rows);

        CollectionAssert.AreEqual(new[] { Canonical, "[1, {\"k\": true}]", null }, await ReadDocsAsync(db));

        JsonResult query = await Sql(new
        {
            databaseName = db,
            sql = "SELECT name, doc, doc ->> 's' AS s FROM events WHERE doc @> @probe",
            parameters = new Dictionary<string, object> { ["@probe"] = RestJsonb("{\"tags\": [\"vip\"]}") },
        }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)query.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1, response.Total);
        CollectionAssert.AreEqual(new[] { CoreColumnType.String, CoreColumnType.Jsonb, CoreColumnType.String }, response.Columns.Select(c => c.Type));

        // The row holds the document as JSON, not a string that holds it, and the number keeps its digits.
        string json = JsonSerializer.Serialize(response, WireJson);
        using JsonDocument parsed = JsonDocument.Parse(json);
        JsonElement row = parsed.RootElement.GetProperty("rows")[0];
        Assert.AreEqual(JsonValueKind.Object, row[1].ValueKind, json);
        Assert.AreEqual("12345678901234567890.50", row[1].GetProperty("n").GetRawText());
        Assert.AreEqual("é", row[1].GetProperty("s").GetString());
        Assert.AreEqual("é", row[2].GetString());
    }

    /// <summary>
    /// The default REST row writes a document that is JSON null and SQL NULL as the same token. With
    /// <c>jsonbAsText</c> each jsonb cell is a string that holds the document text, so the two differ,
    /// and each jsonb column carries <c>jsonbAsText: true</c> to tell the client which form it reads.
    /// A string document is the case that needs the mark: <c>"abc"</c> is valid in both forms.
    /// </summary>
    [Test]
    public async Task Rest_JsonbAsText_KeepsJsonNullApartFromSqlNull_AndMarksTheColumn()
    {
        string db = await CreateDatabaseWithEventsAsync();

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO events (name, doc) VALUES ('a', @a), ('b', @b), ('c', NULL), ('d', @d)",
            parameters = new Dictionary<string, object>
            {
                ["@a"] = RestJsonb("null"),
                ["@b"] = RestJsonb("\"abc\""),
                ["@d"] = RestJsonb(Document),
            },
        }).ExecuteNonSQLQuery();
        Assert.AreEqual("ok", ((ExecuteNonSQLQueryResponse)insert.Value!).Status);

        const string select = "SELECT name, doc FROM events ORDER BY name";

        // Default form: JSON null and SQL NULL are the same token, and no column is marked.
        string plain = JsonSerializer.Serialize(
            (ExecuteSQLQueryResponse)(await Sql(new { databaseName = db, sql = select }).ExecuteSQLQuery()).Value!, WireJson);
        using (JsonDocument parsed = JsonDocument.Parse(plain))
        {
            JsonElement rows = parsed.RootElement.GetProperty("rows");
            Assert.AreEqual(JsonValueKind.Null, rows[0][1].ValueKind, plain);
            Assert.AreEqual(JsonValueKind.Null, rows[2][1].ValueKind, plain);
            Assert.AreEqual("abc", rows[1][1].GetString());
            Assert.IsFalse(parsed.RootElement.GetProperty("columns")[1].TryGetProperty("jsonbAsText", out _), plain);
        }

        // Text form: every document is its text in a string, SQL NULL is still null.
        string text = JsonSerializer.Serialize(
            (ExecuteSQLQueryResponse)(await Sql(new { databaseName = db, sql = select, jsonbAsText = true }).ExecuteSQLQuery()).Value!, WireJson);
        using (JsonDocument parsed = JsonDocument.Parse(text))
        {
            JsonElement columns = parsed.RootElement.GetProperty("columns");
            Assert.IsFalse(columns[0].TryGetProperty("jsonbAsText", out _), text);
            Assert.IsTrue(columns[1].GetProperty("jsonbAsText").GetBoolean(), text);

            JsonElement rows = parsed.RootElement.GetProperty("rows");
            Assert.AreEqual("null", rows[0][1].GetString());
            Assert.AreEqual("\"abc\"", rows[1][1].GetString());
            Assert.AreEqual(JsonValueKind.Null, rows[2][1].ValueKind, text);
            Assert.AreEqual(Canonical, rows[3][1].GetString());
            Assert.AreEqual("a", rows[0][0].GetString());
        }

        // RETURNING rows of /execute-sql-non-query use the same form and the same mark.
        string returning = JsonSerializer.Serialize(
            (ExecuteNonSQLQueryResponse)(await Sql(new
            {
                databaseName = db,
                sql = "INSERT INTO events (name, doc) VALUES ('e', @e), ('f', NULL) RETURNING doc",
                parameters = new Dictionary<string, object> { ["@e"] = RestJsonb("null") },
                jsonbAsText = true,
            }).ExecuteNonSQLQuery()).Value!, WireJson);
        using (JsonDocument parsed = JsonDocument.Parse(returning))
        {
            Assert.IsTrue(parsed.RootElement.GetProperty("columns")[0].GetProperty("jsonbAsText").GetBoolean(), returning);

            JsonElement rows = parsed.RootElement.GetProperty("returningRows");
            Assert.AreEqual("null", rows[0][0].GetString(), returning);
            Assert.AreEqual(JsonValueKind.Null, rows[1][0].ValueKind, returning);
        }
    }

    [Test]
    public async Task Rest_InvalidJsonbParameter_IsRefused_AndWritesNothing()
    {
        string db = await CreateDatabaseWithEventsAsync();

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO events (name, doc) VALUES ('bad', @a)",
            parameters = new Dictionary<string, object> { ["@a"] = RestJsonb("{\"a\": ") },
        }).ExecuteNonSQLQuery();

        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText, ReadErrorCode(insert.Value!));
        Assert.AreEqual(0, (await ReadDocsAsync(db)).Count);
    }

    /// <summary>
    /// The error code of a REST reply. A request body the controller cannot bind is refused before it
    /// has a typed response, so the code may sit on either response shape.
    /// </summary>
    private static string? ReadErrorCode(object value)
    {
        using JsonDocument doc = JsonDocument.Parse(JsonSerializer.Serialize(value, value.GetType(), WireJson));
        return doc.RootElement.TryGetProperty("code", out JsonElement code) ? code.GetString() : null;
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Grpc_JsonValue_WritesAndReadsBackWithTheJsonbType()
    {
        string db = await CreateDatabaseWithEventsAsync();

        SqlRequest insert = new() { Database = db, Sql = "INSERT INTO events (name, doc) VALUES ('a', @a), ('b', @b)" };
        insert.Parameters.Add("@a", new Value { JsonValue = Document });
        insert.Parameters.Add("@b", new Value { StringValue = "\"plain\"" });
        NonQueryReply reply = await grpc.ExecuteNonQuery(insert, new TestServerCallContext());
        Assert.AreEqual(2, reply.AffectedRows);

        CollectionAssert.AreEqual(new[] { Canonical, "\"plain\"" }, await ReadDocsAsync(db));

        SqlRequest select = new() { Database = db, Sql = "SELECT name, doc, doc -> 'tags' AS tags FROM events WHERE doc ? 'tags'" };
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(select, writer, new TestServerCallContext(CancellationToken.None));

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(writer.Written, 1, "name", "doc", "tags");
        Assert.AreEqual(ProtoColType.Jsonb, schema.Columns[1].Type);
        Assert.AreEqual(ProtoColType.Jsonb, schema.Columns[2].Type);

        Dictionary<string, Value> row = GrpcAssert.RowValues(writer.Written, schema);
        Assert.AreEqual(Value.KindOneofCase.JsonValue, row["doc"].KindCase);
        Assert.AreEqual(Canonical, row["doc"].JsonValue);
        Assert.AreEqual("[\"vip\", null]", row["tags"].JsonValue);
    }

    [Test]
    public async Task Grpc_InvalidJsonValue_IsInvalidArgument()
    {
        string db = await CreateDatabaseWithEventsAsync();

        SqlRequest insert = new() { Database = db, Sql = "INSERT INTO events (name, doc) VALUES ('bad', @a)" };
        insert.Parameters.Add("@a", new Value { JsonValue = "[1," });

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.ExecuteNonQuery(insert, new TestServerCallContext()))!;

        Assert.AreEqual(StatusCode.InvalidArgument, error.StatusCode);
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText, error.Trailers.GetValue("camus-error-code"));
        Assert.AreEqual(0, (await ReadDocsAsync(db)).Count);
    }
}
