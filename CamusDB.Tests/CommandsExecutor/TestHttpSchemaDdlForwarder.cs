/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
[NonParallelizable]
public sealed class TestHttpSchemaDdlForwarder
{
    private static readonly ILogger<ICamusDB> Logger = NullLogger<ICamusDB>.Instance;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private static HttpSchemaDdlForwarder BuildForwarder(FakeMessageHandler handler, Uri? resolvedUri = null)
    {
        Uri baseUri = resolvedUri ?? new Uri("http://fake-leader:5095");
        HttpClient client = new(handler);
        return new HttpSchemaDdlForwarder(client, _ => baseUri, Logger);
    }

    private static StringContent OkResponse(bool applied) =>
        new(JsonSerializer.Serialize(
            new SchemaDdlForwardResponse { Status = "ok", Applied = applied },
            JsonOptions),
            Encoding.UTF8, "application/json");

    private static StringContent NotLeaderResponse() =>
        new(JsonSerializer.Serialize(
            new SchemaDdlForwardResponse { Status = "not-leader" },
            JsonOptions),
            Encoding.UTF8, "application/json");

    private static StringContent FailedResponse(string code, string message) =>
        new(JsonSerializer.Serialize(
            new SchemaDdlForwardResponse { Status = "failed", Code = code, Message = message },
            JsonOptions),
            Encoding.UTF8, "application/json");

    // ── ForwardCreateTableAsync ────────────────────────────────────────────────

    [Test]
    public async Task ForwardCreateTable_PostsToCorrectUrl()
    {
        Uri baseUri = new("http://leader-node:5095");
        FakeMessageHandler handler = new(OkResponse(true));

        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler, baseUri);

        CreateTableTicket ticket = new(
            databaseName: "mydb",
            tableName: "robots",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints: [],
            ifNotExists: false
        );

        await forwarder.ForwardCreateTableAsync("leader-node:7070", ticket, CancellationToken.None);

        Assert.AreEqual(HttpMethod.Post, handler.LastRequest!.Method);
        Assert.AreEqual("http://leader-node:5095/internal/schema-ddl/create-table", handler.LastRequest.RequestUri!.ToString());
    }

    [Test]
    public async Task ForwardCreateTable_IncludesOperationIdInBody()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new(
            databaseName: "mydb",
            tableName: "robots",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints: [],
            ifNotExists: false
        );

        await forwarder.ForwardCreateTableAsync("leader:7070", ticket, CancellationToken.None);

        string body = await handler.LastRequest!.Content!.ReadAsStringAsync();
        using JsonDocument doc = JsonDocument.Parse(body);
        string operationId = doc.RootElement.GetProperty("operationId").GetString()!;

        Assert.IsFalse(string.IsNullOrEmpty(operationId), "operationId must be present in request body");
        Assert.AreEqual(32, operationId.Length, "operationId must be a 32-char hex GUID (no dashes)");
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsTrueWhenApplied()
    {
        FakeMessageHandler handler = new(OkResponse(applied: true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, CancellationToken.None);

        Assert.AreEqual(true, result);
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsFalseWhenNotApplied()
    {
        FakeMessageHandler handler = new(OkResponse(applied: false));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], ifNotExists: true);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, CancellationToken.None);

        Assert.AreEqual(false, result);
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsNullWhenNotLeader()
    {
        FakeMessageHandler handler = new(NotLeaderResponse(), HttpStatusCode.ServiceUnavailable);
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, CancellationToken.None);

        Assert.IsNull(result);
    }

    [Test]
    public void ForwardCreateTable_ThrowsCamusDBExceptionOnFailed()
    {
        FakeMessageHandler handler = new(FailedResponse("CA0042", "Table already exists"), HttpStatusCode.InternalServerError);
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await forwarder.ForwardCreateTableAsync("leader:7070", ticket, CancellationToken.None));

        Assert.NotNull(ex);
        Assert.AreEqual("CA0042", ex!.Code);
        Assert.That(ex.Message, Does.Contain("Table already exists"));
    }

    // ── ForwardAlterTableAsync ─────────────────────────────────────────────────

    [Test]
    public async Task ForwardAlterTable_PostsToCorrectUrl()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterTableTicket ticket = new("mydb", "robots", AlterTableOperation.AddColumn, new ColumnInfo("year", ColumnType.Integer64));
        await forwarder.ForwardAlterTableAsync("leader:7070", ticket, CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.EndWith("/internal/schema-ddl/alter-table"));
    }

    [Test]
    public async Task ForwardAlterTable_SerializesColumnCorrectly()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterTableTicket ticket = new("mydb", "robots", AlterTableOperation.AddColumn, new ColumnInfo("year", ColumnType.Integer64, notNull: true));
        await forwarder.ForwardAlterTableAsync("leader:7070", ticket, CancellationToken.None);

        string body = await handler.LastRequest!.Content!.ReadAsStringAsync();
        using JsonDocument doc = JsonDocument.Parse(body);
        JsonElement column = doc.RootElement.GetProperty("column");

        Assert.AreEqual("year", column.GetProperty("name").GetString());
        Assert.IsTrue(column.GetProperty("notNull").GetBoolean());
    }

    // ── ForwardAlterIndexAsync ─────────────────────────────────────────────────

    [Test]
    public async Task ForwardAlterIndex_PostsToCorrectUrl()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterIndexTicket ticket = new("mydb", "robots", "name_idx",
            [new ColumnIndexInfo("name", OrderType.Ascending)],
            AlterIndexOperation.AddIndex);

        await forwarder.ForwardAlterIndexAsync("leader:7070", ticket, CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.EndWith("/internal/schema-ddl/alter-index"));
    }

    [Test]
    public async Task ForwardAlterIndex_SerializesIndexNameAndColumns()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterIndexTicket ticket = new("mydb", "robots", "name_idx",
            [new ColumnIndexInfo("name", OrderType.Ascending)],
            AlterIndexOperation.AddUniqueIndex);

        await forwarder.ForwardAlterIndexAsync("leader:7070", ticket, CancellationToken.None);

        string body = await handler.LastRequest!.Content!.ReadAsStringAsync();
        using JsonDocument doc = JsonDocument.Parse(body);

        Assert.AreEqual("name_idx", doc.RootElement.GetProperty("indexName").GetString());
        Assert.AreEqual(1, doc.RootElement.GetProperty("columns").GetArrayLength());
        Assert.AreEqual("name", doc.RootElement.GetProperty("columns")[0].GetProperty("name").GetString());
    }

    // ── ForwardDropTableAsync ──────────────────────────────────────────────────

    [Test]
    public async Task ForwardDropTable_PostsToCorrectUrl()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        DropTableTicket ticket = new("mydb", "robots", ifExists: false);
        await forwarder.ForwardDropTableAsync("leader:7070", ticket, CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.EndWith("/internal/schema-ddl/drop-table"));
    }

    // ── FakeMessageHandler ────────────────────────────────────────────────────

    private sealed class FakeMessageHandler : HttpMessageHandler
    {
        private readonly HttpContent responseContent;
        private readonly HttpStatusCode statusCode;

        public HttpRequestMessage? LastRequest { get; private set; }

        public FakeMessageHandler(HttpContent content, HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            this.responseContent = content;
            this.statusCode = statusCode;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            return Task.FromResult(new HttpResponseMessage(statusCode) { Content = responseContent });
        }
    }
}
