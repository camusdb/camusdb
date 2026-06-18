/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
public sealed class TestHttpSchemaDdlForwarder
{
    private static readonly ILogger<ICamusDB> Logger = NullLogger<ICamusDB>.Instance;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private const string StableOperationId = "aabbccdd11223344aabbccdd11223344";

    private static HttpSchemaDdlForwarder BuildForwarder(HttpMessageHandler handler, Uri? resolvedUri = null)
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

        await forwarder.ForwardCreateTableAsync("leader-node:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.AreEqual(HttpMethod.Post, handler.LastRequest!.Method);
        Assert.AreEqual("http://leader-node:5095/internal/schema-ddl/create-table", handler.LastRequest.RequestUri!.ToString());
    }

    [Test]
    public async Task ForwardCreateTable_PropagatesCallerOperationId()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [new ColumnInfo("id", ColumnType.Id)], [], false);

        await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        string body = await handler.LastRequest!.Content!.ReadAsStringAsync();
        using JsonDocument doc = JsonDocument.Parse(body);
        string operationId = doc.RootElement.GetProperty("operationId").GetString()!;

        Assert.AreEqual(StableOperationId, operationId,
            "forwarder must use the caller-supplied operationId, not mint a new one");
    }

    [Test]
    public async Task ForwardCreateTable_SameOperationIdAcrossRetries()
    {
        // Simulate two not-leader responses then success. The handler collects
        // every operationId it receives so the test can verify they are all equal.
        MultiResponseHandler handler = new(
            NotLeaderResponse(), HttpStatusCode.ServiceUnavailable,
            NotLeaderResponse(), HttpStatusCode.ServiceUnavailable,
            OkResponse(true),   HttpStatusCode.OK
        );
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);
        CreateTableTicket ticket = new("mydb", "robots", [], [], false);

        // The caller holds the stable id (generated once in TryForwardDdlAsync).
        await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);
        await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);
        await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.AreEqual(3, handler.ReceivedOperationIds.Count, "expected 3 calls");
        Assert.IsTrue(
            handler.ReceivedOperationIds.TrueForAll(id => id == StableOperationId),
            "all retries must carry the same operationId"
        );
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsTrueWhenApplied()
    {
        FakeMessageHandler handler = new(OkResponse(applied: true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.AreEqual(true, result);
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsFalseWhenNotApplied()
    {
        FakeMessageHandler handler = new(OkResponse(applied: false));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], ifNotExists: true);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.AreEqual(false, result);
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsNullWhenNotLeader()
    {
        FakeMessageHandler handler = new(NotLeaderResponse(), HttpStatusCode.ServiceUnavailable);
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.IsNull(result);
    }

    [Test]
    public void ForwardCreateTable_ThrowsCamusDBExceptionOnFailed()
    {
        FakeMessageHandler handler = new(FailedResponse("CA0042", "Table already exists"), HttpStatusCode.InternalServerError);
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None));

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
        await forwarder.ForwardAlterTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.EndWith("/internal/schema-ddl/alter-table"));
    }

    [Test]
    public async Task ForwardAlterTable_SerializesColumnCorrectly()
    {
        FakeMessageHandler handler = new(OkResponse(true));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterTableTicket ticket = new("mydb", "robots", AlterTableOperation.AddColumn, new ColumnInfo("year", ColumnType.Integer64, notNull: true));
        await forwarder.ForwardAlterTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

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

        await forwarder.ForwardAlterIndexAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

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

        await forwarder.ForwardAlterIndexAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

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
        await forwarder.ForwardDropTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.EndWith("/internal/schema-ddl/drop-table"));
    }

    // ── Transport-failure → null ─────────────────────────────────────────

    [Test]
    public async Task ForwardCreateTable_ReturnsNullOnHttpRequestException()
    {
        // Connection refused / DNS failure / leader mid-election TCP reset.
        // The leader never saw the request so returning null is safe — lets
        // TryForwardDdlAsync elect a fresh leader and retry.
        ErrorMessageHandler handler = new(new HttpRequestException("Connection refused"));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [new ColumnInfo("id", ColumnType.Id)], [], false);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.IsNull(result, "HttpRequestException must be converted to null (retry signal)");
    }

    [Test]
    public async Task ForwardAlterTable_ReturnsNullOnHttpRequestException()
    {
        ErrorMessageHandler handler = new(new HttpRequestException("Network unreachable"));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterTableTicket ticket = new("mydb", "robots", AlterTableOperation.AddColumn, new ColumnInfo("year", ColumnType.Integer64));
        bool? result = await forwarder.ForwardAlterTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.IsNull(result);
    }

    [Test]
    public async Task ForwardAlterIndex_ReturnsNullOnHttpRequestException()
    {
        ErrorMessageHandler handler = new(new HttpRequestException("Network unreachable"));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        AlterIndexTicket ticket = new("mydb", "robots", "name_idx",
            [new ColumnIndexInfo("name", OrderType.Ascending)],
            AlterIndexOperation.AddIndex);
        bool? result = await forwarder.ForwardAlterIndexAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.IsNull(result);
    }

    [Test]
    public async Task ForwardDropTable_ReturnsNullOnHttpRequestException()
    {
        ErrorMessageHandler handler = new(new HttpRequestException("Connection refused"));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        DropTableTicket ticket = new("mydb", "robots", ifExists: false);
        bool? result = await forwarder.ForwardDropTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.IsNull(result);
    }

    [Test]
    public async Task ForwardCreateTable_ReturnsNullOnHttpClientTimeout()
    {
        // TaskCanceledException with no caller cancellation = HTTP-client timeout.
        ErrorMessageHandler handler = new(new TaskCanceledException("Request timed out"));
        HttpSchemaDdlForwarder forwarder = BuildForwarder(handler);

        CreateTableTicket ticket = new("mydb", "robots", [], [], false);
        bool? result = await forwarder.ForwardCreateTableAsync("leader:7070", ticket, StableOperationId, CancellationToken.None);

        Assert.IsNull(result, "HTTP-client timeout must be converted to null (retry signal)");
    }

    // ── FakeMessageHandler ─────────────────────────────────────────────────────

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

    // Throws the supplied exception from SendAsync to simulate transport failures.
    private sealed class ErrorMessageHandler(Exception exception) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromException<HttpResponseMessage>(exception);
    }

    // Cycles through a pre-set list of (content, statusCode) pairs and records
    // the operationId from each incoming request body.
    private sealed class MultiResponseHandler : HttpMessageHandler
    {
        private readonly (HttpContent Content, HttpStatusCode Status)[] responses;
        private int index;

        public List<string> ReceivedOperationIds { get; } = [];

        public MultiResponseHandler(params object[] alternating)
        {
            responses = new (HttpContent, HttpStatusCode)[alternating.Length / 2];
            for (int i = 0; i < alternating.Length; i += 2)
                responses[i / 2] = ((HttpContent)alternating[i], (HttpStatusCode)alternating[i + 1]);
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string body = await request.Content!.ReadAsStringAsync(cancellationToken);
            using JsonDocument doc = JsonDocument.Parse(body);
            ReceivedOperationIds.Add(doc.RootElement.GetProperty("operationId").GetString()!);

            var (content, status) = responses[index % responses.Length];
            index++;
            return new HttpResponseMessage(status) { Content = content };
        }
    }
}
