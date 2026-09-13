/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.App.Controllers;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Runs the real fragment endpoint (<see cref="QueryFragmentController"/>) over an engine with
/// data, captures the bytes it writes, and feeds them through the real
/// <see cref="HttpQueryFragmentTransport"/>. This is the only place both ends of the wire meet
/// in one test: the controller's negotiation and framing must produce exactly what the
/// transport's decoders expect, in both encodings, including the terminal stats frame.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestQueryFragmentControllerWire : BaseTest
{
    private ILogger<ICamusDB> Logger => logger;

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor, HashSet<string> LateRowIds);

    private async Task<Fixture> SetupRobots()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 6; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values:
                [
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(2000 + i)) },
                    }
                ]));
        }
        await database.Transactions.CommitAsync(txn);

        // A fragment ships the KV row id, which is not the `id` column value: learn the expected
        // ids from the engine's own scan of the same predicate.
        HashSet<string> late = new(StringComparer.Ordinal);
        txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: "SELECT * FROM robots WHERE year > 2002", parameters: null));
        await foreach (QueryResultRow row in cursor)
            late.Add(row.RowId.ToString());
        await database.Transactions.CommitAsync(txn);
        Assert.AreEqual(3, late.Count);

        return new Fixture(dbname, database, executor, late);
    }

    private static async Task<QueryFragmentRequest> BuildRequest(Fixture fx, bool wantStats = false, int? schemaVersion = null)
    {
        TableDescriptor table = await fx.Database.TableDescriptors["robots"];
        KvTransaction snapshot = await fx.Database.Transactions.BeginReadOnlyAsync(promote: false);
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year > 2002").extendedOne!;

        return new QueryFragmentRequest
        {
            FragmentId = Guid.NewGuid().ToString("n"),
            DatabaseName = fx.DbName,
            DatabaseId = fx.Database.Id,
            TableName = "robots",
            TableId = table.Id,
            SchemaVersion = schemaVersion ?? table.Schema.Version,
            ReadTsNode = snapshot.ReadTimestamp.N,
            ReadTsPhysical = snapshot.ReadTimestamp.L,
            ReadTsCounter = snapshot.ReadTimestamp.C,
            FilterJson = NodeAstWireCodec.Serialize(where),
            WantStats = wantStats,
        };
    }

    /// <summary>Invokes the controller action directly and returns the status, content type, and body bytes it produced.</summary>
    private async Task<(int Status, string? ContentType, byte[] Body)> Invoke(Fixture fx, QueryFragmentRequest request, string? accept)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(request);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        if (accept is not null)
            http.Request.Headers.Accept = accept;
        MemoryStream body = new();
        http.Response.Body = body;

        QueryFragmentController controller = new(fx.Executor, new HttpTransactionCoordinator(fx.Executor), Logger, Options)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };

        await controller.ExecuteFragment();
        await http.Response.BodyWriter.CompleteAsync();

        return (http.Response.StatusCode, http.Response.ContentType, body.ToArray());
    }

    /// <summary>Feeds captured controller output through the production transport.</summary>
    private static async Task<List<QueryFragmentRow>> ReadThroughTransport(byte[] body, string? contentType, int chunk)
    {
        CannedHandler handler = new(body, contentType, chunk);
        PeerEndpointResolver resolver = new(["peer:2070"], ["http://peer:5095"], 5095, false, NullLogger<ICamusDB>.Instance);
        HttpQueryFragmentTransport transport = new(new HttpClient(handler), resolver, "secret");

        List<QueryFragmentRow> rows = [];
        await foreach (QueryFragmentRow row in transport.ExecuteFragmentAsync("peer:2070", new QueryFragmentRequest(), default))
            rows.Add(row);
        return rows;
    }

    private static void AssertSurvivors(Fixture fx, List<QueryFragmentRow> rows, bool expectStats)
    {
        List<QueryFragmentRow> dataRows = rows.Where(r => r.Stats is null).ToList();
        Assert.AreEqual(3, dataRows.Count, "year > 2002 keeps three of six rows");
        Assert.That(dataRows.Select(r => r.RowIdHex!).ToHashSet(StringComparer.Ordinal), Is.EquivalentTo(fx.LateRowIds));
        Assert.IsTrue(dataRows.All(r => r.Data is { Length: > 0 }), "every survivor ships its raw row bytes");
        Assert.IsTrue(dataRows.All(r => r.CellsJson is null && r.MatchIndices is null));

        if (expectStats)
        {
            Assert.IsNotNull(rows[^1].Stats, "stats frame is terminal");
            Assert.AreEqual(3, rows[^1].Stats!.RowsShipped);
            Assert.AreEqual(6, rows[^1].Stats!.RowsScanned);
            Assert.AreEqual(1, rows.Count(r => r.Stats is not null));
        }
        else
        {
            Assert.IsTrue(rows.All(r => r.Stats is null));
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task BinaryNegotiated_ControllerOutputIsReadByTheTransport(bool wantStats)
    {
        Fixture fx = await SetupRobots();
        QueryFragmentRequest request = await BuildRequest(fx, wantStats);

        (int status, string? contentType, byte[] body) = await Invoke(fx, request,
            accept: $"{QueryFragmentWireCodec.BinaryContentType}, {QueryFragmentWireCodec.NdjsonContentType}");

        Assert.AreEqual(200, status);
        Assert.AreEqual(QueryFragmentWireCodec.BinaryContentType, contentType);
        Assert.AreEqual(1, body[0], "first byte is a row-frame kind, not '{'");

        AssertSurvivors(fx, await ReadThroughTransport(body, contentType, chunk: 3), wantStats);
        AssertSurvivors(fx, await ReadThroughTransport(body, contentType, chunk: int.MaxValue), wantStats);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task NoAcceptHeader_FallsBackToNdjson_ReadByTheTransport(bool wantStats)
    {
        Fixture fx = await SetupRobots();
        QueryFragmentRequest request = await BuildRequest(fx, wantStats);

        (int status, string? contentType, byte[] body) = await Invoke(fx, request, accept: null);

        Assert.AreEqual(200, status);
        Assert.AreEqual(QueryFragmentWireCodec.NdjsonContentType, contentType);

        string text = Encoding.UTF8.GetString(body);
        Assert.AreEqual(wantStats ? 4 : 3, text.Count(c => c == '\n'), "one line per frame, newline-terminated");
        StringAssert.StartsWith("{\"RowIdHex\":\"", text, "field names are the legacy contract");
        StringAssert.DoesNotContain("null", text, "nulls are omitted; older readers tolerate a missing property");

        AssertSurvivors(fx, await ReadThroughTransport(body, contentType, chunk: 3), wantStats);
    }

    [Test]
    public async Task AcceptWithZeroQualityBinary_FallsBackToNdjson()
    {
        Fixture fx = await SetupRobots();
        QueryFragmentRequest request = await BuildRequest(fx);

        (int status, string? contentType, byte[] body) = await Invoke(fx, request,
            accept: $"{QueryFragmentWireCodec.BinaryContentType};q=0, {QueryFragmentWireCodec.NdjsonContentType}");

        Assert.AreEqual(200, status);
        Assert.AreEqual(QueryFragmentWireCodec.NdjsonContentType, contentType);
        AssertSurvivors(fx, await ReadThroughTransport(body, contentType, chunk: 64), expectStats: false);
    }

    [Test]
    public async Task AcceptWithUnrelatedMediaTypes_FallsBackToNdjson()
    {
        Fixture fx = await SetupRobots();
        QueryFragmentRequest request = await BuildRequest(fx);

        (int status, string? contentType, byte[] body) = await Invoke(fx, request, accept: "application/json, */*");

        Assert.AreEqual(200, status);
        Assert.AreEqual(QueryFragmentWireCodec.NdjsonContentType, contentType);
        AssertSurvivors(fx, await ReadThroughTransport(body, contentType, chunk: 64), expectStats: false);
    }

    [Test]
    public async Task NdjsonOutput_IsReadableByAnOlderClientDeserializer()
    {
        Fixture fx = await SetupRobots();
        QueryFragmentRequest request = await BuildRequest(fx, wantStats: true);

        (_, _, byte[] body) = await Invoke(fx, request, accept: null);

        // The exact DTO an older transport deserializes each line into.
        List<LegacyWireLine> lines = Encoding.UTF8.GetString(body)
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonSerializer.Deserialize<LegacyWireLine>(l)!)
            .ToList();

        Assert.AreEqual(4, lines.Count);
        Assert.That(lines.Take(3).Select(l => l.RowIdHex!).ToHashSet(StringComparer.Ordinal), Is.EquivalentTo(fx.LateRowIds));
        Assert.IsTrue(lines.Take(3).All(l => l.Data is { Length: > 0 } && l.Error is null && l.Stats is null));
        Assert.AreEqual(new QueryFragmentScanStats(6, 3), lines[3].Stats);
    }

    [Test]
    public async Task FailureBeforeFirstRow_IsAPlain500_InEitherEncoding()
    {
        Fixture fx = await SetupRobots();
        QueryFragmentRequest request = await BuildRequest(fx, schemaVersion: 999);

        foreach (string? accept in new[] { QueryFragmentWireCodec.BinaryContentType, null })
        {
            (int status, _, byte[] body) = await Invoke(fx, request, accept);

            Assert.AreEqual(500, status);
            StringAssert.Contains("schema version mismatch", Encoding.UTF8.GetString(body));
        }
    }

    [Test]
    public async Task UnreadableRequestBody_Is400()
    {
        Fixture fx = await SetupRobots();

        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream("{not json"u8.ToArray());
        http.Response.Body = new MemoryStream();

        QueryFragmentController controller = new(fx.Executor, new HttpTransactionCoordinator(fx.Executor), Logger, Options)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };

        await controller.ExecuteFragment();

        Assert.AreEqual(400, http.Response.StatusCode);
    }

    // ── fakes ────────────────────────────────────────────────────────────────

    private sealed class LegacyWireLine
    {
        public string? RowIdHex { get; set; }
        public byte[]? Data { get; set; }
        public string? Cells { get; set; }
        public int[]? Matches { get; set; }
        public QueryFragmentScanStats? Stats { get; set; }
        public string? Error { get; set; }
    }

    private sealed class CannedHandler(byte[] body, string? contentType, int chunk) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            StreamContent content = new(new ChunkedReadStream(body, chunk));
            if (contentType is not null)
                content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
        }
    }

    private sealed class ChunkedReadStream(byte[] data, int chunk) : Stream
    {
        private int position;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => data.Length;
        public override long Position { get => position; set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int n = Math.Min(Math.Min(chunk, count), data.Length - position);
            Array.Copy(data, position, buffer, offset, n);
            position += n;
            return n;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int n = Math.Min(Math.Min(chunk, buffer.Length), data.Length - position);
            data.AsSpan(position, n).CopyTo(buffer.Span);
            position += n;
            return ValueTask.FromResult(n);
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
