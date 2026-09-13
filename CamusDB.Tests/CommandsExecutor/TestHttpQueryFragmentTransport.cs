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
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Config;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Drives <see cref="HttpQueryFragmentTransport"/> against a fake HTTP handler so the request
/// it sends and the two response encodings it reads are asserted at the wire level: the
/// negotiation headers, the UTF-8 request body, binary and NDJSON frame streams delivered in
/// arbitrary chunk sizes, mid-stream error frames, truncation, and non-2xx responses.
/// </summary>
[TestFixture]
public sealed class TestHttpQueryFragmentTransport
{
    private const string Endpoint = "peer-b:2070";

    private const string RowId = "6849f3a1c2e7d50b4f8a91d3";

    private static readonly Uri BaseUri = new("http://peer-b:5095");

    private static PeerEndpointResolver Resolver() =>
        new([Endpoint], [BaseUri.ToString()], httpPort: 5095, peerTlsEnabled: false, NullLogger<ICamusDB>.Instance);

    private static QueryFragmentRequest SampleRequest() => new()
    {
        FragmentId = "frag-1",
        DatabaseName = "db",
        DatabaseId = "db-id",
        TableName = "t",
        TableId = "A0",
        SchemaVersion = 3,
        FilterJson = "{\"nodeType\":1}",
        ReadTsPhysical = 12345,
        MaxSurvivors = 10,
        RequiredColumns = ["a", "b"],
    };

    private static byte[] Payload(int size)
    {
        byte[] payload = new byte[size];
        for (int i = 0; i < size; i++)
            payload[i] = (byte)((i * 13 + 1) & 0xFF);
        return payload;
    }

    private static List<QueryFragmentRow> SampleRows() =>
    [
        new(RowId, Payload(128)),
        new(RowId, Payload(4096), MatchIndices: [0, 2]),
        new(null, null, "{\"count\":{\"t\":3,\"v\":\"2\"}}"),
        new(RowId, Payload(65536)),
        new(null, null, null, new QueryFragmentScanStats(40, 3)),
    ];

    private static byte[] EncodeBinary(IEnumerable<QueryFragmentRow> rows, string? trailingError = null)
    {
        ArrayBufferWriter<byte> output = new();
        foreach (QueryFragmentRow row in rows)
            QueryFragmentWireCodec.WriteBinaryFrame(output, row);
        if (trailingError is not null)
            QueryFragmentWireCodec.WriteBinaryError(output, trailingError);
        return output.WrittenSpan.ToArray();
    }

    private static byte[] EncodeNdjson(IEnumerable<QueryFragmentRow> rows, string? trailingError = null, bool trailingNewline = true)
    {
        ArrayBufferWriter<byte> output = new();
        using Utf8JsonWriter writer = new(output);
        List<QueryFragmentRow> list = rows.ToList();

        for (int i = 0; i < list.Count; i++)
        {
            QueryFragmentWireCodec.WriteNdjsonFrame(writer, list[i]);
            writer.Flush();
            writer.Reset();
            if (i < list.Count - 1 || trailingError is not null || trailingNewline)
                output.Write("\n"u8);
        }

        if (trailingError is not null)
        {
            QueryFragmentWireCodec.WriteNdjsonError(writer, trailingError);
            writer.Flush();
            writer.Reset();
            if (trailingNewline)
                output.Write("\n"u8);
        }

        return output.WrittenSpan.ToArray();
    }

    private static void AssertSameRows(IReadOnlyList<QueryFragmentRow> expected, IReadOnlyList<QueryFragmentRow> actual)
    {
        Assert.AreEqual(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.AreEqual(expected[i].RowIdHex, actual[i].RowIdHex, $"row {i}");
            Assert.AreEqual(expected[i].Data, actual[i].Data, $"row {i}");
            Assert.AreEqual(expected[i].CellsJson, actual[i].CellsJson, $"row {i}");
            Assert.AreEqual(expected[i].MatchIndices, actual[i].MatchIndices, $"row {i}");
            Assert.AreEqual(expected[i].Stats, actual[i].Stats, $"row {i}");
        }
    }

    private static async Task<List<QueryFragmentRow>> Collect(HttpQueryFragmentTransport transport, CancellationToken ct = default)
    {
        List<QueryFragmentRow> rows = [];
        await foreach (QueryFragmentRow row in transport.ExecuteFragmentAsync(Endpoint, SampleRequest(), ct))
            rows.Add(row);
        return rows;
    }

    private static HttpQueryFragmentTransport Transport(FakeHandler handler, string? secret = "s3cret") =>
        new(new HttpClient(handler), Resolver(), secret);

    // ── Request shape ────────────────────────────────────────────────────────

    [Test]
    public async Task Request_AdvertisesBothEncodings_AndSendsUtf8Json()
    {
        FakeHandler handler = new(EncodeBinary([]), QueryFragmentWireCodec.BinaryContentType);

        List<QueryFragmentRow> rows = await Collect(Transport(handler));
        Assert.IsEmpty(rows);

        HttpRequestMessage sent = handler.LastRequest!;
        Assert.AreEqual(HttpMethod.Post, sent.Method);
        Assert.AreEqual(new Uri(BaseUri, "/internal/query-fragment"), sent.RequestUri);
        Assert.AreEqual("s3cret", sent.Headers.GetValues("X-Camus-Node-Secret").Single());

        string[] accept = sent.Headers.Accept.Select(a => a.MediaType!).ToArray();
        Assert.AreEqual(QueryFragmentWireCodec.BinaryContentType, accept[0], "binary preferred");
        Assert.Contains(QueryFragmentWireCodec.NdjsonContentType, accept, "NDJSON always acceptable");

        Assert.AreEqual("application/json", handler.LastRequestContentType!.MediaType);
        QueryFragmentRequest? decoded = JsonSerializer.Deserialize<QueryFragmentRequest>(handler.LastRequestBody!);
        Assert.AreEqual("frag-1", decoded!.FragmentId);
        Assert.AreEqual("A0", decoded.TableId);
        Assert.AreEqual(3, decoded.SchemaVersion);
        Assert.AreEqual(new[] { "a", "b" }, decoded.RequiredColumns);
        Assert.AreEqual(12345, decoded.ReadTsPhysical);
    }

    [Test]
    public async Task Request_WithoutNodeSecret_SendsNoSecretHeader()
    {
        FakeHandler handler = new(EncodeBinary([]), QueryFragmentWireCodec.BinaryContentType);
        await Collect(Transport(handler, secret: null));
        Assert.IsFalse(handler.LastRequest!.Headers.Contains("X-Camus-Node-Secret"));
    }

    // ── Response decoding ────────────────────────────────────────────────────

    [TestCase(1)]
    [TestCase(7)]
    [TestCase(1000)]
    [TestCase(1 << 20)]
    public async Task BinaryResponse_DecodesEveryFrame_AtAnyChunkSize(int chunk)
    {
        List<QueryFragmentRow> expected = SampleRows();
        FakeHandler handler = new(EncodeBinary(expected), QueryFragmentWireCodec.BinaryContentType, chunk);

        AssertSameRows(expected, await Collect(Transport(handler)));
    }

    [TestCase(1)]
    [TestCase(7)]
    [TestCase(1000)]
    [TestCase(1 << 20)]
    public async Task NdjsonResponse_DecodesEveryFrame_AtAnyChunkSize(int chunk)
    {
        List<QueryFragmentRow> expected = SampleRows();
        FakeHandler handler = new(EncodeNdjson(expected), QueryFragmentWireCodec.NdjsonContentType, chunk);

        AssertSameRows(expected, await Collect(Transport(handler)));
    }

    [Test]
    public async Task OlderServer_ThatIgnoresAccept_IsReadAsNdjson()
    {
        // An older build never negotiates: it answers NDJSON with every property present.
        List<QueryFragmentRow> expected = SampleRows();
        StringBuilder legacy = new();
        foreach (QueryFragmentRow row in expected)
            legacy.Append(JsonSerializer.Serialize(new
            {
                row.RowIdHex, row.Data, Cells = row.CellsJson, Matches = row.MatchIndices, row.Stats, Error = (string?)null,
            })).Append('\n');

        FakeHandler handler = new(Encoding.UTF8.GetBytes(legacy.ToString()), QueryFragmentWireCodec.NdjsonContentType, chunk: 33);

        AssertSameRows(expected, await Collect(Transport(handler)));
    }

    [Test]
    public async Task MissingContentType_IsReadAsNdjson()
    {
        List<QueryFragmentRow> expected = SampleRows();
        FakeHandler handler = new(EncodeNdjson(expected), contentType: null);

        AssertSameRows(expected, await Collect(Transport(handler)));
    }

    [Test]
    public async Task Ndjson_BlankLinesAndCrLf_AreTolerated()
    {
        List<QueryFragmentRow> expected = SampleRows();
        string text = Encoding.UTF8.GetString(EncodeNdjson(expected)).Replace("\n", "\r\n\r\n   \r\n");
        FakeHandler handler = new(Encoding.UTF8.GetBytes(text), QueryFragmentWireCodec.NdjsonContentType, chunk: 5);

        AssertSameRows(expected, await Collect(Transport(handler)));
    }

    [Test]
    public async Task Ndjson_FinalLineWithoutNewline_IsStillDelivered()
    {
        List<QueryFragmentRow> expected = SampleRows();
        FakeHandler handler = new(EncodeNdjson(expected, trailingNewline: false), QueryFragmentWireCodec.NdjsonContentType, chunk: 11);

        AssertSameRows(expected, await Collect(Transport(handler)));
    }

    // ── Failure paths ────────────────────────────────────────────────────────

    [Test]
    public void MidStreamErrorFrame_Binary_ThrowsAfterDeliveringEarlierRows()
    {
        List<QueryFragmentRow> expected = SampleRows();
        FakeHandler handler = new(EncodeBinary(expected, "remote scan aborted"), QueryFragmentWireCodec.BinaryContentType, chunk: 100);

        List<QueryFragmentRow> delivered = [];
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await foreach (QueryFragmentRow row in Transport(handler).ExecuteFragmentAsync(Endpoint, SampleRequest(), default))
                delivered.Add(row);
        })!;

        StringAssert.Contains("failed mid-stream: remote scan aborted", ex.Message);
        StringAssert.Contains(Endpoint, ex.Message);
        AssertSameRows(expected, delivered);
    }

    [Test]
    public void MidStreamErrorFrame_Ndjson_ThrowsAfterDeliveringEarlierRows()
    {
        List<QueryFragmentRow> expected = SampleRows();
        FakeHandler handler = new(EncodeNdjson(expected, "remote scan aborted"), QueryFragmentWireCodec.NdjsonContentType, chunk: 100);

        List<QueryFragmentRow> delivered = [];
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await foreach (QueryFragmentRow row in Transport(handler).ExecuteFragmentAsync(Endpoint, SampleRequest(), default))
                delivered.Add(row);
        })!;

        StringAssert.Contains("failed mid-stream: remote scan aborted", ex.Message);
        AssertSameRows(expected, delivered);
    }

    [Test]
    public void ErrorFrameWithoutTrailingNewline_Ndjson_StillThrows()
    {
        FakeHandler handler = new(EncodeNdjson([SampleRows()[0]], "late failure", trailingNewline: false), QueryFragmentWireCodec.NdjsonContentType);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Collect(Transport(handler)))!;
        StringAssert.Contains("late failure", ex.Message);
    }

    [Test]
    public void TruncatedBinaryStream_FailsAsMidFrame()
    {
        byte[] full = EncodeBinary(SampleRows());
        FakeHandler handler = new(full.AsSpan(0, full.Length - 10).ToArray(), QueryFragmentWireCodec.BinaryContentType, chunk: 64);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Collect(Transport(handler)))!;
        StringAssert.Contains("ended mid-frame", ex.Message);
    }

    [Test]
    public void TruncatedNdjsonStream_FailsAsUnreadable()
    {
        byte[] full = EncodeNdjson(SampleRows());
        FakeHandler handler = new(full.AsSpan(0, full.Length - 10).ToArray(), QueryFragmentWireCodec.NdjsonContentType, chunk: 64);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Collect(Transport(handler)))!;
        StringAssert.Contains("unreadable frame", ex.Message);
    }

    [Test]
    public void NonSuccessStatus_ThrowsWithBody()
    {
        FakeHandler handler = new(Encoding.UTF8.GetBytes("schema version mismatch"), "text/plain", status: HttpStatusCode.InternalServerError);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Collect(Transport(handler)))!;
        StringAssert.Contains("HTTP 500", ex.Message);
        StringAssert.Contains("schema version mismatch", ex.Message);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex.Code);
    }

    [Test]
    public void Cancellation_AbortsTheStream()
    {
        using CancellationTokenSource cts = new();
        FakeHandler handler = new(EncodeBinary(SampleRows()), QueryFragmentWireCodec.BinaryContentType, chunk: 1, cancelAfterFirstRead: cts);

        Assert.CatchAsync<OperationCanceledException>(async () => await Collect(Transport(handler), cts.Token));
    }

    // ── fakes ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Serves one canned response body, delivered <paramref name="chunk"/> bytes per read so the
    /// transport's pipe sees frames split at arbitrary points. Captures the request body and
    /// headers for assertions.
    /// </summary>
    private sealed class FakeHandler : HttpMessageHandler
    {
        private readonly byte[] body;
        private readonly string? contentType;
        private readonly int chunk;
        private readonly HttpStatusCode status;
        private readonly CancellationTokenSource? cancelAfterFirstRead;

        public HttpRequestMessage? LastRequest { get; private set; }

        public byte[]? LastRequestBody { get; private set; }

        public MediaTypeHeaderValue? LastRequestContentType { get; private set; }

        public FakeHandler(byte[] body, string? contentType, int chunk = int.MaxValue,
            HttpStatusCode status = HttpStatusCode.OK, CancellationTokenSource? cancelAfterFirstRead = null)
        {
            this.body = body;
            this.contentType = contentType;
            this.chunk = chunk;
            this.status = status;
            this.cancelAfterFirstRead = cancelAfterFirstRead;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            LastRequestContentType = request.Content?.Headers.ContentType;
            LastRequestBody = request.Content is null ? null : await request.Content.ReadAsByteArrayAsync(cancellationToken);

            StreamContent content = new(new TrickleStream(body, chunk, cancelAfterFirstRead));
            if (contentType is not null)
                content.Headers.ContentType = new MediaTypeHeaderValue(contentType);

            return new HttpResponseMessage(status) { Content = content };
        }
    }

    /// <summary>Read-only stream that returns at most <c>chunk</c> bytes per read.</summary>
    private sealed class TrickleStream : Stream
    {
        private readonly byte[] data;
        private readonly int chunk;
        private readonly CancellationTokenSource? cancelAfterFirstRead;
        private int position;

        public TrickleStream(byte[] data, int chunk, CancellationTokenSource? cancelAfterFirstRead)
        {
            this.data = data;
            this.chunk = chunk;
            this.cancelAfterFirstRead = cancelAfterFirstRead;
        }

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
            cancelAfterFirstRead?.Cancel();
            return n;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int n = Math.Min(Math.Min(chunk, buffer.Length), data.Length - position);
            data.AsSpan(position, n).CopyTo(buffer.Span);
            position += n;
            cancelAfterFirstRead?.Cancel();
            return ValueTask.FromResult(n);
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
