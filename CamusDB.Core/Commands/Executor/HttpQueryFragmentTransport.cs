/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.IO.Pipelines;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Config;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Production <see cref="IQueryFragmentTransport"/>: POSTs the fragment request to the target
/// node's <c>/internal/query-fragment</c> endpoint (node-secret authenticated, exactly like
/// the settings forwarder) and streams the response frames. No transparent retries — the
/// coordinator owns retry/fallback, and a fragment is not idempotent mid-stream. The
/// <see cref="HttpClient"/> must have an infinite timeout: a fragment stream lives as long as
/// the scan it feeds, and cancellation (which aborts the request, and with it the remote
/// execution) is the only legitimate way to end it early.
///
/// <para>Wire format is negotiated per request (see <see cref="QueryFragmentWireCodec"/>): the
/// request advertises the binary encoding in <c>Accept</c> and the response's
/// <c>Content-Type</c> decides which decoder runs. A peer on an older build answers with NDJSON,
/// which is always accepted. Both decoders work on the UTF-8 bytes of the response through a
/// <see cref="PipeReader"/>: no per-line string, no per-frame DTO, and row bytes are copied
/// exactly once into the array the coordinator decodes.</para>
/// </summary>
public sealed class HttpQueryFragmentTransport : IQueryFragmentTransport
{
    private static readonly MediaTypeWithQualityHeaderValue AcceptBinary = new(QueryFragmentWireCodec.BinaryContentType);

    private static readonly MediaTypeWithQualityHeaderValue AcceptNdjson = new(QueryFragmentWireCodec.NdjsonContentType);

    private static readonly MediaTypeHeaderValue JsonContentType = new("application/json") { CharSet = "utf-8" };

    /// <summary>
    /// Read buffer for the response pipe. Larger than the pipe default so a 64 KiB row rarely
    /// spans more than two segments; the codec copes with any segmentation regardless.
    /// </summary>
    private const int ReadBufferSize = 64 * 1024;

    private readonly HttpClient httpClient;

    private readonly PeerEndpointResolver resolver;

    private readonly string? nodeSecret;

    public HttpQueryFragmentTransport(HttpClient httpClient, PeerEndpointResolver resolver, string? nodeSecret)
    {
        httpClient.Timeout = Timeout.InfiniteTimeSpan;
        this.httpClient = httpClient;
        this.resolver = resolver;
        this.nodeSecret = nodeSecret;
    }

    public async IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentAsync(
        string targetRaftEndpoint,
        QueryFragmentRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Uri target = new(resolver.Resolve(targetRaftEndpoint), "/internal/query-fragment");

        // Serialize straight to UTF-8: the request carries the serialized filter and, for a
        // broadcast join, every build row, so the UTF-16 detour of a string body is not free.
        ByteArrayContent content = new(JsonSerializer.SerializeToUtf8Bytes(request));
        content.Headers.ContentType = JsonContentType;

        using HttpRequestMessage message = new(HttpMethod.Post, target) { Content = content };
        message.Headers.Accept.Add(AcceptBinary);
        message.Headers.Accept.Add(AcceptNdjson);

        if (!string.IsNullOrEmpty(nodeSecret))
            message.Headers.TryAddWithoutValidation("X-Camus-Node-Secret", nodeSecret);

        using HttpResponseMessage response = await httpClient
            .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            string body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Query fragment on '{targetRaftEndpoint}' failed with HTTP {(int)response.StatusCode}: {body}");
        }

        bool binary = string.Equals(
            response.Content.Headers.ContentType?.MediaType,
            QueryFragmentWireCodec.BinaryContentType,
            StringComparison.OrdinalIgnoreCase);

        await using Stream stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        PipeReader pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: ReadBufferSize, leaveOpen: true));

        try
        {
            while (true)
            {
                ReadResult result = await pipe.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                // Frames are yielded while the pipe buffer is still held; that is allowed as long
                // as AdvanceTo runs before the next ReadAsync, and every yielded row owns its bytes.
                while (TryReadFrame(ref buffer, binary, targetRaftEndpoint, out QueryFragmentWireFrame frame))
                {
                    if (frame.Error is not null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            $"Query fragment on '{targetRaftEndpoint}' failed mid-stream: {frame.Error}");

                    yield return frame.Row!;
                }

                if (!result.IsCompleted)
                {
                    pipe.AdvanceTo(buffer.Start, buffer.End);
                    continue;
                }

                // End of stream. Whatever is left has no complete frame in it.
                QueryFragmentWireFrame? last = null;

                if (!buffer.IsEmpty && !IsBlank(buffer))
                {
                    // A binary stream that ends inside a frame was cut off. An NDJSON stream may
                    // legitimately end without a trailing newline: parse the remainder as the
                    // final line (a truncated line fails as unreadable).
                    if (binary)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            $"Query fragment on '{targetRaftEndpoint}' ended mid-frame");

                    last = QueryFragmentWireCodec.ReadNdjsonFrame(buffer, targetRaftEndpoint);
                }

                pipe.AdvanceTo(buffer.End);

                if (last is { } lastFrame)
                {
                    if (lastFrame.Error is not null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            $"Query fragment on '{targetRaftEndpoint}' failed mid-stream: {lastFrame.Error}");

                    yield return lastFrame.Row!;
                }

                break;
            }
        }
        finally
        {
            await pipe.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Pulls one complete frame off the front of <paramref name="buffer"/> in the negotiated
    /// encoding. NDJSON frames end at a newline (blank lines are skipped); binary frames are
    /// self-delimiting. Returns false when the buffer holds no complete frame yet.
    /// </summary>
    private static bool TryReadFrame(ref ReadOnlySequence<byte> buffer, bool binary, string peer, out QueryFragmentWireFrame frame)
    {
        if (binary)
            return QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, peer, out frame);

        frame = default;
        SequenceReader<byte> reader = new(buffer);

        while (reader.TryReadTo(out ReadOnlySequence<byte> line, (byte)'\n', advancePastDelimiter: true))
        {
            buffer = buffer.Slice(reader.Position);

            if (IsBlank(line))
                continue;

            frame = QueryFragmentWireCodec.ReadNdjsonFrame(line, peer);
            return true;
        }

        return false;
    }

    private static bool IsBlank(in ReadOnlySequence<byte> line)
    {
        if (line.IsEmpty)
            return true;

        SequenceReader<byte> reader = new(line);
        reader.AdvancePastAny((byte)'\r', (byte)' ', (byte)'\t');
        return reader.End;
    }
}
