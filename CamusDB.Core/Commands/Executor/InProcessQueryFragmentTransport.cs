/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text.Json;

using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// The in-process twin of <see cref="HttpQueryFragmentTransport"/>: it resolves the target node by
/// its Raft endpoint and runs the fragment on that node's <see cref="CommandExecutor"/>. It is what
/// a cluster whose members share one process — the cluster test harness, and the browser
/// playground — ships span fragments through.
///
/// <para><b>The wire round trip is deliberate.</b> Each request is encoded to UTF-8 JSON and
/// decoded again, and each returned frame goes through both codecs of
/// <see cref="QueryFragmentWireCodec"/>, binary first and then NDJSON. The peer therefore sees
/// exactly the request the HTTP transport would deliver, and the coordinator sees exactly the
/// frames the fragment controller would return. A shortcut that passed the objects straight
/// through would be faster and would leave both codecs untested by every cluster test, so an
/// encoding bug would only appear against a real network.</para>
///
/// <para>No retry, as the interface requires: a failure surfaces from the stream, and the
/// coordinator falls back to scanning the rest of the span itself.</para>
/// </summary>
public sealed class InProcessQueryFragmentTransport : IQueryFragmentTransport
{
    private readonly InProcessClusterNodes nodes;

    public InProcessQueryFragmentTransport(InProcessClusterNodes nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);

        this.nodes = nodes;
    }

    public async IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentAsync(
        string targetRaftEndpoint,
        QueryFragmentRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        CommandExecutor executor = nodes.Get(targetRaftEndpoint).Executor;

        byte[] requestJson = JsonSerializer.SerializeToUtf8Bytes(request, QueryFragmentJsonContext.Default.QueryFragmentRequest);
        QueryFragmentRequest decoded = JsonSerializer.Deserialize(requestJson, QueryFragmentJsonContext.Default.QueryFragmentRequest)
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "A fragment request did not survive its own encoding");

        await foreach (QueryFragmentRow row in executor.ExecuteQueryFragment(decoded, cancellationToken).ConfigureAwait(false))
            yield return RoundTripThroughWire(row, targetRaftEndpoint);
    }

    /// <summary>
    /// Encodes and decodes one frame through the binary codec and then through the NDJSON codec,
    /// and returns the twice-decoded frame. Either codec that drops or mangles a member — the row
    /// id, the bytes, the cells, the match indices, the stats — then shows up as a wrong query
    /// result in a cluster test.
    /// </summary>
    private static QueryFragmentRow RoundTripThroughWire(QueryFragmentRow row, string peer)
    {
        ArrayBufferWriter<byte> binary = new();
        QueryFragmentWireCodec.WriteBinaryFrame(binary, row);
        ReadOnlySequence<byte> binaryBytes = new(binary.WrittenMemory);

        if (!QueryFragmentWireCodec.TryReadBinaryFrame(ref binaryBytes, peer, out QueryFragmentWireFrame fromBinary)
            || !binaryBytes.IsEmpty || fromBinary.Row is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "A binary fragment frame did not round-trip");

        ArrayBufferWriter<byte> ndjson = new();
        using (Utf8JsonWriter writer = new(ndjson))
        {
            QueryFragmentWireCodec.WriteNdjsonFrame(writer, fromBinary.Row);
            writer.Flush();
        }

        QueryFragmentWireFrame fromNdjson = QueryFragmentWireCodec.ReadNdjsonFrame(new ReadOnlySequence<byte>(ndjson.WrittenMemory), peer);

        return fromNdjson.Row
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "An NDJSON fragment frame did not round-trip");
    }
}
