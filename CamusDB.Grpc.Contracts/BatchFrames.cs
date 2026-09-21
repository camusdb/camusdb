
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Google.Protobuf;

namespace CamusDB.Grpc;

/// <summary>
/// The contract of <c>BatchExecute</c> stream frames: one stream message that carries several ops
/// (request side) or several response messages (response side), so the fixed cost of a stream message
/// is paid once per frame instead of once per op or per row. A frame is a transport optimization only:
/// it gives no atomicity and no ordering that the stream does not already give.
///
/// <para><b>Negotiation is announced, never probed.</b> A server built before frames runs an unknown op
/// kind as a NON_QUERY, so a frame sent to it would be executed as SQL. The server therefore writes
/// <see cref="HeaderName"/> on the response headers of every stream it opens, and a client sends a
/// frame only on a stream where it saw that header. The server sends a response frame only on a stream
/// that already carried a request frame. Both decisions are per stream, so a rotated stream negotiates
/// again by itself and a rolling upgrade is safe in either order.</para>
///
/// <para><b>The limits are the sender's duty.</b> The transport rejects a message over its size limit
/// before the message is parsed, and that resets the stream every other op shares. A receiver cannot
/// refuse such a frame item by item, so <see cref="MaxBytes"/> sits far below the 4 MB default gRPC
/// message limit and every sender must stay inside it. The public client keeps a mirror of these
/// constants; the two must agree, as the two copies of <c>camus_sql.proto</c> must.</para>
/// </summary>
public static class BatchFrames
{
    /// <summary>
    /// Response header a server writes when a <c>BatchExecute</c> stream opens to say it reads request
    /// frames. Its value is the highest contract version the server reads.
    /// </summary>
    public const string HeaderName = "camusdb-batch-frames";

    /// <summary>
    /// Request header a client writes when it opens a <c>BatchExecute</c> stream to say it reads
    /// response frames. Its value is the highest contract version the client reads. A server built
    /// before frames ignores it. It exists so that a client that never sends a request frame — a
    /// single caller — still receives its results as frames; a request frame remains a proof too.
    /// </summary>
    public const string AcceptHeaderName = "camusdb-batch-frames-accept";

    /// <summary>The contract version this build reads and writes.</summary>
    public const int Version = 1;

    /// <summary>Most items one frame may carry. A server refuses the items past it.</summary>
    public const int MaxItems = 256;

    /// <summary>
    /// Most serialized item bytes one frame may carry. A message larger than this on its own travels
    /// as a plain single message.
    /// </summary>
    public const int MaxBytes = 1024 * 1024;

    /// <summary>
    /// What a message of <paramref name="serializedSize"/> bytes adds to a frame: its own bytes, plus
    /// the field tag and the length prefix it is wrapped in as a repeated item.
    /// </summary>
    public static int ItemCost(int serializedSize)
        => 1 + CodedOutputStream.ComputeLengthSize(serializedSize) + serializedSize;
}
