/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// One decoded frame of a fragment response stream: a surviving row, a partial-aggregate row,
/// a terminal stats frame (all carried in <see cref="Row"/>), or a terminal error frame
/// (<see cref="Error"/>). Exactly one of the two is set. The server cannot change the HTTP
/// status once rows have started streaming, so a mid-stream failure arrives as an error frame;
/// the transport throws on it and the coordinator's local-resume fallback takes over.
/// </summary>
public readonly record struct QueryFragmentWireFrame(QueryFragmentRow? Row, string? Error);

/// <summary>
/// The two wire encodings of a query-fragment response stream, and the codec for each. Both
/// carry the same frames (<see cref="QueryFragmentRow"/> plus the error frame) and both are
/// written and read without an intermediate UTF-16 string or per-frame DTO.
///
/// <para><b>NDJSON</b> (<see cref="NdjsonContentType"/>) is the original, always-available
/// encoding: one JSON object per line, PascalCase property names, row bytes as base64. It is
/// what a peer on an older build sends and expects, so the field names and the tolerance for
/// <c>null</c> values and unknown properties are part of the compatibility contract — an old
/// server writes every property including nulls; an old client ignores properties it does not
/// know. Do not rename a field.</para>
///
/// <para><b>Binary</b> (<see cref="BinaryContentType"/>) is the negotiated fast path: the
/// client advertises it in <c>Accept</c>, and a server that understands it answers with that
/// content type. A server on an older build ignores the unknown media type and streams NDJSON;
/// a client on an older build never asks, so mixed-version peers always interoperate. Row bytes
/// travel verbatim (no base64 expansion), integers are little-endian and fixed-width, and every
/// variable-length field is length-prefixed so the reader can stop on a partial frame without
/// scanning. A layout change is a new media type, never a silent field reshuffle.</para>
///
/// <para>Frame precedence is the same in both encodings and mirrors what
/// <c>ExecuteQueryFragment</c> yields: a frame with stats is the terminal stats frame, else a
/// frame with cells is a partial-aggregate row, else the frame must carry a row id and data (a
/// broadcast-join probe row additionally carries its match indices).</para>
/// </summary>
public static class QueryFragmentWireCodec
{
    public const string NdjsonContentType = "application/x-ndjson";

    /// <summary>Versioned by name: a layout change ships as a new media type, so an old peer never misreads it.</summary>
    public const string BinaryContentType = "application/x-camus-query-fragment-v1";

    /// <summary>
    /// Upper bound on any length prefix the binary reader accepts. A corrupt or hostile
    /// prefix must not turn into an arbitrarily large allocation; a legitimate row is far
    /// below this.
    /// </summary>
    public const int MaxFrameBytes = 256 * 1024 * 1024;

    private const byte KindRow = 1;

    private const byte KindCells = 2;

    private const byte KindStats = 3;

    private const byte KindError = 4;

    /// <summary>Null match-index array on the wire; distinct from an empty one, which the coordinator merges as zero matches.</summary>
    private const int NullMatches = -1;

    // ── NDJSON ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Writes one frame as a JSON object. The caller owns the writer's lifecycle: flush it, then
    /// write the newline, then <see cref="Utf8JsonWriter.Reset()"/> before the next frame.
    /// Null members are omitted — an older reader tolerates a missing property.
    /// </summary>
    public static void WriteNdjsonFrame(Utf8JsonWriter writer, QueryFragmentRow row)
    {
        writer.WriteStartObject();

        if (row.RowIdHex is not null)
            writer.WriteString("RowIdHex"u8, row.RowIdHex);

        if (row.Data is not null)
            writer.WriteBase64String("Data"u8, row.Data);

        if (row.CellsJson is not null)
            writer.WriteString("Cells"u8, row.CellsJson);

        if (row.MatchIndices is not null)
        {
            writer.WriteStartArray("Matches"u8);

            int[] matches = row.MatchIndices;
            for (int i = 0; i < matches.Length; i++)
                writer.WriteNumberValue(matches[i]);

            writer.WriteEndArray();
        }

        if (row.Stats is not null)
        {
            writer.WriteStartObject("Stats"u8);
            writer.WriteNumber("RowsScanned"u8, row.Stats.RowsScanned);
            writer.WriteNumber("RowsShipped"u8, row.Stats.RowsShipped);
            writer.WriteEndObject();
        }

        writer.WriteEndObject();
    }

    /// <summary>Writes the terminal error frame; same lifecycle rules as <see cref="WriteNdjsonFrame"/>.</summary>
    public static void WriteNdjsonError(Utf8JsonWriter writer, string error)
    {
        writer.WriteStartObject();
        writer.WriteString("Error"u8, error);
        writer.WriteEndObject();
    }

    /// <summary>
    /// Decodes one NDJSON line (without its newline) straight from UTF-8. Unknown properties are
    /// skipped and explicit nulls are accepted, which is what makes an older server's output
    /// readable. Throws <see cref="CamusDBException"/> when the line is not a frame at all or
    /// is a row frame missing its id or data.
    /// </summary>
    public static QueryFragmentWireFrame ReadNdjsonFrame(in ReadOnlySequence<byte> line, string peer)
    {
        Utf8JsonReader reader = new(line, isFinalBlock: true, state: default);

        string? rowId = null;
        byte[]? data = null;
        string? cells = null;
        int[]? matches = null;
        QueryFragmentScanStats? stats = null;
        string? error = null;

        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                throw Unreadable(peer);

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    break;

                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw Unreadable(peer);

                if (reader.ValueTextEquals("RowIdHex"u8))
                {
                    reader.Read();
                    rowId = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else if (reader.ValueTextEquals("Data"u8))
                {
                    reader.Read();
                    data = reader.TokenType == JsonTokenType.Null ? null : reader.GetBytesFromBase64();
                }
                else if (reader.ValueTextEquals("Cells"u8))
                {
                    reader.Read();
                    cells = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else if (reader.ValueTextEquals("Matches"u8))
                {
                    reader.Read();
                    matches = reader.TokenType == JsonTokenType.Null ? null : ReadIntArray(ref reader, peer);
                }
                else if (reader.ValueTextEquals("Stats"u8))
                {
                    reader.Read();
                    stats = reader.TokenType == JsonTokenType.Null ? null : ReadStats(ref reader, peer);
                }
                else if (reader.ValueTextEquals("Error"u8))
                {
                    reader.Read();
                    error = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else
                {
                    reader.Skip();
                }
            }
        }
        catch (JsonException)
        {
            throw Unreadable(peer);
        }

        if (error is not null)
            return new QueryFragmentWireFrame(null, error);

        if (stats is not null)
            return new QueryFragmentWireFrame(new QueryFragmentRow(null, null, null, stats), null);

        if (cells is not null)
            return new QueryFragmentWireFrame(new QueryFragmentRow(null, null, cells), null);

        if (rowId is null || data is null)
            throw IncompleteRow(peer);

        return new QueryFragmentWireFrame(new QueryFragmentRow(rowId, data, MatchIndices: matches), null);
    }

    private static int[] ReadIntArray(ref Utf8JsonReader reader, string peer)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
            throw Unreadable(peer);

        // Match lists are short (one probe row's matches), so a growable scratch is enough.
        int[] scratch = new int[8];
        int count = 0;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            if (count == scratch.Length)
                Array.Resize(ref scratch, scratch.Length * 2);

            scratch[count++] = reader.GetInt32();
        }

        if (count == scratch.Length)
            return scratch;

        int[] result = new int[count];
        Array.Copy(scratch, result, count);
        return result;
    }

    private static QueryFragmentScanStats ReadStats(ref Utf8JsonReader reader, string peer)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw Unreadable(peer);

        long scanned = 0, shipped = 0;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw Unreadable(peer);

            if (reader.ValueTextEquals("RowsScanned"u8))
            {
                reader.Read();
                scanned = reader.GetInt64();
            }
            else if (reader.ValueTextEquals("RowsShipped"u8))
            {
                reader.Read();
                shipped = reader.GetInt64();
            }
            else
            {
                reader.Skip();
            }
        }

        return new QueryFragmentScanStats(scanned, shipped);
    }

    // ── Binary ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends one frame to <paramref name="output"/>. The frame is complete once the call
    /// returns; the caller decides when to flush. Layout (little-endian):
    /// <list type="bullet">
    /// <item><c>1</c> row: u8 idLength, id bytes, i32 dataLength, data, i32 matchCount (−1 = no match list), i32×matchCount</item>
    /// <item><c>2</c> cells: i32 length, UTF-8 cells JSON</item>
    /// <item><c>3</c> stats: i64 rowsScanned, i64 rowsShipped</item>
    /// <item><c>4</c> error: i32 length, UTF-8 message</item>
    /// </list>
    /// </summary>
    public static void WriteBinaryFrame(IBufferWriter<byte> output, QueryFragmentRow row)
    {
        if (row.Stats is not null)
        {
            Span<byte> span = output.GetSpan(1 + 8 + 8);
            span[0] = KindStats;
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(1), row.Stats.RowsScanned);
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(9), row.Stats.RowsShipped);
            output.Advance(17);
            return;
        }

        if (row.CellsJson is not null)
        {
            WriteLengthPrefixedString(output, KindCells, row.CellsJson);
            return;
        }

        if (row.RowIdHex is null || row.Data is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A query-fragment row frame needs both a row id and row bytes");

        int idLength = Encoding.UTF8.GetByteCount(row.RowIdHex);
        if (idLength > byte.MaxValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Query-fragment row id is {idLength} bytes; the binary frame allows at most {byte.MaxValue}");

        byte[] data = row.Data;

        Span<byte> header = output.GetSpan(1 + 1 + idLength + 4);
        header[0] = KindRow;
        header[1] = (byte)idLength;
        Encoding.UTF8.GetBytes(row.RowIdHex, header.Slice(2, idLength));
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(2 + idLength), data.Length);
        output.Advance(2 + idLength + 4);

        output.Write(data);

        int[]? matches = row.MatchIndices;
        int matchBytes = 4 + (matches is null ? 0 : matches.Length * 4);
        Span<byte> tail = output.GetSpan(matchBytes);

        if (matches is null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(tail, NullMatches);
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(tail, matches.Length);
            for (int i = 0; i < matches.Length; i++)
                BinaryPrimitives.WriteInt32LittleEndian(tail.Slice(4 + i * 4), matches[i]);
        }

        output.Advance(matchBytes);
    }

    /// <summary>Appends the terminal error frame.</summary>
    public static void WriteBinaryError(IBufferWriter<byte> output, string error)
        => WriteLengthPrefixedString(output, KindError, error);

    private static void WriteLengthPrefixedString(IBufferWriter<byte> output, byte kind, string text)
    {
        int length = Encoding.UTF8.GetByteCount(text);
        Span<byte> span = output.GetSpan(1 + 4 + length);
        span[0] = kind;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1), length);
        Encoding.UTF8.GetBytes(text, span.Slice(5, length));
        output.Advance(5 + length);
    }

    /// <summary>
    /// Tries to decode one complete frame from the front of <paramref name="buffer"/>. Returns
    /// false, leaving the buffer untouched, when the frame is not yet complete; the caller reads
    /// more and retries. On success the buffer is advanced past the frame. Throws
    /// <see cref="CamusDBException"/> on an unknown frame kind or an out-of-range length — the
    /// stream is corrupt and cannot be resynchronized, so the coordinator must fall back.
    /// </summary>
    public static bool TryReadBinaryFrame(ref ReadOnlySequence<byte> buffer, string peer, out QueryFragmentWireFrame frame)
    {
        frame = default;
        SequenceReader<byte> reader = new(buffer);

        if (!reader.TryRead(out byte kind))
            return false;

        switch (kind)
        {
            case KindRow:
            {
                if (!reader.TryRead(out byte idLength))
                    return false;

                if (reader.Remaining < idLength)
                    return false;

                Span<byte> idBytes = stackalloc byte[byte.MaxValue];
                idBytes = idBytes.Slice(0, idLength);
                reader.TryCopyTo(idBytes);
                reader.Advance(idLength);
                string rowId = Encoding.UTF8.GetString(idBytes);

                if (!reader.TryReadLittleEndian(out int dataLength))
                    return false;

                CheckLength(dataLength, peer);

                if (reader.Remaining < dataLength)
                    return false;

                byte[] data = new byte[dataLength];
                reader.TryCopyTo(data);
                reader.Advance(dataLength);

                if (!reader.TryReadLittleEndian(out int matchCount))
                    return false;

                int[]? matches = null;

                if (matchCount != NullMatches)
                {
                    if (matchCount < 0 || matchCount > MaxFrameBytes / 4)
                        throw Corrupt(peer, $"match count {matchCount}");

                    if (reader.Remaining < (long)matchCount * 4)
                        return false;

                    matches = new int[matchCount];
                    for (int i = 0; i < matchCount; i++)
                    {
                        reader.TryReadLittleEndian(out int match);
                        matches[i] = match;
                    }
                }

                frame = new QueryFragmentWireFrame(new QueryFragmentRow(rowId, data, MatchIndices: matches), null);
                break;
            }

            case KindCells:
            {
                if (!TryReadLengthPrefixedString(ref reader, peer, out string? cells))
                    return false;

                frame = new QueryFragmentWireFrame(new QueryFragmentRow(null, null, cells), null);
                break;
            }

            case KindStats:
            {
                if (!reader.TryReadLittleEndian(out long scanned) || !reader.TryReadLittleEndian(out long shipped))
                    return false;

                frame = new QueryFragmentWireFrame(new QueryFragmentRow(null, null, null, new QueryFragmentScanStats(scanned, shipped)), null);
                break;
            }

            case KindError:
            {
                if (!TryReadLengthPrefixedString(ref reader, peer, out string? error))
                    return false;

                frame = new QueryFragmentWireFrame(null, error);
                break;
            }

            default:
                throw Corrupt(peer, $"frame kind {kind}");
        }

        buffer = buffer.Slice(reader.Position);
        return true;
    }

    private static bool TryReadLengthPrefixedString(ref SequenceReader<byte> reader, string peer, out string text)
    {
        text = "";

        if (!reader.TryReadLittleEndian(out int length))
            return false;

        CheckLength(length, peer);

        if (reader.Remaining < length)
            return false;

        // Decodes straight from the (possibly multi-segment) sequence: no intermediate copy.
        text = Encoding.UTF8.GetString(reader.UnreadSequence.Slice(0, length));
        reader.Advance(length);
        return true;
    }

    private static void CheckLength(int length, string peer)
    {
        if (length < 0 || length > MaxFrameBytes)
            throw Corrupt(peer, $"length prefix {length}");
    }

    private static CamusDBException Corrupt(string peer, string what) =>
        new(CamusDBErrorCodes.InvalidInternalOperation,
            $"Query fragment on '{peer}' returned a corrupt binary frame ({what})");

    private static CamusDBException Unreadable(string peer) =>
        new(CamusDBErrorCodes.InvalidInternalOperation,
            $"Query fragment on '{peer}' returned an unreadable frame");

    private static CamusDBException IncompleteRow(string peer) =>
        new(CamusDBErrorCodes.InvalidInternalOperation,
            $"Query fragment on '{peer}' returned an incomplete row frame");
}
