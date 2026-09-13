/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit coverage for both fragment wire encodings. The interop tests pin the NDJSON contract
/// against the shape an older build produces and consumes (a DTO serialized with every property,
/// nulls included), because that is what a mixed-version cluster exchanges.
/// </summary>
[TestFixture]
public sealed class TestQueryFragmentWireCodec
{
    private const string Peer = "peer-a:8080";

    private const string RowId = "6849f3a1c2e7d50b4f8a91d3";

    /// <summary>The exact wire DTO an older build serialized per line — kept here so the contract is explicit.</summary>
    private sealed class LegacyWireLine
    {
        public string? RowIdHex { get; set; }
        public byte[]? Data { get; set; }
        public string? Cells { get; set; }
        public int[]? Matches { get; set; }
        public QueryFragmentScanStats? Stats { get; set; }
        public string? Error { get; set; }
    }

    private static byte[] Payload(int size)
    {
        byte[] payload = new byte[size];
        for (int i = 0; i < size; i++)
            payload[i] = (byte)((i * 31 + 7) & 0xFF);
        return payload;
    }

    private static byte[] EncodeNdjson(QueryFragmentRow? row, string? error = null)
    {
        ArrayBufferWriter<byte> output = new();
        using (Utf8JsonWriter writer = new(output))
        {
            if (row is not null)
                QueryFragmentWireCodec.WriteNdjsonFrame(writer, row);
            else
                QueryFragmentWireCodec.WriteNdjsonError(writer, error!);
            writer.Flush();
        }
        return output.WrittenSpan.ToArray();
    }

    private static QueryFragmentWireFrame DecodeNdjson(byte[] line) =>
        QueryFragmentWireCodec.ReadNdjsonFrame(new ReadOnlySequence<byte>(line), Peer);

    private static byte[] EncodeBinary(QueryFragmentRow? row, string? error = null)
    {
        ArrayBufferWriter<byte> output = new();
        if (row is not null)
            QueryFragmentWireCodec.WriteBinaryFrame(output, row);
        else
            QueryFragmentWireCodec.WriteBinaryError(output, error!);
        return output.WrittenSpan.ToArray();
    }

    private static QueryFragmentWireFrame DecodeBinary(byte[] bytes)
    {
        ReadOnlySequence<byte> buffer = new(bytes);
        Assert.IsTrue(QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, Peer, out QueryFragmentWireFrame frame));
        Assert.IsTrue(buffer.IsEmpty, "a single frame must be consumed exactly");
        return frame;
    }

    private static void AssertSameRow(QueryFragmentRow expected, QueryFragmentRow? actual)
    {
        Assert.IsNotNull(actual);
        Assert.AreEqual(expected.RowIdHex, actual!.RowIdHex);
        Assert.AreEqual(expected.Data, actual.Data);
        Assert.AreEqual(expected.CellsJson, actual.CellsJson);
        Assert.AreEqual(expected.MatchIndices, actual.MatchIndices);
        Assert.AreEqual(expected.Stats, actual.Stats);
    }

    // ── Row frames ───────────────────────────────────────────────────────────

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(128)]
    [TestCase(4096)]
    [TestCase(65536)]
    public void RowFrame_RoundTrips_InBothEncodings(int size)
    {
        QueryFragmentRow row = new(RowId, Payload(size));

        AssertSameRow(row, DecodeNdjson(EncodeNdjson(row)).Row);
        AssertSameRow(row, DecodeBinary(EncodeBinary(row)).Row);
    }

    [Test]
    public void Binary_RowFrame_CarriesRawBytesNotBase64()
    {
        byte[] data = Payload(4096);
        byte[] frame = EncodeBinary(new QueryFragmentRow(RowId, data));

        // kind + idLen + 24-char id + i32 length + data + i32 null-matches
        Assert.AreEqual(1 + 1 + 24 + 4 + data.Length + 4, frame.Length);
        Assert.AreEqual(data, frame.AsSpan(1 + 1 + 24 + 4, data.Length).ToArray());
    }

    [Test]
    public void MatchIndices_NullEmptyAndPopulated_SurviveBothEncodings()
    {
        foreach (int[]? matches in new int[]?[] { null, [], [3, 1, 2, int.MaxValue, 0] })
        {
            QueryFragmentRow row = new(RowId, Payload(16), MatchIndices: matches);

            AssertSameRow(row, DecodeNdjson(EncodeNdjson(row)).Row);
            AssertSameRow(row, DecodeBinary(EncodeBinary(row)).Row);
        }
    }

    [Test]
    public void MatchIndices_LongerThanReaderScratch_RoundTrip()
    {
        int[] matches = new int[37];
        for (int i = 0; i < matches.Length; i++)
            matches[i] = i * 3;

        QueryFragmentRow row = new(RowId, Payload(8), MatchIndices: matches);

        AssertSameRow(row, DecodeNdjson(EncodeNdjson(row)).Row);
        AssertSameRow(row, DecodeBinary(EncodeBinary(row)).Row);
    }

    // ── Cells, stats, error ──────────────────────────────────────────────────

    [Test]
    public void CellsFrame_RoundTrips_WithNonAsciiText()
    {
        QueryFragmentRow row = new(null, null, "{\"count\":{\"t\":3,\"v\":\"12\"},\"name\":\"ñandú — 日本\"}");

        AssertSameRow(row, DecodeNdjson(EncodeNdjson(row)).Row);
        AssertSameRow(row, DecodeBinary(EncodeBinary(row)).Row);
    }

    [Test]
    public void StatsFrame_RoundTrips()
    {
        QueryFragmentRow row = new(null, null, null, new QueryFragmentScanStats(123456789012L, 42));

        AssertSameRow(row, DecodeNdjson(EncodeNdjson(row)).Row);
        AssertSameRow(row, DecodeBinary(EncodeBinary(row)).Row);
    }

    [Test]
    public void ErrorFrame_RoundTrips_AndCarriesNoRow()
    {
        const string message = "schema version mismatch: this node has 3, coordinator planned 2 — \"quoted\"";

        QueryFragmentWireFrame ndjson = DecodeNdjson(EncodeNdjson(null, message));
        Assert.IsNull(ndjson.Row);
        Assert.AreEqual(message, ndjson.Error);

        QueryFragmentWireFrame binary = DecodeBinary(EncodeBinary(null, message));
        Assert.IsNull(binary.Row);
        Assert.AreEqual(message, binary.Error);
    }

    [Test]
    public void Binary_RowWithoutIdOrData_IsRefusedAtWriteTime()
    {
        ArrayBufferWriter<byte> output = new();

        Assert.Throws<CamusDBException>(() =>
            QueryFragmentWireCodec.WriteBinaryFrame(output, new QueryFragmentRow(null, Payload(4))));
        Assert.Throws<CamusDBException>(() =>
            QueryFragmentWireCodec.WriteBinaryFrame(output, new QueryFragmentRow(RowId, null)));
    }

    // ── NDJSON interop with an older build ───────────────────────────────────

    [Test]
    public void Ndjson_ReadsWhatAnOlderServerWrites_NullsIncluded()
    {
        byte[] data = Payload(300);

        byte[] rowLine = JsonSerializer.SerializeToUtf8Bytes(new LegacyWireLine { RowIdHex = RowId, Data = data });
        StringAssert.Contains("\"Cells\":null", Encoding.UTF8.GetString(rowLine), "the legacy writer emits nulls");
        AssertSameRow(new QueryFragmentRow(RowId, data), DecodeNdjson(rowLine).Row);

        byte[] cellsLine = JsonSerializer.SerializeToUtf8Bytes(new LegacyWireLine { Cells = "{\"c\":1}" });
        AssertSameRow(new QueryFragmentRow(null, null, "{\"c\":1}"), DecodeNdjson(cellsLine).Row);

        byte[] statsLine = JsonSerializer.SerializeToUtf8Bytes(new LegacyWireLine { Stats = new QueryFragmentScanStats(10, 2) });
        AssertSameRow(new QueryFragmentRow(null, null, null, new QueryFragmentScanStats(10, 2)), DecodeNdjson(statsLine).Row);

        byte[] joinLine = JsonSerializer.SerializeToUtf8Bytes(new LegacyWireLine { RowIdHex = RowId, Data = data, Matches = [5, 6] });
        AssertSameRow(new QueryFragmentRow(RowId, data, MatchIndices: [5, 6]), DecodeNdjson(joinLine).Row);

        byte[] errorLine = JsonSerializer.SerializeToUtf8Bytes(new LegacyWireLine { Error = "boom" });
        Assert.AreEqual("boom", DecodeNdjson(errorLine).Error);
    }

    [Test]
    public void Ndjson_WritesWhatAnOlderClientReads()
    {
        byte[] data = Payload(300);

        LegacyWireLine? row = JsonSerializer.Deserialize<LegacyWireLine>(EncodeNdjson(new QueryFragmentRow(RowId, data, MatchIndices: [1])));
        Assert.AreEqual(RowId, row!.RowIdHex);
        Assert.AreEqual(data, row.Data);
        Assert.AreEqual(new[] { 1 }, row.Matches);
        Assert.IsNull(row.Cells);
        Assert.IsNull(row.Stats);
        Assert.IsNull(row.Error);

        LegacyWireLine? stats = JsonSerializer.Deserialize<LegacyWireLine>(
            EncodeNdjson(new QueryFragmentRow(null, null, null, new QueryFragmentScanStats(7, 3))));
        Assert.AreEqual(new QueryFragmentScanStats(7, 3), stats!.Stats);

        LegacyWireLine? error = JsonSerializer.Deserialize<LegacyWireLine>(EncodeNdjson(null, "boom"));
        Assert.AreEqual("boom", error!.Error);
    }

    [Test]
    public void Ndjson_UnknownPropertiesAreSkipped_ForwardCompatibility()
    {
        byte[] line = Encoding.UTF8.GetBytes(
            "{\"Future\":{\"nested\":[1,2,{\"x\":null}]},\"RowIdHex\":\"" + RowId + "\",\"Extra\":\"s\",\"Data\":\"AQID\"}\r");

        AssertSameRow(new QueryFragmentRow(RowId, [1, 2, 3]), DecodeNdjson(line).Row);
    }

    [Test]
    public void Ndjson_FramePrecedence_MatchesTheOriginalTransport()
    {
        // Error wins over everything, then stats, then cells, then row.
        Assert.AreEqual("e", DecodeNdjson(JsonSerializer.SerializeToUtf8Bytes(
            new LegacyWireLine { RowIdHex = RowId, Data = [1], Stats = new(1, 1), Error = "e" })).Error);

        QueryFragmentRow? stats = DecodeNdjson(JsonSerializer.SerializeToUtf8Bytes(
            new LegacyWireLine { RowIdHex = RowId, Data = [1], Cells = "{}", Stats = new(1, 1) })).Row;
        Assert.IsNotNull(stats!.Stats);
        Assert.IsNull(stats.RowIdHex);

        QueryFragmentRow? cells = DecodeNdjson(JsonSerializer.SerializeToUtf8Bytes(
            new LegacyWireLine { RowIdHex = RowId, Data = [1], Cells = "{}" })).Row;
        Assert.AreEqual("{}", cells!.CellsJson);
        Assert.IsNull(cells.RowIdHex);
    }

    [Test]
    public void Ndjson_MalformedLines_ThrowDomainErrors()
    {
        Assert.Throws<CamusDBException>(() => DecodeNdjson(Encoding.UTF8.GetBytes("[1,2]")));
        Assert.Throws<CamusDBException>(() => DecodeNdjson(Encoding.UTF8.GetBytes("{\"RowIdHex\":\"" + RowId + "\"")));
        Assert.Throws<CamusDBException>(() => DecodeNdjson(Encoding.UTF8.GetBytes("not json")));
        Assert.Throws<CamusDBException>(() => DecodeNdjson(Encoding.UTF8.GetBytes("{\"RowIdHex\":\"" + RowId + "\"}")),
            "a row without data is incomplete");
        Assert.Throws<CamusDBException>(() => DecodeNdjson(Encoding.UTF8.GetBytes("{\"Data\":\"AQID\"}")),
            "a row without an id is incomplete");
        Assert.Throws<CamusDBException>(() => DecodeNdjson(Encoding.UTF8.GetBytes("{}")));
    }

    [Test]
    public void Ndjson_MultiSegmentLine_DecodesWithoutCopy()
    {
        byte[] line = EncodeNdjson(new QueryFragmentRow(RowId, Payload(5000), MatchIndices: [9]));

        for (int segment = 1; segment <= 64; segment *= 4)
        {
            QueryFragmentWireFrame frame = QueryFragmentWireCodec.ReadNdjsonFrame(Segmented(line, segment), Peer);
            AssertSameRow(new QueryFragmentRow(RowId, Payload(5000), MatchIndices: [9]), frame.Row);
        }
    }

    // ── Binary framing under partial delivery ────────────────────────────────

    private static List<(QueryFragmentRow? Row, string? Error)> SampleStream()
    {
        return
        [
            (new QueryFragmentRow(RowId, Payload(0)), null),
            (new QueryFragmentRow(RowId, Payload(128), MatchIndices: [1, 2]), null),
            (new QueryFragmentRow(RowId, Payload(4096)), null),
            (new QueryFragmentRow(null, null, "{\"sum\":{\"t\":3,\"v\":\"9\"}}"), null),
            (new QueryFragmentRow(RowId, Payload(65536), MatchIndices: []), null),
            (new QueryFragmentRow(null, null, null, new QueryFragmentScanStats(5, 4)), null),
            (null, "tail error"),
        ];
    }

    private static byte[] EncodeStream(List<(QueryFragmentRow? Row, string? Error)> frames)
    {
        ArrayBufferWriter<byte> output = new();
        foreach ((QueryFragmentRow? row, string? error) in frames)
        {
            if (row is not null)
                QueryFragmentWireCodec.WriteBinaryFrame(output, row);
            else
                QueryFragmentWireCodec.WriteBinaryError(output, error!);
        }
        return output.WrittenSpan.ToArray();
    }

    [Test]
    public void Binary_EveryPrefix_YieldsOnlyCompleteFrames_AndTheWholeStreamDecodes()
    {
        List<(QueryFragmentRow? Row, string? Error)> expected = SampleStream();
        byte[] stream = EncodeStream(expected);

        // Frame boundaries: decode the whole stream once to learn where each frame ends.
        List<int> boundaries = [];
        {
            ReadOnlySequence<byte> all = new(stream);
            while (QueryFragmentWireCodec.TryReadBinaryFrame(ref all, Peer, out _))
                boundaries.Add(stream.Length - (int)all.Length);
        }
        Assert.AreEqual(expected.Count, boundaries.Count);
        Assert.AreEqual(stream.Length, boundaries[^1]);

        // Every prefix decodes exactly the frames that are complete within it and never more.
        // Stepping by a prime keeps the run fast while still cutting inside every field kind.
        for (int prefix = 0; prefix <= stream.Length; prefix += 37)
        {
            ReadOnlySequence<byte> buffer = new(stream, 0, prefix);
            int decoded = 0;
            while (QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, Peer, out _))
                decoded++;

            int complete = 0;
            while (complete < boundaries.Count && boundaries[complete] <= prefix)
                complete++;

            Assert.AreEqual(complete, decoded, $"prefix {prefix}");
        }

        // Cut exactly one byte short of each boundary: that frame must not be produced.
        foreach (int boundary in boundaries)
        {
            ReadOnlySequence<byte> buffer = new(stream, 0, boundary - 1);
            int decoded = 0;
            while (QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, Peer, out _))
                decoded++;
            Assert.AreEqual(boundaries.IndexOf(boundary), decoded);
        }
    }

    [Test]
    public void Binary_MultiSegmentStream_DecodesEveryFrameIdentically()
    {
        List<(QueryFragmentRow? Row, string? Error)> expected = SampleStream();
        byte[] stream = EncodeStream(expected);

        foreach (int segment in new[] { 1, 3, 17, 1024, 65536 })
        {
            ReadOnlySequence<byte> buffer = Segmented(stream, segment);
            List<QueryFragmentWireFrame> frames = [];
            while (QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, Peer, out QueryFragmentWireFrame frame))
                frames.Add(frame);

            Assert.IsTrue(buffer.IsEmpty, $"segment {segment}: stream fully consumed");
            Assert.AreEqual(expected.Count, frames.Count, $"segment {segment}");

            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i].Row is { } row)
                    AssertSameRow(row, frames[i].Row);
                else
                    Assert.AreEqual(expected[i].Error, frames[i].Error);
            }
        }
    }

    [Test]
    public void Binary_CorruptStream_FailsClosed()
    {
        // Unknown frame kind.
        ReadOnlySequence<byte> badKind = new(new byte[] { 99, 0, 0, 0 });
        Assert.Throws<CamusDBException>(() => QueryFragmentWireCodec.TryReadBinaryFrame(ref badKind, Peer, out _));

        // Negative data length in a row frame.
        byte[] negative = EncodeBinary(new QueryFragmentRow(RowId, Payload(4)));
        BitConverter.GetBytes(-5).CopyTo(negative, 1 + 1 + 24);
        ReadOnlySequence<byte> negativeSeq = new(negative);
        Assert.Throws<CamusDBException>(() => QueryFragmentWireCodec.TryReadBinaryFrame(ref negativeSeq, Peer, out _));

        // Length prefix above the cap must throw rather than wait for (or allocate) 2 GiB.
        byte[] huge = EncodeBinary(new QueryFragmentRow(null, null, "{}"));
        BitConverter.GetBytes(int.MaxValue).CopyTo(huge, 1);
        ReadOnlySequence<byte> hugeSeq = new(huge);
        Assert.Throws<CamusDBException>(() => QueryFragmentWireCodec.TryReadBinaryFrame(ref hugeSeq, Peer, out _));

        // Negative match count other than the null sentinel.
        byte[] badMatches = EncodeBinary(new QueryFragmentRow(RowId, Payload(4)));
        BitConverter.GetBytes(-2).CopyTo(badMatches, badMatches.Length - 4);
        ReadOnlySequence<byte> badMatchesSeq = new(badMatches);
        Assert.Throws<CamusDBException>(() => QueryFragmentWireCodec.TryReadBinaryFrame(ref badMatchesSeq, Peer, out _));
    }

    [Test]
    public void Binary_IncompleteFrame_LeavesBufferUntouched()
    {
        byte[] frame = EncodeBinary(new QueryFragmentRow(RowId, Payload(100), MatchIndices: [1, 2, 3]));
        ReadOnlySequence<byte> buffer = new(frame, 0, frame.Length - 1);
        long before = buffer.Length;

        Assert.IsFalse(QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, Peer, out QueryFragmentWireFrame frameOut));
        Assert.AreEqual(before, buffer.Length);
        Assert.IsNull(frameOut.Row);
        Assert.IsNull(frameOut.Error);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            Segment next = new(memory, RunningIndex + Memory.Length);
            Next = next;
            return next;
        }
    }

    /// <summary>Splits <paramref name="bytes"/> into a linked sequence of <paramref name="segmentSize"/>-byte segments.</summary>
    private static ReadOnlySequence<byte> Segmented(byte[] bytes, int segmentSize)
    {
        if (bytes.Length == 0)
            return ReadOnlySequence<byte>.Empty;

        Segment first = new(bytes.AsMemory(0, Math.Min(segmentSize, bytes.Length)), 0);
        Segment last = first;

        for (int offset = segmentSize; offset < bytes.Length; offset += segmentSize)
            last = last.Append(bytes.AsMemory(offset, Math.Min(segmentSize, bytes.Length - offset)));

        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }
}
