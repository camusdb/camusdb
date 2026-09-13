/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Encode-plus-decode cost of one fragment stream of <see cref="Rows"/> rows of
/// <see cref="RowBytes"/> bytes each, for the three wire paths:
///
/// <list type="bullet">
/// <item><see cref="LegacyNdjson"/> reproduces the original transport exactly: the server
/// serializes a per-row DTO with <see cref="JsonSerializer"/> (row bytes as base64), the client
/// reads each line into a UTF-16 string through a <see cref="StreamReader"/> and deserializes
/// the DTO from that string.</item>
/// <item><see cref="Utf8Ndjson"/> is the same NDJSON bytes written and read through
/// <see cref="QueryFragmentWireCodec"/> — no DTO, no line string, base64 still on the wire.</item>
/// <item><see cref="Binary"/> is the negotiated binary framing: raw row bytes, fixed-width
/// integers.</item>
/// </list>
///
/// Each benchmark returns the wire size so nothing is dead-code eliminated and the
/// bytes-per-row of each encoding can be read off the result. Allocated bytes divided by
/// <see cref="Rows"/> gives the per-row allocation of encode + decode together; the decoded
/// row's own <c>byte[]</c> (which the coordinator must own) is the floor every path shares.
/// </summary>
[MemoryDiagnoser]
public class QueryFragmentWireBenchmarks
{
    private const string RowId = "6849f3a1c2e7d50b4f8a91d3";

    private const string Peer = "peer";

    [Params(128, 4096, 65536)]
    public int RowBytes { get; set; }

    [Params(256)]
    public int Rows { get; set; }

    private QueryFragmentRow[] rows = [];

    private sealed class LegacyWireLine
    {
        public string? RowIdHex { get; set; }
        public byte[]? Data { get; set; }
        public string? Cells { get; set; }
        public int[]? Matches { get; set; }
        public QueryFragmentScanStats? Stats { get; set; }
        public string? Error { get; set; }
    }

    [GlobalSetup]
    public void Setup()
    {
        rows = new QueryFragmentRow[Rows];

        for (int r = 0; r < Rows; r++)
        {
            byte[] payload = new byte[RowBytes];
            for (int i = 0; i < payload.Length; i++)
                payload[i] = (byte)((i * 31 + r) & 0xFF);

            rows[r] = new QueryFragmentRow(RowId, payload);
        }
    }

    [Benchmark(Baseline = true)]
    public long LegacyNdjson()
    {
        MemoryStream wire = new();
        byte[] newline = "\n"u8.ToArray();

        for (int r = 0; r < rows.Length; r++)
        {
            QueryFragmentRow row = rows[r];
            JsonSerializer.Serialize(wire, new LegacyWireLine { RowIdHex = row.RowIdHex, Data = row.Data, Cells = row.CellsJson, Stats = row.Stats, Matches = row.MatchIndices });
            wire.Write(newline);
        }

        long wireBytes = wire.Length;
        wire.Position = 0;

        long decodedBytes = 0;
        using StreamReader reader = new(wire, Encoding.UTF8);

        while (reader.ReadLine() is { } line)
        {
            if (line.Length == 0)
                continue;

            LegacyWireLine parsed = JsonSerializer.Deserialize<LegacyWireLine>(line)!;
            decodedBytes += parsed.Data!.Length;
        }

        return wireBytes + decodedBytes;
    }

    [Benchmark]
    public long Utf8Ndjson()
    {
        ArrayBufferWriter<byte> wire = new(1 << 16);

        using (Utf8JsonWriter writer = new(wire, new JsonWriterOptions { SkipValidation = true }))
        {
            for (int r = 0; r < rows.Length; r++)
            {
                QueryFragmentWireCodec.WriteNdjsonFrame(writer, rows[r]);
                writer.Flush();
                writer.Reset();
                wire.GetSpan(1)[0] = (byte)'\n';
                wire.Advance(1);
            }
        }

        long wireBytes = wire.WrittenCount;
        ReadOnlySequence<byte> buffer = new(wire.WrittenMemory);
        SequenceReader<byte> lines = new(buffer);
        long decodedBytes = 0;

        while (lines.TryReadTo(out ReadOnlySequence<byte> line, (byte)'\n', advancePastDelimiter: true))
        {
            if (line.IsEmpty)
                continue;

            QueryFragmentWireFrame frame = QueryFragmentWireCodec.ReadNdjsonFrame(line, Peer);
            decodedBytes += frame.Row!.Data!.Length;
        }

        return wireBytes + decodedBytes;
    }

    [Benchmark]
    public long Binary()
    {
        ArrayBufferWriter<byte> wire = new(1 << 16);

        for (int r = 0; r < rows.Length; r++)
            QueryFragmentWireCodec.WriteBinaryFrame(wire, rows[r]);

        long wireBytes = wire.WrittenCount;
        ReadOnlySequence<byte> buffer = new(wire.WrittenMemory);
        long decodedBytes = 0;

        while (QueryFragmentWireCodec.TryReadBinaryFrame(ref buffer, Peer, out QueryFragmentWireFrame frame))
            decodedBytes += frame.Row!.Data!.Length;

        return wireBytes + decodedBytes;
    }
}
