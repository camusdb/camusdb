/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Isolated before/after for the Bytes cell of the JSON row writer.
///
/// <para>
/// <see cref="Base64String"/> reproduces the previous implementation exactly: allocate the base64
/// text as a UTF-16 <see cref="string"/>, then hand it to <c>WriteStringValue</c>.
/// <see cref="Base64Scratch"/> is what <see cref="CompactRowJsonWriter"/> now does — encode into a
/// stack buffer for a small payload and a pooled buffer for a large one, and write the span.
/// </para>
///
/// <para>
/// Both variants route the text through the writer's JSON encoder, so the bytes on the wire are
/// identical; only the intermediate string disappears. The written length is returned so neither
/// call can be optimized away.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class Base64CellBenchmarks
{
    [Params(64, 512, 4096, 65536)]
    public int PayloadBytes { get; set; }

    [Params(100)]
    public int CellCount { get; set; }

    private ColumnValue _value = null!;

    [GlobalSetup]
    public void Setup()
    {
        byte[] payload = new byte[PayloadBytes];

        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)((i * 31) & 0xFF);

        _value = new ColumnValue(payload);
    }

    [Benchmark(Baseline = true)]
    public long Base64String()
    {
        ArrayBufferWriter<byte> buffer = new(1 << 16);
        using Utf8JsonWriter writer = new(buffer);

        writer.WriteStartArray();

        for (int i = 0; i < CellCount; i++)
            writer.WriteStringValue(Convert.ToBase64String(_value.BytesValue!));

        writer.WriteEndArray();
        writer.Flush();

        return buffer.WrittenCount;
    }

    [Benchmark]
    public long Base64Scratch()
    {
        ArrayBufferWriter<byte> buffer = new(1 << 16);
        using Utf8JsonWriter writer = new(buffer);

        writer.WriteStartArray();

        for (int i = 0; i < CellCount; i++)
            CompactRowJsonWriter.WriteValue(writer, _value);

        writer.WriteEndArray();
        writer.Flush();

        return buffer.WrittenCount;
    }
}
