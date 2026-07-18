
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Allocation benchmarks for the spill codec's buffer reuse. The acceptance signal is that
/// steady-state spill writing and reading allocate no full-frame array per row — small frames use a
/// stack buffer, larger ones a pooled buffer, and the reader reuses one growable rented buffer.
///
/// Each op processes <see cref="RowCount"/> rows, so "Allocated" is per 1 000 rows (matching the other
/// tables in BENCH-RESULTS.md). The <c>*_PerRowArrayBaseline</c> methods reconstruct the previous
/// <c>new byte[4 + payloadSize]</c>-per-row shape as the explicit before contrast. <see cref="Width"/>
/// sizes a String column so one param sits below the 512-byte stack threshold and the others force the
/// pooled path — both must stay flat in allocation.
/// </summary>
[Config(typeof(ValueSlotConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class SpillCodecBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // Small (< 512 B frame → stackalloc), medium and large (> 512 B → ArrayPool).
    [Params(32, 512, 4_096)]
    public int Width { get; set; }

    private QueryResultRow[] _rows = null!;
    private byte[] _encodedBuffer = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        string filler = new('x', Width);
        _rows = new QueryResultRow[RowCount];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> row = new(StringComparer.Ordinal)
            {
                ["id"]   = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString()),
                ["n"]    = new ColumnValue(ColumnType.Integer64, (long)i),
                ["name"] = new ColumnValue(ColumnType.String, filler),
                ["ok"]   = ColumnValue.FromBool(i % 2 == 0),
            };
            _rows[i] = new QueryResultRow(new ObjectIdValue(i, i + 1, i + 2), row);
        }

        // A single in-memory buffer of framed records, consumed by the decode benchmarks.
        using MemoryStream ms = new();
        for (int i = 0; i < RowCount; i++)
            SpillRowCodec.EncodeToStream(ms, _rows[i]);
        _encodedBuffer = ms.ToArray();
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    /// <summary>Current path: stack/pool frame, no per-row array. Should be flat (~0) across width.</summary>
    [Benchmark(Baseline = true, Description = "Encode_Pooled")]
    public long Encode_Pooled()
    {
        for (int i = 0; i < RowCount; i++)
            SpillRowCodec.EncodeToStream(Stream.Null, _rows[i]);
        return Stream.Null.Position;
    }

    /// <summary>Pre-optimization contrast: a fresh <c>byte[]</c> frame per row.</summary>
    [Benchmark(Description = "Encode_PerRowArrayBaseline")]
    public long Encode_PerRowArrayBaseline()
    {
        long total = 0;
        for (int i = 0; i < RowCount; i++)
        {
            byte[] frame = SpillRowCodec.Encode(_rows[i]);
            Stream.Null.Write(frame, 0, frame.Length);
            total += frame.Length;
        }
        return total;
    }

    // ── Decode ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Current reader shape: one reusable buffer, each payload decoded from its active prefix. The
    /// per-record allocation is only the decoded values' own storage (strings/ids), never a framing
    /// array — so allocation is flat in <see cref="Width"/> beyond the string payload itself.
    /// </summary>
    [Benchmark(Description = "Decode_ReuseBuffer")]
    public int Decode_ReuseBuffer()
    {
        byte[] reusable = new byte[64];
        int offset = 0, rows = 0;
        while (offset < _encodedBuffer.Length)
        {
            int payloadLen = BitConverter.ToInt32(_encodedBuffer, offset);
            offset += 4;
            if (payloadLen > reusable.Length)
                reusable = new byte[payloadLen];
            Array.Copy(_encodedBuffer, offset, reusable, 0, payloadLen);
            offset += payloadLen;

            QueryResultRow row = SpillRowCodec.DecodePayload(reusable.AsSpan(0, payloadLen));
            rows += row.Row.Count;
        }
        return rows;
    }

    /// <summary>Pre-optimization contrast: a fresh payload <c>byte[]</c> per record.</summary>
    [Benchmark(Description = "Decode_NewArrayPerRecord")]
    public int Decode_NewArrayPerRecord()
    {
        int offset = 0, rows = 0;
        while (offset < _encodedBuffer.Length)
        {
            int payloadLen = BitConverter.ToInt32(_encodedBuffer, offset);
            offset += 4;
            byte[] payload = new byte[payloadLen];
            Array.Copy(_encodedBuffer, offset, payload, 0, payloadLen);
            offset += payloadLen;

            QueryResultRow row = SpillRowCodec.DecodePayload(payload);
            rows += row.Row.Count;
        }
        return rows;
    }
}
