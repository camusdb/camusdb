
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Storage-boundary allocation benchmarks for the zero-copy KV envelope. These cross the
/// <see cref="BranchKvCodec"/> boundary that <c>AllocationPrimitivesBenchmarks</c> deliberately skips
/// (it starts from an already-encoded payload), so they exercise the actual acceptance criteria of the
/// envelope optimization: envelope removal allocates nothing payload-sized, and a row write produces one
/// final storage array rather than a raw-row array plus an envelope array.
///
/// Each benchmark processes <see cref="RowCount"/> rows per op so the reported "Allocated" is per
/// 1 000 rows, matching the units used by the other allocation tables in <c>BENCH-RESULTS.md</c>. The
/// <see cref="PayloadSize"/> param sizes a <c>Bytes</c> column so the payload-copying baselines scale
/// with it while the zero-copy paths must stay flat.
///
/// The <c>*_Baseline</c> benchmarks reconstruct the pre-optimization shape (two-array encode /
/// payload-copying decode) as the explicit "before" contrast; they do not call removed code.
/// </summary>
[Config(typeof(RowDecodeConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class EnvelopeRoundTripBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // Size in bytes of the "big" Bytes column. Copying paths scale with this; zero-copy paths must not.
    [Params(16, 4_096, 65_536)]
    public int PayloadSize { get; set; }

    private static TableColumnSchema Col(string name, ColumnType type)
        => new(name, name, type, false, null, SchemaElementState.Public);

    private static readonly ObjectIdValue RowId = new(1, 2, 3);
    private static readonly HLCTimestamp TxId = default;

    private TableSchema _schema = null!;

    // One decoded-input dictionary per row, reused by the encode benchmarks.
    private Dictionary<string, ColumnValue>[] _rows = null!;

    // Pre-enveloped storage values (byte 0 = Value marker), consumed by the decode benchmarks.
    private byte[][] _stored = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _schema = new TableSchema
        {
            Id            = "bench-table",
            Name          = "bench",
            Version       = 0,
            Columns       =
            [
                Col("id",     ColumnType.Id),
                Col("keep",   ColumnType.String),
                Col("big",    ColumnType.Bytes),
                Col("age",    ColumnType.Integer64),
                Col("active", ColumnType.Bool),
            ],
        };
        _schema.SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = _schema.Columns }];

        byte[] bigValue = new byte[PayloadSize];
        for (int b = 0; b < bigValue.Length; b++)
            bigValue[b] = (byte)(b & 0xFF);

        _rows = new Dictionary<string, ColumnValue>[RowCount];
        _stored = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> row = new()
            {
                ["id"]     = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString()),
                ["keep"]   = new ColumnValue(ColumnType.String, "k_" + i),
                ["big"]    = new ColumnValue(bigValue),
                ["age"]    = new ColumnValue(ColumnType.Integer64, (long)(20 + i % 80)),
                ["active"] = ColumnValue.True,
            };
            _rows[i] = row;
            _stored[i] = RowEncoder.EncodeStorageValue(_schema, row, RowId);
        }
    }

    // ── Envelope removal (decode side) ────────────────────────────────────────

    /// <summary>
    /// Current path: peel the one-byte marker as a zero-copy <see cref="ReadOnlyMemory{T}"/> slice.
    /// <see cref="BranchKvValue"/> is a struct and the slice shares the stored array, so allocation must
    /// be ~0 and FLAT across payload size. This is the headline acceptance signal for the envelope work.
    /// </summary>
    [Benchmark(Description = "EnvelopeDecode_ZeroCopy")]
    public int EnvelopeDecode_ZeroCopy()
    {
        int total = 0;
        for (int i = 0; i < RowCount; i++)
        {
            BranchKvValue decoded = BranchKvCodec.Decode(_stored[i]);
            total += decoded.HasPayload ? decoded.Payload.Length : 0;
        }
        return total;
    }

    /// <summary>
    /// Pre-optimization contrast: <c>data[1..]</c> copied the whole payload into a fresh array just to
    /// drop the marker. Allocation here scales linearly with <see cref="PayloadSize"/> — the waste the
    /// zero-copy slice removed.
    /// </summary>
    [Benchmark(Description = "EnvelopeDecode_CopyBaseline")]
    public long EnvelopeDecode_CopyBaseline()
    {
        long total = 0;
        for (int i = 0; i < RowCount; i++)
        {
            byte[] stored = _stored[i];
            byte[] payloadCopy = stored.AsSpan(1).ToArray();
            total += payloadCopy.Length;
        }
        return total;
    }

    // ── Row write (encode side) ───────────────────────────────────────────────

    /// <summary>
    /// Current path: serialize the row straight into its final storage array with byte 0 reserved for the
    /// Value marker — one allocation per row.
    /// </summary>
    [Benchmark(Description = "StorageEncode_SingleAlloc")]
    public int StorageEncode_SingleAlloc()
    {
        int total = 0;
        for (int i = 0; i < RowCount; i++)
            total += RowEncoder.EncodeStorageValue(_schema, _rows[i], RowId).Length;
        return total;
    }

    /// <summary>
    /// Pre-optimization contrast: encode the bare row, then <see cref="BranchKvCodec.EncodeValue"/>
    /// allocates a second array and copies the whole payload in to prepend the marker — two allocations
    /// and a payload-sized copy per row.
    /// </summary>
    [Benchmark(Description = "StorageEncode_TwoAllocBaseline")]
    public int StorageEncode_TwoAllocBaseline()
    {
        int total = 0;
        for (int i = 0; i < RowCount; i++)
        {
            byte[] bare = RowEncoder.Encode(_schema, _rows[i], RowId);
            total += BranchKvCodec.EncodeValue(bare).Length;
        }
        return total;
    }

    // ── Full storage round trip ───────────────────────────────────────────────

    /// <summary>
    /// The end-to-end path the audit's acceptance evidence asks for: encode a row to its final storage
    /// value, peel the envelope with a zero-copy slice, and decode the row from that slice — no payload
    /// copy at the storage boundary in either direction. This is a true storage round trip, not an
    /// isolated <see cref="RowEncoder.Decode"/>.
    /// </summary>
    [Benchmark(Description = "StorageRoundTrip_EncodeDecode")]
    public async Task<QueryRow> StorageRoundTrip_EncodeDecode()
    {
        RowEncoder.RowDecodeState cache = new();
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
        {
            byte[] storageValue = RowEncoder.EncodeStorageValue(_schema, _rows[i], RowId);
            BranchKvValue decoded = BranchKvCodec.Decode(storageValue);
            last = await RowEncoder.DecodeToQueryRowAsync(_schema, TxId, RowId, decoded.Payload,
            CamusDBOptions.Default,
                decodeState: cache).ConfigureAwait(false);
        }
        return last!;
    }
}
