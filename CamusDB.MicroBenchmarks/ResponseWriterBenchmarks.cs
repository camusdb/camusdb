
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

// The benchmarks project still carries an earlier standalone ValueSlot prototype; the sink path
// under test uses the adopted Core cell, so alias it explicitly.
using CoreValueSlot = CamusDB.Core.CommandsExecutor.Models.ValueSlot;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Allocation benchmarks for the REST query response row writer. The old path built a
/// <c>List&lt;object?[]&gt;</c> with <see cref="CompactRowEncoder.EncodeRow"/> (one array per row plus a
/// box per scalar / a <c>long[]</c> per UUID / a nested array per Array) and let System.Text.Json
/// serialize that graph; the new path streams each row straight to the <see cref="Utf8JsonWriter"/> via
/// <see cref="CompactRowJsonWriter"/>. Both write to the same buffer, so the delta is exactly the
/// intermediate object graph the streaming writer removes.
///
/// Allocated is per <see cref="RowCount"/> rows (1 000). The <c>Rows</c> value is the schema-aligned
/// result buffer that both paths share; only the encode-to-JSON step differs.
/// </summary>
[Config(typeof(ValueSlotConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ResponseWriterBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // numeric: all boxable scalars (worst case for the old boxing graph); mixed: adds Id/Uuid/Bytes.
    [Params("numeric", "mixed")]
    public string Shape { get; set; } = "numeric";

    private DerivedColumnSchema[] _schema = null!;
    private QueryResultRow[] _rows = null!;
    private QueryResultRow[] _slotRows = null!;
    private JsonSerializerOptions _options = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        _schema = Shape == "numeric"
            ?
            [
                new("a", ColumnType.Integer64), new("b", ColumnType.Integer64),
                new("c", ColumnType.Float64),   new("d", ColumnType.Bool), new("e", ColumnType.Integer64),
            ]
            :
            [
                new("id", ColumnType.Id), new("n", ColumnType.Integer64),
                new("s", ColumnType.String), new("u", ColumnType.Uuid), new("by", ColumnType.Bytes),
            ];

        byte[] payload = [1, 2, 3, 4, 5, 6, 7, 8];
        _rows = new QueryResultRow[RowCount];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> dict = new(StringComparer.Ordinal);
            if (Shape == "numeric")
            {
                dict["a"] = new ColumnValue(ColumnType.Integer64, (long)i);
                dict["b"] = new ColumnValue(ColumnType.Integer64, (long)(i * 7));
                dict["c"] = new ColumnValue(ColumnType.Float64, i + 0.25);
                dict["d"] = ColumnValue.FromBool(i % 2 == 0);
                dict["e"] = new ColumnValue(ColumnType.Integer64, (long)(-i));
            }
            else
            {
                dict["id"] = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString());
                dict["n"] = new ColumnValue(ColumnType.Integer64, (long)i);
                dict["s"] = new ColumnValue(ColumnType.String, "row_" + i);
                dict["u"] = ColumnValue.FromUuid(new Guid(i, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8));
                dict["by"] = new ColumnValue(payload);
            }
            _rows[i] = new QueryResultRow(new ObjectIdValue(i, i + 1, i + 2), dict);
        }

        // Same values as slot-backed QueryRows — the backing filtered scans produce. Serializing them
        // repeatedly is safe: the sink's TryGetSlot path never materializes or caches a cell.
        RowLayout layout = new(_schema.Select(c => c.Name));
        _slotRows = new QueryResultRow[RowCount];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> dict = (Dictionary<string, ColumnValue>)_rows[i].Row;
            CoreValueSlot[] slots = new CoreValueSlot[layout.Count];
            for (int c = 0; c < _schema.Length; c++)
                slots[layout.IndexOf(_schema[c].Name)] = CoreValueSlot.FromColumnValue(dict[_schema[c].Name]);
            ObjectIdValue id = new(i, i + 1, i + 2);
            _slotRows[i] = new QueryResultRow(id, QueryRow.FromSlots(id, layout, slots));
        }
    }

    /// <summary>Old path: object?[] per row + boxes, then STJ serializes the graph.</summary>
    [Benchmark(Baseline = true, Description = "Encode_ObjectGraph")]
    public long Encode_ObjectGraph()
    {
        ArrayBufferWriter<byte> buffer = new(1 << 16);
        using Utf8JsonWriter writer = new(buffer);

        List<object?[]> encoded = new(RowCount);
        for (int i = 0; i < RowCount; i++)
            encoded.Add(CompactRowEncoder.EncodeRow(_rows[i].Row, _schema));
        JsonSerializer.Serialize(writer, encoded, _options);

        return buffer.WrittenCount;
    }

    /// <summary>New path: stream each row directly to the writer, no intermediate graph.</summary>
    [Benchmark(Description = "Encode_StreamDirect")]
    public long Encode_StreamDirect()
    {
        ArrayBufferWriter<byte> buffer = new(1 << 16);
        using Utf8JsonWriter writer = new(buffer);

        RowLayout? boundLayout = null;
        int[]? ordinals = null;
        writer.WriteStartArray();
        for (int i = 0; i < RowCount; i++)
            CompactRowJsonWriter.WriteRow(writer, _rows[i], _schema, ref boundLayout, ref ordinals);
        writer.WriteEndArray();
        writer.Flush();

        return buffer.WrittenCount;
    }

    /// <summary>
    /// Slot-direct path: rows are slot-backed (as a filtered scan produces them) and each projected
    /// cell streams straight from its <see cref="ValueSlot"/> — no <see cref="ColumnValue"/> is ever
    /// materialized at the sink. The delta vs <see cref="Encode_StreamDirect"/> is the per-cell
    /// boundary conversion this path removes.
    /// </summary>
    [Benchmark(Description = "Encode_StreamDirect_Slots")]
    public long Encode_StreamDirect_Slots()
    {
        ArrayBufferWriter<byte> buffer = new(1 << 16);
        using Utf8JsonWriter writer = new(buffer);

        RowLayout? boundLayout = null;
        int[]? ordinals = null;
        writer.WriteStartArray();
        for (int i = 0; i < RowCount; i++)
            CompactRowJsonWriter.WriteRow(writer, _slotRows[i], _schema, ref boundLayout, ref ordinals);
        writer.WriteEndArray();
        writer.Flush();

        return buffer.WrittenCount;
    }
}
