
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Gated spike for the experimental <see cref="ValueSlot"/> value representation. Measures the two
/// things the adoption gate hinges on:
///   1. Decode allocation — a row of N scalar columns costs N per-cell <see cref="ColumnValue"/>
///      objects today vs a single <c>ValueSlot[]</c>. This is the headline win.
///   2. Copy cost on the hot compare path — a 32-byte struct passed by <c>in</c> vs an 8-byte
///      reference. This is the regression risk that a naive class→struct swap would hit.
/// It also measures the boundary cost (decode to slots, then convert every cell back to
/// <see cref="ColumnValue"/>) so the "row escapes to the API" case is not hidden.
///
/// Both decoders read the SAME stored bytes with the SAME <see cref="Serializator"/> primitives and
/// walk the columns in stream order into an ordinal array; they differ ONLY in the per-cell target
/// (<see cref="ColumnValue"/> object vs inline <see cref="ValueSlot"/>), so the delta isolates the
/// per-cell allocation and the copy behavior — nothing else.
///
/// Allocated is reported per <see cref="RowCount"/> rows (1 000), matching the other tables in
/// BENCH-RESULTS.md.
/// </summary>
/// <summary>
/// Fast in-process config for the gated spike: a single launch with short warmup/iteration counts.
/// Allocation is deterministic (does not need MediumRun's statistical rigor) and elapsed just needs
/// to be directionally trustworthy, so ShortRun keeps the 25-case matrix under a couple of minutes.
/// </summary>
public class ValueSlotConfig : ManualConfig
{
    public ValueSlotConfig()
    {
        AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance));
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddLogger(ConsoleLogger.Default);
        AddExporter(MarkdownExporter.GitHub);
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}

[Config(typeof(ValueSlotConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ValueSlotBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // Row shapes chosen to stress different corners of the value representation:
    //   numeric   — all-scalar, the best case (every cell is a heap object today, zero with slots)
    //   string    — string-heavy, where the payload alloc is unavoidable and shared by both paths
    //   uuid_id   — Id + Uuid, exercising the inline 96-bit / 128-bit packing (Id kills a 24-char string)
    //   wide      — 20 mixed columns, amplifying per-cell object count
    //   null_heavy — mostly NULL, where slots reuse a shared value and the class reuses a singleton
    [Params("numeric", "string", "uuid_id", "wide", "null_heavy")]
    public string Shape { get; set; } = "numeric";

    private ColumnType[] _types = null!;
    private byte[][] _stored = null!;

    private static readonly ObjectIdValue RowId = new(7, 8, 9);

    [GlobalSetup]
    public void GlobalSetup()
    {
        (TableSchema schema, ColumnType[] types) = BuildSchema(Shape);
        _types = types;

        _stored = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> row = BuildRow(Shape, schema, i);
            _stored[i] = RowEncoder.Encode(schema, row, RowId);
        }
    }

    // ── Benchmarks ─────────────────────────────────────────────────────────────

    /// <summary>Current shape: one <see cref="ColumnValue"/> object per cell, into a ColumnValue[].</summary>
    [Benchmark(Baseline = true, Description = "Decode_ColumnValue")]
    public ColumnValue[] Decode_ColumnValue()
    {
        ColumnValue[] last = null!;
        for (int i = 0; i < RowCount; i++)
            last = DecodeToColumnValues(_stored[i]);
        return last;
    }

    /// <summary>Candidate: inline slots into a single ValueSlot[], zero per-cell heap objects for scalars.</summary>
    [Benchmark(Description = "Decode_ValueSlot")]
    public ValueSlot[] Decode_ValueSlot()
    {
        ValueSlot[] last = null!;
        for (int i = 0; i < RowCount; i++)
            last = DecodeToValueSlots(_stored[i]);
        return last;
    }

    /// <summary>
    /// Realistic "row leaves the engine" cost: decode to slots, then materialize every cell as a
    /// <see cref="ColumnValue"/>. If the slot path only pays this when a row actually escapes (and
    /// most intermediate rows are filtered/aggregated away), this is the worst case for adoption.
    /// </summary>
    [Benchmark(Description = "Decode_ValueSlot_ThenConvert")]
    public ColumnValue[] Decode_ValueSlot_ThenConvert()
    {
        ColumnValue[] last = null!;
        for (int i = 0; i < RowCount; i++)
        {
            ValueSlot[] slots = DecodeToValueSlots(_stored[i]);
            ColumnValue[] converted = new ColumnValue[slots.Length];
            for (int c = 0; c < slots.Length; c++)
                converted[c] = slots[c].ToColumnValue();
            last = converted;
        }
        return last;
    }

    /// <summary>Hot compare loop over the reference type (sort/hash-key style, first column each pair).</summary>
    [Benchmark(Description = "Compare_ColumnValue")]
    public long Compare_ColumnValue()
    {
        ColumnValue[][] rows = new ColumnValue[RowCount][];
        for (int i = 0; i < RowCount; i++) rows[i] = DecodeToColumnValues(_stored[i]);

        long acc = 0;
        for (int i = 1; i < RowCount; i++)
            acc += rows[i][0].CompareTo(rows[i - 1][0]);
        return acc;
    }

    /// <summary>Same compare loop over slots, passed by <c>in</c> — the copy-cost regression probe.</summary>
    [Benchmark(Description = "Compare_ValueSlot")]
    public long Compare_ValueSlot()
    {
        ValueSlot[][] rows = new ValueSlot[RowCount][];
        for (int i = 0; i < RowCount; i++) rows[i] = DecodeToValueSlots(_stored[i]);

        long acc = 0;
        for (int i = 1; i < RowCount; i++)
            acc += rows[i][0].CompareTo(rows[i - 1][0]);
        return acc;
    }

    // ── Local decoders — mirror RowEncoder.ReadColumnValue, differ only in the per-cell target ──

    private ColumnValue[] DecodeToColumnValues(byte[] stored)
    {
        ReadOnlySpan<byte> data = stored;
        int pointer = 0;
        SkipRowHeader(data, ref pointer);

        ColumnValue[] values = new ColumnValue[_types.Length];
        for (int i = 0; i < _types.Length; i++)
            values[i] = ReadColumnValue(_types[i], data, ref pointer);
        return values;
    }

    private ValueSlot[] DecodeToValueSlots(byte[] stored)
    {
        ReadOnlySpan<byte> data = stored;
        int pointer = 0;
        SkipRowHeader(data, ref pointer);

        ValueSlot[] values = new ValueSlot[_types.Length];
        for (int i = 0; i < _types.Length; i++)
            values[i] = ReadValueSlot(_types[i], data, ref pointer);
        return values;
    }

    private static void SkipRowHeader(ReadOnlySpan<byte> data, ref int pointer)
    {
        Serializator.ReadType(data, ref pointer);       // schema type marker
        Serializator.ReadInt32(data, ref pointer);      // schema version
        Serializator.ReadType(data, ref pointer);       // rowId type marker
        Serializator.ReadObjectId(data, ref pointer);   // rowId
    }

    private static ColumnValue ReadColumnValue(ColumnType type, ReadOnlySpan<byte> data, ref int pointer)
    {
        int t = Serializator.ReadType(data, ref pointer);
        if (t == SerializatorTypes.TypeNull) return ColumnValue.Null;

        return type switch
        {
            ColumnType.Id        => new ColumnValue(ColumnType.Id, Serializator.ReadObjectId(data, ref pointer).ToString()),
            ColumnType.Integer64 => new ColumnValue(ColumnType.Integer64, Serializator.ReadInt64(data, ref pointer)),
            ColumnType.String    => new ColumnValue(ColumnType.String, Serializator.ReadString(data, ref pointer)),
            ColumnType.Float64   => new ColumnValue(ColumnType.Float64, Serializator.ReadDouble(data, ref pointer)),
            ColumnType.Bool      => ColumnValue.FromBool(Serializator.ReadBool(data, ref pointer)),
            ColumnType.Float32   => new ColumnValue(ColumnType.Float32, Serializator.ReadFloat(data, ref pointer)),
            ColumnType.Date      => new ColumnValue(ColumnType.Date, Serializator.ReadInt64(data, ref pointer)),
            ColumnType.DateTime  => new ColumnValue(ColumnType.DateTime, Serializator.ReadInt64(data, ref pointer)),
            ColumnType.Uuid      => ReadUuidColumnValue(data, ref pointer),
            _ => throw new Exception("Unsupported bench type " + type),
        };
    }

    private static ColumnValue ReadUuidColumnValue(ReadOnlySpan<byte> data, ref int pointer)
    {
        (long high, long low) = Serializator.ReadUuid(data, ref pointer);
        return new ColumnValue(ColumnType.Uuid, high, low);
    }

    private static ValueSlot ReadValueSlot(ColumnType type, ReadOnlySpan<byte> data, ref int pointer)
    {
        int t = Serializator.ReadType(data, ref pointer);
        if (t == SerializatorTypes.TypeNull) return ValueSlot.Null;

        switch (type)
        {
            case ColumnType.Id:        return ValueSlot.FromId(Serializator.ReadObjectId(data, ref pointer));
            case ColumnType.Integer64: return ValueSlot.FromLong(ColumnType.Integer64, Serializator.ReadInt64(data, ref pointer));
            case ColumnType.String:    return ValueSlot.FromString(Serializator.ReadString(data, ref pointer));
            case ColumnType.Float64:   return ValueSlot.FromDouble(ColumnType.Float64, Serializator.ReadDouble(data, ref pointer));
            case ColumnType.Bool:      return ValueSlot.FromBool(Serializator.ReadBool(data, ref pointer));
            case ColumnType.Float32:   return ValueSlot.FromDouble(ColumnType.Float32, Serializator.ReadFloat(data, ref pointer));
            case ColumnType.Date:      return ValueSlot.FromLong(ColumnType.Date, Serializator.ReadInt64(data, ref pointer));
            case ColumnType.DateTime:  return ValueSlot.FromLong(ColumnType.DateTime, Serializator.ReadInt64(data, ref pointer));
            case ColumnType.Uuid:
            {
                (long high, long low) = Serializator.ReadUuid(data, ref pointer);
                return ValueSlot.FromUuid(high, low);
            }
            default: throw new Exception("Unsupported bench type " + type);
        }
    }

    // ── Shape builders ─────────────────────────────────────────────────────────

    private static TableColumnSchema Col(string name, ColumnType type)
        => new(name, name, type, false, null, SchemaElementState.Public);

    private static (TableSchema, ColumnType[]) BuildSchema(string shape)
    {
        ColumnType[] types = shape switch
        {
            "numeric"    => [ColumnType.Id, ColumnType.Integer64, ColumnType.Integer64, ColumnType.Float64, ColumnType.Bool, ColumnType.Date],
            "string"     => [ColumnType.Id, ColumnType.String, ColumnType.String, ColumnType.String],
            "uuid_id"    => [ColumnType.Id, ColumnType.Uuid, ColumnType.Uuid, ColumnType.Integer64],
            "null_heavy" => [ColumnType.Id, ColumnType.Integer64, ColumnType.String, ColumnType.Float64, ColumnType.Bool, ColumnType.Uuid],
            "wide"       => BuildWideTypes(),
            _ => throw new ArgumentException("Unknown shape " + shape),
        };

        TableColumnSchema[] columns = new TableColumnSchema[types.Length];
        for (int i = 0; i < types.Length; i++)
            columns[i] = Col("c" + i, types[i]);

        TableSchema schema = new()
        {
            Id = "bench",
            Name = "bench",
            Version = 0,
            Columns = [.. columns],
        };
        schema.SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = schema.Columns }];
        return (schema, types);
    }

    private static ColumnType[] BuildWideTypes()
    {
        ColumnType[] cycle = [ColumnType.Integer64, ColumnType.String, ColumnType.Float64, ColumnType.Bool, ColumnType.Date, ColumnType.Uuid];
        ColumnType[] types = new ColumnType[20];
        types[0] = ColumnType.Id;
        for (int i = 1; i < types.Length; i++)
            types[i] = cycle[(i - 1) % cycle.Length];
        return types;
    }

    private static Dictionary<string, ColumnValue> BuildRow(string shape, TableSchema schema, int i)
    {
        Dictionary<string, ColumnValue> row = new(schema.Columns!.Count);
        for (int c = 0; c < schema.Columns.Count; c++)
        {
            TableColumnSchema col = schema.Columns[c];

            // null_heavy: every non-id column past the first two is NULL.
            if (shape == "null_heavy" && c >= 2)
            {
                row[col.Name] = ColumnValue.Null;
                continue;
            }

            row[col.Name] = BuildValue(col.Type, i, c);
        }
        return row;
    }

    private static ColumnValue BuildValue(ColumnType type, int i, int c) => type switch
    {
        ColumnType.Id        => new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + c, i + c + 1).ToString()),
        ColumnType.Integer64 => new ColumnValue(ColumnType.Integer64, (long)(i * 31 + c)),
        ColumnType.String    => new ColumnValue(ColumnType.String, "s_" + i + "_" + c),
        ColumnType.Float64   => new ColumnValue(ColumnType.Float64, i + c * 0.5),
        ColumnType.Float32   => new ColumnValue(ColumnType.Float32, (double)(i + c)),
        ColumnType.Bool      => ColumnValue.FromBool((i + c) % 2 == 0),
        ColumnType.Date      => new ColumnValue(ColumnType.Date, new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks + i),
        ColumnType.DateTime  => new ColumnValue(ColumnType.DateTime, new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks + i),
        ColumnType.Uuid      => ColumnValue.FromUuid(new Guid(i, (short)c, 0, 1, 2, 3, 4, 5, 6, 7, 8)),
        _ => throw new Exception("Unsupported bench value type " + type),
    };
}
