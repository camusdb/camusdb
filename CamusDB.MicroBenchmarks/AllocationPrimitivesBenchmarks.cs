
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Allocation benchmarks for the low-risk decode primitives:
///
///   ProjectedSkip_WideExcluded — decodes a narrow projection ({keep, age}) over rows that also carry
///     a large String column ("big") which is projected OUT. The excluded column's payload is sized by
///     <see cref="ExcludedStringSize"/>. Because a skipped String/Bytes value is now advanced past
///     without materializing a managed string, allocated-bytes/row must stay essentially FLAT as the
///     excluded payload grows from 16 B to 64 KB. A rising curve would mean the skip still allocates.
///
///   NullBoolHeavyDecode — decodes rows whose cells are mostly NULL and Bool. Those two types resolve
///     to the shared <see cref="ColumnValue.Null"/>/<see cref="ColumnValue.True"/>/<see cref="ColumnValue.False"/>
///     singletons instead of a fresh object per cell, so allocation is dominated by the owning
///     <c>ColumnValue[]</c> per row, not per-cell scalars.
///
/// These use only public <see cref="RowEncoder"/> APIs so they run in-process via the shared config.
/// </summary>
[Config(typeof(RowDecodeConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class AllocationPrimitivesBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // Size in UTF-16 chars of the projected-OUT "big" String column. Flat allocation across these
    // values is the acceptance signal for allocation-free skip.
    [Params(16, 4_096, 65_536)]
    public int ExcludedStringSize { get; set; }

    private static TableColumnSchema Col(string name, ColumnType type)
        => new(name, name, type, false, null, SchemaElementState.Public);

    private static TableSchema MakeSchema(params TableColumnSchema[] columns)
    {
        List<TableColumnSchema> cols = new(columns);
        return new TableSchema
        {
            Id            = "bench-table",
            Name          = "bench",
            Version       = 0,
            Columns       = cols,
            SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = cols }],
        };
    }

    private static readonly ObjectIdValue RowId = new(1, 2, 3);
    private static readonly HLCTimestamp TxId = default;

    private TableSchema _wideSchema = null!;
    private IReadOnlySet<string> _narrowProjection = null!;
    private byte[][] _wideRows = null!;

    private TableSchema _nullBoolSchema = null!;
    private byte[][] _nullBoolRows = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _wideSchema = MakeSchema(
            Col("id", ColumnType.Id),
            Col("keep", ColumnType.String),
            Col("big", ColumnType.String),
            Col("age", ColumnType.Integer64),
            Col("active", ColumnType.Bool));

        _narrowProjection = new HashSet<string>(["keep", "age"], StringComparer.Ordinal);

        string bigValue = new('x', ExcludedStringSize);
        _wideRows = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> row = new()
            {
                ["id"]     = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString()),
                ["keep"]   = new ColumnValue(ColumnType.String, "k_" + i),
                ["big"]    = new ColumnValue(ColumnType.String, bigValue),
                ["age"]    = new ColumnValue(ColumnType.Integer64, (long)(20 + i % 80)),
                ["active"] = ColumnValue.True,
            };
            _wideRows[i] = RowEncoder.Encode(_wideSchema, row, RowId);
        }

        // Mostly-NULL / Bool row: 2 present scalars, 6 null/bool cells.
        _nullBoolSchema = MakeSchema(
            Col("id", ColumnType.Id),
            Col("age", ColumnType.Integer64),
            Col("b0", ColumnType.Bool),
            Col("b1", ColumnType.Bool),
            Col("b2", ColumnType.Bool),
            Col("n0", ColumnType.String),
            Col("n1", ColumnType.Integer64),
            Col("n2", ColumnType.Float64));

        _nullBoolRows = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> row = new()
            {
                ["id"]  = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString()),
                ["age"] = new ColumnValue(ColumnType.Integer64, (long)i),
                ["b0"]  = ColumnValue.FromBool((i & 1) == 0),
                ["b1"]  = ColumnValue.False,
                ["b2"]  = ColumnValue.True,
                ["n0"]  = ColumnValue.Null,
                ["n1"]  = ColumnValue.Null,
                ["n2"]  = ColumnValue.Null,
            };
            _nullBoolRows[i] = RowEncoder.Encode(_nullBoolSchema, row, RowId);
        }
    }

    [Benchmark(Description = "ProjectedSkip_WideExcluded")]
    public async Task<QueryRow> ProjectedSkip_WideExcluded()
    {
        RowEncoder.RowDecodeState cache = new();
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeToQueryRowAsync(_wideSchema, TxId, RowId, _wideRows[i],
                _narrowProjection, decodeState: cache).ConfigureAwait(false);
        return last!;
    }

    [Benchmark(Description = "NullBoolHeavyDecode")]
    public async Task<QueryRow> NullBoolHeavyDecode()
    {
        RowEncoder.RowDecodeState cache = new();
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeToQueryRowAsync(_nullBoolSchema, TxId, RowId, _nullBoolRows[i],
                decodeState: cache).ConfigureAwait(false);
        return last!;
    }
}
