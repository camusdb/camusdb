
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Allocation/throughput benchmarks for the covering-index (INCLUDE) value codec — the hot paths the
/// covering-index performance review (finding #4/#7) asked to quantify.
///
/// Two axes:
///   <see cref="IncludeCount"/> — number of stored/payload columns in the index.
///   <see cref="Wide"/>        — false: narrow Integer64 payloads; true: 64-char String payloads.
///
/// Each benchmark processes <see cref="RowCount"/> tuples per op, so "Allocated" reads per 1 000 rows,
/// matching the other tables in <c>BENCH-RESULTS.md</c>.
///
/// The headline contrast is <c>DecodeAll_Baseline</c> (the pre-optimization path: decode <b>every</b>
/// included column into a <see cref="CompositeColumnValue"/>) versus <c>DecodeProjectOne_Selective</c>
/// (the current path: decode only the one projected column, skipping the rest via
/// <see cref="RowEncoder.SkipColumnValue"/>, writing straight into the output array). The selective
/// path's allocation must stay ~flat as <see cref="IncludeCount"/> and <see cref="Wide"/> grow, while
/// the baseline scales with both — the evidence that adding INCLUDE columns does not tax a query that
/// projects only one of them.
/// </summary>
[Config(typeof(RowDecodeConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class IndexIncludeCodecBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    [Params(1, 4, 8, 16)]
    public int IncludeCount { get; set; }

    [Params(false, true)]
    public bool Wide { get; set; }

    private string[] _includeColumns = null!;
    private ColumnType[] _includeTypes = null!;

    // Plan projecting ONLY the last include position (worst case for the selective path: it must skip
    // every earlier column before reading the projected one). Output slot 0 receives it.
    private int[] _planProjectLast = null!;

    private Dictionary<string, ColumnValue>[] _rows = null!;   // encode inputs
    private byte[][] _tuples = null!;                          // decode inputs (encoded tuples)

    [GlobalSetup]
    public void GlobalSetup()
    {
        _includeColumns = new string[IncludeCount];
        _includeTypes = new ColumnType[IncludeCount];
        for (int c = 0; c < IncludeCount; c++)
        {
            _includeColumns[c] = "c" + c;
            _includeTypes[c] = Wide ? ColumnType.String : ColumnType.Integer64;
        }

        _planProjectLast = new int[IncludeCount];
        Array.Fill(_planProjectLast, -1);
        _planProjectLast[IncludeCount - 1] = 0;

        string wideValue = new('x', 64);

        _rows = new Dictionary<string, ColumnValue>[RowCount];
        _tuples = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            Dictionary<string, ColumnValue> row = new(IncludeCount);
            for (int c = 0; c < IncludeCount; c++)
            {
                row["c" + c] = Wide
                    ? new ColumnValue(ColumnType.String, wideValue + "_" + i)
                    : new ColumnValue(ColumnType.Integer64, (long)(i * 31 + c));
            }
            _rows[i] = row;
            _tuples[i] = IndexIncludeValueCodec.EncodeTuple(_includeColumns, row);
        }
    }

    // ── Write path (INSERT / backfill): serialize the include tuple per row ───────────────────

    [Benchmark(Description = "EncodeTuple")]
    public long EncodeTuple()
    {
        long total = 0;
        for (int i = 0; i < RowCount; i++)
            total += IndexIncludeValueCodec.EncodeTuple(_includeColumns, _rows[i]).Length;
        return total;
    }

    // ── Read path (covering scan): decode all vs decode only the projected column ─────────────

    /// <summary>
    /// Pre-optimization contrast: decode EVERY included column into a <see cref="CompositeColumnValue"/>
    /// (allocates a <see cref="ColumnValue"/>[] plus the composite plus, for Wide, N strings), regardless
    /// of how many columns the query projects. Allocation scales with <see cref="IncludeCount"/> and
    /// <see cref="Wide"/>.
    /// </summary>
    [Benchmark(Description = "DecodeAll_Baseline")]
    public int DecodeAll_Baseline()
    {
        int total = 0;
        for (int i = 0; i < RowCount; i++)
        {
            CompositeColumnValue all = IndexIncludeValueCodec.DecodeTuple(_includeTypes, _tuples[i]);
            total += all.Values.Length;
        }
        return total;
    }

    /// <summary>
    /// Current path: decode only the single projected column straight into the output array; the other
    /// <c>IncludeCount - 1</c> columns are skipped (advanced past, never materialized). Allocation must
    /// stay flat as <see cref="IncludeCount"/>/<see cref="Wide"/> grow — the E4 win.
    /// </summary>
    [Benchmark(Description = "DecodeProjectOne_Selective")]
    public int DecodeProjectOne_Selective()
    {
        int total = 0;
        ColumnValue[] output = new ColumnValue[1];
        for (int i = 0; i < RowCount; i++)
        {
            IndexIncludeValueCodec.DecodeTupleInto(_includeTypes, _planProjectLast, output, _tuples[i]);
            total += output[0] is null ? 0 : 1;
        }
        return total;
    }
}
