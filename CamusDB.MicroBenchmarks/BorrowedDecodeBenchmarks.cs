
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Three-way decode-backing A/B — eager <c>ColumnValue[]</c> vs slot-backed vs borrowed
/// (<see cref="RowView"/>) — over a realistic scan segment (decode → filter via the real
/// <see cref="SqlExecutor.EvalExpr"/> → project only surviving rows). Answers the deferred Phase-6
/// question: does the borrowed path win, by how much, and where does it lose?
///
/// <para>Shapes:</para>
/// <list type="bullet">
///   <item><c>NumericFilterProject</c> — <c>WHERE f &lt; PassPercent</c>; the reject path never
///     materializes projection cells, and the borrowed path additionally skips the per-row
///     <c>ValueSlot[]</c> and decodes only the cells actually read.</item>
///   <item><c>StringEqualFilterProject</c> — <c>WHERE cat = 'c0'</c> (≈10% pass). On the borrowed path
///     the equality is byte-native, so the 90% rejected rows never materialize the <c>cat</c> string.</item>
///   <item><c>SelectStar</c> — every cell of every row read: the non-selective case where borrowed's
///     lazy re-read + cache has no reject to save on (the potential loss to watch).</item>
/// </list>
/// Allocated is per <see cref="RowCount"/> (1 000) rows.
/// </summary>
[Config(typeof(ValueSlotConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BorrowedDecodeBenchmarks
{
    public enum DecodeMode { Eager, Slot, Borrowed }

    [Params(1_000)]
    public int RowCount { get; set; }

    [Params(10, 90)]
    public int PassPercent { get; set; }

    [Params(DecodeMode.Eager, DecodeMode.Slot, DecodeMode.Borrowed)]
    public DecodeMode Mode { get; set; }

    private TableSchema _schema = null!;
    private byte[][] _payloads = null!;
    private int _filterOrd;
    private int[] _projOrds = null!;
    private NodeAst _numericPredicate = null!;
    private NodeAst _stringPredicate = null!;
    private static readonly ObjectIdValue RowId = new(1, 2, 3);
    private static readonly HLCTimestamp TxId = default;

    [GlobalSetup]
    public void GlobalSetup()
    {
        CamusDBConfig.SlotBackedDecode = Mode == DecodeMode.Slot;
        CamusDBConfig.BorrowedDecode = Mode == DecodeMode.Borrowed
            ? BorrowedDecodePolicy.ForceBorrowed
            : BorrowedDecodePolicy.ForceEager;

        _schema = new TableSchema
        {
            Id = "bench", Name = "bench", Version = 0,
            Columns =
            [
                Col("id",  ColumnType.Id),
                Col("f",   ColumnType.Integer64),   // numeric filter column
                Col("cat", ColumnType.String),      // low-cardinality string filter column
                Col("p1",  ColumnType.String),      // projection columns
                Col("p2",  ColumnType.Integer64),
                Col("p3",  ColumnType.Float64),
                Col("p4",  ColumnType.String),
            ],
        };
        _schema.SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = _schema.Columns }];

        _payloads = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            long f = i % 100; // passes numeric filter when f < PassPercent
            Dictionary<string, ColumnValue> row = new()
            {
                ["id"]  = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString()),
                ["f"]   = new ColumnValue(ColumnType.Integer64, f),
                ["cat"] = new ColumnValue(ColumnType.String, "c" + (i % 10)), // c0..c9 → 'c0' matches ~10%
                ["p1"]  = new ColumnValue(ColumnType.String, "s_" + i),
                ["p2"]  = new ColumnValue(ColumnType.Integer64, (long)(i * 3)),
                ["p3"]  = new ColumnValue(ColumnType.Float64, i + 0.5),
                ["p4"]  = new ColumnValue(ColumnType.String, "t_" + i),
            };
            _payloads[i] = RowEncoder.EncodeStorageValue(_schema, row, RowId);
        }

        RowEncoder.RowDecodeState warm = new();
        QueryRow sample = DecodeAsync(0, warm).GetAwaiter().GetResult();
        _filterOrd = sample.Layout.IndexOf("f");
        _projOrds = [sample.Layout.IndexOf("p1"), sample.Layout.IndexOf("p2"),
                     sample.Layout.IndexOf("p3"), sample.Layout.IndexOf("p4")];

        // `f < PassPercent`, through the real evaluator.
        NodeAst fIdent = new(NodeType.Identifier, null, null, null, null, null, null, null, "f");
        _numericPredicate = new NodeAst(NodeType.ExprLessThan, fIdent, NodeAst.FromLong(PassPercent),
            null, null, null, null, null, null);

        // `cat = 'c0'` — a string-equality predicate that exercises the byte-native fast path on Borrowed.
        NodeAst catIdent = new(NodeType.Identifier, null, null, null, null, null, null, null, "cat");
        NodeAst catLiteral = new(NodeType.String, null, null, null, null, null, null, null, "c0");
        _stringPredicate = new NodeAst(NodeType.ExprEquals, catIdent, catLiteral,
            null, null, null, null, null, null);
    }

    private static TableColumnSchema Col(string name, ColumnType type)
        => new(name, name, type, false, null, SchemaElementState.Public);

    private async Task<QueryRow> DecodeAsync(int i, RowEncoder.RowDecodeState cache)
    {
        BranchKvValue env = BranchKvCodec.Decode(_payloads[i]);
        return await RowEncoder.DecodeToQueryRowAsync(_schema, TxId, RowId, env.Payload, CamusDBConfig.Ambient, decodeState: cache)
            .ConfigureAwait(false);
    }

    [Benchmark(Description = "numeric filter+project")]
    public async Task<long> NumericFilterProject()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);
            if (!SqlExecutor.EvalExpr(_numericPredicate, row, null, null).BoolValue)
                continue;
            for (int p = 0; p < _projOrds.Length; p++)
                acc += row.GetColumnValue(_projOrds[p]).Type == ColumnType.Null ? 0 : 1;
        }
        return acc;
    }

    [Benchmark(Description = "string-equality filter+project")]
    public async Task<long> StringEqualFilterProject()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);
            if (!SqlExecutor.EvalExpr(_stringPredicate, row, null, null).BoolValue)
                continue;
            for (int p = 0; p < _projOrds.Length; p++)
                acc += row.GetColumnValue(_projOrds[p]).Type == ColumnType.Null ? 0 : 1;
        }
        return acc;
    }

    [Benchmark(Description = "select-star (all cells)")]
    public async Task<long> SelectStar()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);
            acc += row.Values.Length;
        }
        return acc;
    }
}
