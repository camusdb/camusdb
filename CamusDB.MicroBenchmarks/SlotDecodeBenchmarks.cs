
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
/// End-to-end allocation A/B for slot-backed decode (<see cref="CamusDBConfig.SlotBackedDecode"/>).
/// Models the real scan segment: the decode plan is already pruned to the required columns (filter +
/// projection), each row is decoded, the filter column is read, and only rows that pass the predicate
/// have their projection cells read (materialized). The <c>Slot</c> param flips the config flag so the
/// eager <c>ColumnValue[]</c> path and the slot path are measured on the same machine.
///
/// Expectation: the slot path wins in proportion to how many rows the filter rejects — a rejected row
/// decodes to slots but never materializes its projection cells, whereas the eager path already built
/// them at decode. A fully non-selective consume (SELECT *) is roughly neutral (slot-array overhead vs
/// the saved boxes), which the <c>SelectStar</c> benchmark shows. Allocated is per <see cref="RowCount"/>
/// (1 000) rows.
/// </summary>
[Config(typeof(ValueSlotConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class SlotDecodeBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // Fraction of rows that pass the WHERE predicate (so 90 => most rows materialize projection cells,
    // 10 => most are rejected and the slot path should save the most).
    [Params(10, 90)]
    public int PassPercent { get; set; }

    [Params(true, false)]
    public bool Slot { get; set; }

    private TableSchema _schema = null!;
    private byte[][] _payloads = null!;
    private int _filterOrd;
    private int[] _projOrds = null!;
    private NodeAst _predicate = null!;
    private static readonly ObjectIdValue RowId = new(1, 2, 3);
    private static readonly HLCTimestamp TxId = default;

    [GlobalSetup]
    public void GlobalSetup()
    {
        CamusDBConfig.SlotBackedDecode = Slot;

        _schema = new TableSchema
        {
            Id = "bench", Name = "bench", Version = 0,
            Columns =
            [
                Col("id", ColumnType.Id),
                Col("f",  ColumnType.Integer64),   // filter column
                Col("p1", ColumnType.String),      // projection columns
                Col("p2", ColumnType.Integer64),
                Col("p3", ColumnType.Float64),
                Col("p4", ColumnType.String),
            ],
        };
        _schema.SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = _schema.Columns }];

        _payloads = new byte[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            // f in [0,100): a row "passes" when f < PassPercent.
            long f = i % 100;
            Dictionary<string, ColumnValue> row = new()
            {
                ["id"] = new ColumnValue(ColumnType.Id, new ObjectIdValue(i, i + 1, i + 2).ToString()),
                ["f"]  = new ColumnValue(ColumnType.Integer64, f),
                ["p1"] = new ColumnValue(ColumnType.String, "s_" + i),
                ["p2"] = new ColumnValue(ColumnType.Integer64, (long)(i * 3)),
                ["p3"] = new ColumnValue(ColumnType.Float64, i + 0.5),
                ["p4"] = new ColumnValue(ColumnType.String, "t_" + i),
            };
            byte[] storage = RowEncoder.EncodeStorageValue(_schema, row, RowId);
            _payloads[i] = storage;
        }

        // Ordinals are resolved once from a decoded row's layout (shared across the run).
        RowEncoder.RowDecodeState warm = new();
        QueryRow sample = DecodeAsync(0, warm).GetAwaiter().GetResult();
        _filterOrd = sample.Layout.IndexOf("f");
        _projOrds = [sample.Layout.IndexOf("p1"), sample.Layout.IndexOf("p2"),
                     sample.Layout.IndexOf("p3"), sample.Layout.IndexOf("p4")];

        // Predicate AST for `f < PassPercent`, evaluated through the real SqlExecutor.EvalExpr path.
        NodeAst ident = new(NodeType.Identifier, null, null, null, null, null, null, null, "f");
        _predicate = new NodeAst(NodeType.ExprLessThan, ident, NodeAst.FromLong(PassPercent),
            null, null, null, null, null, null);
    }

    private static TableColumnSchema Col(string name, ColumnType type)
        => new(name, name, type, false, null, SchemaElementState.Public);

    private async Task<QueryRow> DecodeAsync(int i, RowEncoder.RowDecodeState cache)
    {
        BranchKvValue env = BranchKvCodec.Decode(_payloads[i]);
        return await RowEncoder.DecodeToQueryRowAsync(_schema, TxId, RowId, env.Payload, decodeState: cache)
            .ConfigureAwait(false);
    }

    /// <summary>Scan → filter → project: only rows passing the predicate materialize projection cells.</summary>
    [Benchmark(Description = "scan+filter+project")]
    public async Task<long> FilterProject()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);

            // Filter: read only the filter cell.
            if (row.GetColumnValue(_filterOrd).LongValue >= PassPercent)
                continue;

            // Passing rows: materialize the projection cells (the sink/projector would).
            for (int p = 0; p < _projOrds.Length; p++)
                acc += row.GetColumnValue(_projOrds[p]).Type == ColumnType.Null ? 0 : 1;
        }
        return acc;
    }

    /// <summary>
    /// The production filter path: predicate evaluated through <see cref="SqlExecutor.EvalExpr"/>, which
    /// reads the referenced column via per-cell <c>GetColumnValue</c> — not whole-row <c>Values</c> — so
    /// a rejected row never materializes its projection cells. Only passing rows materialize projection
    /// cells. Confirms the selective win is realized through the actual evaluator, not a hand-coded read.
    /// </summary>
    [Benchmark(Description = "eval-filter+project (real EvalExpr)")]
    public async Task<long> EvalFilterProject()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);

            if (!SqlExecutor.EvalExpr(_predicate, row, null, null).BoolValue)
                continue;

            for (int p = 0; p < _projOrds.Length; p++)
                acc += row.GetColumnValue(_projOrds[p]).Type == ColumnType.Null ? 0 : 1;
        }
        return acc;
    }

    /// <summary>
    /// Hash-operator key extraction (GROUP BY / DISTINCT): every row is probed by its key columns only.
    /// Models the case where the scan decoded more columns than the key — the per-cell path materializes
    /// just the key cells, not the whole row. NOTE: in the real pipeline the decode plan is pruned to the
    /// columns the query needs, so the decoded layout usually equals the key set and this subset case is
    /// uncommon; it characterizes the ceiling of the whole-row-trap removal, not a typical query.
    /// </summary>
    [Benchmark(Description = "distinct/group-by keys (subset of decoded)")]
    public async Task<long> KeySubset()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);
            // Two key columns read per row (e.g. GROUP BY p1, p2) — the rest of the decoded row is untouched.
            acc += row.GetColumnValue(_projOrds[0]).Type == ColumnType.Null ? 0 : 1;
            acc += row.GetColumnValue(_projOrds[1]).Type == ColumnType.Null ? 0 : 1;
        }
        return acc;
    }

    /// <summary>SELECT *: every cell of every row is read — the non-selective, fully-materializing case.</summary>
    [Benchmark(Description = "select-star (all cells)")]
    public async Task<long> SelectStar()
    {
        RowEncoder.RowDecodeState cache = new();
        long acc = 0;
        for (int i = 0; i < RowCount; i++)
        {
            QueryRow row = await DecodeAsync(i, cache).ConfigureAwait(false);
            ColumnValue[] values = row.Values;
            acc += values.Length;
        }
        return acc;
    }
}
