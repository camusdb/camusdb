
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// BenchmarkDotNet config that runs benchmarks in-process via <see cref="InProcessEmitToolchain"/>,
/// avoiding the subprocess / project-file-discovery path that fails when agent git worktrees
/// produce duplicate <c>CamusDB.MicroBenchmarks.csproj</c> entries under the solution root.
/// Allocation numbers come from <c>GC.GetAllocatedBytesForCurrentThread()</c> so MemoryDiagnoser
/// works correctly even without a separate process.
/// </summary>
public class RowDecodeConfig : ManualConfig
{
    public RowDecodeConfig()
    {
        AddJob(Job.MediumRun.WithToolchain(InProcessEmitToolchain.Instance));
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddLogger(ConsoleLogger.Default);
        AddExporter(MarkdownExporter.GitHub);
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}

/// <summary>
/// Microbenchmarks for the row-decode path — measures wall time and allocations for
/// the old dictionary decode vs the new layout-backed <see cref="QueryRow"/> decode.
///
/// Three shapes are covered:
///   FullRow   — all columns decoded, single-table (no visibility override).
///   Projected — a subset of columns via <c>requiredColumns</c>.
///   SchemaHistory — row was written under an older schema version; a new column is injected.
///
/// Each shape is benchmarked at 1 000 rows to give an allocations/row figure.
/// The three variants per shape reveal the allocation profile:
///   _Dictionary  — old path; one <c>Dictionary&lt;string,ColumnValue&gt;</c> per row.
///   _QueryRow    — new path with a per-scan layout cache; one <c>ColumnValue[]</c> per row.
///   _QueryRowNoCache — new path, cache disabled; shows the per-row layout build cost
///                      that existed before the layout-cache fix (captured for posterity).
///
/// Acceptance signal: _QueryRow allocations/row must be strictly lower than _Dictionary.
/// </summary>
[Config(typeof(RowDecodeConfig))]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class RowDecodeBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    // ── schema helpers ────────────────────────────────────────────────────────

    private static TableColumnSchema Col(string name, ColumnType type,
        SchemaElementState state = SchemaElementState.Public)
        => new(name, name, type, false, null, state);

    private static TableSchema MakeSchema(int version, params TableColumnSchema[] columns)
    {
        List<TableColumnSchema> cols = new(columns);
        List<TableSchemaHistory> history = new();
        for (int v = 0; v <= version; v++)
            history.Add(new TableSchemaHistory { Version = v, Columns = cols });

        return new TableSchema
        {
            Id    = "bench-table",
            Name  = "bench",
            Version      = version,
            Columns      = cols,
            SchemaHistory = history
        };
    }

    // ── benchmark state ───────────────────────────────────────────────────────

    private TableSchema _schemaFull     = null!;
    private TableSchema _schemaHistory  = null!;   // v1 with an added column
    private TableSchema _schemaOldVer   = null!;   // v0 sub-schema used to encode history rows

    private IReadOnlySet<string> _projected = null!;

    private byte[][] _fullRows     = null!;
    private byte[][] _projRows     = null!;
    private byte[][] _historyRows  = null!;   // encoded under v0 schema

    private static readonly ObjectIdValue RowId = new(1, 2, 3);
    private static readonly HLCTimestamp TxId   = default;

    [GlobalSetup]
    public void GlobalSetup()
    {
        // Full 5-column schema
        _schemaFull = MakeSchema(0,
            Col("id",       ColumnType.Id),
            Col("name",     ColumnType.String),
            Col("age",      ColumnType.Integer64),
            Col("score",    ColumnType.Float64),
            Col("active",   ColumnType.Bool));

        // Projected: only name + age
        _projected = new HashSet<string>(["name", "age"], StringComparer.Ordinal);

        // Schema-history: v0 has 3 columns; v1 adds "email".
        TableColumnSchema[] v0Cols = [
            Col("id",   ColumnType.Id),
            Col("name", ColumnType.String),
            Col("age",  ColumnType.Integer64),
        ];
        TableColumnSchema[] v1Cols = [
            Col("id",    ColumnType.Id),
            Col("name",  ColumnType.String),
            Col("age",   ColumnType.Integer64),
            Col("email", ColumnType.String, SchemaElementState.Public),
        ];

        // v0 sub-schema for encoding old rows
        _schemaOldVer = MakeSchema(0, v0Cols);

        // v1 schema with both history versions for decode
        _schemaHistory = new TableSchema
        {
            Id      = "hist-table",
            Name    = "hist",
            Version = 1,
            Columns = new List<TableColumnSchema>(v1Cols),
            SchemaHistory =
            [
                new TableSchemaHistory { Version = 0, Columns = new List<TableColumnSchema>(v0Cols) },
                new TableSchemaHistory { Version = 1, Columns = new List<TableColumnSchema>(v1Cols) },
            ]
        };

        // Pre-encode RowCount rows for each shape so decode benchmarks only measure the decode.
        _fullRows    = EncodeRows(_schemaFull,   RowCount, BuildFullRow);
        _projRows    = EncodeRows(_schemaFull,   RowCount, BuildFullRow);      // same bytes; projection is decode-side
        _historyRows = EncodeRows(_schemaOldVer, RowCount, BuildV0Row);
    }

    // ── encoding helpers ─────────────────────────────────────────────────────

    private static byte[][] EncodeRows(TableSchema schema, int count,
        Func<int, Dictionary<string, ColumnValue>> rowFactory)
    {
        byte[][] rows = new byte[count][];
        for (int i = 0; i < count; i++)
            rows[i] = RowEncoder.Encode(schema, rowFactory(i), RowId);
        return rows;
    }

    private static Dictionary<string, ColumnValue> BuildFullRow(int i) => new()
    {
        ["id"]     = new ColumnValue(ColumnType.Id,        new ObjectIdValue(i, i + 1, i + 2).ToString()),
        ["name"]   = new ColumnValue(ColumnType.String,    "user_" + i),
        ["age"]    = new ColumnValue(ColumnType.Integer64, (long)(20 + i % 80)),
        ["score"]  = new ColumnValue(ColumnType.Float64,   i * 1.5),
        ["active"] = ColumnValue.True,
    };

    private static Dictionary<string, ColumnValue> BuildV0Row(int i) => new()
    {
        ["id"]   = new ColumnValue(ColumnType.Id,        new ObjectIdValue(i, i + 1, i + 2).ToString()),
        ["name"] = new ColumnValue(ColumnType.String,    "user_" + i),
        ["age"]  = new ColumnValue(ColumnType.Integer64, (long)(20 + i % 80)),
    };

    // ── Full-row benchmarks ───────────────────────────────────────────────────

    [Benchmark(Baseline = true, Description = "FullRow_Dictionary")]
    public Dictionary<string, ColumnValue> FullRow_Dictionary()
    {
        // Old sync path — one Dictionary per row, no reuse.
        Dictionary<string, ColumnValue>? last = null;
        for (int i = 0; i < RowCount; i++)
            last = RowEncoder.Decode(_schemaFull, RowId, _fullRows[i]);
        return last!;
    }

    [Benchmark(Description = "FullRow_QueryRow")]
    public async Task<QueryRow> FullRow_QueryRow()
    {
        // New path with per-scan layout cache — layout built once per schema version.
        Dictionary<int, RowLayout> cache = new();
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeToQueryRowAsync(_schemaFull, TxId, RowId, _fullRows[i],
                layoutCache: cache).ConfigureAwait(false);
        return last!;
    }

    [Benchmark(Description = "FullRow_QueryRowNoCache")]
    public async Task<QueryRow> FullRow_QueryRowNoCache()
    {
        // New path WITHOUT cache — layout rebuilt per row; captures pre-fix regression.
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeToQueryRowAsync(_schemaFull, TxId, RowId, _fullRows[i],
                layoutCache: null).ConfigureAwait(false);
        return last!;
    }

    // ── Projected benchmarks (subset of columns) ──────────────────────────────

    [Benchmark(Description = "Projected_Dictionary")]
    public Dictionary<string, ColumnValue> Projected_Dictionary()
    {
        Dictionary<string, ColumnValue>? last = null;
        for (int i = 0; i < RowCount; i++)
            last = RowEncoder.Decode(_schemaFull, RowId, _projRows[i], _projected);
        return last!;
    }

    [Benchmark(Description = "Projected_QueryRow")]
    public async Task<QueryRow> Projected_QueryRow()
    {
        Dictionary<int, RowLayout> cache = new();
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeToQueryRowAsync(_schemaFull, TxId, RowId, _projRows[i],
                _projected, layoutCache: cache).ConfigureAwait(false);
        return last!;
    }

    // ── Schema-history benchmarks (row written under older schema) ────────────

    [Benchmark(Description = "SchemaHistory_Dictionary")]
    public async Task<Dictionary<string, ColumnValue>> SchemaHistory_Dictionary()
    {
        // Old async path — decodes v0 bytes under v1 schema, injects "email" default.
        Dictionary<string, ColumnValue>? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeAsync(_schemaHistory, TxId, RowId, _historyRows[i],
                visibilitySchemaVersion: 1).ConfigureAwait(false);
        return last!;
    }

    [Benchmark(Description = "SchemaHistory_QueryRow")]
    public async Task<QueryRow> SchemaHistory_QueryRow()
    {
        Dictionary<int, RowLayout> cache = new();
        QueryRow? last = null;
        for (int i = 0; i < RowCount; i++)
            last = await RowEncoder.DecodeToQueryRowAsync(_schemaHistory, TxId, RowId, _historyRows[i],
                visibilitySchemaVersion: 1, layoutCache: cache).ConfigureAwait(false);
        return last!;
    }
}
