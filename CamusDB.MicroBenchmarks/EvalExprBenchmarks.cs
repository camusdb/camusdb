
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Isolated micro-benchmarks for EvalExpr — measures pure expression-interpretation cost
/// with no KV I/O, one EvalExpr call per benchmark iteration.
///
/// Pair each result against the corresponding shape in <see cref="FullQueryBenchmarks"/>
/// to estimate what fraction of end-to-end query time expression interpretation consumes.
///
/// All benchmarks use a <see cref="QueryRowNameResolver"/> matching real query execution:
///   - base benchmarks use a single-source (bare-key) resolver — the common single-table path
///   - _Qualified variants use a two-source (alias.column-key) resolver — the join path,
///     which exercises TrySplitQualified + FormatQualifiedKey (string allocation) per
///     identifier. These are the shapes Task 4 targets for elimination.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class EvalExprBenchmarks
{
    // ── expression AST nodes (shared across resolver variants) ───────────────

    private NodeAst _filterAnd = null!;        // a > 10 AND b < 20
    private NodeAst _projectionAdd = null!;    // a + 1
    private NodeAst _projectionUpper = null!;  // upper(name)
    private NodeAst _groupKey = null!;         // category  (identifier)
    private NodeAst _aggregateArg = null!;     // value      (identifier, arg to SUM)
    private NodeAst _likePrefix = null!;       // name LIKE 'prefix%'
    private NodeAst _likeContains = null!;     // name LIKE '%contains%'
    private NodeAst _updateExpr = null!;       // upper(name)  (idempotent UPDATE SET expr)
    private NodeAst _inSmall = null!;          // value IN (1,2,3,4)
    private NodeAst _inLarge = null!;          // value IN (1..1000)

    // ── row dictionaries ─────────────────────────────────────────────────────

    // Bare keys — used with _resolverBare (single-table path, no qualification).
    private Dictionary<string, ColumnValue> _row = null!;

    // Qualified keys ("bench.col") — used with _resolverQualified (join path).
    private Dictionary<string, ColumnValue> _rowQualified = null!;

    // ── resolvers ────────────────────────────────────────────────────────────

    // Single-source resolver: UsesQualifiedRowKeys() == false → returns bare column
    // name after a columnNameToAliases dict lookup. This is the real single-table path.
    private QueryRowNameResolver _resolverBare = null!;

    // Two-source resolver: UsesQualifiedRowKeys() == true → splits the identifier,
    // checks aliasToSource, then formats "bench.col" (allocates a string). This is
    // the join path that Task 4 targets.
    private QueryRowNameResolver _resolverQualified = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        // ── row dicts ──────────────────────────────────────────────────────

        _row = new Dictionary<string, ColumnValue>
        {
            ["a"]        = new ColumnValue(ColumnType.Integer64, 15L),
            ["b"]        = new ColumnValue(ColumnType.Integer64, 10L),
            ["value"]    = new ColumnValue(ColumnType.Integer64, 42L),
            ["name"]     = new ColumnValue(ColumnType.String,    "PREFIX_HELLO_WORLD"),
            ["category"] = new ColumnValue(ColumnType.Integer64, 3L),
        };

        _rowQualified = new Dictionary<string, ColumnValue>
        {
            ["bench.a"]        = new ColumnValue(ColumnType.Integer64, 15L),
            ["bench.b"]        = new ColumnValue(ColumnType.Integer64, 10L),
            ["bench.value"]    = new ColumnValue(ColumnType.Integer64, 42L),
            ["bench.name"]     = new ColumnValue(ColumnType.String,    "PREFIX_HELLO_WORLD"),
            ["bench.category"] = new ColumnValue(ColumnType.Integer64, 3L),
        };

        // ── resolvers ──────────────────────────────────────────────────────

        // Stub TableDescriptor: KvTableStore is null because the resolver only
        // reads Schema.Columns — it never touches the storage layer.
        TableSchema benchSchema = new()
        {
            Id      = "bench-id",
            Name    = "bench",
            Version = 1,
            Columns =
            [
                Col("a",        ColumnType.Integer64),
                Col("b",        ColumnType.Integer64),
                Col("value",    ColumnType.Integer64),
                Col("name",     ColumnType.String),
                Col("category", ColumnType.Integer64),
            ],
        };
        TableDescriptor benchTable = new("bench-id", "bench", benchSchema, null!);
        BoundTableSource benchSource = new(new TableSource("bench"), benchTable, "bench");

        // Single-source resolver (bare-key path).
        _resolverBare = new QueryRowNameResolver([benchSource]);

        // "other" table with distinct columns so no ambiguity when combined with bench.
        TableSchema otherSchema = new()
        {
            Id      = "other-id",
            Name    = "other",
            Version = 1,
            Columns = [Col("x", ColumnType.Integer64)],
        };
        TableDescriptor otherTable = new("other-id", "other", otherSchema, null!);
        BoundTableSource otherSource = new(new TableSource("other"), otherTable, "other");

        // Two-source resolver (qualified-key path): UsesQualifiedRowKeys() == true.
        _resolverQualified = new QueryRowNameResolver([benchSource, otherSource]);

        // ── AST nodes ──────────────────────────────────────────────────────

        // a > 10 AND b < 20
        _filterAnd = And(
            BinaryOp(NodeType.ExprGreaterThan, Identifier("a"), Int(10)),
            BinaryOp(NodeType.ExprLessThan,    Identifier("b"), Int(20))
        );

        // a + 1
        _projectionAdd = BinaryOp(NodeType.ExprAdd, Identifier("a"), Int(1));

        // upper(name)
        _projectionUpper = FuncCall("upper", Identifier("name"));

        // category  (used as group-by key)
        _groupKey = Identifier("category");

        // value  (used as SUM argument)
        _aggregateArg = Identifier("value");

        // name LIKE 'prefix%'
        _likePrefix = BinaryOp(NodeType.ExprLike, Identifier("name"), Str("prefix%"));

        // name LIKE '%row%'  — matches every row (all names are "PREFIX_HELLO_WORLD" here,
        // but the pattern aligns with the full-query ScanLikeContains shape)
        _likeContains = BinaryOp(NodeType.ExprLike, Identifier("name"), Str("%row%"));

        // upper(name)  (UPDATE SET expression — mirrors the idempotent full-query shape)
        _updateExpr = FuncCall("upper", Identifier("name"));

        // value IN (1,2,3,4)
        _inSmall = InMembership(Identifier("value"), BuildIntList(4));

        // value IN (1..1000)
        _inLarge = InMembership(Identifier("value"), BuildIntList(1000));
    }

    // ── benchmarks — all use _resolverBare to match the real single-table path ──

    [Benchmark(Description = "filter: a > 10 AND b < 20")]
    public ColumnValue FilterTwoPredicates() =>
        SQLExecutorBaseCreator.EvalExpr(_filterAnd, _row, null, _resolverBare);

    [Benchmark(Description = "project: a + 1")]
    public ColumnValue ProjectionArithmetic() =>
        SQLExecutorBaseCreator.EvalExpr(_projectionAdd, _row, null, _resolverBare);

    [Benchmark(Description = "project: upper(name)")]
    public ColumnValue ProjectionUpperScalar() =>
        SQLExecutorBaseCreator.EvalExpr(_projectionUpper, _row, null, _resolverBare);

    /// <summary>
    /// Single-table resolver: ResolveUnqualifiedColumn does one dict lookup in
    /// columnNameToAliases and returns the bare key (no string allocation).
    /// </summary>
    [Benchmark(Description = "group key: category — bare resolver (single-table)")]
    public ColumnValue GroupByKey() =>
        SQLExecutorBaseCreator.EvalExpr(_groupKey, _row, null, _resolverBare);

    /// <summary>
    /// Join resolver: ResolveUnqualifiedColumn resolves to "bench", then
    /// FormatQualifiedKey allocates "bench.category" each call — the overhead
    /// Task 4 (prepared identifier binding) would eliminate.
    /// </summary>
    [Benchmark(Description = "group key: category — qualified resolver (join path)")]
    public ColumnValue GroupByKey_Qualified() =>
        SQLExecutorBaseCreator.EvalExpr(_groupKey, _rowQualified, null, _resolverQualified);

    /// <summary>Single-table resolver: one dict lookup, bare key, no allocation.</summary>
    [Benchmark(Description = "aggregate arg: value — bare resolver (single-table)")]
    public ColumnValue AggregateArg() =>
        SQLExecutorBaseCreator.EvalExpr(_aggregateArg, _row, null, _resolverBare);

    /// <summary>
    /// Join resolver: FormatQualifiedKey allocates "bench.value" each call.
    /// Delta vs AggregateArg() isolates the per-row string-split + format cost.
    /// </summary>
    [Benchmark(Description = "aggregate arg: value — qualified resolver (join path)")]
    public ColumnValue AggregateArg_Qualified() =>
        SQLExecutorBaseCreator.EvalExpr(_aggregateArg, _rowQualified, null, _resolverQualified);

    [Benchmark(Description = "LIKE prefix: name LIKE 'prefix%'")]
    public ColumnValue LikePrefix() =>
        SQLExecutorBaseCreator.EvalExpr(_likePrefix, _row, null, _resolverBare);

    [Benchmark(Description = "LIKE contains: name LIKE '%row%'")]
    public ColumnValue LikeContains() =>
        SQLExecutorBaseCreator.EvalExpr(_likeContains, _row, null, _resolverBare);

    [Benchmark(Description = "UPDATE SET expr: upper(name) (mirrors idempotent full-query shape)")]
    public ColumnValue UpdateSetExpression() =>
        SQLExecutorBaseCreator.EvalExpr(_updateExpr, _row, null, _resolverBare);

    [Benchmark(Description = "IN small (4 elements): value IN (1..4)")]
    public ColumnValue InListSmall() =>
        SQLExecutorBaseCreator.EvalExpr(_inSmall, _row, null, _resolverBare);

    [Benchmark(Description = "IN large (1000 elements): value IN (1..1000)")]
    public ColumnValue InListLarge() =>
        SQLExecutorBaseCreator.EvalExpr(_inLarge, _row, null, _resolverBare);

    // ── helpers ──────────────────────────────────────────────────────────────

    private static TableColumnSchema Col(string name, ColumnType type) =>
        new(id: name + "-id", name: name, type: type, notNull: false, defaultValue: null,
            state: SchemaElementState.Public);

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst Int(long value) =>
        NodeAst.FromLong(value);

    private static NodeAst Str(string value) =>
        new(NodeType.String, null, null, null, null, null, null, null, value);

    private static NodeAst BinaryOp(NodeType op, NodeAst left, NodeAst right) =>
        new(op, left, right, null, null, null, null, null, null);

    private static NodeAst And(NodeAst left, NodeAst right) =>
        BinaryOp(NodeType.ExprAnd, left, right);

    private static NodeAst FuncCall(string name, NodeAst arg) =>
        // ScalarFunctionEvaluator expects: leftAst.yytext = function name, rightAst = argument(s)
        new(NodeType.ExprFuncCall,
            new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, name),
            arg,
            null, null, null, null, null, null);

    private static NodeAst InMembership(NodeAst lhs, NodeAst valueList) =>
        new(NodeType.ExprInMembership, lhs, valueList, null, null, null, null, null, null);

    private static NodeAst BuildIntList(int count)
    {
        NodeAst current = Int(1);
        for (int i = 2; i <= count; i++)
        {
            current = new NodeAst(
                NodeType.ExprList,
                current,
                Int(i),
                null, null, null, null, null, null);
        }
        return current;
    }
}
