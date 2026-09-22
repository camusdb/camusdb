/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Kahuna;
using Microsoft.Extensions.Logging;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// The table shape a batch-INSERT benchmark loads into. A logical restore writes through the same
/// statement path whatever the table looks like, but the per-row cost does not: a secondary index
/// adds one KV mutation and one statistics update per row, and a covering index also encodes an
/// INCLUDE tuple per row.
/// </summary>
public enum InsertBenchIndexShape
{
    /// <summary>Primary key only. This is the shape a <c>--defer-indexes</c> restore loads into.</summary>
    PrimaryKeyOnly,

    /// <summary>Primary key plus one plain secondary index on <c>category</c>.</summary>
    Secondary,

    /// <summary>Primary key plus one secondary index on <c>category</c> that stores <c>name</c>.</summary>
    Covering,
}

/// <summary>
/// Builds the embedded node, the database and the <c>bench</c> table that every batch-INSERT
/// benchmark in this file measures against, and renders the INSERT statement text.
///
/// <para>The statement text copies what <c>camus-dump</c> writes, because the parse and shape cost
/// of a restore is the cost of <em>that</em> text. An <c>Id</c> column is rendered as
/// <c>STR_ID('&lt;24 hex&gt;')</c>, not as a bare literal, so the scalar-function evaluation per row
/// is inside the measurement.</para>
///
/// <para>Storage is in memory. A commit therefore still runs the full Raft and transaction path but
/// does not fsync, so the commit numbers here are a floor. A persistent and a three-node measurement
/// are separate work.</para>
/// </summary>
internal static class InsertBenchHarness
{
    private static readonly ILoggerFactory LoggerFactoryInstance =
        LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    internal static readonly ILogger<ICamusDB> Logger = LoggerFactoryInstance.CreateLogger<ICamusDB>();

    /// <summary>Padding that makes a wide row about 1 KB of payload.</summary>
    private const int WidePayloadChars = 1000;

    internal sealed class Context
    {
        public EmbeddedKahuna Node { get; init; } = null!;
        public DatabaseRegistry Registry { get; init; } = null!;
        public CommandExecutor Executor { get; init; } = null!;
        public string DbName { get; init; } = null!;
        public DatabaseDescriptor Db { get; init; } = null!;
        public TableDescriptor Table { get; init; } = null!;

        public async Task DisposeAsync()
        {
            await Executor.DisposeAsync();
            await Registry.DisposeAsync();
            await Node.DisposeAsync();
        }
    }

    /// <summary>
    /// Starts a node and creates <c>bench</c> with the requested index shape and row width.
    /// <paramref name="options"/> lets a caller turn the parser cache off, which is the shape a
    /// restore actually sees: every statement of a dump file is unique text.
    /// </summary>
    public static async Task<Context> StartAsync(
        string nodeName,
        InsertBenchIndexShape indexShape,
        bool wideRows,
        CamusDBOptions options)
    {
        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = nodeName,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync(nodeName + "-warmup", CancellationToken.None);
        await node.FlushAsync();

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, options);

        CommandExecutor executor = new(
            new CommandValidator(options), new CatalogsManager(Logger), Logger, options,
            sharedNode: node, registry: registry, isClusterMode: true);

        string dbName = "b" + Guid.NewGuid().ToString("N");
        CamusDBConfig.DataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-insertbench-" + dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        List<ColumnInfo> columns =
        [
            new ColumnInfo("id", ColumnType.Id),
            new ColumnInfo("value", ColumnType.Integer64),
            new ColumnInfo("name", ColumnType.String, notNull: false),
            new ColumnInfo("category", ColumnType.Integer64, notNull: false),
        ];

        if (wideRows)
            columns.Add(new ColumnInfo("payload", ColumnType.String, notNull: false));

        List<ConstraintInfo> constraints =
        [
            new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
        ];

        if (indexShape == InsertBenchIndexShape.Secondary)
            constraints.Add(new ConstraintInfo(ConstraintType.IndexMulti, "category_idx", [new("category", OrderType.Ascending)]));
        else if (indexShape == InsertBenchIndexShape.Covering)
            constraints.Add(new ConstraintInfo(ConstraintType.IndexMulti, "category_idx", [new("category", OrderType.Ascending)], includeColumns: ["name"]));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbName,
            tableName: "bench",
            columns: columns.ToArray(),
            constraints: constraints.ToArray(),
            ifNotExists: false));

        TableDescriptor table = await executor.OpenTableWithDescriptor(db, new OpenTableTicket(dbName, "bench"));

        return new Context { Node = node, Registry = registry, Executor = executor, DbName = dbName, Db = db, Table = table };
    }

    /// <summary>
    /// Renders one multi-row INSERT of <paramref name="rows"/> rows in the shape
    /// <c>camus-dump</c> emits. Every row carries a distinct generated row id, so the statement can
    /// be executed repeatedly against a table that is truncated between iterations.
    /// </summary>
    public static string BuildInsertSql(int rows, bool wideRows)
    {
        string payload = new('x', WidePayloadChars);

        StringBuilder sb = new(rows * (wideRows ? WidePayloadChars + 96 : 96));

        sb.Append("INSERT INTO bench (id, value, name, category");
        if (wideRows)
            sb.Append(", payload");
        sb.Append(") VALUES\n  ");

        for (int i = 0; i < rows; i++)
        {
            if (i > 0)
                sb.Append(",\n  ");

            sb.Append("(STR_ID('").Append(ObjectIdGenerator.Generate().ToString()).Append("'), ");
            sb.Append(i).Append(", 'prefix_row_").Append(i).Append("', ").Append(i % 10);

            if (wideRows)
                sb.Append(", '").Append(payload).Append('\'');

            sb.Append(')');
        }

        sb.Append(';');

        return sb.ToString();
    }

    /// <summary>
    /// Builds the same rows the shaper would produce, without going through SQL. Used by the
    /// benchmarks that measure the write path alone, so the parse and shape cost stays out of them.
    /// </summary>
    public static List<Dictionary<string, ColumnValue>> BuildRowValues(int rows, bool wideRows)
    {
        string payload = new('x', WidePayloadChars);

        List<Dictionary<string, ColumnValue>> values = new(rows);

        for (int i = 0; i < rows; i++)
        {
            Dictionary<string, ColumnValue> row = new(wideRows ? 5 : 4)
            {
                ["id"] = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
                ["value"] = new(ColumnType.Integer64, (long)i),
                ["name"] = new(ColumnType.String, "prefix_row_" + i),
                ["category"] = new(ColumnType.Integer64, (long)(i % 10)),
            };

            if (wideRows)
                row["payload"] = new(ColumnType.String, payload);

            values.Add(row);
        }

        return values;
    }

    /// <summary>Empties <c>bench</c> between measured iterations without dropping the relation.</summary>
    public static async Task TruncateAsync(Context ctx)
    {
        KvTransaction tx = await ctx.Db.Transactions.BeginAsync();
        await ctx.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, ctx.DbName, "TRUNCATE TABLE bench", null));
    }
}

/// <summary>
/// Parse and shape cost of one multi-row INSERT, by row count. These two phases touch no storage,
/// so they run under the default job and are the only phases measured with full statistical rigor.
///
/// <para><c>Parse</c> is the lexer and parser alone. <c>ParseAndShape</c> adds the walk of the
/// VALUES tree that builds one dictionary per row, which is where coercion, defaults and the
/// <c>STR_ID</c> evaluation happen. The difference between the two is the shape cost.
/// <c>ParseThroughCache</c> repeats the same text through the executor's parser cache, so it reports
/// what a cache hit saves — a number a restore never collects, because every dump statement is
/// unique text.</para>
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class InsertParseBenchmarks
{
    // 5 000 is the largest row count measured here. The recursive VALUES walk in
    // SQLExecutorInsertCreator overflows the stack at about 8 000 rows, which would end the
    // benchmark process rather than report a number.
    [Params(1, 100, 500, 1_000, 5_000)]
    public int Rows { get; set; }

    private InsertBenchHarness.Context _ctx = null!;
    private string _sql = null!;
    private KvTransaction _shapeTx = null!;
    private ExecuteSQLTicket _ticket;

    [GlobalSetup]
    public void GlobalSetup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _ctx = await InsertBenchHarness.StartAsync(
            "insert-parse-bench", InsertBenchIndexShape.PrimaryKeyOnly, wideRows: false, CamusDBOptions.Default);

        _sql = InsertBenchHarness.BuildInsertSql(Rows, wideRows: false);

        // Shaping never writes, so a deferred-start transaction is enough to carry the ticket.
        _shapeTx = await _ctx.Db.Transactions.BeginAsync(deferStart: true);
        _ticket = new ExecuteSQLTicket(_shapeTx, _ctx.DbName, _sql, null);

        // Warm the executor cache entry that ParseThroughCache measures.
        _ctx.Executor.ParseSql(_sql);
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _ctx.DisposeAsync().GetAwaiter().GetResult();

    [Benchmark(Description = "INSERT: lex + parse only (no cache)")]
    public NodeAst Parse() => SQLParserProcessor.Parse(_sql);

    [Benchmark(Description = "INSERT: parse through the executor parser cache (hit)")]
    public NodeAst ParseThroughCache() => _ctx.Executor.ParseSql(_sql);

    [Benchmark(Description = "INSERT: parse + shape VALUES into row dictionaries")]
    public async Task<InsertTicket> ParseAndShape()
    {
        NodeAst ast = SQLParserProcessor.Parse(_sql);

        return await new SQLExecutorInsertCreator(new SequenceStatementBinder(new SequenceAllocator(CamusDBConfig.Ambient)))
            .CreateInsertTicket(_ctx.Executor, _ctx.Db, _ticket, ast);
    }
}

/// <summary>
/// Write-path cost of one multi-row INSERT, by row count: validation, row encoding, KV staging,
/// commit and statistics.
///
/// <para>Every measured invocation leaves rows behind, so this class runs one invocation per
/// iteration (<see cref="RunStrategy.Monitoring"/>) and truncates the table in the iteration setup,
/// which BenchmarkDotNet excludes from the measurement. That also keeps the table size constant, so
/// a later iteration is not measured against a larger index.</para>
///
/// <para>The phases are read by subtraction, and the subtraction is not exact:
/// <c>StageAndRollback</c> carries the rollback cost, and <c>CommitEmpty</c> carries only the fixed
/// part of a commit. Read <c>StageAndCommit</c> minus <c>CommitEmpty</c> as staging plus the
/// row-proportional part of the commit, not as staging alone.</para>
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class InsertWriteBenchmarks
{
    // 5 000 is the largest row count measured here. The recursive VALUES walk in
    // SQLExecutorInsertCreator overflows the stack at about 8 000 rows, which would end the
    // benchmark process rather than report a number.
    [Params(1, 100, 500, 1_000, 5_000)]
    public int Rows { get; set; }

    private InsertBenchHarness.Context _ctx = null!;
    private string _sql = null!;
    private List<Dictionary<string, ColumnValue>> _values = null!;

    [GlobalSetup]
    public void GlobalSetup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _ctx = await InsertBenchHarness.StartAsync(
            "insert-write-bench", InsertBenchIndexShape.PrimaryKeyOnly, wideRows: false, CamusDBOptions.Default);

        _sql = InsertBenchHarness.BuildInsertSql(Rows, wideRows: false);
        _values = InsertBenchHarness.BuildRowValues(Rows, wideRows: false);
    }

    [IterationSetup]
    public void IterationSetup() => InsertBenchHarness.TruncateAsync(_ctx).GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup() => _ctx.DisposeAsync().GetAwaiter().GetResult();

    [Benchmark(Description = "INSERT: begin + commit, no rows (fixed transaction cost)")]
    public async Task CommitEmpty()
    {
        KvTransaction tx = await _ctx.Db.Transactions.BeginAsync();
        await _ctx.Db.Transactions.CommitAsync(tx);
    }

    [Benchmark(Description = "INSERT: validate + encode + stage, then roll back")]
    public async Task StageAndRollback()
    {
        KvTransaction tx = await _ctx.Db.Transactions.BeginAsync();
        await _ctx.Executor.Insert(new InsertTicket(tx, _ctx.DbName, "bench", _values));
        await _ctx.Db.Transactions.RollbackAsync(tx);
    }

    [Benchmark(Description = "INSERT: validate + encode + stage + commit")]
    public async Task StageAndCommit()
    {
        KvTransaction tx = await _ctx.Db.Transactions.BeginAsync();
        await _ctx.Executor.Insert(new InsertTicket(tx, _ctx.DbName, "bench", _values));
        await _ctx.Db.Transactions.CommitAsync(tx);
    }

    [Benchmark(Description = "INSERT: statistics tracking only")]
    public void TrackStatistics()
        => _ctx.Executor.Statistics.TrackInsert(_ctx.Db, _ctx.Table, _values.Count, _values);

    [Benchmark(Description = "INSERT: SQL text through durable commit (end to end)")]
    public async Task EndToEndSql()
    {
        KvTransaction tx = await _ctx.Db.Transactions.BeginAsync();
        await _ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, _ctx.DbName, _sql, null));
        await _ctx.Db.Transactions.CommitAsync(tx);
    }
}

/// <summary>
/// End-to-end cost of one 500-row INSERT across the table shapes a restore meets: narrow and wide
/// rows, and a primary key alone against a primary key plus a plain or covering secondary index.
///
/// <para>The row count is fixed so the shape is the only variable. The primary-key-only arm is the
/// shape a <c>--defer-indexes</c> restore loads into, so the gap between it and the other two arms
/// is what deferring the indexes removes from the load itself — not the total restore time, which
/// must also carry the backfill.</para>
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class InsertShapeBenchmarks
{
    private const int Rows = 500;

    [Params(InsertBenchIndexShape.PrimaryKeyOnly, InsertBenchIndexShape.Secondary, InsertBenchIndexShape.Covering)]
    public InsertBenchIndexShape IndexShape { get; set; }

    [Params(false, true)]
    public bool WideRows { get; set; }

    private InsertBenchHarness.Context _ctx = null!;
    private string _sql = null!;

    [GlobalSetup]
    public void GlobalSetup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _ctx = await InsertBenchHarness.StartAsync("insert-shape-bench", IndexShape, WideRows, CamusDBOptions.Default);
        _sql = InsertBenchHarness.BuildInsertSql(Rows, WideRows);
    }

    [IterationSetup]
    public void IterationSetup() => InsertBenchHarness.TruncateAsync(_ctx).GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup() => _ctx.DisposeAsync().GetAwaiter().GetResult();

    [Benchmark(Description = "INSERT 500 rows: SQL text through durable commit (end to end)")]
    public async Task EndToEndSql()
    {
        KvTransaction tx = await _ctx.Db.Transactions.BeginAsync();
        await _ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, _ctx.DbName, _sql, null));
        await _ctx.Db.Transactions.CommitAsync(tx);
    }
}
