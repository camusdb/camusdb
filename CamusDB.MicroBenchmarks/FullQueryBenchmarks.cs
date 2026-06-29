
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using BenchmarkDotNet.Attributes;
using Kahuna;
using Microsoft.Extensions.Logging;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// End-to-end query benchmarks — measures full wall-clock cost including KV fetch,
/// row decode, expression evaluation, and serialization.
///
/// Compare each result against the corresponding shape in <see cref="EvalExprBenchmarks"/>
/// to estimate what fraction of total query time EvalExpr consumes.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class FullQueryBenchmarks
{
    /// <summary>
    /// Number of rows in the benchmark table.
    /// Primary gate run uses 1 000 rows. To measure the slope (fraction rising as fixed
    /// per-query overhead amortizes) run a separate dedicated class rather than adding a second
    /// Params value here — BenchmarkDotNet spawns a new process per (method × param), so 9
    /// benchmarks × 50k setup each = O(hour) total setup cost.
    /// </summary>
    [Params(1_000)]
    public int RowCount { get; set; }

    private EmbeddedKahuna _node = null!;
    private DatabaseRegistry _registry = null!;
    private CommandExecutor _executor = null!;
    private string _dbName = null!;
    private DatabaseDescriptor _db = null!;
    private string _inLargeSql = null!;

    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger =
        LoggerFactory.CreateLogger<ICamusDB>();

    [GlobalSetup]
    public void GlobalSetup()
    {
        SetupAsync().GetAwaiter().GetResult();
    }

    private async Task SetupAsync()
    {
        _node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName   = "bench-node",
            Storage    = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await _node.StartAsync(CancellationToken.None);
        await _node.WaitForLeaderAsync("bench-warmup", CancellationToken.None);
        await _node.FlushAsync();

        _registry = await DatabaseRegistry.OpenAsync(_node);

        CommandValidator validator    = new();
        CatalogsManager  catalogsMgr = new(Logger);
        _executor = new CommandExecutor(validator, catalogsMgr, Logger,
            sharedNode: _node, registry: _registry, isClusterMode: true);

        _dbName = Guid.NewGuid().ToString("N");

        CamusDBConfig.DataDirectory = Path.Combine(
            Path.GetTempPath(), "camusdb-bench-" + _dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        _db = await _executor.CreateDatabase(new CreateDatabaseTicket(_dbName, ifNotExists: false));

        await _executor.CreateTable(new CreateTableTicket(
            databaseName: _dbName,
            tableName: "bench",
            columns:
            [
                new ColumnInfo("id",       ColumnType.Id),
                // value is intentionally left unindexed so residual-filter and IN benchmarks
                // exercise a genuine full-scan EvalExpr path. Do not add a secondary index here.
                new ColumnInfo("value",    ColumnType.Integer64),
                new ColumnInfo("name",     ColumnType.String,    notNull: false),
                new ColumnInfo("category", ColumnType.Integer64, notNull: false),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        // Bulk-insert rows in batches of 500.
        const int BatchSize = 500;
        for (int batch = 0; batch < RowCount; batch += BatchSize)
        {
            KvTransaction tx = await _db.Transactions.BeginAsync();
            int end  = Math.Min(batch + BatchSize, RowCount);
            List<Dictionary<string, ColumnValue>> rows = new(end - batch);

            for (int i = batch; i < end; i++)
            {
                rows.Add(new Dictionary<string, ColumnValue>
                {
                    ["id"]       = new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()),
                    ["value"]    = new(ColumnType.Integer64, (long)i),
                    ["name"]     = new(ColumnType.String,    "prefix_row_" + i),
                    ["category"] = new(ColumnType.Integer64, (long)(i % 10)),
                });
            }

            await _executor.Insert(new InsertTicket(txnState: tx, databaseName: _dbName, tableName: "bench", values: rows));
            await _db.Transactions.CommitAsync(tx);
        }

        // Pre-build the large IN list SQL once.
        StringBuilder sb = new("SELECT id, value FROM bench WHERE value IN (");
        for (int i = 0; i < 1000; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(i);
        }
        sb.Append(')');
        _inLargeSql = sb.ToString();

    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        CleanupAsync().GetAwaiter().GetResult();
    }

    private async Task CleanupAsync()
    {
        await _executor.DisposeAsync();
        await _registry.DisposeAsync();
        await _node.DisposeAsync();
    }

    // ── query benchmarks ─────────────────────────────────────────────────────

    [Benchmark(Description = "scan: WHERE a > 10 AND b < 20 (residual filter)")]
    public async Task<int> ScanResidualFilter()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT id, value FROM bench WHERE value > 10 AND value < 200",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "scan: SELECT value + 1, upper(name) (computed projection)")]
    public async Task<int> ScanComputedProjection()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT value + 1, upper(name) FROM bench",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "scan: GROUP BY category, COUNT(*), SUM(value)")]
    public async Task<int> ScanGroupedAggregate()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT category, COUNT(*), SUM(value) FROM bench GROUP BY category",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "scan: WHERE name LIKE 'prefix%' (LIKE prefix)")]
    public async Task<int> ScanLikePrefix()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT id, name FROM bench WHERE name LIKE 'prefix%'",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "scan: WHERE name LIKE '%row%' (LIKE contains)")]
    public async Task<int> ScanLikeContains()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT id, name FROM bench WHERE name LIKE '%row%'",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "scan: WHERE value IN (1,2,3,4) (IN small)")]
    public async Task<int> ScanInListSmall()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT id, value FROM bench WHERE value IN (1, 2, 3, 4)",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "scan: WHERE value IN (1..1000) (IN large)")]
    public async Task<int> ScanInListLarge()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName, _inLargeSql, parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Description = "DML: UPDATE bench SET name = upper(name) WHERE value < 100 (idempotent)")]
    public async Task UpdateExpressionBased()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "UPDATE bench SET name = upper(name) WHERE value < 100",
            parameters: null);

        await _executor.ExecuteNonSQLQuery(ticket);
        await _db.Transactions.CommitAsync(tx);
    }

    [Benchmark(Description = "DML: INSERT 100 rows (large multi-row insert)")]
    public async Task LargeMultiRowInsert()
    {
        StringBuilder sb = new("INSERT INTO bench (id, value, name, category) VALUES ");
        for (int i = 0; i < 100; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append($"('{ObjectIdGenerator.Generate()}', {i}, 'batch_row_{i}', {i % 10})");
        }

        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName, sb.ToString(), parameters: null);

        await _executor.ExecuteNonSQLQuery(ticket);
        await _db.Transactions.CommitAsync(tx);
    }
}
