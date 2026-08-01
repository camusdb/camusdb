
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// Benchmarks for secondary-index scan paths:
///
///   - Covered range scan: every projected column is in the index; no primary-row fetch.
///   - Non-covered range scan: a non-indexed column is projected; primary rows are fetched
///     via the paged batch path (<c>GetRowsBatch</c>).
///
/// Compare <c>CoveredRangeScan</c> vs <c>NonCoveredRangeScan</c> to quantify the fetch+decode
/// savings when covering is applicable. Use the ratio to calibrate the index-selectivity threshold
/// above which covering delivers a meaningful win.
///
/// <c>RowCount</c> is kept at 1 000 (matching <see cref="FullQueryBenchmarks"/>) so results are
/// directly comparable. The selective predicate (<c>year &lt; RowCount</c>) returns all rows so
/// each benchmark exercises the full scan cost, not point-lookup overhead.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class IndexScanBenchmarks
{
    [Params(1_000)]
    public int RowCount { get; set; }

    private EmbeddedKahuna _node = null!;
    private DatabaseRegistry _registry = null!;
    private CommandExecutor _executor = null!;
    private string _dbName = null!;
    private DatabaseDescriptor _db = null!;

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
            NodeName   = "bench-node-idx",
            Storage    = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await _node.StartAsync(CancellationToken.None);
        await _node.WaitForLeaderAsync("bench-warmup", CancellationToken.None);
        await _node.FlushAsync();

        _registry = await DatabaseRegistry.OpenAsync(_node, CamusDBConfig.Ambient);

        CommandValidator validator    = new();
        CatalogsManager  catalogsMgr = new(Logger);
        _executor = new CommandExecutor(validator, catalogsMgr, Logger, CamusDBConfig.Ambient,
            sharedNode: _node, registry: _registry, isClusterMode: true);

        _dbName = Guid.NewGuid().ToString("N");

        CamusDBConfig.DataDirectory = Path.Combine(
            Path.GetTempPath(), "camusdb-bench-idx-" + _dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        _db = await _executor.CreateDatabase(new CreateDatabaseTicket(_dbName, ifNotExists: false));

        await _executor.CreateTable(new CreateTableTicket(
            databaseName: _dbName,
            tableName: "bench",
            columns:
            [
                new ColumnInfo("id",      ColumnType.Id),
                new ColumnInfo("year",    ColumnType.Integer64),
                new ColumnInfo("name",    ColumnType.String),
                new ColumnInfo("enabled", ColumnType.Bool),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        // Non-unique secondary index on year — used for covered and non-covered range scans.
        await _executor.AlterIndex(new AlterIndexTicket(
            databaseName: _dbName,
            tableName: "bench",
            indexName: "year_idx",
            columns: [new("year", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        ));

        const int BatchSize = 500;
        for (int batch = 0; batch < RowCount; batch += BatchSize)
        {
            KvTransaction tx = await _db.Transactions.BeginAsync();
            int end = Math.Min(batch + BatchSize, RowCount);
            List<Dictionary<string, ColumnValue>> rows = new(end - batch);

            for (int i = batch; i < end; i++)
            {
                rows.Add(new Dictionary<string, ColumnValue>
                {
                    ["id"]      = new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()),
                    ["year"]    = new(ColumnType.Integer64, (long)i),
                    ["name"]    = new(ColumnType.String,    "robot_" + i),
                    ["enabled"] = new(ColumnType.Bool,      i % 2 == 0),
                });
            }

            await _executor.Insert(new InsertTicket(
                txnState: tx, databaseName: _dbName, tableName: "bench", values: rows));
            await _db.Transactions.CommitAsync(tx);
        }
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

    // ── index scan benchmarks ────────────────────────────────────────────────

    /// <summary>
    /// Covered range scan: SELECT year projects only the indexed column — no primary-row fetch.
    /// Every required column (year) is in year_idx; the plan is IndexOnly=true.
    /// </summary>
    [Benchmark(Baseline = true, Description = "index: covered range scan (SELECT year, no row fetch)")]
    public async Task<int> CoveredRangeScan()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            $"SELECT year FROM bench WHERE year < {RowCount}",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    /// <summary>
    /// Non-covered range scan: SELECT enabled projects a column not in year_idx — primary rows
    /// are fetched via the paged batch path (GetRowsBatch, 64 ids per call).
    /// </summary>
    [Benchmark(Description = "index: non-covered range scan (SELECT enabled, batch row fetch)")]
    public async Task<int> NonCoveredRangeScan()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            $"SELECT enabled FROM bench WHERE year < {RowCount}",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    /// <summary>
    /// Full table scan baseline: no index used, full primary-row decode for every row.
    /// Establishes what a table scan costs vs the indexed paths above.
    /// </summary>
    [Benchmark(Description = "table: full scan baseline (SELECT enabled, no index)")]
    public async Task<int> FullTableScanBaseline()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName,
            "SELECT enabled FROM bench",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);
        int count = 0;
        await foreach (QueryResultRow _ in cursor) count++;
        await _db.Transactions.CommitAsync(tx);
        return count;
    }
}
