
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
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
/// Shared node/table plumbing for the batched-DML benchmarks. Each benchmark class owns its own
/// embedded Kahuna node and a single <c>bench(id, value, name, category)</c> table where
/// <c>value = 0..N-1</c>, so a <c>WHERE value &gt;= 0</c> predicate matches every seeded row.
/// </summary>
internal static class DmlBenchHarness
{
    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactory.CreateLogger<ICamusDB>();

    public static async Task<(EmbeddedKahuna node, DatabaseRegistry registry, CommandExecutor executor, string dbName, DatabaseDescriptor db)>
        StartAsync()
    {
        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = "dml-bench-node",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("dml-bench-warmup", CancellationToken.None);
        await node.FlushAsync();

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, CamusDBConfig.Ambient);

        CommandExecutor executor = new(new CommandValidator(), new CatalogsManager(Logger), Logger, CamusDBConfig.Ambient,
            sharedNode: node, registry: registry, isClusterMode: true);

        string dbName = Guid.NewGuid().ToString("N");
        CamusDBConfig.DataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-dmlbench-" + dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbName,
            tableName: "bench",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("value", ColumnType.Integer64),
                new ColumnInfo("name", ColumnType.String, notNull: false),
                new ColumnInfo("category", ColumnType.Integer64, notNull: false),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])
            ],
            ifNotExists: false));

        return (node, registry, executor, dbName, db);
    }

    /// <summary>Inserts <paramref name="count"/> rows (value = 0..count-1) in batches of 500.</summary>
    public static async Task SeedAsync(CommandExecutor executor, DatabaseDescriptor db, string dbName, int count)
    {
        const int BatchSize = 500;
        for (int batch = 0; batch < count; batch += BatchSize)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            int end = Math.Min(batch + BatchSize, count);
            List<Dictionary<string, ColumnValue>> rows = new(end - batch);
            for (int i = batch; i < end; i++)
            {
                rows.Add(new Dictionary<string, ColumnValue>
                {
                    ["id"] = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
                    ["value"] = new(ColumnType.Integer64, (long)i),
                    ["name"] = new(ColumnType.String, "prefix_row_" + i),
                    ["category"] = new(ColumnType.Integer64, (long)(i % 10)),
                });
            }
            await executor.Insert(new InsertTicket(txnState: tx, databaseName: dbName, tableName: "bench", values: rows));
            await db.Transactions.CommitAsync(tx);
        }
    }
}

/// <summary>
/// Wall-clock + allocation of the batched UPDATE path (<c>UpdateRowsBatch</c>) at 100 / 1 000 /
/// 10 000 matched rows. The statement is idempotent (<c>upper</c> of an already-uppercased name is a
/// no-op after the first run), so the table is seeded once in <see cref="GlobalSetup"/> and every
/// measured invocation touches the same rows without needing a per-iteration reseed.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BatchUpdateBenchmarks
{
    [Params(100, 1_000, 10_000)]
    public int MatchedRows { get; set; }

    private EmbeddedKahuna _node = null!;
    private DatabaseRegistry _registry = null!;
    private CommandExecutor _executor = null!;
    private string _dbName = null!;
    private DatabaseDescriptor _db = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        (_node, _registry, _executor, _dbName, _db) = DmlBenchHarness.StartAsync().GetAwaiter().GetResult();
        DmlBenchHarness.SeedAsync(_executor, _db, _dbName, MatchedRows).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup() => CleanupAsync().GetAwaiter().GetResult();

    private async Task CleanupAsync()
    {
        await _executor.DisposeAsync();
        await _registry.DisposeAsync();
        await _node.DisposeAsync();
    }

    [Benchmark(Description = "DML: UPDATE bench SET name = upper(name) WHERE value >= 0 (all matched, idempotent)")]
    public async Task UpdateAllMatched()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        await _executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, _dbName,
            "UPDATE bench SET name = upper(name) WHERE value >= 0", parameters: null));
        await _db.Transactions.CommitAsync(tx);
    }
}

/// <summary>
/// Wall-clock + allocation of the batched DELETE path (<c>DeleteRowsBatch</c>) at 100 / 1 000 /
/// 10 000 matched rows. DELETE is destructive, so this uses <see cref="RunStrategy.Monitoring"/>
/// (one invocation per iteration, no invocation-count pilot) with an <see cref="IterationSetup"/>
/// that re-seeds the exact matched set before each measured delete.
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BatchDeleteBenchmarks
{
    [Params(100, 1_000, 10_000)]
    public int MatchedRows { get; set; }

    private EmbeddedKahuna _node = null!;
    private DatabaseRegistry _registry = null!;
    private CommandExecutor _executor = null!;
    private string _dbName = null!;
    private DatabaseDescriptor _db = null!;

    [GlobalSetup]
    public void GlobalSetup()
        => (_node, _registry, _executor, _dbName, _db) = DmlBenchHarness.StartAsync().GetAwaiter().GetResult();

    [IterationSetup]
    public void IterationSetup()
        => DmlBenchHarness.SeedAsync(_executor, _db, _dbName, MatchedRows).GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup() => CleanupAsync().GetAwaiter().GetResult();

    private async Task CleanupAsync()
    {
        await _executor.DisposeAsync();
        await _registry.DisposeAsync();
        await _node.DisposeAsync();
    }

    [Benchmark(Description = "DML: DELETE FROM bench WHERE value >= 0 (all matched)")]
    public async Task DeleteAllMatched()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        await _executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, _dbName,
            "DELETE FROM bench WHERE value >= 0", parameters: null));
        await _db.Transactions.CommitAsync(tx);
    }
}
