
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;

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
/// End-to-end cost of an exact nearest-neighbour query: real SQL, real scan, real decode, with the
/// query vector arriving as a bind parameter the way a client sends it.
///
/// <para>Where <see cref="VectorDistanceBenchmarks"/> isolates one distance evaluation, this measures
/// what a user actually waits for, and it is the number the documentation quotes.</para>
///
/// <para>The pairing is the point. <c>Top10</c> and <c>FullSort</c> run the same query over the same
/// rows and differ only in whether a LIMIT lets the sort be bounded, so the gap between them is the
/// bounded operator's contribution and nothing else. <c>ScanOnly</c> is the same scan with no
/// ordering at all — the floor neither ordering can go below.</para>
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class VectorSearchBenchmarks
{
    /// <summary>Rows in the table. Setup cost is a full insert per row, so this stays modest.</summary>
    [Params(10_000)]
    public int RowCount { get; set; }

    /// <summary>768 is the common embedding width; 3072 bytes per row.</summary>
    private const int Dimensions = 768;

    private EmbeddedKahuna _node = null!;
    private DatabaseRegistry _registry = null!;
    private CommandExecutor _executor = null!;
    private string _dbName = null!;
    private DatabaseDescriptor _db = null!;
    private Dictionary<string, ColumnValue> _queryVector = null!;

    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactory.CreateLogger<ICamusDB>();

    private static ColumnValue Vector(Random random)
    {
        byte[] bytes = new byte[Dimensions * 4];

        for (int i = 0; i < Dimensions; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), (float)(random.NextDouble() * 2d - 1d));

        return new ColumnValue(bytes);
    }

    [GlobalSetup]
    public void GlobalSetup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = "vector-bench-node",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await _node.StartAsync(CancellationToken.None);
        await _node.WaitForLeaderAsync("bench-warmup", CancellationToken.None);
        await _node.FlushAsync();

        _registry = await DatabaseRegistry.OpenAsync(_node, CamusDBOptions.Default);

        CommandValidator validator = new(CamusDBOptions.Default);
        CatalogsManager catalogs = new(Logger);
        _executor = new CommandExecutor(validator, catalogs, Logger, CamusDBOptions.Default,
            sharedNode: _node, registry: _registry, isClusterMode: true);

        _dbName = "vb" + Guid.NewGuid().ToString("N");

        CamusDBConfig.DataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-vector-bench-" + _dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        _db = await _executor.CreateDatabase(new CreateDatabaseTicket(_dbName, ifNotExists: false));

        await _executor.CreateTable(new CreateTableTicket(
            databaseName: _dbName,
            tableName: "docs",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("embedding", ColumnType.Bytes, notNull: false, maxLength: Dimensions * 4),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])
            ],
            ifNotExists: false));

        // A fixed seed keeps successive runs comparable; the values do not affect cost, because
        // every element is read and multiplied regardless of magnitude.
        Random random = new(20260820);

        const int BatchSize = 250;

        for (int batch = 0; batch < RowCount; batch += BatchSize)
        {
            KvTransaction tx = await _db.Transactions.BeginAsync();
            int end = Math.Min(batch + BatchSize, RowCount);
            List<Dictionary<string, ColumnValue>> rows = new(end - batch);

            for (int i = batch; i < end; i++)
            {
                rows.Add(new Dictionary<string, ColumnValue>
                {
                    ["id"] = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
                    ["embedding"] = Vector(random),
                });
            }

            await _executor.Insert(new InsertTicket(
                txnState: tx, databaseName: _dbName, tableName: "docs", values: rows));
            await _db.Transactions.CommitAsync(tx);
        }

        _queryVector = new Dictionary<string, ColumnValue> { ["@q"] = Vector(random) };
    }

    [GlobalCleanup]
    public void GlobalCleanup() => CleanupAsync().GetAwaiter().GetResult();

    private async Task CleanupAsync()
    {
        await _executor.DisposeAsync();
        await _registry.DisposeAsync();
        await _node.DisposeAsync();
    }

    private async Task<int> RunAsync(string sql)
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, _dbName, sql, _queryVector);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await _executor.ExecuteSQLQuery(ticket);

        int count = 0;
        await foreach (QueryResultRow _ in cursor)
            count++;

        await _db.Transactions.CommitAsync(tx);
        return count;
    }

    [Benchmark(Baseline = true, Description = "nearest 10 (bounded top-k)")]
    public Task<int> Top10() =>
        RunAsync("SELECT id FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 10");

    [Benchmark(Description = "nearest 100 (bounded top-k)")]
    public Task<int> Top100() =>
        RunAsync("SELECT id FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 100");

    [Benchmark(Description = "rank every row (full sort, no LIMIT)")]
    public Task<int> FullSort() =>
        RunAsync("SELECT id FROM docs ORDER BY l2_distance(embedding, @q)");

    /// <summary>The same scan with no ordering — the cost neither ordering path can go below.</summary>
    [Benchmark(Description = "scan only, no ordering")]
    public Task<int> ScanOnly() =>
        RunAsync("SELECT id FROM docs");
}
