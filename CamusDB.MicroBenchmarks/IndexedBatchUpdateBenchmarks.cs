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
/// Wall-clock and allocation of a batched UPDATE over a table carrying 0, 1, 4 or 8 secondary
/// indexes, for the two shapes the index-update collector distinguishes.
///
/// <para>
/// <b>unchanged</b> rewrites only a non-indexed column, so every index keeps its entry — the common
/// shape, and the one the collector now resolves by comparing the indexed cells directly instead of
/// building both composite keys first. <b>changed</b> rewrites every indexed column, so every index
/// really does emit a delete and an insert; it is here to show the fast path costs the write shape
/// nothing.
/// </para>
///
/// <para>
/// The row count is fixed at 1 000 so the index count is the only variable that moves, and both
/// statements are re-runnable: <c>upper</c> of an uppercased value is a no-op, and incrementing an
/// indexed column stays in range for the measured iterations.
/// </para>
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class IndexedBatchUpdateBenchmarks
{
    private const int RowCount = 400;
    private const int IndexedColumns = 8;

    private static readonly ILoggerFactory LoggerFactoryInstance =
        LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactoryInstance.CreateLogger<ICamusDB>();

    [Params(0, 1, 4, 8)]
    public int Indexes { get; set; }

    [Params("unchanged", "changed")]
    public string Shape { get; set; } = "unchanged";

    private EmbeddedKahuna _node = null!;
    private DatabaseRegistry _registry = null!;
    private CommandExecutor _executor = null!;
    private string _dbName = null!;
    private DatabaseDescriptor _db = null!;
    private string _sql = null!;

    [GlobalSetup]
    public void GlobalSetup() => SetupAsync().GetAwaiter().GetResult();

    private async Task SetupAsync()
    {
        _node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = "indexed-update-bench-node",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await _node.StartAsync(CancellationToken.None);
        await _node.WaitForLeaderAsync("indexed-update-bench-warmup", CancellationToken.None);
        await _node.FlushAsync();

        _registry = await DatabaseRegistry.OpenAsync(_node, CamusDBOptions.Default);

        _executor = new CommandExecutor(
            new CommandValidator(CamusDBOptions.Default), new CatalogsManager(Logger), Logger, CamusDBOptions.Default,
            sharedNode: _node, registry: _registry, isClusterMode: true);

        _dbName = Guid.NewGuid().ToString("N");
        CamusDBConfig.DataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-idxupdbench-" + _dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        _db = await _executor.CreateDatabase(new CreateDatabaseTicket(_dbName, ifNotExists: false));

        List<ColumnInfo> columns =
        [
            new ColumnInfo("id", ColumnType.Id),
            new ColumnInfo("sel", ColumnType.Integer64),
            new ColumnInfo("payload", ColumnType.String, notNull: false),
        ];

        List<ConstraintInfo> constraints =
        [
            new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
        ];

        for (int c = 0; c < IndexedColumns; c++)
            columns.Add(new ColumnInfo("c" + c, ColumnType.Integer64));

        for (int c = 0; c < Indexes; c++)
            constraints.Add(new ConstraintInfo(ConstraintType.IndexMulti, "c" + c + "_idx", [new("c" + c, OrderType.Ascending)]));

        await _executor.CreateTable(new CreateTableTicket(
            databaseName: _dbName, tableName: "idxbench",
            columns: columns.ToArray(), constraints: constraints.ToArray(), ifNotExists: false));

        const int BatchSize = 500;

        for (int batch = 0; batch < RowCount; batch += BatchSize)
        {
            KvTransaction tx = await _db.Transactions.BeginAsync();
            int end = Math.Min(batch + BatchSize, RowCount);
            List<Dictionary<string, ColumnValue>> rows = new(end - batch);

            for (int i = batch; i < end; i++)
            {
                Dictionary<string, ColumnValue> row = new()
                {
                    ["id"] = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
                    ["sel"] = new(ColumnType.Integer64, 0L),
                    ["payload"] = new(ColumnType.String, "PREFIX_ROW_" + i),
                };

                for (int c = 0; c < IndexedColumns; c++)
                    row["c" + c] = new ColumnValue(ColumnType.Integer64, (long)((i * (c + 1)) % 97));

                rows.Add(row);
            }

            await _executor.Insert(new InsertTicket(txnState: tx, databaseName: _dbName, tableName: "idxbench", values: rows));
            await _db.Transactions.CommitAsync(tx);
        }

        if (Shape == "unchanged")
        {
            _sql = "UPDATE idxbench SET payload = upper(payload) WHERE sel >= 0";
        }
        else
        {
            // Every indexed column moves, so every index emits a delete and an insert. With zero
            // indexes there is nothing to move, so the non-indexed column stands in.
            List<string> assignments = [];

            for (int c = 0; c < Math.Max(1, Indexes); c++)
                assignments.Add(Indexes == 0 ? "payload = upper(payload)" : $"c{c} = c{c} + 1");

            _sql = "UPDATE idxbench SET " + string.Join(", ", assignments) + " WHERE sel >= 0";
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup() => CleanupAsync().GetAwaiter().GetResult();

    private async Task CleanupAsync()
    {
        await _executor.DisposeAsync();
        await _registry.DisposeAsync();
        await _node.DisposeAsync();
    }

    [Benchmark(Description = "DML: batched UPDATE over N secondary indexes")]
    public async Task Update()
    {
        KvTransaction tx = await _db.Transactions.BeginAsync();
        await _executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, _dbName, _sql, parameters: null));
        await _db.Transactions.CommitAsync(tx);
    }
}
