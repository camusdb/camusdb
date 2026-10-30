/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Config;

/// <summary>
/// The Solarium storage engine (<c>kahuna.storage</c> / <c>kahuna.wal_storage</c> = <c>solarium</c>):
/// the config keys reach the Kahuna options, the providers are registered only when selected, the
/// memtable budget is floored at the Solarium WAL's flush unit, and a real node on Solarium serves
/// SQL and keeps its rows across a restart.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSolariumStorageEngine
{
    private const long OneMb = 1024L * 1024;

    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddFilter("Camus", LogLevel.Warning));

    [Test]
    public void ConfigReader_AcceptsSolariumAndItsKeys()
    {
        ConfigDefinition config = new ConfigReader().Read(
            "kahuna:\n  storage: solarium\n  wal_storage: solarium\n" +
            "  solarium_environment: default\n" +
            "  storage_compaction_rate_mbps: 50\n" +
            "  storage_max_background_compactions: 4");

        Assert.That(config.Kahuna.Storage, Is.EqualTo("solarium"));
        Assert.That(config.Kahuna.WalStorage, Is.EqualTo("solarium"));
        Assert.That(config.Kahuna.SolariumEnvironment, Is.EqualTo("default"));
        Assert.That(config.Kahuna.StorageCompactionRateMbps, Is.EqualTo(50));
        Assert.That(config.Kahuna.StorageMaxBackgroundCompactions, Is.EqualTo(4));
    }

    [TestCase("kahuna:\n  storage: solarum", "'kahuna.storage'")]
    [TestCase("kahuna:\n  wal_storage: lmdb", "'kahuna.wal_storage'")]
    [TestCase("kahuna:\n  solarium_environment: epoll", "'kahuna.solarium_environment'")]
    [TestCase("kahuna:\n  storage_compaction_rate_mbps: -1", "'kahuna.storage_compaction_rate_mbps'")]
    [TestCase("kahuna:\n  storage_max_background_compactions: -1", "'kahuna.storage_max_background_compactions'")]
    public void ConfigReader_RejectsInvalidStorageValues(string yaml, string field)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => new ConfigReader().Read(yaml))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain(field));
    }

    [Test]
    public void SolariumStorage_RegistersBothProviders()
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-registers", new KahunaOptionsConfig { Storage = "solarium", WalStorage = "solarium" }, CamusDBOptions.Default);

        Assert.That(options.StorageProviders.FindBackend("solarium"), Is.TypeOf<Solarium.Kahuna.SolariumPersistenceBackendProvider>());
        Assert.That(options.StorageProviders.FindWal("solarium"), Is.TypeOf<Solarium.Kahuna.SolariumWalProvider>());
        Assert.That(options.ExtensionOptions, Is.Empty, "an unset environment leaves Solarium on its default");
    }

    [Test]
    public void DefaultBaselines_RegisterNoSolarium()
    {
        EmbeddedKahunaOptions standalone = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-absent", new KahunaOptionsConfig { SolariumEnvironment = "io_uring" }, CamusDBOptions.Default);

        Assert.That(standalone.StorageProviders.FindBackend("solarium"), Is.Null);
        Assert.That(standalone.StorageProviders.FindWal("solarium"), Is.Null);
        Assert.That(standalone.ExtensionOptions, Is.Empty, "the environment is ignored unless a layer runs on Solarium");
    }

    [Test]
    public void SolariumEnvironment_ReachesTheExtensionOption()
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-env", new KahunaOptionsConfig { WalStorage = "solarium", SolariumEnvironment = "io_uring" }, CamusDBOptions.Default);

        Assert.That(options.Storage, Is.EqualTo("rocksdb"));
        Assert.That(options.WalStorage, Is.EqualTo("solarium"));
        Assert.That(options.ExtensionOptions[EmbeddedKahunaOptionsBuilder.SolariumEnvironmentOption], Is.EqualTo("io_uring"));
    }

    [Test]
    public void CompactionPacingKeys_ReachTheKahunaOptions()
    {
        EmbeddedKahunaOptions unset = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-pacing-unset", new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(unset.StorageCompactionRateBytesPerSec, Is.Zero);
        Assert.That(unset.StorageMaxBackgroundCompactions, Is.Zero);

        EmbeddedKahunaOptions paced = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-pacing",
            new KahunaOptionsConfig { StorageCompactionRateMbps = 50, StorageMaxBackgroundCompactions = 3 },
            CamusDBOptions.Default);

        // MB/s as Kahuna's own --storage-compaction-rate-mbps reads it: 10^6 bytes.
        Assert.That(paced.StorageCompactionRateBytesPerSec, Is.EqualTo(50_000_000L));
        Assert.That(paced.StorageMaxBackgroundCompactions, Is.EqualTo(3));
    }

    [Test]
    public void SolariumWal_FloorsTheMemtableBudgetAtItsFlushUnit()
    {
        // 8 GiB: the proportional budget is a quarter of an 819 MiB cache (204 MiB), below the 256 MiB a
        // Solarium WAL needs to flush at its own 128 MiB unit. Half the cache (409 MiB) holds it.
        EmbeddedKahunaOptions solarium = new() { KeyValueWorkers = 32, Storage = "solarium", WalStorage = "solarium" };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(solarium, new KahunaOptionsConfig(), 8L * 1024 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(solarium.StorageSharedCacheMb, Is.EqualTo(819));
        Assert.That(solarium.StorageSharedMemtableBudgetMb, Is.EqualTo(256));

        // A Solarium store beside a SQLite WAL has no Raft log in the budget, so no floor applies.
        EmbeddedKahunaOptions storeOnly = new() { KeyValueWorkers = 32, Storage = "solarium", WalStorage = "sqlite" };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(storeOnly, new KahunaOptionsConfig(), 8L * 1024 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(storeOnly.StorageSharedMemtableBudgetMb, Is.EqualTo(204));
    }

    [Test]
    public void SolariumWalFloor_YieldsToHalfTheCacheOnASmallMachine()
    {
        // 2 GiB: a 204 MiB cache. The floor may take at most half of it, so reads keep a real cache.
        EmbeddedKahunaOptions options = new() { KeyValueWorkers = 32, Storage = "solarium", WalStorage = "solarium" };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 2L * 1024 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(options.StorageSharedCacheMb, Is.EqualTo(204));
        Assert.That(options.StorageSharedMemtableBudgetMb, Is.EqualTo(102));
    }

    [Test]
    public void SolariumBudgets_AreSizedFromTheMachineLikeRocksDb()
    {
        // The reference node: a 4,096 MiB container with DOTNET_GCHeapHardLimitPercent at 60%, so the GC
        // reports a 2,458 MiB budget. Both engines hold the shared budgets in native memory (Solarium since
        // SolariumDb 0.1.4: memtable arenas and cached data blocks), so both are sized from the container:
        // a 409 MiB cache, and the memtable budget at the Solarium WAL's 256 MiB floor capped at half the
        // cache (204). Only the actor caches read the heap budget.
        EmbeddedKahunaOptions solarium = new() { KeyValueWorkers = 32, Storage = "solarium", WalStorage = "solarium" };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            solarium, new KahunaOptionsConfig(), machineMemoryBytes: 4096 * OneMb, managedHeapBytes: 2458 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(solarium.StorageSharedCacheMb, Is.EqualTo(409));
        Assert.That(solarium.StorageSharedMemtableBudgetMb, Is.EqualTo(204));
        Assert.That(solarium.MaxBytesPerActor, Is.EqualTo(2458 * OneMb / 16 / 32));

        // A Solarium store beside a RocksDB WAL: the same cache, and no Raft-log floor (an unshared RocksDB
        // WAL has its own memtables), leaving the proportional quarter.
        EmbeddedKahunaOptions storeOnly = new() { KeyValueWorkers = 32, Storage = "solarium", WalStorage = "rocksdb" };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            storeOnly, new KahunaOptionsConfig(), machineMemoryBytes: 4096 * OneMb, managedHeapBytes: 2458 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(storeOnly.StorageSharedCacheMb, Is.EqualTo(409));
        Assert.That(storeOnly.StorageSharedMemtableBudgetMb, Is.EqualTo(102));

        // The heap budget does not move the shared budgets at all.
        EmbeddedKahunaOptions noLimit = new() { KeyValueWorkers = 32, Storage = "solarium", WalStorage = "solarium" };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            noLimit, new KahunaOptionsConfig(), machineMemoryBytes: 4096 * OneMb, managedHeapBytes: 4096 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(noLimit.StorageSharedCacheMb, Is.EqualTo(409));
        Assert.That(noLimit.StorageSharedMemtableBudgetMb, Is.EqualTo(204));
    }

    [Test]
    public void ExplicitSolariumBudgets_AlwaysWin()
    {
        // Through the full build, on whatever memory this machine reports: an operator's explicit pair
        // is never resized.
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-explicit-pair",
            new KahunaOptionsConfig { Storage = "solarium", WalStorage = "solarium", RocksdbSharedMemoryBudgetMb = 768, RocksdbSharedMemtableBudgetMb = 256 },
            CamusDBOptions.Default);

        Assert.That(options.StorageSharedCacheMb, Is.EqualTo(768));
        Assert.That(options.StorageSharedMemtableBudgetMb, Is.EqualTo(256));
    }

    [Test]
    public void ExplicitMemtableBudget_BeatsTheSolariumFloor()
    {
        // Through the full build: the override step sets the explicit budget, and the sizing step must
        // then leave it alone. 64 MiB is at or below the smallest cache the sizing can choose.
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/solarium-explicit-budget",
            new KahunaOptionsConfig { Storage = "solarium", WalStorage = "solarium", RocksdbSharedMemtableBudgetMb = 64 },
            CamusDBOptions.Default);

        Assert.That(options.StorageSharedMemtableBudgetMb, Is.EqualTo(64));
    }

    /// <summary>
    /// Boots a real standalone node with both layers on Solarium, drives a create/insert/select
    /// round-trip through <see cref="CommandExecutor"/>, closes the node, and opens a second node on
    /// the same data directory. The row must come back: the first node's writes reached the Solarium
    /// store or the Solarium WAL, and the second node recovered them from disk.
    /// </summary>
    [Test]
    public async Task SolariumNode_ServesSqlAndKeepsRowsAcrossARestart()
    {
        string dataPath = Path.Combine(Path.GetTempPath(), "camusdb-solarium-boot-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataPath);

        KahunaOptionsConfig kahuna = new() { Storage = "solarium", WalStorage = "solarium" };
        string dbname = "solarium" + Guid.NewGuid().ToString("n");

        try
        {
            await using (SolariumNode first = await SolariumNode.StartAsync(dataPath, kahuna))
            {
                DatabaseDescriptor database = await first.Executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));

                KvTransaction txn = await database.Transactions.BeginAsync();

                ExecuteDDLSQLResult ddl = await first.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                    txnState: txn, database: dbname,
                    sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL)",
                    parameters: null));
                Assert.That(ddl.Success, Is.True);

                ExecuteNonSQLResult insert = await first.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: txn, database: dbname,
                    sql: "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), \"astro boy\", 3000)",
                    parameters: null));
                Assert.That(insert.ModifiedRows, Is.EqualTo(1));

                await database.Transactions.CommitAsync(txn);

                List<QueryResultRow> rows = await SelectRobots(first.Executor, database, dbname);
                Assert.That(rows.Count, Is.EqualTo(1));
            }

            string store = Path.Combine(dataPath, "kv", EmbeddedKahunaOptionsBuilder.CurrentStorageRevision);
            string wal = Path.Combine(dataPath, "wal", EmbeddedKahunaOptionsBuilder.CurrentStorageRevision);
            Assert.That(Directory.EnumerateFileSystemEntries(store).Any(), Is.True, $"the Solarium store must write under {store}");
            Assert.That(Directory.EnumerateFileSystemEntries(wal).Any(), Is.True, $"the Solarium WAL must write under {wal}");

            await using (SolariumNode second = await SolariumNode.StartAsync(dataPath, kahuna))
            {
                DatabaseDescriptor reopened = await second.Executor.OpenDatabase(dbname);

                List<QueryResultRow> rows = await SelectRobots(second.Executor, reopened, dbname);
                Assert.That(rows.Count, Is.EqualTo(1));
                Assert.That(rows[0].Row["name"].StrValue, Is.EqualTo("astro boy"));
                Assert.That(rows[0].Row["year"].LongValue, Is.EqualTo(3000));
            }
        }
        finally
        {
            try { Directory.Delete(dataPath, recursive: true); } catch { }
        }
    }

    private static async Task<List<QueryResultRow>> SelectRobots(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: txn, database: dbname, sql: "SELECT name, year FROM robots", parameters: null));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    /// <summary>One standalone node, its database registry and its executor, closed in reverse order.</summary>
    private sealed class SolariumNode : IAsyncDisposable
    {
        private readonly EmbeddedKahuna node;
        private readonly DatabaseRegistry registry;

        private SolariumNode(EmbeddedKahuna node, DatabaseRegistry registry, CommandExecutor executor)
        {
            this.node = node;
            this.registry = registry;
            Executor = executor;
        }

        public CommandExecutor Executor { get; }

        public static async Task<SolariumNode> StartAsync(string dataPath, KahunaOptionsConfig kahuna)
        {
            EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, kahuna, CamusDBOptions.Default);

            EmbeddedKahuna node = new(options, LoggerFactory);
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("warmup", CancellationToken.None);
            await node.FlushAsync();

            DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, CamusDBOptions.Default);

            ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();
            CommandExecutor executor = new(
                new CommandValidator(CamusDBOptions.Default), new CatalogsManager(logger), logger, CamusDBOptions.Default,
                sharedNode: node, registry: registry, isClusterMode: false);

            return new SolariumNode(node, registry, executor);
        }

        public async ValueTask DisposeAsync()
        {
            await Executor.DisposeAsync();
            await registry.DisposeAsync();
            await node.DisposeAsync();
        }
    }
}
