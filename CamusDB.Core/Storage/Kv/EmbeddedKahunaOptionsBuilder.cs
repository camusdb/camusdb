
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please see the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;
using Kahuna;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Builds <see cref="EmbeddedKahunaOptions"/> from resolved CamusDB config, starting from the
/// mode-specific baseline literals and applying optional <see cref="KahunaOptionsConfig"/> overrides.
/// </summary>
public static class EmbeddedKahunaOptionsBuilder
{
    /// <summary>Baseline cluster options matching the historical hardcoded <c>Program.cs</c> literals.</summary>
    public static EmbeddedKahunaOptions ClusterBaseline(ConfigDefinition config)
    {
        string dataDir = !string.IsNullOrEmpty(config.DataDir)
            ? config.DataDir
            : CamusDBConfig.DataDirectory;

        return new EmbeddedKahunaOptions
        {
            NodeName = !string.IsNullOrEmpty(config.NodeName) ? config.NodeName : Environment.MachineName,
            NodeId = config.RaftNodeId,
            Host = config.RaftHost,
            Port = config.RaftPort,
            InitialPartitions = config.InitialPartitions,
            Storage = "sqlite",
            StoragePath = Path.Combine(dataDir, "kv"),
            StorageRevision = "v1",
            WalStorage = "sqlite",
            WalPath = Path.Combine(dataDir, "wal"),
            WalRevision = "v1",
            StartElectionTimeout = 2000,
            EndElectionTimeout = 4000,
            RocksDbSharedMemoryEnabled = true,
            RocksDbSharedMemoryBudgetMb = 320,
            RocksDbSharedMemtableBudgetMb = 128,
        };
    }

    /// <summary>Baseline standalone options matching the historical <c>SqliteOptions</c> factory.</summary>
    public static EmbeddedKahunaOptions StandaloneBaseline(string dataPath)
    {
        return new EmbeddedKahunaOptions
        {
            NodeName = "camusdb-embedded",
            Storage = "sqlite",
            StoragePath = Path.Combine(dataPath, "kv"),
            StorageRevision = "v1",
            WalStorage = "sqlite",
            WalPath = Path.Combine(dataPath, "wal"),
            WalRevision = "v1",
            InitialPartitions = 1,
            RocksDbSharedMemoryEnabled = true,
            RocksDbSharedMemoryBudgetMb = 320,
            RocksDbSharedMemtableBudgetMb = 128,
        };
    }

    /// <summary>Baseline standalone options using RocksDB for both KV and WAL.</summary>
    public static EmbeddedKahunaOptions StandaloneRocksDbBaseline(string dataPath)
    {
        return new EmbeddedKahunaOptions
        {
            NodeName = "camusdb-embedded",
            Storage = "rocksdb",
            StoragePath = Path.Combine(dataPath, "kv"),
            StorageRevision = "v1",
            WalStorage = "rocksdb",
            WalPath = Path.Combine(dataPath, "wal"),
            WalRevision = "v1",
            InitialPartitions = 1,
            RocksDbSharedMemoryEnabled = true,
            RocksDbSharedMemoryBudgetMb = 320,
            RocksDbSharedMemtableBudgetMb = 128,
        };
    }

    /// <summary>Cluster baseline plus <c>kahuna:</c> overrides.</summary>
    public static EmbeddedKahunaOptions BuildCluster(ConfigDefinition config)
        => ApplyOverrides(ClusterBaseline(config), config.Kahuna);

    /// <summary>Standalone baseline plus <c>kahuna:</c> overrides.</summary>
    public static EmbeddedKahunaOptions BuildStandalone(string dataPath, KahunaOptionsConfig kahuna)
        => ApplyOverrides(StandaloneBaseline(dataPath), kahuna);

    /// <summary>Standalone RocksDB baseline plus <c>kahuna:</c> overrides.</summary>
    public static EmbeddedKahunaOptions BuildStandaloneRocksDb(string dataPath, KahunaOptionsConfig kahuna)
        => ApplyOverrides(StandaloneRocksDbBaseline(dataPath), kahuna);

    /// <summary>
    /// Applies nullable Kahuna overrides. Unset fields keep the baseline value unchanged.
    /// </summary>
    public static EmbeddedKahunaOptions ApplyOverrides(
        EmbeddedKahunaOptions baseline,
        KahunaOptionsConfig kahuna)
    {
        if (kahuna.Storage is not null)
            baseline.Storage = kahuna.Storage;

        if (kahuna.StorageRevision is not null)
            baseline.StorageRevision = kahuna.StorageRevision;

        if (kahuna.WalStorage is not null)
            baseline.WalStorage = kahuna.WalStorage;

        if (kahuna.WalRevision is not null)
            baseline.WalRevision = kahuna.WalRevision;

        if (kahuna.WalSyncWrites is bool walSync)
            baseline.WalSyncWrites = walSync;

        if (kahuna.DefaultTransactionTimeoutMs is int txnTimeout)
            baseline.DefaultTransactionTimeout = txnTimeout;

        if (kahuna.LocksWorkers is int locksWorkers)
            baseline.LocksWorkers = locksWorkers;

        if (kahuna.KeyValueWorkers is int kvWorkers)
            baseline.KeyValueWorkers = kvWorkers;

        if (kahuna.BackgroundWriterWorkers is int bgWorkers)
            baseline.BackgroundWriterWorkers = bgWorkers;

        if (kahuna.ReadIoThreads is int readIo)
            baseline.ReadIOThreads = readIo;

        if (kahuna.WriteIoThreads is int writeIo)
            baseline.WriteIOThreads = writeIo;

        if (kahuna.StartElectionTimeoutMs is int startElection)
            baseline.StartElectionTimeout = startElection;

        if (kahuna.EndElectionTimeoutMs is int endElection)
            baseline.EndElectionTimeout = endElection;

        if (kahuna.StartElectionTimeoutIncrementMs is int startInc)
            baseline.StartElectionTimeoutIncrement = startInc;

        if (kahuna.EndElectionTimeoutIncrementMs is int endInc)
            baseline.EndElectionTimeoutIncrement = endInc;

        if (kahuna.HeartbeatIntervalMs is int heartbeat)
            baseline.HeartbeatInterval = TimeSpan.FromMilliseconds(heartbeat);

        if (kahuna.VotingTimeoutMs is int voting)
            baseline.VotingTimeout = TimeSpan.FromMilliseconds(voting);

        if (kahuna.MaxEntriesPerActor is int maxEntries)
            baseline.MaxEntriesPerActor = maxEntries;

        if (kahuna.MaxBytesPerActor is long maxBytes)
            baseline.MaxBytesPerActor = maxBytes;

        if (kahuna.CacheEntryTtlMs is int cacheTtl)
            baseline.CacheEntryTtl = TimeSpan.FromMilliseconds(cacheTtl);

        if (kahuna.CacheEntriesToRemove is int cacheEvict)
            baseline.CacheEntriesToRemove = cacheEvict;

        if (kahuna.CollectionIntervalMs is int collectInterval)
            baseline.CollectionInterval = TimeSpan.FromMilliseconds(collectInterval);

        if (kahuna.CompactEveryOperations is int compactEvery)
            baseline.CompactEveryOperations = compactEvery;

        if (kahuna.CompactNumberEntries is int compactEntries)
            baseline.CompactNumberEntries = compactEntries;

        if (kahuna.MaxEntriesPerCompaction is int maxPerCompaction)
            baseline.MaxEntriesPerCompaction = maxPerCompaction;

        if (kahuna.RocksdbSharedMemory is bool sharedMem)
            baseline.RocksDbSharedMemoryEnabled = sharedMem;

        if (kahuna.RocksdbSharedMemoryBudgetMb is int sharedBudget)
            baseline.RocksDbSharedMemoryBudgetMb = sharedBudget;

        if (kahuna.RocksdbSharedMemtableBudgetMb is int memtableBudget)
            baseline.RocksDbSharedMemtableBudgetMb = memtableBudget;

        // Kahuna only builds the shared bundle when sharing is enabled and both databases are RocksDB;
        // otherwise the budgets are ignored. In that build case the memtable sub-budget must fit inside
        // the total cache budget, or Kahuna's RocksDbSharedResources.CreateWithUnifiedBudget throws an
        // ArgumentOutOfRangeException while the node is starting. KahunaOptionsConfig.Validate only sees
        // the raw config, so a single-knob override (e.g. lowering only the total budget below the
        // baseline memtable default) slips past it and would surface as a raw crash at boot. Re-check the
        // effective, post-merge pair here so it fails fast with a clear config error instead.
        if (baseline.RocksDbSharedMemoryEnabled
            && baseline.Storage == "rocksdb"
            && baseline.WalStorage == "rocksdb"
            && baseline.RocksDbSharedMemtableBudgetMb > baseline.RocksDbSharedMemoryBudgetMb)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'kahuna.rocksdb_shared_memtable_budget_mb' ({baseline.RocksDbSharedMemtableBudgetMb}) must be <= " +
                $"'kahuna.rocksdb_shared_memory_budget_mb' ({baseline.RocksDbSharedMemoryBudgetMb})");
        }

        return baseline;
    }
}
