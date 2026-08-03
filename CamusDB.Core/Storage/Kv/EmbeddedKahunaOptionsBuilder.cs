
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
    /// <summary>
    /// Baseline cluster options. Uses RocksDB for both KV and WAL — the same default backend as the
    /// standalone baseline (<see cref="StandaloneRocksDbBaseline"/>), so both modes are RocksDB unless
    /// a <c>kahuna:</c> config block overrides the backend. Keeps the cluster-specific election timeouts
    /// (2000/4000 ms). A <c>kahuna: { storage: sqlite, wal_storage: sqlite }</c> override restores the
    /// former sqlite behavior for callers that need it.
    /// </summary>
    public static EmbeddedKahunaOptions ClusterBaseline(ConfigDefinition config, CamusDBOptions options)
    {
        string dataDir = !string.IsNullOrEmpty(config.DataDir)
            ? config.DataDir
            : options.DataDirectory;

        return new EmbeddedKahunaOptions
        {
            NodeName = !string.IsNullOrEmpty(config.NodeName) ? config.NodeName : Environment.MachineName,
            NodeId = config.RaftNodeId,
            Host = config.RaftHost,
            Port = config.RaftPort,
            InitialPartitions = config.InitialPartitions,
            Storage = "rocksdb",
            StoragePath = Path.Combine(dataDir, "kv"),
            StorageRevision = "v1",
            WalStorage = "rocksdb",
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
            RaftWalSingleFsyncCommit = true,
            // Key/value shard actor count stays at Kahuna's default (ProcessorCount): with the standalone
            // default of a single Raft partition, TPC-C measured ProcessorCount actors at 28.8 tx/s vs
            // 15.3 tx/s with 8x — extra actors only fragment per-actor caches when one partition already
            // serializes the write path. Multi-partition deployments can raise kahuna.key_value_workers
            // (with 4 partitions on the pre-WriteIOThreads-fix stack, 8x actors measured ~3x at 64 clients).
            RocksDbSharedMemoryEnabled = true,
            RocksDbSharedMemoryBudgetMb = 320,
            RocksDbSharedMemtableBudgetMb = 128,
        };
    }

    /// <summary>
    /// Baseline standalone options using RocksDB for both KV and WAL. Enables the single-fsync commit fast
    /// path by default: an auto-commit proposal acks once its propose quorum is durable and its commit
    /// marker rides the next durable flush, removing one serial fsync from the commit path without
    /// weakening durability. This is the recommended standalone default (Kahuna's own embedded default is
    /// off); a <c>kahuna.wal_single_fsync_commit</c> config value still overrides it.
    /// </summary>
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
            RaftWalSingleFsyncCommit = true,
            // Key/value shard actor count stays at Kahuna's default (ProcessorCount): with the standalone
            // default of a single Raft partition, TPC-C measured ProcessorCount actors at 28.8 tx/s vs
            // 15.3 tx/s with 8x — extra actors only fragment per-actor caches when one partition already
            // serializes the write path. Multi-partition deployments can raise kahuna.key_value_workers
            // (with 4 partitions on the pre-WriteIOThreads-fix stack, 8x actors measured ~3x at 64 clients).
            RocksDbSharedMemoryEnabled = true,
            RocksDbSharedMemoryBudgetMb = 320,
            RocksDbSharedMemtableBudgetMb = 128,
        };
    }

    /// <summary>Cluster baseline plus <c>kahuna:</c> overrides.</summary>
    public static EmbeddedKahunaOptions BuildCluster(ConfigDefinition config, CamusDBOptions options)
        => ApplyOverrides(ClusterBaseline(config, options), config.Kahuna, options);

    /// <summary>Standalone baseline plus <c>kahuna:</c> overrides.</summary>
    public static EmbeddedKahunaOptions BuildStandalone(string dataPath, KahunaOptionsConfig kahuna, CamusDBOptions options)
        => ApplyOverrides(StandaloneBaseline(dataPath), kahuna, options);

    /// <summary>Standalone RocksDB baseline plus <c>kahuna:</c> overrides.</summary>
    public static EmbeddedKahunaOptions BuildStandaloneRocksDb(string dataPath, KahunaOptionsConfig kahuna, CamusDBOptions options)
        => ApplyOverrides(StandaloneRocksDbBaseline(dataPath), kahuna, options);

    /// <summary>
    /// Applies nullable Kahuna overrides. Unset fields keep the baseline value unchanged.
    /// </summary>
    public static EmbeddedKahunaOptions ApplyOverrides(
        EmbeddedKahunaOptions baseline,
        KahunaOptionsConfig kahuna,
        CamusDBOptions options)
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

        if (kahuna.WalGroupCommitLingerMs is int lingerMs)
            baseline.RaftWalGroupCommitLingerMs = lingerMs;

        if (kahuna.WalSingleFsyncCommit is bool singleFsync)
            baseline.RaftWalSingleFsyncCommit = singleFsync;

        if (kahuna.DefaultTransactionTimeoutMs is int txnTimeout)
            baseline.DefaultTransactionTimeout = txnTimeout;

        if (kahuna.MaxTransactionTimeoutMs is int maxTxnTimeout)
            baseline.MaxTransactionTimeout = maxTxnTimeout;

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

        if (kahuna.BackupDir is not null)
            baseline.BackupDir = kahuna.BackupDir;

        if (kahuna.PitrWindowSeconds is int pitrWindow)
            baseline.PitrWindow = TimeSpan.FromSeconds(pitrWindow);

        if (kahuna.BaseSnapshotIntervalSeconds is int baseSnapshot)
            baseline.BaseSnapshotInterval = TimeSpan.FromSeconds(baseSnapshot);

        if (kahuna.RestoreRoot is not null)
            baseline.RestoreRoot = kahuna.RestoreRoot;

        if (kahuna.AllowUnconfinedRemoteRestore is bool allowUnconfined)
            baseline.AllowUnconfinedRemoteRestore = allowUnconfined;

        if (kahuna.BackupRetentionMaxChains is int retentionChains)
            baseline.BackupRetentionMaxChains = retentionChains;

        if (kahuna.BackupRetentionMaxAgeSeconds is int retentionAge)
            baseline.BackupRetentionMaxAge = TimeSpan.FromSeconds(retentionAge);

        if (kahuna.BackupRetentionMaxBytes is long retentionBytes)
            baseline.BackupRetentionMaxBytes = retentionBytes;

        if (kahuna.BackupGcIntervalSeconds is int gcInterval)
            baseline.BackupGcInterval = TimeSpan.FromSeconds(gcInterval);

        if (kahuna.BackupRestoreThrottleBytesPerSec is long throttle)
            baseline.BackupRestoreThrottleBytesPerSec = throttle;

        if (kahuna.BackupClusterId is not null)
            baseline.BackupClusterId = kahuna.BackupClusterId;

        if (kahuna.BackupMacKeyFile is not null)
            baseline.BackupMacKeyFile = kahuna.BackupMacKeyFile;

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

        // Compose the session-timeout cap with the engine's serializable lifetime. Every session is
        // started with Timeout = MaxSerializableTransactionLifetimeMs, and Kahuna clamps that to the
        // node's MaxTransactionTimeout — so if the node cap is left at its 300 s default, a configured
        // 1 h lifetime is silently truncated and the reaper reclaims the session (and its range locks /
        // MVCC snapshot) an hour early. Lift the node cap to admit the configured lifetime. An operator
        // who pinned an explicit 'max_transaction_timeout_ms' below the lifetime is rejected in
        // ConfigDefinition.Validate before we get here, so this only ever raises an unset/derived cap;
        // it never lowers an explicit one (max(...) guards the case where an explicit cap already
        // exceeds the lifetime). A non-positive lifetime disables the engine cap, leaving the node
        // default untouched.
        int lifetimeMs = options.MaxSerializableTransactionLifetimeMs;
        if (lifetimeMs > 0 && baseline.MaxTransactionTimeout < lifetimeMs)
            baseline.MaxTransactionTimeout = lifetimeMs;

        ApplyMemoryProportionalDefaults(baseline, kahuna);

        return baseline;
    }

    /// <summary>
    /// Sizes the read caches proportionally to the machine instead of the old fixed values, for every
    /// knob the operator did not set explicitly. Measured motivation: the fixed 320 MB RocksDB block
    /// cache forced a 1.2 GB TPC-C working set through disk reads on nearly every statement — raising
    /// it to 2 GB on a 16 GB machine took the same workload from 24.5 to 119.6 tx/s at 8 clients
    /// (and resolved a pipelining collapse to 3.4 tx/s that was pure read-I/O queueing).
    ///
    /// <para>Sizing policy, all against <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> (which
    /// respects container memory limits): RocksDB block cache = 25% of RAM clamped to [320 MB, 8 GB];
    /// memtable budget = a quarter of that, clamped to [128 MB, 1 GB]; key/value actor caches = 12.5%
    /// of RAM divided across the shard actors, clamped to [64 MB, 4 GB] per actor, with the per-actor
    /// entry cap derived at an assumed ~512 B/entry, clamped to [50k, 4M]. Roughly 40% of RAM across
    /// both cache layers at the defaults — sized as a database server's working assumption, like a
    /// buffer pool. Every value remains overridable via the corresponding <c>kahuna.*</c> key.</para>
    ///
    /// <para>Runs after every explicit override has been applied, and touches only knobs whose
    /// config value is null — an operator's explicit setting always wins. Must also run after the
    /// <c>key_value_workers</c> override, since the per-actor split divides by the final actor count.</para>
    /// </summary>
    private static void ApplyMemoryProportionalDefaults(EmbeddedKahunaOptions baseline, KahunaOptionsConfig kahuna)
    {
        long totalRam = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        if (totalRam <= 0)
            return; // Unknown memory size: keep the fixed baseline values.

        const long OneMb = 1024L * 1024;

        if (kahuna.RocksdbSharedMemoryBudgetMb is null)
            baseline.RocksDbSharedMemoryBudgetMb = (int)Math.Clamp(totalRam / 4 / OneMb, 320, 8192);

        if (kahuna.RocksdbSharedMemtableBudgetMb is null)
            baseline.RocksDbSharedMemtableBudgetMb = (int)Math.Clamp(baseline.RocksDbSharedMemoryBudgetMb / 4, 128, 1024);

        int actorCount = Math.Max(1, baseline.KeyValueWorkers);

        if (kahuna.MaxBytesPerActor is null)
            baseline.MaxBytesPerActor = Math.Clamp(totalRam / 8 / actorCount, 64L * OneMb, 4096L * OneMb);

        if (kahuna.MaxEntriesPerActor is null)
            baseline.MaxEntriesPerActor = (int)Math.Clamp(baseline.MaxBytesPerActor / 512, 50_000, 4_000_000);
    }
}
