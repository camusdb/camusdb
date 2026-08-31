
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please see the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config;
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
    ///
    /// <para>Enables the single-fsync commit fast path by default, matching both standalone baselines:
    /// an auto-commit proposal acks once its propose quorum is durable and its commit marker rides the
    /// next durable flush, removing one serial fsync from the commit critical path without weakening
    /// durability. Kahuna's own embedded default is off, so the value is stated here rather than
    /// inherited — a future change to Kahuna's default must not silently move CamusDB's. A
    /// <c>kahuna.wal_single_fsync_commit</c> config value still overrides it.</para>
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
            RaftWalSingleFsyncCommit = true,
            StartElectionTimeout = 2000,
            EndElectionTimeout = 4000,
            // Range auto-split stays off unless an operator asks for it. Kahuna's own default is 1000
            // sampled keys, so inheriting it would make every key-range-routed table start splitting
            // itself as soon as key_range_sharding is switched on — a rebalancing policy arriving as a
            // side effect of a routing flag. Splitting a chosen range on demand is unaffected: a manual
            // split does not consult this threshold. Override with kahuna.range_split_threshold.
            RangeSplitThreshold = 0,
            // The load branch stays off for the same reason, and is stated rather than inherited:
            // it is the knob an operator reaches for, so a future change to Kahuna's default must
            // not switch heat-based splitting on here. Override with
            // kahuna.range_split_load_threshold.
            RangeSplitLoadThreshold = 0,
            RocksDbSharedMemoryEnabled = true,
            RocksDbSharedMemoryBudgetMb = 320,
            RocksDbSharedMemtableBudgetMb = 128,
            // With join_existing the peer list is the SEED list of the running cluster rather than
            // the founding roster: the node contacts a seed, enters the committed roster as a
            // learner, and is promoted once caught up. ConfigDefinition.Validate has already
            // required cluster mode and a non-empty peer list when the flag is set.
            JoinExistingSeeds = config.JoinExisting ? [.. config.Peers] : null,
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
            //
            // Range auto-split stays off unless an operator asks for it. Kahuna's own default is 1000
            // sampled keys, so inheriting it would make every key-range-routed table start splitting
            // itself as soon as key_range_sharding is switched on — a rebalancing policy arriving as a
            // side effect of a routing flag. Splitting a chosen range on demand is unaffected: a manual
            // split does not consult this threshold. Override with kahuna.range_split_threshold.
            RangeSplitThreshold = 0,
            // The load branch stays off for the same reason, and is stated rather than inherited:
            // it is the knob an operator reaches for, so a future change to Kahuna's default must
            // not switch heat-based splitting on here. Override with
            // kahuna.range_split_load_threshold.
            RangeSplitLoadThreshold = 0,
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
            //
            // Range auto-split stays off unless an operator asks for it. Kahuna's own default is 1000
            // sampled keys, so inheriting it would make every key-range-routed table start splitting
            // itself as soon as key_range_sharding is switched on — a rebalancing policy arriving as a
            // side effect of a routing flag. Splitting a chosen range on demand is unaffected: a manual
            // split does not consult this threshold. Override with kahuna.range_split_threshold.
            RangeSplitThreshold = 0,
            // The load branch stays off for the same reason, and is stated rather than inherited:
            // it is the knob an operator reaches for, so a future change to Kahuna's default must
            // not switch heat-based splitting on here. Override with
            // kahuna.range_split_load_threshold.
            RangeSplitLoadThreshold = 0,
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

        // Priority admission gate. Left at the Kahuna defaults (ceiling 0 = no gate) unless an
        // operator sets them, so a default-configured node admits every transaction immediately and
        // priority is recorded for observability only. MaxConcurrentTransactions (the script-path
        // ceiling) is intentionally not wired — CamusDB never uses that path.
        if (kahuna.MaxConcurrentSessions is int maxSessions)
            baseline.MaxConcurrentSessions = maxSessions;

        if (kahuna.DefaultAdmissionWaitMs is int defaultAdmissionWait)
            baseline.DefaultAdmissionWaitMs = defaultAdmissionWait;

        if (kahuna.MaxAdmissionWaitMs is int maxAdmissionWait)
            baseline.MaxAdmissionWaitMs = maxAdmissionWait;

        if (kahuna.TransactionPriorityReservedSlots is int reservedSlots)
            baseline.TransactionPriorityReservedSlots = reservedSlots;

        if (kahuna.TransactionPriorityAgingThreshold is int agingThreshold)
            baseline.TransactionPriorityAgingThreshold = agingThreshold;

        if (kahuna.TransactionPriorityMaxQueued is int maxQueued)
            baseline.TransactionPriorityMaxQueued = maxQueued;

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

        if (kahuna.RangeSplitThreshold is int rangeSplitThreshold)
            baseline.RangeSplitThreshold = rangeSplitThreshold;

        if (kahuna.RangeSplitMinRangeSize is int rangeSplitMinRangeSize)
            baseline.RangeSplitMinRangeSize = rangeSplitMinRangeSize;

        if (kahuna.RangeSplitLoadThreshold is double rangeSplitLoadThreshold)
            baseline.RangeSplitLoadThreshold = rangeSplitLoadThreshold;

        if (kahuna.RangeSplitLoadMinQueueDepth is int rangeSplitLoadQueueDepth)
            baseline.RangeSplitLoadMinQueueDepth = rangeSplitLoadQueueDepth;

        if (kahuna.RangeSplitLoadMinCommitWaitMs is double rangeSplitLoadCommitWait)
            baseline.RangeSplitLoadMinCommitWaitMs = rangeSplitLoadCommitWait;

        if (kahuna.RangeSplitLoadWindowMs is int rangeSplitLoadWindow)
            baseline.RangeSplitLoadWindow = TimeSpan.FromMilliseconds(rangeSplitLoadWindow);

        if (kahuna.RangeSplitLoadPollIntervalMs is int rangeSplitLoadPoll)
            baseline.RangeSplitLoadPollInterval = TimeSpan.FromMilliseconds(rangeSplitLoadPoll);

        if (kahuna.RangeSplitLoadImbalanceMax is double rangeSplitImbalanceMax)
            baseline.RangeSplitLoadImbalanceMax = rangeSplitImbalanceMax;

        if (kahuna.RangeSplitSettleWindowMs is int rangeSplitSettleWindow)
            baseline.RangeSplitSettleWindow = TimeSpan.FromMilliseconds(rangeSplitSettleWindow);

        if (kahuna.RangeSplitIndivisibleCooldownMs is int rangeSplitIndivisibleCooldown)
            baseline.RangeSplitIndivisibleCooldown = TimeSpan.FromMilliseconds(rangeSplitIndivisibleCooldown);

        if (kahuna.RangeMoveSettleTimeoutMs is int rangeMoveSettleTimeout)
            baseline.RangeMoveSettleTimeout = TimeSpan.FromMilliseconds(rangeMoveSettleTimeout);

        // The retained terminal record is what decides, after the fact, whether a durable write that
        // outlived its transaction was a commit or a leaked leg. Both bounds must be raised together:
        // age pruning and the size cap evict independently, so a long TTL under the default cap still
        // loses the record on any run that commits more transactions than the cap holds.
        if (kahuna.TransactionOutcomeRetentionTtlMs is int outcomeRetentionTtl)
            baseline.TransactionOutcomeRetentionTtl = TimeSpan.FromMilliseconds(outcomeRetentionTtl);

        if (kahuna.TransactionOutcomeRetentionMax is int outcomeRetentionMax)
            baseline.TransactionOutcomeRetentionMax = outcomeRetentionMax;

        // Per-page scan retry budget. Kahuna's default already sits below the shipped client command
        // deadline (5 s vs 10 s) so the named failure is observable; a deployment that raises its client
        // deadline may raise this with it, keeping the budget strictly below the deadline.
        if (kahuna.ScanPageRetryBudgetMs is int scanPageRetryBudget)
            baseline.ScanPageRetryBudgetMs = scanPageRetryBudget;

        if (kahuna.RangeMergeMinSize is int rangeMergeMinSize)
            baseline.RangeMergeMinSize = rangeMergeMinSize;

        if (kahuna.EnableLoadReports is bool enableLoadReports)
            baseline.EnableLoadReports = enableLoadReports;

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

        if (kahuna.ReplicationFactor is int replicationFactor)
            baseline.ReplicationFactor = replicationFactor;

        if (kahuna.Zone is not null)
            baseline.Zone = kahuna.Zone;

        if (kahuna.EnablePlacementRebalancer is bool placementRebalancer)
            baseline.EnablePlacementRebalancer = placementRebalancer;

        if (kahuna.PlacementPassIntervalMs is int placementPass)
            baseline.PlacementPassInterval = TimeSpan.FromMilliseconds(placementPass);

        if (kahuna.MaxReplicaMovesPerPass is int movesPerPass)
            baseline.MaxReplicaMovesPerPass = movesPerPass;

        if (kahuna.MaxConcurrentReplicaTransfers is int replicaTransfers)
            baseline.MaxConcurrentReplicaTransfers = replicaTransfers;

        if (kahuna.MaxConcurrentReplicaRepairs is int replicaRepairs)
            baseline.MaxConcurrentReplicaRepairs = replicaRepairs;

        if (kahuna.ReplicaCountDeadband is int replicaDeadband)
            baseline.ReplicaCountDeadband = replicaDeadband;

        if (kahuna.DecommissionDrainTimeoutMs is int drainTimeout)
            baseline.DecommissionDrainTimeout = TimeSpan.FromMilliseconds(drainTimeout);

        if (kahuna.EnableLeaderBalancer is bool leaderBalancer)
            baseline.EnableLeaderBalancer = leaderBalancer;

        if (kahuna.LeaderBalancerIntervalMs is int balancerInterval)
            baseline.LeaderBalancerInterval = TimeSpan.FromMilliseconds(balancerInterval);

        if (kahuna.LeaderBalancerReportIntervalMs is int balancerReportInterval)
            baseline.LeaderBalancerReportInterval = TimeSpan.FromMilliseconds(balancerReportInterval);

        if (kahuna.LeaderBalancerReportTtlMs is int balancerReportTtl)
            baseline.LeaderBalancerReportTtl = TimeSpan.FromMilliseconds(balancerReportTtl);

        if (kahuna.MinLeaderStabilityMs is int leaderStability)
            baseline.MinLeaderStability = TimeSpan.FromMilliseconds(leaderStability);

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
        // The same composition is mirrored client-side to decide when a coordinator-unknown
        // transaction is old enough to have its holdings released, so both read it from
        // KahunaSessionLifetime rather than each computing it.
        baseline.MaxTransactionTimeout = KahunaSessionLifetime.MaxSessionTimeoutMs(
            baseline.MaxTransactionTimeout, options.MaxSerializableTransactionLifetimeMs);

        if (options.MemoryProfile == MemoryProfile.Dev)
            ApplySmallFixedCacheDefaults(baseline, kahuna);
        else
            ApplyMemoryProportionalDefaults(baseline, kahuna);

        // Kahuna only builds the shared bundle when sharing is enabled and both databases are RocksDB;
        // otherwise the budgets are ignored. In that build case the memtable sub-budget must fit inside
        // the total cache budget, or Kahuna's RocksDbSharedResources.CreateWithUnifiedBudget throws an
        // ArgumentOutOfRangeException while the node is starting. KahunaOptionsConfig.Validate only sees
        // the raw config, so a single-knob override (e.g. setting only the memtable budget above the
        // total the sizing step chose) slips past it and would surface as a raw crash at boot. Checking
        // the fully-merged pair — after the sizing defaults, not before them — is what makes the check
        // see the values the node will actually be built with, and it fails fast with a clear config
        // error instead.
        if (baseline is { RocksDbSharedMemoryEnabled: true, Storage: "rocksdb", WalStorage: "rocksdb" }
            && baseline.RocksDbSharedMemtableBudgetMb > baseline.RocksDbSharedMemoryBudgetMb)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'kahuna.rocksdb_shared_memtable_budget_mb' ({baseline.RocksDbSharedMemtableBudgetMb}) must be <= " +
                $"'kahuna.rocksdb_shared_memory_budget_mb' ({baseline.RocksDbSharedMemoryBudgetMb})");
        }

        return baseline;
    }

    /// <summary>
    /// Sizes the same four cache knobs as <see cref="ApplyMemoryProportionalDefaults"/>, but to small
    /// fixed budgets that ignore how much memory the machine has: about 64 MiB of RocksDB block cache
    /// (16 MiB of it for memtables) and ~32 MiB of key/value actor caches (1 MiB per actor once the
    /// 1 MiB floor binds, so a node running more than 32 actors gets proportionally more), roughly
    /// 96 MiB in total against the ~1.3 GiB the proportional sizing takes on an 8 GiB box.
    ///
    /// <para>Selected by <c>memory_profile: dev</c> / <c>--memory-profile dev</c>, for a node sharing a
    /// machine with the application being developed against it. The budgets are ceilings on caching
    /// only — nothing about correctness or durability changes — so the cost is read throughput once the
    /// working set no longer fits: the same TPC-C workload that motivated the proportional sizing ran
    /// roughly 5x slower against a 320 MB block cache. That is an acceptable trade on a laptop and a bad
    /// one on a server, which is why this is opt-in rather than a small default.</para>
    ///
    /// <para>Fixed rather than a smaller fraction of RAM because the point of the profile is a
    /// predictable ceiling: an operator asking for a small node wants the same small node on a 64 GiB
    /// workstation as in a 2 GiB container. Like the proportional path it only fills knobs the operator
    /// left unset, so a single explicit <c>kahuna.*</c> budget can be raised without leaving the
    /// profile, and it runs after the <c>key_value_workers</c> override for the same reason.</para>
    /// </summary>
    private static void ApplySmallFixedCacheDefaults(EmbeddedKahunaOptions baseline, KahunaOptionsConfig kahuna)
    {
        const long OneMb = 1024L * 1024;
        const int BlockCacheMb = 64;
        const int MemtableMb = 16;
        const long ActorLayerBytes = 32 * OneMb;

        if (kahuna.RocksdbSharedMemoryBudgetMb is null)
            baseline.RocksDbSharedMemoryBudgetMb = BlockCacheMb;

        // Derived from whatever the total ended up being, not pinned: an operator who set only the total
        // to something below 16 MiB would otherwise get a memtable sub-budget larger than the cache it
        // is charged against, which the cross-field check above rejects at boot.
        if (kahuna.RocksdbSharedMemtableBudgetMb is null)
            baseline.RocksDbSharedMemtableBudgetMb = Math.Min(MemtableMb, baseline.RocksDbSharedMemoryBudgetMb);

        // Divide by the actor count the node will actually run, not the possibly-unset config value:
        // Kahuna fills the worker default (32 or more) only after these options reach it, so dividing
        // by max(1, unset = 0) would budget the whole 32 MiB layer to every actor and turn the
        // profile's ~100 MiB promise into a gigabyte.
        int actorCount = EffectiveActorCount(baseline);

        // Budgeted as a layer and split, matching the proportional path: a per-actor constant would be
        // multiplied by one actor per CPU, so the same profile would mean 32 MiB on a 4-core laptop and
        // 256 MiB on a 32-core one. The 1 MiB per-actor floor keeps an individual cache non-degenerate
        // on a machine with many cores.
        if (kahuna.MaxBytesPerActor is null)
            baseline.MaxBytesPerActor = Math.Max(ActorLayerBytes / actorCount, OneMb);

        // Both caps evict, so the smaller binds; deriving the entry cap from the byte cap at the same
        // assumed ~512 B/entry keeps the two describing one cache rather than two.
        if (kahuna.MaxEntriesPerActor is null)
            baseline.MaxEntriesPerActor = (int)Math.Max(baseline.MaxBytesPerActor / 512, 2_000);
    }

    /// <summary>
    /// Sizes the read caches proportionally to the machine instead of the old fixed values, for every
    /// knob the operator did not set explicitly. This is the <see cref="MemoryProfile.Prod"/> sizing;
    /// <see cref="MemoryProfile.Dev"/> replaces it with <see cref="ApplySmallFixedCacheDefaults"/>. Measured motivation: the fixed 320 MB RocksDB block
    /// cache forced a 1.2 GB TPC-C working set through disk reads on nearly every statement — raising
    /// it to 2 GB on a 16 GB machine took the same workload from 24.5 to 119.6 tx/s at 8 clients
    /// (and resolved a pipelining collapse to 3.4 tx/s that was pure read-I/O queueing).
    ///
    /// <para>Sizing policy, all against <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> (which
    /// respects container memory limits): RocksDB block cache = 10% of RAM; memtable budget = a
    /// quarter of the block cache; key/value actor caches = 6.25% of RAM (at least 64 MB for the layer
    /// as a whole) divided across the shard actors, with the per-actor entry cap derived at an assumed
    /// ~512 B/entry. Ceilings: 2 GB block cache, 1 GB memtables, 2 GB per actor, 4M entries. Roughly
    /// 16% of RAM across both cache layers at the defaults, and never more than 4 GB in total however
    /// large the machine is. The fractions and the ceilings are deliberately modest: an unconfigured
    /// node is far more often a developer workstation or a CI container sharing the box with a
    /// compiler and an IDE than a dedicated database server, and an over-eager default there costs
    /// the whole machine. A dedicated server should raise all four keys explicitly — see the worked
    /// example in <c>config.yml</c>. Every value remains overridable via the corresponding
    /// <c>kahuna.*</c> key.</para>
    ///
    /// <para>Floors yield on small machines (<see cref="ClampWithYieldingFloor"/>). The historic
    /// floors — 320 MB block cache, 128 MB memtables, 8 MB per actor, 10k entries — apply only while
    /// each fits its proportional share; below that the percentage governs, down to degenerate
    /// minimums of 64 MB cache, 16 MB memtables, 1 MB per actor, and 2k entries. Fixed floors on a
    /// small container are how this sizing used to overcommit: a 1536 MiB node took the 320 MB cache
    /// floor (21% of the container, native memory) plus the 128 MB memtable floor plus per-actor
    /// floors multiplied by 32+ actors inside the managed heap — roughly 40% of its memory pinned in
    /// caches for a working set of a few MB — and was OOM-killed under load.</para>
    ///
    /// <para>Runs after every explicit override has been applied, and touches only knobs whose
    /// config value is null — an operator's explicit setting always wins. Must also run after the
    /// <c>key_value_workers</c> override, since the per-actor split divides by the final actor count
    /// (<see cref="EffectiveActorCount"/>).</para>
    /// </summary>
    private static void ApplyMemoryProportionalDefaults(EmbeddedKahunaOptions baseline, KahunaOptionsConfig kahuna)
    {
        long totalRam = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        if (totalRam <= 0)
            return; // Unknown memory size: keep the fixed baseline values.

        ApplyMemoryProportionalDefaults(baseline, kahuna, totalRam);
    }

    /// <summary>
    /// Core of the proportional sizing, driven by an explicit memory size. Split out (internal) so
    /// tests can assert the floor behavior at container sizes the test machine does not have; the
    /// public path above always passes the machine's real, limit-aware total.
    /// </summary>
    internal static void ApplyMemoryProportionalDefaults(EmbeddedKahunaOptions baseline, KahunaOptionsConfig kahuna, long totalRam)
    {
        const long OneMb = 1024L * 1024;

        if (kahuna.RocksdbSharedMemoryBudgetMb is null)
            baseline.RocksDbSharedMemoryBudgetMb = (int)(ClampWithYieldingFloor(
                totalRam / 10, historicFloor: 320 * OneMb, degenerateFloor: 64 * OneMb, ceiling: 2048 * OneMb) / OneMb);

        if (kahuna.RocksdbSharedMemtableBudgetMb is null)
            baseline.RocksDbSharedMemtableBudgetMb = (int)ClampWithYieldingFloor(
                baseline.RocksDbSharedMemoryBudgetMb / 4, historicFloor: 128, degenerateFloor: 16, ceiling: 1024);

        int actorCount = EffectiveActorCount(baseline);

        if (kahuna.MaxBytesPerActor is null)
        {
            // Budget the actor caches as a total and then split, instead of flooring each actor
            // independently. A per-actor floor gets multiplied by the actor count — 32 or more by
            // default — so on a machine with little RAM relative to its actor count it silently
            // overshoots the intended share of memory: an 8 MB floor across 32 actors claims 256 MB
            // of a 1.5 GB container, inside the managed heap, when the layer was meant to take
            // 6.25%. The floor therefore applies to the whole layer; the per-actor floor yields to
            // the split, down to a 1 MiB minimum that keeps an individual cache from shrinking to a
            // size that cannot hold a useful working set at all.
            long actorLayerBytes = Math.Max(totalRam / 16, 64L * OneMb);
            baseline.MaxBytesPerActor = ClampWithYieldingFloor(
                actorLayerBytes / actorCount, historicFloor: 8L * OneMb, degenerateFloor: OneMb, ceiling: 2048L * OneMb);
        }

        // Kahuna evicts when *either* cap is exceeded, so the smaller of the two binds. The entry cap
        // is derived from the byte cap at an assumed ~512 B/entry, and its floor yields with it so the
        // two keep describing one cache: a fixed 10k-entry floor over a 1-3 MiB byte budget would
        // describe a cache larger than its own byte budget allows.
        if (kahuna.MaxEntriesPerActor is null)
            baseline.MaxEntriesPerActor = (int)ClampWithYieldingFloor(
                baseline.MaxBytesPerActor / 512, historicFloor: 10_000, degenerateFloor: 2_000, ceiling: 4_000_000);
    }

    /// <summary>
    /// The key/value shard actor count the node will actually run: the configured value when set,
    /// else the default Kahuna's configuration validation fills in for an unset count (32, or 4 per
    /// core when that is larger). The per-actor cache budgets are divided by this count <b>before</b>
    /// the options reach Kahuna — the worker default is filled in later, inside Kahuna — so the
    /// sizing here must anticipate that default rather than divide by the unset value: dividing by
    /// <c>max(1, 0)</c> budgets the entire actor layer to every one of the 32+ actors, multiplying
    /// the layer by the actor count. If Kahuna's default changes, this must change with it.
    /// </summary>
    internal static int EffectiveActorCount(EmbeddedKahunaOptions baseline) =>
        baseline.KeyValueWorkers > 0 ? baseline.KeyValueWorkers : Math.Max(32, Environment.ProcessorCount * 4);

    /// <summary>
    /// Clamps a proportionally-sized budget between a floor and a ceiling, with a floor that yields
    /// on small machines: the effective floor is <paramref name="historicFloor"/> or the proportional
    /// value itself, whichever is smaller, and never below <paramref name="degenerateFloor"/>. On a
    /// machine large enough that the proportional value reaches the historic floor this is exactly
    /// <c>Math.Clamp(proportional, historicFloor, ceiling)</c>; on a smaller one the proportional
    /// value governs, so a floor can never claim a multiple of its intended share of the machine's
    /// memory. The degenerate floor is where shrinking stops, because a smaller cache no longer holds
    /// a useful working set at all.
    /// </summary>
    internal static long ClampWithYieldingFloor(long proportional, long historicFloor, long degenerateFloor, long ceiling)
    {
        long floor = Math.Min(historicFloor, Math.Max(proportional, degenerateFloor));
        return Math.Clamp(proportional, floor, ceiling);
    }
}
