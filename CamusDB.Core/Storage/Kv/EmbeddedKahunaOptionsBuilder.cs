
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please see the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using Kahuna;
using Kommander;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Builds <see cref="EmbeddedKahunaOptions"/> from resolved CamusDB config, starting from the
/// mode-specific baseline literals and applying optional <see cref="KahunaOptionsConfig"/> overrides.
/// </summary>
public static class EmbeddedKahunaOptionsBuilder
{
    /// <summary>
    /// The on-disk revision every CamusDB baseline pins for both the key-value store and the Raft
    /// WAL. Kahuna opens <c>{StoragePath}/{revision}</c> and <c>{WalPath}/{revision}</c>, so a new
    /// revision starts from an empty store and an empty log while the previous revision's
    /// directories stay on disk untouched.
    ///
    /// <para><c>v2</c> is the grouped key layout (<c>{dbId}:{tableId}|r/…</c> and
    /// <c>{dbId}:{tableId}|i:{indexId}/…</c>, see <see cref="KvKeyBuilder"/>). <c>v1</c> data
    /// used <c>:r</c> / <c>:i:</c> and is not readable through the current key builder; the WAL is
    /// bumped with it because a replay of <c>v1</c> log entries into a <c>v2</c> store would plant
    /// unreachable keys. Data crosses revisions by logical dump and reimport only — see
    /// <c>docs/logical-dump-and-reimport.md</c>. A <c>kahuna.storage_revision</c> config value
    /// overrides this for the store, and is not a supported way to read <c>v1</c> data.</para>
    /// </summary>
    public const string CurrentStorageRevision = "v2";

    /// <summary>The revision the grouped key layout replaced; named so startup can warn when its directory is still present.</summary>
    public const string PreviousStorageRevision = "v1";

    /// <summary>
    /// The node-to-node transport security Kommander and Kahuna dial peers with, or null for their
    /// default (no node authentication).
    ///
    /// <para>With <see cref="ConfigDefinition.RaftMutualTlsEnabled"/> on, both present
    /// <see cref="ConfigDefinition.RaftCertificate"/> as a client certificate, and Kommander's
    /// <c>RaftService</c> and Kahuna's node transport gate require one from every caller. Neither keeps a
    /// thumbprint allow-list here: with an empty list Kommander accepts any certificate that completed the
    /// handshake, which is safe only because the Raft listener checks the chain against
    /// <see cref="ConfigDefinition.RaftPeerCa"/> during that handshake (see <c>RaftPeerTrust</c> in the
    /// host). The archive has no password, as the listener loads the same file without one.</para>
    /// </summary>
    internal static RaftTransportSecurityOptions? PeerTransportSecurity(ConfigDefinition config)
    {
        if (!config.RaftMutualTlsEnabled)
            return null;

        return new RaftTransportSecurityOptions
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
            ClientCertificatePath = config.RaftCertificate,
            RequireTls = true,
        };
    }

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
    ///
    /// <para>Enables one-phase apply-time validation by default, so a single-partition read-modify-write
    /// commits in one durable Raft round instead of two. A cluster carrying prepared-intent snapshots
    /// written before the ledger existed, or one mid rolling upgrade across Kahuna versions, must set
    /// <c>kahuna.one_phase_apply_time_validation: false</c> until that is no longer true — see the
    /// comment on the field below and docs/configuration.md.</para>
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
            TransportSecurity = PeerTransportSecurity(config),
            InitialPartitions = config.InitialPartitions,
            Storage = "rocksdb",
            StoragePath = Path.Combine(dataDir, "kv"),
            StorageRevision = CurrentStorageRevision,
            WalStorage = "rocksdb",
            WalPath = Path.Combine(dataDir, "wal"),
            WalRevision = CurrentStorageRevision,
            RaftWalSingleFsyncCommit = true,
            // One-phase apply-time validation is ON by default. With CamusDB's grouped key layout a
            // single-row read-modify-write has one participant partition and reads on the anchor, so it
            // commits in one durable Raft round instead of two: measured +24% throughput on a
            // three-partition accounts workload at a 99.8% one-phase rate. Stated here rather than
            // inherited — Kahuna's own embedded default is off, and a future change to it must not move
            // CamusDB's. Kahuna enforces three rules, and the third is an UPGRADE STEP: a node that
            // starts with the gate on over a prepared-intent snapshot written before the ledger existed
            // fails at startup, so a cluster carrying pre-ledger data must run once with
            // kahuna.one_phase_apply_time_validation: false, let a checkpoint rewrite every partition's
            // snapshot with its ledger, then drop the override. The other two: the same value on every
            // node of the group (for this key and staged_base_fence_retention_ms), and never enabled
            // across mixed Kahuna versions — an old node skips the check and commits where a current
            // node refuses, which forks the state machine. Rolling upgrades must therefore set the
            // override to false until the last old node is gone. See docs/configuration.md.
            OnePhaseApplyTimeValidation = true,
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
            StorageSharedCacheMb = 320,
            StorageSharedMemtableBudgetMb = 128,
            // Buffered SST reads (Kahuna's default is direct I/O). A block-cache miss then comes from
            // the page cache instead of the device, where it would queue behind the Raft WAL's fsyncs;
            // the as-of history read runs on the key/value actor thread, so a device-bound miss stalls
            // every key on that actor. See KahunaOptionsConfig.RocksdbDirectReads for the measurement.
            // Override with kahuna.rocksdb_direct_reads.
            RocksDbDirectReads = false,
            // Hold 1 ms after a batch completes before dispatching the next (Kahuna's default is 0).
            // At full occupancy the aggregator keeps one Raft round in flight and re-dispatches the
            // instant it completes, so batches stay thin; the hold lets more items accumulate per
            // round. The timer that ends the hold fires about 1 ms late, so the effective hold is the
            // configured value plus ~1 ms: 1 ms configured buys ~2 ms of accumulation. Measured on the
            // tmpfs bank shape at 128 workers with a 1.6 ms Raft round: 2 -> 1 ms gave +2.5% throughput
            // and -0.5 ms on the commit; 0 ms made the commit fastest but ran 2.8x the rounds, which
            // slowed reads and updates on the shared executor, and the closed loop lost 5%. Free at low
            // load: an idle aggregator still dispatches on arrival. Override with
            // kahuna.key_value_write_post_completion_hold_ms.
            KeyValueWritePostCompletionHoldMs = DefaultKeyValueWritePostCompletionHoldMs,
            // Materialize on resolve: the commit's settle installs the committed values on every replica and no
            // materialization record is written. See DefaultDurableMaterializeOnResolve for the evidence and
            // the rolling-upgrade rule. Override with kahuna.durable_materialize_on_resolve.
            DurableMaterializeOnResolve = DefaultDurableMaterializeOnResolve,
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
            StorageRevision = CurrentStorageRevision,
            WalStorage = "sqlite",
            WalPath = Path.Combine(dataPath, "wal"),
            WalRevision = CurrentStorageRevision,
            InitialPartitions = 1,
            RaftWalSingleFsyncCommit = true,
            // One-phase apply-time validation is ON by default, as in ClusterBaseline — see the comment
            // there for the rules. Single-node standalone cannot hit the mixed-version fork, but the
            // upgrade step still applies: a data directory whose prepared-intent snapshot predates the
            // ledger must start once with kahuna.one_phase_apply_time_validation: false.
            OnePhaseApplyTimeValidation = true,
            // Key/value shard actor count stays at Kahuna's default, max(32, 4 x CPU cores); see
            // EffectiveActorCount. With a single Raft partition, extra actors only fragment the per-actor
            // caches, because one partition already serializes the write path. Set
            // kahuna.key_value_workers to pin the count.
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
            StorageSharedCacheMb = 320,
            StorageSharedMemtableBudgetMb = 128,
            // Buffered SST reads (Kahuna's default is direct I/O). A block-cache miss then comes from
            // the page cache instead of the device, where it would queue behind the Raft WAL's fsyncs;
            // the as-of history read runs on the key/value actor thread, so a device-bound miss stalls
            // every key on that actor. See KahunaOptionsConfig.RocksdbDirectReads for the measurement.
            // Override with kahuna.rocksdb_direct_reads.
            RocksDbDirectReads = false,
            // Hold 1 ms after a batch completes before dispatching the next (Kahuna's default is 0).
            // At full occupancy the aggregator keeps one Raft round in flight and re-dispatches the
            // instant it completes, so batches stay thin; the hold lets more items accumulate per
            // round. The timer that ends the hold fires about 1 ms late, so the effective hold is the
            // configured value plus ~1 ms: 1 ms configured buys ~2 ms of accumulation. Measured on the
            // tmpfs bank shape at 128 workers with a 1.6 ms Raft round: 2 -> 1 ms gave +2.5% throughput
            // and -0.5 ms on the commit; 0 ms made the commit fastest but ran 2.8x the rounds, which
            // slowed reads and updates on the shared executor, and the closed loop lost 5%. Free at low
            // load: an idle aggregator still dispatches on arrival. Override with
            // kahuna.key_value_write_post_completion_hold_ms.
            KeyValueWritePostCompletionHoldMs = DefaultKeyValueWritePostCompletionHoldMs,
            // Materialize on resolve: the commit's settle installs the committed values on every replica and no
            // materialization record is written. See DefaultDurableMaterializeOnResolve for the evidence and
            // the rolling-upgrade rule. Override with kahuna.durable_materialize_on_resolve.
            DurableMaterializeOnResolve = DefaultDurableMaterializeOnResolve,
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
            StorageRevision = CurrentStorageRevision,
            WalStorage = "rocksdb",
            WalPath = Path.Combine(dataPath, "wal"),
            WalRevision = CurrentStorageRevision,
            InitialPartitions = 1,
            RaftWalSingleFsyncCommit = true,
            // One-phase apply-time validation is ON by default, as in ClusterBaseline — see the comment
            // there for the rules. Single-node standalone cannot hit the mixed-version fork, but the
            // upgrade step still applies: a data directory whose prepared-intent snapshot predates the
            // ledger must start once with kahuna.one_phase_apply_time_validation: false.
            OnePhaseApplyTimeValidation = true,
            // Key/value shard actor count stays at Kahuna's default, max(32, 4 x CPU cores); see
            // EffectiveActorCount. With a single Raft partition, extra actors only fragment the per-actor
            // caches, because one partition already serializes the write path. Set
            // kahuna.key_value_workers to pin the count.
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
            StorageSharedCacheMb = 320,
            StorageSharedMemtableBudgetMb = 128,
            // Buffered SST reads (Kahuna's default is direct I/O). A block-cache miss then comes from
            // the page cache instead of the device, where it would queue behind the Raft WAL's fsyncs;
            // the as-of history read runs on the key/value actor thread, so a device-bound miss stalls
            // every key on that actor. See KahunaOptionsConfig.RocksdbDirectReads for the measurement.
            // Override with kahuna.rocksdb_direct_reads.
            RocksDbDirectReads = false,
            // Hold 1 ms after a batch completes before dispatching the next (Kahuna's default is 0).
            // At full occupancy the aggregator keeps one Raft round in flight and re-dispatches the
            // instant it completes, so batches stay thin; the hold lets more items accumulate per
            // round. The timer that ends the hold fires about 1 ms late, so the effective hold is the
            // configured value plus ~1 ms: 1 ms configured buys ~2 ms of accumulation. Measured on the
            // tmpfs bank shape at 128 workers with a 1.6 ms Raft round: 2 -> 1 ms gave +2.5% throughput
            // and -0.5 ms on the commit; 0 ms made the commit fastest but ran 2.8x the rounds, which
            // slowed reads and updates on the shared executor, and the closed loop lost 5%. Free at low
            // load: an idle aggregator still dispatches on arrival. Override with
            // kahuna.key_value_write_post_completion_hold_ms.
            KeyValueWritePostCompletionHoldMs = DefaultKeyValueWritePostCompletionHoldMs,
            // Materialize on resolve: the commit's settle installs the committed values on every replica and no
            // materialization record is written. See DefaultDurableMaterializeOnResolve for the evidence and
            // the rolling-upgrade rule. Override with kahuna.durable_materialize_on_resolve.
            DurableMaterializeOnResolve = DefaultDurableMaterializeOnResolve,
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

        RegisterStorageExtensions(baseline, kahuna);

        if (kahuna.WalSyncWrites is bool walSync)
            baseline.WalSyncWrites = walSync;

        if (kahuna.WalGroupCommitLingerMs is int lingerMs)
            baseline.RaftWalGroupCommitLingerMs = lingerMs;

        if (kahuna.WalSingleFsyncCommit is bool singleFsync)
            baseline.RaftWalSingleFsyncCommit = singleFsync;

        // Raft WAL shard column-family sizing. Each of the eight is left null on the baseline when the
        // key is absent, because null is what Kahuna reads as "leave Kommander's default for that
        // field". CamusDB deliberately has no baseline of its own here: these are overrides of a
        // default that is tuned and measured in Kommander.
        if (kahuna.WalShardWriteBufferSizeMb is int walShardWriteBufferMb)
            baseline.RaftWalShardWriteBufferSizeMb = walShardWriteBufferMb;

        if (kahuna.WalShardMinWriteBufferNumberToMerge is int walShardMinToMerge)
            baseline.RaftWalShardMinWriteBufferNumberToMerge = walShardMinToMerge;

        if (kahuna.WalShardMaxWriteBufferNumber is int walShardMaxBuffers)
            baseline.RaftWalShardMaxWriteBufferNumber = walShardMaxBuffers;

        if (kahuna.WalShardLevel0FileNumCompactionTrigger is int walShardCompactionTrigger)
            baseline.RaftWalShardLevel0FileNumCompactionTrigger = walShardCompactionTrigger;

        if (kahuna.WalShardLevel0SlowdownWritesTrigger is int walShardSlowdownTrigger)
            baseline.RaftWalShardLevel0SlowdownWritesTrigger = walShardSlowdownTrigger;

        if (kahuna.WalShardLevel0StopWritesTrigger is int walShardStopTrigger)
            baseline.RaftWalShardLevel0StopWritesTrigger = walShardStopTrigger;

        if (kahuna.WalShardMaxBytesForLevelBaseMb is int walShardLevelBaseMb)
            baseline.RaftWalShardMaxBytesForLevelBaseMb = walShardLevelBaseMb;

        if (kahuna.WalShardUniversalCompaction is bool walShardUniversal)
            baseline.RaftWalShardUniversalCompaction = walShardUniversal;

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

        if (kahuna.RecentHeartbeatMs is int recentHeartbeat)
            baseline.RecentHeartbeat = TimeSpan.FromMilliseconds(recentHeartbeat);

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
            baseline.StorageSharedCacheMb = sharedBudget;

        if (kahuna.RocksdbSharedMemtableBudgetMb is int memtableBudget)
            baseline.StorageSharedMemtableBudgetMb = memtableBudget;

        if (kahuna.RocksdbDirectReads is bool directReads)
            baseline.RocksDbDirectReads = directReads;

        if (kahuna.StorageCompactionRateMbps is int compactionRateMbps)
            baseline.StorageCompactionRateBytesPerSec = compactionRateMbps * 1_000_000L;

        if (kahuna.StorageMaxBackgroundCompactions is int maxCompactions)
            baseline.StorageMaxBackgroundCompactions = maxCompactions;

        if (kahuna.KeyValueWriteMaxInFlightBatchesPerPartition is int inFlight)
            baseline.KeyValueWriteMaxInFlightBatchesPerPartition = inFlight;

        if (kahuna.KeyValueWriteLingerMs is int writeLinger)
            baseline.KeyValueWriteLingerMs = writeLinger;

        if (kahuna.KeyValueWritePostCompletionHoldMs is int hold)
            baseline.KeyValueWritePostCompletionHoldMs = hold;

        if (kahuna.KeyValueWritePreciseWake is bool preciseWake)
            baseline.KeyValueWritePreciseWake = preciseWake;

        if (kahuna.KeyValueWriteFillTargetTrigger is bool fillTargetTrigger)
            baseline.KeyValueWriteFillTargetTrigger = fillTargetTrigger;

        if (kahuna.KeyValueWriteMaxBatchItems is int maxBatchItems)
            baseline.KeyValueWriteMaxBatchItems = maxBatchItems;

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

        // The one-phase apply-time gate and the fence horizon it judges against. Both are stated by the
        // operator or not at all: CamusDB keeps Kahuna's defaults (gate off, 600 s horizon) rather than a
        // baseline of its own, because the gate is a per-group property — every node of the cluster must
        // carry the same pair, and a CamusDB-side default would decide that for a cluster whose other
        // nodes may be running an older Kahuna that does not apply the check.
        if (kahuna.OnePhaseApplyTimeValidation is bool onePhaseApplyTime)
            baseline.OnePhaseApplyTimeValidation = onePhaseApplyTime;

        if (kahuna.StagedBaseFenceRetentionMs is int stagedBaseFenceRetention)
            baseline.StagedBaseFenceRetentionMs = stagedBaseFenceRetention;

        // Materialize on resolve: on in both baselines (DefaultDurableMaterializeOnResolve). An explicit
        // value always wins, and an explicit false is what a rolling upgrade pins until every node runs a
        // build that installs on a materializing settle (Kahuna.Core 1.10.1 or later); an older node
        // resolves without installing and loses that write locally. Turning it off is safe at any time.
        if (kahuna.DurableMaterializeOnResolve is bool materializeOnResolve)
            baseline.DurableMaterializeOnResolve = materializeOnResolve;

        // Per-page scan retry budget. Kahuna's default already sits below the shipped client command
        // deadline (5 s vs 10 s) so the named failure is observable; a deployment that raises its client
        // deadline may raise this with it, keeping the budget strictly below the deadline.
        if (kahuna.ScanPageRetryBudgetMs is int scanPageRetryBudget)
            baseline.ScanPageRetryBudgetMs = scanPageRetryBudget;

        if (kahuna.PersistentRevisionRetentionCount is int revisionRetentionCount)
            baseline.PersistentRevisionRetentionCount = revisionRetentionCount;

        if (kahuna.PersistentRevisionRetentionAgeSeconds is int revisionRetentionAge)
            baseline.PersistentRevisionRetentionAge = TimeSpan.FromSeconds(revisionRetentionAge);

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

        // Kahuna validates the shared cache and memtable budget on every engine, not only when it builds
        // the shared RocksDB bundle: whenever the cache is above 0, a memtable budget larger than it
        // stops the node at boot, because the memtables are charged into the cache. The sizing defaults
        // always give a cache above 0, so the rule applies to every node CamusDB builds, sharing on or
        // off. KahunaOptionsConfig.Validate only sees the raw config, so a single-knob override (e.g.
        // setting only the memtable budget above the total the sizing step chose) slips past it and
        // would surface as a raw Kahuna error at boot. Checking the fully-merged pair — after the sizing
        // defaults, not before them — is what makes the check see the values the node will actually be
        // built with, and it fails fast with a clear config error instead.
        if (baseline.StorageSharedCacheMb > 0
            && baseline.StorageSharedMemtableBudgetMb > baseline.StorageSharedCacheMb)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'kahuna.rocksdb_shared_memtable_budget_mb' ({baseline.StorageSharedMemtableBudgetMb}) must be <= " +
                $"'kahuna.rocksdb_shared_memory_budget_mb' ({baseline.StorageSharedCacheMb})");
        }

        // Persistent MVCC revision retention. Every version of every row is a physical row in the
        // KV store, and Kahuna's own default never prunes one — a hot table's history, and the disk
        // behind it, then grow without bound for the life of the store. When the operator set
        // neither retention knob, bound the history by age, aligned with the effective PITR window:
        // an as-of restore inside the window still finds every revision it needs (a quiet key's
        // as-of image comes from its current row, which pruning never removes), and snapshot
        // readers that must reach further back — branch forks — pin their own snapshot floors,
        // which pruning honors. Decided after the overrides so the alignment tracks an overridden
        // PITR window. Memory storage keeps no revision rows, so there is nothing to prune there.
        if (kahuna is { PersistentRevisionRetentionCount: null, PersistentRevisionRetentionAgeSeconds: null }
            && baseline.Storage != "memory"
            && baseline.PersistentRevisionRetentionCount == 0
            && baseline.PersistentRevisionRetentionAge == TimeSpan.Zero)
            baseline.PersistentRevisionRetentionAge = baseline.PitrWindow;

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
            baseline.StorageSharedCacheMb = BlockCacheMb;

        // Derived from whatever the total ended up being, not pinned: an operator who set only the total
        // to something below 16 MiB would otherwise get a memtable sub-budget larger than the cache it
        // is charged against, which the cross-field check above rejects at boot.
        if (kahuna.RocksdbSharedMemtableBudgetMb is null)
            baseline.StorageSharedMemtableBudgetMb = Math.Min(MemtableMb, baseline.StorageSharedCacheMb);

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
    /// <para><b>Two memory sizes, one per kind of cache.</b> The RocksDB block cache and memtables are
    /// native allocations, so they are sized from the memory the whole process may use
    /// (<see cref="MachineMemory.ReadTotal"/>: the container's cgroup limit, else physical RAM). The
    /// key/value actor caches live in the managed heap, so they are sized from the heap's budget
    /// (<see cref="MachineMemory.ManagedHeapBytes"/>). The two differ whenever a GC heap limit is in
    /// force, and sizing the native pair from the heap limit is the mistake this split corrects: a
    /// 4,096 MB container with <c>GCHeapHardLimitPercent</c> 60% reported 2,458 MB, which gave a
    /// 245 MB block cache and a 61 MB memtable budget. Under a heap limit the RocksDB budgets therefore
    /// sit <i>outside</i> it and count against the container beside it.</para>
    ///
    /// <para><b>Solarium is the exception.</b> Solarium (<see cref="UsesSolarium"/>) keeps its cache and
    /// its memtables as managed objects, so on a Solarium node the same two budgets live <i>inside</i>
    /// the heap limit, next to the actor caches and the application heap, and are sized from the
    /// managed heap budget instead: a 4,096 MB container with a 60% heap limit gets a 245 MB cache
    /// and a 122 MB memtable budget rather than 409 and 192. Sizing them from the container is the
    /// mirror-image mistake: a 2-hour fault soak on such a node with explicit 768/256 MiB budgets ran
    /// the heap at 2,100-2,420 MiB of its 2,458 MiB limit and the leader died of a managed
    /// OutOfMemoryException; the same budgets under a 3,072 MiB heap limit passed.</para>
    ///
    /// <para>Sizing policy: RocksDB block cache = 10% of machine memory; memtable budget = a quarter of
    /// the block cache, raised to the Raft log's flush unit where the two databases share one
    /// WriteBufferManager (<see cref="RaftLogFlushUnitFloorMb"/>, gated by
    /// <see cref="RaftWalFlushUnitFloorEnabled"/>, on since Kommander 1.6.6); key/value actor caches = 6.25% of
    /// the managed heap budget (at least 64 MB for the layer as a whole) divided across the shard
    /// actors, with the per-actor entry cap derived at an assumed ~512 B/entry. Ceilings: 2 GB block
    /// cache, 1 GB memtables, 2 GB per actor, 4M entries. Roughly 16% of memory across both cache
    /// layers at the defaults, and never more than 4 GB in total however large the machine is. The
    /// fractions and the ceilings are deliberately modest: an unconfigured node is far more often a
    /// developer workstation or a CI container sharing the box with a compiler and an IDE than a
    /// dedicated database server, and an over-eager default there costs the whole machine. A dedicated
    /// server should raise all four keys explicitly — see the worked example in <c>config.yml</c>.
    /// Every value remains overridable via the corresponding <c>kahuna.*</c> key.</para>
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
    /// <c>key_value_workers</c>, <c>storage</c>, <c>wal_storage</c> and <c>wal_shard_*</c> overrides,
    /// since the per-actor split divides by the final actor count (<see cref="EffectiveActorCount"/>)
    /// and the memtable floor reads the final WAL backend and shard tuning.</para>
    /// </summary>
    private static void ApplyMemoryProportionalDefaults(EmbeddedKahunaOptions baseline, KahunaOptionsConfig kahuna)
    {
        long machineBytes = MachineMemory.ReadTotal().Bytes;
        long managedHeapBytes = MachineMemory.ManagedHeapBytes();

        // Each size stands in for the other when only one is known; neither known keeps the fixed
        // baseline values.
        if (machineBytes <= 0)
            machineBytes = managedHeapBytes;

        if (managedHeapBytes <= 0)
            managedHeapBytes = machineBytes;

        if (machineBytes <= 0)
            return;

        ApplyMemoryProportionalDefaults(baseline, kahuna, machineBytes, managedHeapBytes);
    }

    /// <summary>
    /// Whether the memtable sub-budget is floored at the Raft-log flush unit (<see cref="RaftLogFlushUnitFloorMb"/>).
    /// On since Kommander 1.6.6. It shipped off on Kommander 1.6.5 because an unstarved budget let the Raft-log
    /// RocksDB keep every write-ahead <c>.log</c> file for the life of the process (k182 probe, Kahuna 1.8.1:
    /// 443 MiB to 2,709 MiB in ten minutes per node, growing linearly, against ~230 MB of live tables) — a column
    /// family that never flushes (the meta partition's shard) pinned them and nothing else ever forced it; the
    /// starved budget had been masking that by flushing everything on its cadence. Kommander 1.6.6 bounds the
    /// write-ahead files, so the floor's gain (no budget-forced Raft-log flushes, Kommander's open-time warning
    /// gone, ~2% fewer device bytes per operation) no longer comes with an unbounded directory.
    ///
    /// <para>Test seam: the internal <see cref="ApplyMemoryProportionalDefaults(EmbeddedKahunaOptions, KahunaOptionsConfig, long, long, bool?)"/>
    /// overload takes the gate as a parameter so unit tests exercise both states without mutating this static;
    /// only a <c>[NonParallelizable]</c> end-to-end test may flip it, and must restore it.</para>
    /// </summary>
    internal static bool RaftWalFlushUnitFloorEnabled = true;

    /// <summary>
    /// CamusDB's shipped post-completion hold for Kahuna's key/value write aggregator, in
    /// milliseconds (Kahuna's own default is 0). The aggregator keeps exactly one Raft batch in
    /// flight per partition and re-dispatches the moment a batch completes, so at full occupancy
    /// batches carry whatever arrived during one round; holding before the next dispatch lets more
    /// items accumulate per round. The timer that ends the hold fires about 1 ms late, so the
    /// effective hold is this value plus ~1 ms. The default was 2 ms while the round was ~2.4 ms
    /// (it roughly doubled the items per round, 110 vs 51 at 128 workers on the bank shape, for
    /// +6% throughput and a lower write p50). With the round at ~1.6 ms, a one-variable sweep on the
    /// same shape measured 2 / 1 / 0 ms at 9,193 / 9,421 / 8,755 ops/s: 1 ms keeps ~128 items per
    /// batch and takes 0.5 ms off the commit, while 0 ms runs 2.8x the rounds and slows the reads
    /// and updates that share the partition executor. It costs nothing at low load because an idle
    /// aggregator dispatches on arrival. An explicit <c>kahuna.key_value_write_post_completion_hold_ms</c>
    /// (including 0) always wins.
    /// </summary>
    internal const int DefaultKeyValueWritePostCompletionHoldMs = 1;

    /// <summary>
    /// CamusDB's baseline for <c>kahuna.durable_materialize_on_resolve</c>: on. The commit's settle installs
    /// each committed value from the prepared intent on every replica and Kahuna writes no materialization
    /// record per key, so a two-row transaction costs about four Raft entries and two scheduler submissions
    /// on its anchor partition instead of six and four. Measured on the tmpfs bank shape: 6.01 -> 4.00
    /// entries per commit, +2% throughput; on a device under two-hour fault soaks: 4.0 entries, cluster CPU
    /// -9% per operation, conservation exact and no divergence on two consecutive clean runs across two
    /// released Kahuna versions (1.10.4 and 1.10.5), which is the bar this default waited for. The record
    /// shape is a per-group property: every node must run Kahuna.Core 1.10.1 or later before any node has
    /// it on, so a rolling upgrade from an older build pins <c>durable_materialize_on_resolve: false</c>
    /// on every node until the last one is upgraded, then removes the key. An explicit value always wins.
    /// </summary>
    internal const bool DefaultDurableMaterializeOnResolve = true;

    /// <summary>
    /// Core of the proportional sizing for a machine with no GC heap limit, where the native and the
    /// managed sizes are the same number. Test seam; see the four-argument overload.
    /// </summary>
    internal static void ApplyMemoryProportionalDefaults(
        EmbeddedKahunaOptions baseline,
        KahunaOptionsConfig kahuna,
        long totalRam,
        bool? raftLogFlushUnitFloor = null)
        => ApplyMemoryProportionalDefaults(baseline, kahuna, totalRam, totalRam, raftLogFlushUnitFloor);

    /// <summary>
    /// Core of the proportional sizing, driven by explicit memory sizes. Split out (internal) so
    /// tests can assert the floor behavior at container and heap-limit sizes the test machine does
    /// not have; the private path above always passes the real sizes.
    /// <paramref name="machineMemoryBytes"/> sizes the native RocksDB budgets and
    /// <paramref name="managedHeapBytes"/> sizes the managed actor caches, and the shared budgets
    /// too when Solarium holds them (<see cref="UsesSolarium"/>).
    /// <paramref name="raftLogFlushUnitFloor"/> overrides <see cref="RaftWalFlushUnitFloorEnabled"/>
    /// when given (a test seam); null takes the shipped gate.
    /// </summary>
    internal static void ApplyMemoryProportionalDefaults(
        EmbeddedKahunaOptions baseline,
        KahunaOptionsConfig kahuna,
        long machineMemoryBytes,
        long managedHeapBytes,
        bool? raftLogFlushUnitFloor = null)
    {
        const long OneMb = 1024L * 1024;

        // The shared cache and memtable budget are native RocksDB memory, sized from the container,
        // unless Solarium holds them: its cache and memtables are managed objects and count against
        // the heap limit, so there the heap budget is the base (see the summary).
        long sharedBudgetBaseBytes = UsesSolarium(baseline) ? managedHeapBytes : machineMemoryBytes;

        if (kahuna.RocksdbSharedMemoryBudgetMb is null)
            baseline.StorageSharedCacheMb = (int)(ClampWithYieldingFloor(
                sharedBudgetBaseBytes / 10, historicFloor: 320 * OneMb, degenerateFloor: 64 * OneMb, ceiling: 2048 * OneMb) / OneMb);

        if (kahuna.RocksdbSharedMemtableBudgetMb is null)
        {
            int proportionalMb = (int)ClampWithYieldingFloor(
                baseline.StorageSharedCacheMb / 4, historicFloor: 128, degenerateFloor: 16, ceiling: 1024);

            int floorMb = (raftLogFlushUnitFloor ?? RaftWalFlushUnitFloorEnabled) ? RaftLogFlushUnitFloorMb(baseline) : 0;
            baseline.StorageSharedMemtableBudgetMb = Math.Max(proportionalMb, floorMb);
        }

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
            long actorLayerBytes = Math.Max(managedHeapBytes / 16, 64L * OneMb);
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
    /// True when Kahuna will build one shared block cache and WriteBufferManager for the key/value
    /// store and the Raft WAL. That happens only with sharing on and both databases on RocksDB
    /// (see <c>EmbeddedKahunaNode.CreateSharedResources</c>); otherwise no engine uses the two budget
    /// knobs. Kahuna still validates the pair on every engine, so the memtable budget must stay at or
    /// below the cache even when this is false.
    /// </summary>
    internal static bool SharesRocksDbMemory(EmbeddedKahunaOptions options) =>
        options is { RocksDbSharedMemoryEnabled: true, Storage: "rocksdb", WalStorage: "rocksdb" };

    /// <summary>
    /// The shared memtable budget below which the Raft log cannot flush at its own flush unit: one
    /// flush unit for a single active shard (<c>write_buffer_size × min_write_buffer_number_to_merge</c>)
    /// plus one memtable of headroom for whatever shares the budget — 192 MiB at Kommander's defaults.
    /// This is the exact figure Kommander's <c>RocksDbWAL</c> checks at open, and it warns when the
    /// budget is smaller.
    ///
    /// <para>Why it matters: RocksDB flushes the oldest memtable of whichever database trips an
    /// over-budget WriteBufferManager, and the WAL trips it on nearly every Raft batch. An undersized
    /// budget therefore replaces the configured 128 MiB flush unit with a few-MiB one (every one of
    /// 349 flushes in a 10-minute write probe at a 61 MiB budget was budget-forced), and log entries
    /// that would have died in the memtable reach disk once.</para>
    ///
    /// <para>Unset <c>wal_shard_*</c> fields take <see cref="Kommander.WAL.RocksDbWalTuning.Default"/>,
    /// read from Kommander rather than copied, so a Kommander retune moves this floor with it.</para>
    /// </summary>
    internal static long RaftLogFlushUnitHeadroomBytes(EmbeddedKahunaOptions options)
    {
#if CAMUSDB_BROWSER
        // The browser build has no RocksDB WAL, so there is no Raft-log flush unit to fit. Both
        // callers return before this point anyway, because the browser node never shares RocksDB memory.
        return 0;
#else
        Kommander.WAL.RocksDbWalTuning defaults = Kommander.WAL.RocksDbWalTuning.Default;

        long writeBufferBytes = options.RaftWalShardWriteBufferSizeMb is int writeBufferMb
            ? writeBufferMb * 1024L * 1024
            : defaults.ShardWriteBufferSizeBytes;

        int merge = options.RaftWalShardMinWriteBufferNumberToMerge ?? defaults.ShardMinWriteBufferNumberToMerge;

        return writeBufferBytes * merge + writeBufferBytes;
#endif
    }

    /// <summary>
    /// The smallest memtable budget the proportional sizing may choose: the Raft log's flush unit plus
    /// headroom (<see cref="RaftLogFlushUnitHeadroomBytes"/>) when the store and the WAL share one
    /// WriteBufferManager, the Solarium WAL's flush unit plus headroom
    /// (<see cref="SolariumWalFlushUnitHeadroomBytes"/>) when the WAL is Solarium, which always charges
    /// its memtables to the node's shared budget, else 0 (no floor — an unshared WAL has its own
    /// memtables and ignores the budget).
    ///
    /// <para>The floor yields to <b>half of the total budget</b>. The memtable budget is charged inside
    /// the block cache, so a memtable floor that took the whole cache would leave no read cache under
    /// write load. Half is the largest share that still leaves reads a real cache, and it is enough
    /// for the reference node: a 4,096 MB container has a 409 MiB total, whose half holds the
    /// 192 MiB unit. A smaller node (below about 3.75 GiB at Kommander's defaults) keeps a
    /// budget-forced flush cadence and Kommander's warning; raise both budgets or lower the shard
    /// tuning there.</para>
    ///
    /// <para>The trade-off was taken knowingly: a follower restart took 13.2 s at a 61 MiB budget and
    /// 18.4 s with a budget that fits the flush unit, because a wider flush unit is a wider replay
    /// unit (measured on Kommander 1.6.0, before its reclaim fix).</para>
    ///
    /// <para>Applied only by the <see cref="MemoryProfile.Prod"/> sizing, only when the memtable
    /// key is unset, and only while <see cref="RaftWalFlushUnitFloorEnabled"/> is set — on since
    /// Kommander 1.6.6 bounded the Raft-log write-ahead files (see the flag). The <see cref="MemoryProfile.Dev"/> profile
    /// promises a fixed small footprint and keeps its 16 MiB budget. This method computes the floor
    /// itself, regardless of the gate; the caller applies the gate.</para>
    /// </summary>
    internal static int RaftLogFlushUnitFloorMb(EmbeddedKahunaOptions options)
    {
        long neededBytes;

        if (SharesRocksDbMemory(options))
            neededBytes = RaftLogFlushUnitHeadroomBytes(options);
        else if (options.WalStorage == SolariumEngine)
            neededBytes = SolariumWalFlushUnitHeadroomBytes();
        else
            return 0;

        const long OneMb = 1024L * 1024;
        long neededMb = (neededBytes + OneMb - 1) / OneMb;

        return (int)Math.Min(neededMb, options.StorageSharedCacheMb / 2);
    }

    /// <summary>
    /// Registers the storage engines that ship outside Kahuna.Core with the node's
    /// <see cref="EmbeddedKahunaOptions.StorageProviders"/>, when the store or the WAL selects one.
    /// Kahuna resolves <see cref="EmbeddedKahunaOptions.Storage"/> and
    /// <see cref="EmbeddedKahunaOptions.WalStorage"/> by name in that registry when it builds the node,
    /// so an engine missing from it stops the node at boot with an unknown-engine error.
    ///
    /// <para>Today that is Solarium (SolariumDb.Kahuna), the engine for both <c>solarium</c> names.
    /// Both providers are registered together even when only one layer selects it, because Kahuna looks
    /// up each layer separately and a registration costs nothing. The providers take no storage
    /// environment, so the node's <c>solarium.environment</c> extension option chooses it; that option
    /// is set only from <see cref="KahunaOptionsConfig.SolariumEnvironment"/>, and the same value reaches
    /// both layers, which is what lets a Solarium store and a Solarium WAL share one cache and one
    /// memtable budget (see <see cref="SolariumWalFlushUnitHeadroomBytes"/>).</para>
    ///
    /// <para>Must run after the <c>storage</c> and <c>wal_storage</c> overrides, which pick the names.
    /// The browser build has no Solarium (it needs files and threads), so there the names are
    /// refused here with a config error rather than by Kahuna's registry at boot.</para>
    /// </summary>
    internal static void RegisterStorageExtensions(EmbeddedKahunaOptions baseline, KahunaOptionsConfig kahuna)
    {
        if (!UsesSolarium(baseline))
            return;

#if CAMUSDB_BROWSER
        throw new CamusDBException(
            CamusDBErrorCodes.InvalidConfig,
            $"The browser build has no Solarium engine; storage '{baseline.Storage}' and WAL '{baseline.WalStorage}' must not be 'solarium'");
#else
        baseline.StorageProviders.Register(new Solarium.Kahuna.SolariumPersistenceBackendProvider());
        baseline.StorageProviders.Register(new Solarium.Kahuna.SolariumWalProvider());

        if (kahuna.SolariumEnvironment is { } environment)
            baseline.ExtensionOptions = new Dictionary<string, string>(baseline.ExtensionOptions, StringComparer.Ordinal)
            {
                [SolariumEnvironmentOption] = environment,
            };
#endif
    }

    /// <summary>The extension option through which SolariumDb.Kahuna reads its storage environment.</summary>
    internal const string SolariumEnvironmentOption = "solarium.environment";

    /// <summary>True when the key/value store or the Raft WAL runs on the Solarium engine.</summary>
    internal static bool UsesSolarium(EmbeddedKahunaOptions options) =>
        options.Storage == SolariumEngine || options.WalStorage == SolariumEngine;

    /// <summary>The name that selects Solarium for <c>kahuna.storage</c> and <c>kahuna.wal_storage</c>.</summary>
    internal const string SolariumEngine = "solarium";

    /// <summary>
    /// The share of the managed heap budget above which the memory Solarium charges to the heap is
    /// warned about at startup, in percent. The proportional sizing gives the cache 10% of the heap
    /// budget and the actor caches 6.25%, so an unset node sits near 16% and never warns; a third is
    /// twice that share. The one measured pair of points brackets it: a 4,096 MiB node with explicit
    /// 768 MiB cache and 2458 MiB heap limit (37%) died of a managed OutOfMemoryException 57 minutes into
    /// a fault soak, the same budgets under a 3,072 MiB limit (30%) ran 2 hours with the heap committed
    /// peaking near the limit. The rule is a guard against the configuration that was measured to fail,
    /// not a sizing recommendation: the application heap under load needs about 2 GiB on that node
    /// whatever the budgets are.
    /// </summary>
    internal const int SolariumHeapShareWarnPercent = 33;

    /// <summary>
    /// The bytes a Solarium node's budgets place inside the managed heap: the shared cache budget
    /// (the memtable budget is charged inside it, so it is not added) plus the key/value actor caches
    /// (<see cref="EmbeddedKahunaOptions.MaxBytesPerActor"/> times <see cref="EffectiveActorCount"/>).
    /// The actor caches are managed on every engine; they are counted here because the warning is about
    /// the heap as a whole. 0 when neither layer is Solarium.
    /// </summary>
    internal static long SolariumHeapChargedBytes(EmbeddedKahunaOptions options)
    {
        if (!UsesSolarium(options))
            return 0;

        return options.StorageSharedCacheMb * 1024L * 1024 + options.MaxBytesPerActor * EffectiveActorCount(options);
    }

    /// <summary>
    /// Whether the memory a Solarium node charges to the managed heap (<see cref="SolariumHeapChargedBytes"/>)
    /// takes more than <see cref="SolariumHeapShareWarnPercent"/> of <paramref name="managedHeapBytes"/>, the
    /// heap budget the GC reports. Pure, so tests can drive it with the reference node's sizes; the embedded
    /// node calls it once at construction with the real budget and logs a warning when it is true. Never
    /// true on a RocksDB node, whose engine memory is native, or when the heap budget is unknown (0).
    /// </summary>
    internal static bool SolariumBudgetsCrowdTheHeap(EmbeddedKahunaOptions options, long managedHeapBytes, out long chargedBytes, out int percent)
    {
        chargedBytes = SolariumHeapChargedBytes(options);
        percent = 0;

        if (chargedBytes <= 0 || managedHeapBytes <= 0)
            return false;

        percent = (int)(chargedBytes * 100 / managedHeapBytes);
        return percent > SolariumHeapShareWarnPercent;
    }

    /// <summary>
    /// The shared memtable budget below which a Solarium WAL cannot flush the Raft log at its own flush
    /// unit: one shard memtable (<c>SolariumWalOptions.ShardWriteBufferSizeBytes</c>, 128 MiB) plus one
    /// more for what shares the budget — 256 MiB. This is the figure <c>SolariumWAL</c> checks at open,
    /// and it warns when the budget is smaller. The reason is the one
    /// <see cref="RaftLogFlushUnitHeadroomBytes"/> gives for the RocksDB WAL: the database that writes
    /// next flushes its own oldest memtable once the budget is full, the WAL writes on every Raft batch,
    /// so a small budget turns the 128 MiB flush unit into a few MiB.
    ///
    /// <para>Unlike the RocksDB pair, a Solarium WAL needs no switch to share: Kahuna hands every
    /// engine the node's shared memory whenever a shared size is above 0, and CamusDB's sizing always
    /// sets one. The <c>wal_shard_*</c> keys are RocksDB WAL tuning and do not move this figure; it is
    /// read from Solarium's own defaults, so a Solarium retune moves it with them.</para>
    /// </summary>
    internal static long SolariumWalFlushUnitHeadroomBytes()
    {
#if CAMUSDB_BROWSER
        return 0;
#else
        return 2 * Solarium.Kommander.SolariumWalOptions.Default.ShardWriteBufferSizeBytes;
#endif
    }

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
