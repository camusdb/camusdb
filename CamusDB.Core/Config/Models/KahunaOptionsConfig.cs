
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config.Models;

/// <summary>
/// Allow-listed Kahuna engine tunables surfaced through the <c>kahuna:</c> YAML section and
/// applied to <see cref="Kahuna.EmbeddedKahunaOptions"/> for both cluster and standalone nodes.
/// Unset fields keep the mode-specific CamusDB baseline. Both modes default to RocksDB for KV and WAL
/// (see <see cref="Storage.Kv.EmbeddedKahunaOptionsBuilder"/>: standalone via
/// <see cref="Storage.Kv.EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline"/>, cluster via
/// <see cref="Storage.Kv.EmbeddedKahunaOptionsBuilder.ClusterBaseline"/> which adds election timeouts);
/// a <c>kahuna: { storage: sqlite, wal_storage: sqlite }</c> override restores the sqlite backend.
/// </summary>
public sealed class KahunaOptionsConfig
{
    internal static readonly HashSet<string> AllowedYamlKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "storage",
        "storage_revision",
        "wal_storage",
        "wal_revision",
        "wal_sync_writes",
        "wal_group_commit_linger_ms",
        "wal_single_fsync_commit",
        "default_transaction_timeout_ms",
        "max_transaction_timeout_ms",
        "max_concurrent_sessions",
        "default_admission_wait_ms",
        "max_admission_wait_ms",
        "transaction_priority_reserved_slots",
        "transaction_priority_aging_threshold",
        "transaction_priority_max_queued",
        "locks_workers",
        "key_value_workers",
        "background_writer_workers",
        "read_io_threads",
        "write_io_threads",
        "start_election_timeout_ms",
        "end_election_timeout_ms",
        "start_election_timeout_increment_ms",
        "end_election_timeout_increment_ms",
        "heartbeat_interval_ms",
        "voting_timeout_ms",
        "max_entries_per_actor",
        "max_bytes_per_actor",
        "cache_entry_ttl_ms",
        "cache_entries_to_remove",
        "collection_interval_ms",
        "compact_every_operations",
        "compact_number_entries",
        "max_entries_per_compaction",
        "rocksdb_shared_memory",
        "rocksdb_shared_memory_budget_mb",
        "rocksdb_shared_memtable_budget_mb",
        "backup_dir",
        "pitr_window_seconds",
        "base_snapshot_interval_seconds",
        "restore_root",
        "allow_unconfined_remote_restore",
        "backup_cluster_id",
        "backup_mac_key_file",
        "backup_retention_max_chains",
        "backup_retention_max_age_seconds",
        "backup_retention_max_bytes",
        "backup_gc_interval_seconds",
        "backup_restore_throttle_bytes_per_sec",
        "range_split_threshold",
        "range_split_min_range_size",
        "range_split_load_threshold",
        "range_split_load_min_queue_depth",
        "range_split_load_min_commit_wait_ms",
        "range_split_load_window_ms",
        "range_split_load_poll_interval_ms",
        "range_split_load_imbalance_max",
        "range_split_settle_window_ms",
        "range_split_indivisible_cooldown_ms",
        "range_move_settle_timeout_ms",
        "range_merge_min_size",
        "enable_load_reports",
        "replication_factor",
        "zone",
        "enable_placement_rebalancer",
        "placement_pass_interval_ms",
        "max_replica_moves_per_pass",
        "max_concurrent_replica_transfers",
        "max_concurrent_replica_repairs",
        "replica_count_deadband",
        "decommission_drain_timeout_ms",
        "enable_leader_balancer",
        "leader_balancer_interval_ms",
        "leader_balancer_report_interval_ms",
        "leader_balancer_report_ttl_ms",
        "min_leader_stability_ms",
    };

    /// <summary>Persistence backend: <c>memory</c>, <c>sqlite</c>, or <c>rocksdb</c>.</summary>
    public string? Storage { get; set; }

    public string? StorageRevision { get; set; }

    /// <summary>Raft WAL backend: <c>memory</c>, <c>sqlite</c>, or <c>rocksdb</c>.</summary>
    public string? WalStorage { get; set; }

    public string? WalRevision { get; set; }

    public bool? WalSyncWrites { get; set; }

    /// <summary>
    /// Group-commit linger window in milliseconds (0 = disabled, Kahuna's default). When positive, a WAL
    /// worker briefly waits — bounded by this window — to gather more ready commits into a single group
    /// fsync before flushing, so many concurrent commits amortize one disk barrier instead of each paying
    /// its own. This raises WAL batch density (and thus write throughput) at the cost of a small, bounded
    /// per-commit latency floor; it does <b>not</b> weaken durability, since data is still fsync'd before
    /// the commit is acknowledged. Most impactful on write-heavy concurrent load where fsync latency
    /// dominates (e.g. macOS/APFS). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RaftWalGroupCommitLingerMs"/>.
    /// </summary>
    public int? WalGroupCommitLingerMs { get; set; }

    /// <summary>
    /// Single-fsync commit fast path. When enabled, an auto-commit single-round proposal releases its
    /// client ticket as soon as the propose quorum is durable and demotes the per-entry commit marker to a
    /// lazy write that rides the next durable flush — removing one serial fsync from the commit critical
    /// path without weakening durability. The Kahuna embedded default is <c>false</c> (two-fsync commit);
    /// enabling it is the recommended standalone setting for write throughput. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RaftWalSingleFsyncCommit"/>.
    /// </summary>
    public bool? WalSingleFsyncCommit { get; set; }

    public int? DefaultTransactionTimeoutMs { get; set; }

    /// <summary>
    /// Hard upper bound (milliseconds) on any admitted transaction session timeout. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.MaxTransactionTimeout"/>. The engine passes
    /// <c>MaxSerializableTransactionLifetimeMs</c> as each session's timeout, and Kahuna clamps every
    /// requested timeout to this cap — so leaving it at Kahuna's 300 s default silently truncates a
    /// longer serializable lifetime. When unset, the option builder derives it from the configured
    /// lifetime so the two agree; when set, it must be &gt;= that lifetime (checked in
    /// <see cref="ConfigDefinition.Validate"/>) or the lifetime cap is unreachable. Raising it also
    /// lengthens how long an abandoned session's MVCC snapshot is retained before reaping.
    /// </summary>
    public int? MaxTransactionTimeoutMs { get; set; }

    // ── Transaction admission gate ────────────────────────────────────────────────────────────
    // These configure the Kahuna node's priority admission gate, which decides which of several
    // waiting transactions starts next once the node is at its concurrency ceiling. All default to
    // "off": with no ceiling every transaction is admitted immediately and priority is recorded but
    // never consulted.
    //
    // Kahuna also exposes a second ceiling, `max_concurrent_transactions`, for its script-transaction
    // path. It is deliberately NOT exposed here: CamusDB opens every transaction through
    // LocateAndStartTransaction (the interactive-session path) and never runs a script transaction,
    // so that knob would gate nothing while advertising that it does.

    /// <summary>
    /// Maximum Kahuna coordinator sessions open at once on this node; further transactions queue and
    /// are started in priority order. <c>0</c> (the default) disables the gate.
    ///
    /// <para><b>For CamusDB this is a ceiling on concurrent work, not on connections.</b> The engine
    /// opens a session per transaction — including its own catalog writes, schema checkpoints and
    /// index backfill — so a ceiling below normal concurrency converts a healthy node into a queueing
    /// one. Size it at or above observed healthy concurrency, and pair it with
    /// <see cref="TransactionPriorityReservedSlots"/> so latency-critical engine work can always get
    /// in. (Kahuna's own guide describes this knob for clients that hold one session per user session,
    /// where it bounds connections; that advice does not transfer.)</para>
    ///
    /// <para>A transaction that cannot be admitted queues for at most the admission wait budget
    /// (<see cref="DefaultAdmissionWaitMs"/>, or the engine's <c>transaction_admission_wait_ms</c>)
    /// and is then refused with a retryable error — seconds, not the session lifetime.</para>
    /// </summary>
    public int? MaxConcurrentSessions { get; set; }

    /// <summary>
    /// Milliseconds a transaction that did not request its own budget queues at the gate before being
    /// refused. This is the node-side default behind the engine's <c>transaction_admission_wait_ms</c>;
    /// the Kahuna default is 5 000.
    ///
    /// <para>Keep it well below the session lifetime. It bounds the <i>door-wait</i>, not the
    /// transaction, and a long door-wait makes a saturated node hold requests open instead of shedding
    /// them — which is the opposite of what the gate is for.</para>
    /// </summary>
    public int? DefaultAdmissionWaitMs { get; set; }

    /// <summary>
    /// Hard upper bound (milliseconds) on any admission wait; a caller-supplied budget is clamped to
    /// it, so no client can occupy a queue slot for longer than the operator allows. The Kahuna default
    /// is 30 000. Bounds queue <i>duration</i>, where <see cref="TransactionPriorityMaxQueued"/> bounds
    /// queue <i>depth</i> — a node needs both, since one patient caller can otherwise hold a slot while
    /// everyone else is refused.
    /// </summary>
    public int? MaxAdmissionWaitMs { get; set; }

    /// <summary>
    /// Session slots only <c>High</c>/<c>Critical</c> transactions may occupy, reserved out of
    /// <see cref="MaxConcurrentSessions"/>. Guarantees latency-critical work can start no matter how
    /// much bulk work is offered. <c>0</c> is the default; 1–2 is usually enough, and a large reserve
    /// throttles ordinary traffic because it is subtracted from what ordinary work may use.
    /// </summary>
    public int? TransactionPriorityReservedSlots { get; set; }

    /// <summary>
    /// Milliseconds a queued transaction waits to gain one effective priority level, compounding up to
    /// just below <c>Critical</c>. This is the anti-starvation bound. <c>0</c> disables aging and
    /// permits indefinite starvation of low-priority work.
    ///
    /// <para>At the 1 000 ms default a <c>Background</c> transaction reaches <c>High</c> after roughly
    /// three seconds of waiting, so background work yields for seconds rather than indefinitely. Raise
    /// it to make low-priority work genuinely patient — but it is a single global rate, so raising it
    /// also lengthens the worst-case wait for a genuinely starved ordinary transaction.</para>
    /// </summary>
    public int? TransactionPriorityAgingThreshold { get; set; }

    /// <summary>
    /// Maximum transactions that may wait at the gate before further ones are refused outright with a
    /// retryable backpressure error. Bounds the memory the queue itself consumes during the overload
    /// it exists to survive. <c>0</c> is unbounded; the Kahuna default is 4 096.
    /// </summary>
    public int? TransactionPriorityMaxQueued { get; set; }

    public int? LocksWorkers { get; set; }

    public int? KeyValueWorkers { get; set; }

    public int? BackgroundWriterWorkers { get; set; }

    public int? ReadIoThreads { get; set; }

    public int? WriteIoThreads { get; set; }

    public int? StartElectionTimeoutMs { get; set; }

    public int? EndElectionTimeoutMs { get; set; }

    public int? StartElectionTimeoutIncrementMs { get; set; }

    public int? EndElectionTimeoutIncrementMs { get; set; }

    public int? HeartbeatIntervalMs { get; set; }

    public int? VotingTimeoutMs { get; set; }

    public int? MaxEntriesPerActor { get; set; }

    public long? MaxBytesPerActor { get; set; }

    /// <summary>
    /// Idle time-to-live for an in-memory cache entry before the background collection sweep is
    /// allowed to evict it. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.CacheEntryTtl"/>
    /// (expressed here in milliseconds).
    /// </summary>
    public int? CacheEntryTtlMs { get; set; }

    /// <summary>
    /// Maximum number of aged-out entries the collection sweep evicts per pass. Bounds the work
    /// (and lock hold time) of a single sweep. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.CacheEntriesToRemove"/>.
    /// </summary>
    public int? CacheEntriesToRemove { get; set; }

    /// <summary>
    /// Cadence of the background collection sweep that evicts entries past <see cref="CacheEntryTtlMs"/>.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.CollectionInterval"/> (in milliseconds).
    /// </summary>
    public int? CollectionIntervalMs { get; set; }

    public int? CompactEveryOperations { get; set; }

    /// <summary>
    /// Number of trailing Raft-log entries retained (not compacted away) at each compaction point.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.CompactNumberEntries"/>. Governs the same
    /// compaction pass as <see cref="CompactEveryOperations"/>; tune them together.
    /// </summary>
    public int? CompactNumberEntries { get; set; }

    /// <summary>
    /// Upper bound on how many log entries a single compaction pass may remove, capping its cost.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.MaxEntriesPerCompaction"/>.
    /// </summary>
    public int? MaxEntriesPerCompaction { get; set; }

    /// <summary>
    /// Enables the shared RocksDB block-cache and WriteBufferManager across the KV backend and the
    /// Raft WAL. This is a no-op unless both <c>storage</c> and <c>wal_storage</c> are set to
    /// <c>rocksdb</c> — Kahuna silently skips the shared bundle otherwise.
    /// When both databases are RocksDB, enabling this bounds total cache+memtable memory to one
    /// shared budget (<see cref="RocksdbSharedMemoryBudgetMb"/>) instead of two independent ones.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.RocksDbSharedMemoryEnabled"/>.
    /// </summary>
    public bool? RocksdbSharedMemory { get; set; }

    /// <summary>
    /// Total shared block-cache budget in MiB when <see cref="RocksdbSharedMemory"/> is enabled.
    /// The memtable sub-budget (<see cref="RocksdbSharedMemtableBudgetMb"/>) lives inside this budget
    /// and must be &lt;= this value. Must be &gt; 0 when set.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.RocksDbSharedMemoryBudgetMb"/>.
    /// </summary>
    public int? RocksdbSharedMemoryBudgetMb { get; set; }

    /// <summary>
    /// Memtable sub-budget in MiB, cost-charged into the shared block-cache budget. Must be &gt; 0
    /// when set, and must be &lt;= <see cref="RocksdbSharedMemoryBudgetMb"/> when both are provided.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.RocksDbSharedMemtableBudgetMb"/>.
    /// </summary>
    public int? RocksdbSharedMemtableBudgetMb { get; set; }

    /// <summary>
    /// Filesystem directory where node-wide backups (base images, WAL segments, manifests) are written
    /// and read. Enabling backups is opt-in: when null/empty, backups are <b>disabled</b> and the
    /// backup/PITR admin API reports <see cref="CamusDBErrorCodes.BackupNotConfigured"/>. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BackupDir"/>. Must not be blank when the key is present.
    /// </summary>
    public string? BackupDir { get; set; }

    /// <summary>
    /// Point-in-time-recovery retention window, in <b>seconds</b>: how far back a restore may target.
    /// Also bounds how much WAL Kahuna keeps for recovery. Must be &gt; 0 and &lt;= 21600 (6 hours), the
    /// Kahuna maximum. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.PitrWindow"/> (a
    /// <see cref="System.TimeSpan"/>). Also used by the CamusDB-side restore window guard.
    /// </summary>
    public int? PitrWindowSeconds { get; set; }

    /// <summary>
    /// How often, in <b>seconds</b>, a fresh base image is taken per shard. Smaller = faster restores
    /// (less WAL to replay) at the cost of more frequent snapshots. Must be &gt; 0 and &lt;=
    /// <see cref="PitrWindowSeconds"/> when both are set. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BaseSnapshotInterval"/>.
    /// </summary>
    public int? BaseSnapshotIntervalSeconds { get; set; }

    // ── Range auto-split ──────────────────────────────────────────────────────────────────────
    // Only meaningful together with key_range_sharding; under hash routing a key space is never
    // registered for key-range routing, so no descriptor exists for the split checker to act on.

    /// <summary>
    /// Sampled key count at which a key-range-routed space is automatically split into two child
    /// ranges on separate Raft partitions. <c>0</c> disables count-based auto-splitting — and, when
    /// the load branch is also off (it is: CamusDB never enables it), stops Kahuna from spawning the
    /// periodic split-checker actor at all. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitThreshold"/>.
    ///
    /// <para><b>CamusDB pins this to 0 rather than inheriting Kahuna's default of 1000.</b> The
    /// inherited default would make any table that grows past ~1000 keys split itself the moment
    /// <c>key_range_sharding</c> is switched on, which is a large behavioral change to arrive as a
    /// side effect of a routing flag — and the write path's behavior across a split boundary is not
    /// yet proven under concurrent traffic. An operator who wants automatic rebalancing sets this
    /// explicitly and thereby opts in; splitting a specific range on demand does not depend on this
    /// knob, because a manual split bypasses the threshold entirely.</para>
    /// </summary>
    public int? RangeSplitThreshold { get; set; }

    /// <summary>
    /// Minimum number of keys each child range must retain for an auto-split to be allowed, which
    /// keeps the splitter from carving off a range that is empty or nearly so. Inert while
    /// <see cref="RangeSplitThreshold"/> is 0. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitMinRangeSize"/>; unset keeps Kahuna's
    /// default of 10.
    /// </summary>
    public int? RangeSplitMinRangeSize { get; set; }

    // ── Range auto-split: the load branch ─────────────────────────────────────────────────────
    // The count branch above splits a range because it holds many keys. This branch splits it
    // because its Raft partition is saturated, which is the case the count branch cannot see: a
    // small range that carries the whole write rate. The two branches are independent, and CamusDB
    // pins both off in the baseline.

    /// <summary>
    /// Sustained log operations per second a Raft partition must reach before the load branch
    /// considers splitting a key-range-routed space that it hosts. <c>0</c> disables load-based
    /// auto-splitting, and Kahuna then never spawns the load-check actor. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitLoadThreshold"/>.
    ///
    /// <para><b>CamusDB pins this to 0 in every baseline</b>, for the reason
    /// <see cref="RangeSplitThreshold"/> is pinned there: a deployment asks for a rebalancing
    /// policy explicitly, or it does not get one.</para>
    ///
    /// <para>This rate gate is AND-combined with <see cref="RangeSplitLoadMinQueueDepth"/> and with
    /// the optional <see cref="RangeSplitLoadMinCommitWaitMs"/> gate. The combined predicate must
    /// hold continuously for <see cref="RangeSplitLoadWindowMs"/> before a split is proposed.</para>
    ///
    /// <para><b>Trap:</b> the load signals of a partition whose leader lives on another node reach
    /// this node only while load-report gossip runs. Switch on <see cref="EnableLeaderBalancer"/>,
    /// or <see cref="EnableLoadReports"/> for the gossip alone. With neither, every remote partition
    /// reports 0 operations per second and this branch is silently dead for it.</para>
    /// </summary>
    public double? RangeSplitLoadThreshold { get; set; }

    /// <summary>
    /// Minimum WAL queue depth (pending writes) a partition must also show before the load branch
    /// treats it as saturated. A high operation rate on its own describes a partition that keeps up;
    /// a backlog is what says it does not. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitLoadMinQueueDepth"/>; unset keeps Kahuna's
    /// default of 8.
    /// </summary>
    public int? RangeSplitLoadMinQueueDepth { get; set; }

    /// <summary>
    /// Optional third saturation gate: the commit-wait latency, in milliseconds, that the partition
    /// must also reach. <c>0</c> (the Kahuna default) disables the gate. It can never fire on its
    /// own, because all three gates are AND-combined. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitLoadMinCommitWaitMs"/>.
    /// </summary>
    public double? RangeSplitLoadMinCommitWaitMs { get; set; }

    /// <summary>
    /// How long, in milliseconds, the load predicate must hold continuously before a split fires —
    /// the debounce that keeps a burst from splitting a range. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitLoadWindow"/>; unset keeps Kahuna's default
    /// of 15000. Must exceed <see cref="RangeSplitLoadPollIntervalMs"/>.
    /// </summary>
    public int? RangeSplitLoadWindowMs { get; set; }

    /// <summary>
    /// How often, in milliseconds, the load branch samples the partition signals. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitLoadPollInterval"/>; unset keeps Kahuna's
    /// default of 5000. A poll interval at or above <see cref="RangeSplitLoadWindowMs"/> can never
    /// observe a sustained window, so <see cref="Validate"/> rejects that pair.
    /// </summary>
    public int? RangeSplitLoadPollIntervalMs { get; set; }

    /// <summary>
    /// Write-imbalance fraction at which the indivisibility guard refuses a split: a range whose
    /// best achievable split still puts this fraction of the writes on one child is refused, because
    /// the split would add a Raft partition and relieve nothing. A single hot key produces exactly
    /// that shape. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitLoadImbalanceMax"/>;
    /// unset keeps Kahuna's default of 0.8. Valid range is <c>(0.5, 1.0]</c> — at or below 0.5 no
    /// split is ever accepted, and above 1.0 no split is ever refused.
    /// </summary>
    public double? RangeSplitLoadImbalanceMax { get; set; }

    /// <summary>
    /// How long, in milliseconds, a freshly created child range is left alone before the split
    /// checker re-evaluates it. A child inherits a filtered write histogram, so it starts warm and
    /// would otherwise re-split before its new leader settles. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitSettleWindow"/>; unset keeps Kahuna's
    /// default of 10000.
    ///
    /// <para>Kahuna also requires this window to be at least <see cref="MinLeaderStabilityMs"/>,
    /// otherwise a child could be re-split before the leader balancer is allowed to move its leader.
    /// <see cref="Validate"/> checks the effective pair here, so the mismatch is a CamusDB config
    /// error rather than an exception from the Kahuna node constructor.</para>
    ///
    /// <para>Kahuna reads <c>0</c> as "no settle window". CamusDB does not accept 0, because a
    /// disabled settle window turns one hot range into a split storm.</para>
    /// </summary>
    public int? RangeSplitSettleWindowMs { get; set; }

    /// <summary>
    /// How long, in milliseconds, the count branch stops re-sampling a range after the
    /// indivisibility guard refused to split it. The sample scans up to 4096 keys, so re-running it
    /// every collection interval for a range that cannot be split is pure waste. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeSplitIndivisibleCooldown"/>; unset keeps Kahuna's
    /// default of 300000.
    /// </summary>
    public int? RangeSplitIndivisibleCooldownMs { get; set; }

    /// <summary>
    /// Upper bound, in milliseconds, on how long a range split or merge holds its quiesce while it
    /// drains the moving range's unsettled durable intents before the cutover. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeMoveSettleTimeout"/>; unset keeps Kahuna's
    /// default of 10000.
    ///
    /// <para>This is the knob that decides what one split attempt costs the workload. The quiesce
    /// blocks new prepares in the moving half, so writes there are refused — retryably — for as long
    /// as the drain runs. A longer wait lets more attempts succeed and blocks writes for longer each
    /// time; a shorter one makes each attempt cheap and more likely to be refused. Under sustained
    /// writes a range always carries just-prepared intents, so with the wait disabled every attempt
    /// is refused and the range never divides.</para>
    ///
    /// <para>Kahuna reads 0 or negative as "no wait" (one settle pass, then refuse) and clamps
    /// anything above 15000 to 15000, so the copy and cutover still fit inside the 30-second quiesce
    /// window. CamusDB passes the value through and only refuses a negative one.</para>
    /// </summary>
    public int? RangeMoveSettleTimeoutMs { get; set; }

    /// <summary>
    /// Key count below which two adjacent ranges become eligible to merge back into one. <c>0</c>
    /// disables auto-merge and stops the periodic merge checker. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RangeMergeMinSize"/>; unset keeps Kahuna's default
    /// of 10.
    ///
    /// <para>Tune this together with the split thresholds. CamusDB inherited the default silently
    /// while both split branches were off, which was harmless only because nothing split. A merge
    /// size that is large relative to the split thresholds makes a range split and merge
    /// repeatedly.</para>
    /// </summary>
    public int? RangeMergeMinSize { get; set; }

    /// <summary>
    /// Gossips per-partition load reports (operations per second, WAL queue depth, commit wait) even
    /// when no other component consumes them. Reports already flow when
    /// <see cref="EnableLeaderBalancer"/>, <see cref="EnablePlacementRebalancer"/>, or a non-zero
    /// <see cref="ReplicationFactor"/> is set. This key exists so load splitting can read remote
    /// partition signals without switching the leader balancer on as a side effect. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.EnableLoadReports"/>.
    ///
    /// <para>Gossip alone makes the split decision correct, not effective: nothing then moves the
    /// child leader to another node, so the split costs consensus overhead and relieves nothing.
    /// <see cref="EnableLeaderBalancer"/> is what delivers the relief.</para>
    /// </summary>
    public bool? EnableLoadReports { get; set; }

    /// <summary>
    /// Server-owned root that restore destinations must be contained within. Setting it enables remote
    /// (REST) restore with destinations confined to this tree; leaving it empty keeps remote restore
    /// disabled unless <see cref="AllowUnconfinedRemoteRestore"/> is set. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.RestoreRoot"/>. Must not be blank when the key is present.
    /// </summary>
    public string? RestoreRoot { get; set; }

    /// <summary>
    /// Allows remote restore without a configured <see cref="RestoreRoot"/> (insecure — a restore may
    /// then target any absolute path). Off by default. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.AllowUnconfinedRemoteRestore"/>.
    /// </summary>
    public bool? AllowUnconfinedRemoteRestore { get; set; }

    /// <summary>
    /// Retention cap on the number of backup chains kept in the backup directory (0 = unlimited). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BackupRetentionMaxChains"/>. Garbage collection deletes
    /// only whole chains, keeping a valid full root for every retained leaf.
    /// </summary>
    public int? BackupRetentionMaxChains { get; set; }

    /// <summary>Retention cap on backup age, in <b>seconds</b> (0 = unlimited). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BackupRetentionMaxAge"/>.</summary>
    public int? BackupRetentionMaxAgeSeconds { get; set; }

    /// <summary>Retention cap on total backup bytes (0 = unlimited). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BackupRetentionMaxBytes"/>.</summary>
    public long? BackupRetentionMaxBytes { get; set; }

    /// <summary>How often, in <b>seconds</b>, the background backup GC reaper runs (0 = disabled; GC also
    /// runs after each backup). Maps to <see cref="Kahuna.EmbeddedKahunaOptions.BackupGcInterval"/>.</summary>
    public int? BackupGcIntervalSeconds { get; set; }

    /// <summary>Throughput budget in bytes/sec for a restore's checkpoint copy (0 = unlimited). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BackupRestoreThrottleBytesPerSec"/>.</summary>
    public long? BackupRestoreThrottleBytesPerSec { get; set; }

    /// <summary>
    /// Cluster identity stamped into backup manifests. Set the <b>same</b> value on every node so a
    /// restore refuses to chain artifacts produced by a different cluster (or a stale topology). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.BackupClusterId"/>. Empty on a standalone node is fine.
    /// </summary>
    public string? BackupClusterId { get; set; }

    /// <summary>
    /// Path to the HMAC-SHA-256 key file used to sign and verify backup manifests (authenticity — detects
    /// malicious replacement, not just accidental corruption). Use the <b>same</b> key file on every node,
    /// kept outside the backup directory. When unset, manifests are unsigned and a node with a key
    /// configured will refuse to restore them. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.BackupMacKeyFile"/>.
    /// Must not be blank when the key is present.
    /// </summary>
    public string? BackupMacKeyFile { get; set; }

    // ── Partition placement (replication factor) ──────────────────────────────────────────────
    // Per-partition replica placement instead of full replication. Applies at cluster bootstrap
    // only: Kahuna has no in-place migration from full replication, so restarting an existing
    // cluster with a factor set changes the configured target while already-committed ranges keep
    // their empty (full-replication) replica sets. Standalone nodes accept the keys but a factor
    // above the node count degrades gracefully to one replica per available node.

    /// <summary>
    /// Replica-set size for each data partition. <c>0</c> (Kahuna's default) keeps full
    /// replication: every voter node hosts every partition. Odd values are strongly preferred (an
    /// even factor tolerates no more failures than the next odd factor down); Kahuna logs a startup
    /// warning for even factors. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.ReplicationFactor"/>.
    /// </summary>
    public int? ReplicationFactor { get; set; }

    /// <summary>
    /// Failure-domain label for this node (rack, availability zone). When zones are configured the
    /// placement planner spreads each range's replicas across distinct zones, so a zone outage
    /// cannot take out a whole quorum. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.Zone"/>.
    /// </summary>
    public string? Zone { get; set; }

    /// <summary>
    /// Master switch for <b>ongoing</b> placement moves: repairing under-replicated ranges when a
    /// node dies, trimming over-replication, and smoothing skew as nodes join and leave. Initial
    /// placement at the configured factor is applied at bootstrap regardless of this switch. Maps
    /// to <see cref="Kahuna.EmbeddedKahunaOptions.EnablePlacementRebalancer"/>.
    /// </summary>
    public bool? EnablePlacementRebalancer { get; set; }

    /// <summary>
    /// Cadence of the placement controller pass, in milliseconds (Kahuna default 5000). Every
    /// relocation costs several passes, so this sets the floor on placement convergence speed.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.PlacementPassInterval"/>.
    /// </summary>
    public int? PlacementPassIntervalMs { get; set; }

    /// <summary>
    /// New replica moves initiated per controller pass across all priorities — the blast radius of
    /// a bad plan (Kahuna default 4). Keep it at or above repairs + transfers, or it binds first
    /// and starves repairs. Maps to <see cref="Kahuna.EmbeddedKahunaOptions.MaxReplicaMovesPerPass"/>.
    /// </summary>
    public int? MaxReplicaMovesPerPass { get; set; }

    /// <summary>
    /// In-flight replica backfills initiated by <b>balance</b> moves (Kahuna default 1) — caps
    /// concurrent skew-smoothing so it never starves client traffic. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.MaxConcurrentReplicaTransfers"/>.
    /// </summary>
    public int? MaxConcurrentReplicaTransfers { get; set; }

    /// <summary>
    /// In-flight <b>repair</b> moves: re-replicating under-replicated ranges and shedding replicas
    /// stranded on departed nodes (Kahuna default 3). Budgeted separately from balance transfers so
    /// restoring durability is never serialized behind cosmetic rebalancing. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.MaxConcurrentReplicaRepairs"/>.
    /// </summary>
    public int? MaxConcurrentReplicaRepairs { get; set; }

    /// <summary>
    /// Per-node replica-count imbalance tolerated above the even spread before balancing moves are
    /// planned (Kahuna default 1). Under-replicated ranges bypass the deadband. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.ReplicaCountDeadband"/>.
    /// </summary>
    public int? ReplicaCountDeadband { get; set; }

    /// <summary>
    /// Upper bound, in milliseconds, on how long a graceful decommission waits for the leaving
    /// node's replicas to be evacuated onto survivors before the removal commits anyway (Kahuna
    /// default 120000). Only one node may drain at a time. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.DecommissionDrainTimeout"/>.
    /// </summary>
    public int? DecommissionDrainTimeoutMs { get; set; }

    // ── Raft leader balancing ─────────────────────────────────────────────────────────────────
    // Spreads partition leadership (which replica leads, as opposed to which nodes hold a range)
    // across the cluster by load reports. Off by default in Kahuna; the placement rebalancer does
    // not require it — placement runs its own controller pass on its own interval.

    /// <summary>
    /// Enables the Raft leader balancer. Set the same value on every node. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.EnableLeaderBalancer"/>.
    /// </summary>
    public bool? EnableLeaderBalancer { get; set; }

    /// <summary>
    /// How often the balancing pass runs, in milliseconds (Kahuna default 30000). Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.LeaderBalancerInterval"/>.
    /// </summary>
    public int? LeaderBalancerIntervalMs { get; set; }

    /// <summary>
    /// How often each node publishes its load report, in milliseconds (Kahuna default 5000). Maps
    /// to <see cref="Kahuna.EmbeddedKahunaOptions.LeaderBalancerReportInterval"/>.
    /// </summary>
    public int? LeaderBalancerReportIntervalMs { get; set; }

    /// <summary>
    /// Age, in milliseconds, past which a node's load report is ignored by the balancer (Kahuna
    /// default 20000). Must exceed the report interval or every report expires before it is read.
    /// Maps to <see cref="Kahuna.EmbeddedKahunaOptions.LeaderBalancerReportTtl"/>.
    /// </summary>
    public int? LeaderBalancerReportTtlMs { get; set; }

    /// <summary>
    /// Minimum time, in milliseconds, a partition keeps its leader before the balancer may move
    /// leadership again (Kahuna default 5000) — damping against leadership thrash. Maps to
    /// <see cref="Kahuna.EmbeddedKahunaOptions.MinLeaderStability"/>.
    /// </summary>
    public int? MinLeaderStabilityMs { get; set; }

    /// <summary>
    /// Returns an independent copy. <see cref="CamusDBOptions"/> is immutable, but this class is a
    /// YAML-bound settings object with ordinary setters, so holding the deserialized instance directly
    /// would leave one mutable object reachable from every options value derived by <c>with</c> —
    /// a single mutation would then be visible to engines meant to be configured independently.
    /// A memberwise copy suffices: every property is a scalar or a string.
    /// </summary>
    internal KahunaOptionsConfig Copy() => (KahunaOptionsConfig)MemberwiseClone();

    /// <summary>
    /// Validates allow-listed Kahuna fields. Called from <see cref="ConfigDefinition.Validate"/>.
    /// </summary>
    public void Validate()
    {
        ValidateStorage(Storage, "kahuna.storage");
        ValidateStorage(WalStorage, "kahuna.wal_storage");

        if (WalGroupCommitLingerMs is < 0)
            throw InvalidConfig($"'kahuna.wal_group_commit_linger_ms' must be >= 0, got {WalGroupCommitLingerMs}");

        if (DefaultTransactionTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.default_transaction_timeout_ms' must be > 0, got {DefaultTransactionTimeoutMs}");

        if (MaxTransactionTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.max_transaction_timeout_ms' must be > 0, got {MaxTransactionTimeoutMs}");

        if (MaxConcurrentSessions is < 0)
            throw InvalidConfig($"'kahuna.max_concurrent_sessions' must be >= 0, got {MaxConcurrentSessions}");

        // Kahuna rejects a non-positive budget at startup — it would resolve every unconstrained
        // request to zero and refuse admission before a caller could ever be queued. Fail here, where
        // the offending key is named, rather than letting the node throw on boot.
        if (DefaultAdmissionWaitMs is <= 0)
            throw InvalidConfig(
                $"'kahuna.default_admission_wait_ms' must be > 0, got {DefaultAdmissionWaitMs}; " +
                "a non-positive budget refuses admission before a caller can be queued");

        if (MaxAdmissionWaitMs is <= 0)
            throw InvalidConfig($"'kahuna.max_admission_wait_ms' must be > 0, got {MaxAdmissionWaitMs}");

        // The maximum clamps every caller, including one that asked for nothing and got the default,
        // so a maximum below the default silently truncates the default it is paired with.
        if (DefaultAdmissionWaitMs is int defaultWait && MaxAdmissionWaitMs is int maxWait && defaultWait > maxWait)
            throw InvalidConfig(
                $"'kahuna.default_admission_wait_ms' ({defaultWait}) must be <= " +
                $"'kahuna.max_admission_wait_ms' ({maxWait})");

        if (TransactionPriorityReservedSlots is < 0)
            throw InvalidConfig(
                $"'kahuna.transaction_priority_reserved_slots' must be >= 0, got {TransactionPriorityReservedSlots}");

        if (TransactionPriorityAgingThreshold is < 0)
            throw InvalidConfig(
                $"'kahuna.transaction_priority_aging_threshold' must be >= 0, got {TransactionPriorityAgingThreshold}");

        if (TransactionPriorityMaxQueued is < 0)
            throw InvalidConfig(
                $"'kahuna.transaction_priority_max_queued' must be >= 0, got {TransactionPriorityMaxQueued}");

        // A reserve at or above the ceiling leaves ordinary work no capacity at all: the gate would
        // admit High/Critical only and queue every Normal transaction until aging promoted it. That is
        // a misconfiguration rather than a valid tuning, so reject it instead of deadlocking the node.
        if (MaxConcurrentSessions is int ceiling and > 0 &&
            TransactionPriorityReservedSlots is int reserved && reserved >= ceiling)
            throw InvalidConfig(
                $"'kahuna.transaction_priority_reserved_slots' ({reserved}) must be less than " +
                $"'kahuna.max_concurrent_sessions' ({ceiling}), or ordinary transactions can never start");

        if (DefaultTransactionTimeoutMs is int defaultTimeout && MaxTransactionTimeoutMs is int maxTimeout && defaultTimeout > maxTimeout)
            throw InvalidConfig(
                $"'kahuna.default_transaction_timeout_ms' ({defaultTimeout}) must be <= 'kahuna.max_transaction_timeout_ms' ({maxTimeout})");

        if (LocksWorkers is <= 0)
            throw InvalidConfig($"'kahuna.locks_workers' must be > 0, got {LocksWorkers}");

        if (KeyValueWorkers is <= 0)
            throw InvalidConfig($"'kahuna.key_value_workers' must be > 0, got {KeyValueWorkers}");

        if (BackgroundWriterWorkers is <= 0)
            throw InvalidConfig($"'kahuna.background_writer_workers' must be > 0, got {BackgroundWriterWorkers}");

        if (ReadIoThreads is <= 0)
            throw InvalidConfig($"'kahuna.read_io_threads' must be > 0, got {ReadIoThreads}");

        if (WriteIoThreads is <= 0)
            throw InvalidConfig($"'kahuna.write_io_threads' must be > 0, got {WriteIoThreads}");

        if (StartElectionTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.start_election_timeout_ms' must be > 0, got {StartElectionTimeoutMs}");

        if (EndElectionTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.end_election_timeout_ms' must be > 0, got {EndElectionTimeoutMs}");

        if (StartElectionTimeoutMs is int start && EndElectionTimeoutMs is int end && start >= end)
            throw InvalidConfig(
                $"'kahuna.start_election_timeout_ms' ({start}) must be < 'kahuna.end_election_timeout_ms' ({end})");

        if (StartElectionTimeoutIncrementMs is <= 0)
            throw InvalidConfig(
                $"'kahuna.start_election_timeout_increment_ms' must be > 0, got {StartElectionTimeoutIncrementMs}");

        if (EndElectionTimeoutIncrementMs is <= 0)
            throw InvalidConfig(
                $"'kahuna.end_election_timeout_increment_ms' must be > 0, got {EndElectionTimeoutIncrementMs}");

        if (HeartbeatIntervalMs is <= 0)
            throw InvalidConfig($"'kahuna.heartbeat_interval_ms' must be > 0, got {HeartbeatIntervalMs}");

        if (VotingTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.voting_timeout_ms' must be > 0, got {VotingTimeoutMs}");

        if (MaxEntriesPerActor is <= 0)
            throw InvalidConfig($"'kahuna.max_entries_per_actor' must be > 0, got {MaxEntriesPerActor}");

        if (MaxBytesPerActor is <= 0)
            throw InvalidConfig($"'kahuna.max_bytes_per_actor' must be > 0, got {MaxBytesPerActor}");

        if (CacheEntryTtlMs is <= 0)
            throw InvalidConfig($"'kahuna.cache_entry_ttl_ms' must be > 0, got {CacheEntryTtlMs}");

        if (CacheEntriesToRemove is <= 0)
            throw InvalidConfig($"'kahuna.cache_entries_to_remove' must be > 0, got {CacheEntriesToRemove}");

        if (CollectionIntervalMs is <= 0)
            throw InvalidConfig($"'kahuna.collection_interval_ms' must be > 0, got {CollectionIntervalMs}");

        if (CompactEveryOperations is <= 0)
            throw InvalidConfig($"'kahuna.compact_every_operations' must be > 0, got {CompactEveryOperations}");

        if (CompactNumberEntries is <= 0)
            throw InvalidConfig($"'kahuna.compact_number_entries' must be > 0, got {CompactNumberEntries}");

        if (MaxEntriesPerCompaction is <= 0)
            throw InvalidConfig($"'kahuna.max_entries_per_compaction' must be > 0, got {MaxEntriesPerCompaction}");

        if (RocksdbSharedMemoryBudgetMb is <= 0)
            throw InvalidConfig($"'kahuna.rocksdb_shared_memory_budget_mb' must be > 0, got {RocksdbSharedMemoryBudgetMb}");

        if (RocksdbSharedMemtableBudgetMb is <= 0)
            throw InvalidConfig($"'kahuna.rocksdb_shared_memtable_budget_mb' must be > 0, got {RocksdbSharedMemtableBudgetMb}");

        if (RocksdbSharedMemtableBudgetMb is int memtable && RocksdbSharedMemoryBudgetMb is int total && memtable > total)
            throw InvalidConfig(
                $"'kahuna.rocksdb_shared_memtable_budget_mb' ({memtable}) must be <= 'kahuna.rocksdb_shared_memory_budget_mb' ({total})");

        if (BackupDir is not null && string.IsNullOrWhiteSpace(BackupDir))
            throw InvalidConfig("'kahuna.backup_dir' must not be blank when set (omit the key to disable backups)");

        if (PitrWindowSeconds is <= 0)
            throw InvalidConfig($"'kahuna.pitr_window_seconds' must be > 0, got {PitrWindowSeconds}");

        if (PitrWindowSeconds is int window && window > 21600)
            throw InvalidConfig($"'kahuna.pitr_window_seconds' must be <= 21600 (6 hours), got {window}");

        if (BaseSnapshotIntervalSeconds is <= 0)
            throw InvalidConfig($"'kahuna.base_snapshot_interval_seconds' must be > 0, got {BaseSnapshotIntervalSeconds}");

        // Cross-check the EFFECTIVE pair, not just the provided keys. Kahuna requires
        // BaseSnapshotInterval <= PitrWindow; if we only compared when both YAML keys are present, a
        // one-sided override (e.g. pitr_window_seconds=600 while base-snapshot keeps its 1800s default)
        // would slip past CamusDB and only blow up at Kahuna startup. Fill the unset side with the same
        // defaults EmbeddedKahunaOptions uses (PitrWindow 1h, BaseSnapshotInterval 30m).
        int effectiveWindow = PitrWindowSeconds ?? DefaultPitrWindowSeconds;
        int effectiveSnapshot = BaseSnapshotIntervalSeconds ?? DefaultBaseSnapshotIntervalSeconds;
        if (effectiveSnapshot > effectiveWindow)
            throw InvalidConfig(
                $"effective 'kahuna.base_snapshot_interval_seconds' ({effectiveSnapshot}) must be <= " +
                $"'kahuna.pitr_window_seconds' ({effectiveWindow}); set both when lowering the window below " +
                $"the {DefaultBaseSnapshotIntervalSeconds}s default snapshot interval");

        if (RangeSplitThreshold is < 0)
            throw InvalidConfig($"'kahuna.range_split_threshold' must be >= 0 (0 disables auto-split), got {RangeSplitThreshold}");

        if (RangeSplitMinRangeSize is < 1)
            throw InvalidConfig($"'kahuna.range_split_min_range_size' must be >= 1, got {RangeSplitMinRangeSize}");

        // A range cannot be split into two halves that each hold minRangeSize keys unless it holds at
        // least twice that many, so a threshold below 2*minRangeSize describes a split that can never
        // be granted — the checker would sample, refuse, and back off forever. Rejecting it here turns
        // a silently inert configuration into a startup error.
        if (RangeSplitThreshold is int splitThreshold and > 0
            && splitThreshold < 2 * (RangeSplitMinRangeSize ?? DefaultRangeSplitMinRangeSize))
            throw InvalidConfig(
                $"'kahuna.range_split_threshold' ({splitThreshold}) must be >= twice " +
                $"'kahuna.range_split_min_range_size' ({RangeSplitMinRangeSize ?? DefaultRangeSplitMinRangeSize}), " +
                "or no range can ever satisfy the split policy");

        // ── The load branch ───────────────────────────────────────────────────────────────────
        // Each rule below rejects a configuration that cannot fire, rather than let the node start
        // and do nothing. Every one of these reads as "the feature is broken" to an operator.

        if (RangeSplitLoadThreshold is < 0)
            throw InvalidConfig(
                $"'kahuna.range_split_load_threshold' must be >= 0 (0 disables load-based auto-split), got {RangeSplitLoadThreshold}");

        if (RangeSplitLoadMinQueueDepth is < 0)
            throw InvalidConfig($"'kahuna.range_split_load_min_queue_depth' must be >= 0, got {RangeSplitLoadMinQueueDepth}");

        if (RangeSplitLoadMinCommitWaitMs is < 0)
            throw InvalidConfig(
                $"'kahuna.range_split_load_min_commit_wait_ms' must be >= 0 (0 disables the commit-wait gate), got {RangeSplitLoadMinCommitWaitMs}");

        if (RangeSplitLoadWindowMs is <= 0)
            throw InvalidConfig($"'kahuna.range_split_load_window_ms' must be > 0, got {RangeSplitLoadWindowMs}");

        if (RangeSplitLoadPollIntervalMs is <= 0)
            throw InvalidConfig($"'kahuna.range_split_load_poll_interval_ms' must be > 0, got {RangeSplitLoadPollIntervalMs}");

        if (RangeSplitIndivisibleCooldownMs is < 0)
            throw InvalidConfig($"'kahuna.range_split_indivisible_cooldown_ms' must be >= 0, got {RangeSplitIndivisibleCooldownMs}");

        if (RangeMergeMinSize is < 0)
            throw InvalidConfig($"'kahuna.range_merge_min_size' must be >= 0 (0 disables auto-merge), got {RangeMergeMinSize}");

        // An imbalance ceiling at or below 0.5 refuses every split, because no split can put less
        // than half the writes on its heavier child. Above 1.0 it refuses none, which turns the
        // indivisibility guard off and lets a single hot key split its range forever.
        if (RangeSplitLoadImbalanceMax is double imbalanceMax && (imbalanceMax <= 0.5 || imbalanceMax > 1.0))
            throw InvalidConfig(
                $"'kahuna.range_split_load_imbalance_max' must be > 0.5 and <= 1.0, got {imbalanceMax}");

        // The predicate is sampled at the poll interval and must hold for the whole window. A poll
        // that is slower than the window never produces two consecutive positive samples, so the
        // window is never observed as sustained and no split ever fires. Compare the EFFECTIVE pair,
        // so a window lowered below the 5000 ms default poll interval is caught with the interval
        // key untouched.
        int effectiveLoadWindow = RangeSplitLoadWindowMs ?? DefaultRangeSplitLoadWindowMs;
        int effectiveLoadPoll = RangeSplitLoadPollIntervalMs ?? DefaultRangeSplitLoadPollIntervalMs;
        if (effectiveLoadPoll >= effectiveLoadWindow)
            throw InvalidConfig(
                $"effective 'kahuna.range_split_load_poll_interval_ms' ({effectiveLoadPoll}) must be < " +
                $"'kahuna.range_split_load_window_ms' ({effectiveLoadWindow}), or the load predicate can never " +
                "be observed as sustained and no split can ever fire");

        if (RangeMoveSettleTimeoutMs is < 0)
            throw InvalidConfig(
                $"'kahuna.range_move_settle_timeout_ms' must be >= 0 (0 disables the drain wait), got {RangeMoveSettleTimeoutMs}");

        if (RangeSplitSettleWindowMs is <= 0)
            throw InvalidConfig(
                $"'kahuna.range_split_settle_window_ms' must be > 0, got {RangeSplitSettleWindowMs}");

        // Kahuna refuses a settle window shorter than the leader-stability window: a child could
        // otherwise be re-split before the balancer is permitted to move its leader. That check runs
        // in the Kahuna node constructor and raises an ArgumentException, so catching the pair here
        // turns a startup crash into a named configuration error.
        int effectiveSettleWindow = RangeSplitSettleWindowMs ?? DefaultRangeSplitSettleWindowMs;
        int effectiveLeaderStability = MinLeaderStabilityMs ?? DefaultMinLeaderStabilityMs;
        if (effectiveLeaderStability > 0 && effectiveSettleWindow < effectiveLeaderStability)
            throw InvalidConfig(
                $"effective 'kahuna.range_split_settle_window_ms' ({effectiveSettleWindow}) must be >= " +
                $"'kahuna.min_leader_stability_ms' ({effectiveLeaderStability}), or a fresh child range can be " +
                "re-split before the leader balancer may move its leader");

        if (RestoreRoot is not null && string.IsNullOrWhiteSpace(RestoreRoot))
            throw InvalidConfig("'kahuna.restore_root' must not be blank when set (omit the key to disable confined remote restore)");

        if (BackupMacKeyFile is not null && string.IsNullOrWhiteSpace(BackupMacKeyFile))
            throw InvalidConfig("'kahuna.backup_mac_key_file' must not be blank when set (omit the key to disable manifest signing)");

        if (BackupRetentionMaxChains is < 0)
            throw InvalidConfig($"'kahuna.backup_retention_max_chains' must be >= 0, got {BackupRetentionMaxChains}");

        if (BackupRetentionMaxAgeSeconds is < 0)
            throw InvalidConfig($"'kahuna.backup_retention_max_age_seconds' must be >= 0, got {BackupRetentionMaxAgeSeconds}");

        if (BackupRetentionMaxBytes is < 0)
            throw InvalidConfig($"'kahuna.backup_retention_max_bytes' must be >= 0, got {BackupRetentionMaxBytes}");

        if (BackupGcIntervalSeconds is < 0)
            throw InvalidConfig($"'kahuna.backup_gc_interval_seconds' must be >= 0, got {BackupGcIntervalSeconds}");

        if (BackupRestoreThrottleBytesPerSec is < 0)
            throw InvalidConfig($"'kahuna.backup_restore_throttle_bytes_per_sec' must be >= 0, got {BackupRestoreThrottleBytesPerSec}");

        if (ReplicationFactor is < 0)
            throw InvalidConfig($"'kahuna.replication_factor' must be >= 0 (0 keeps full replication), got {ReplicationFactor}");

        if (Zone is not null && string.IsNullOrWhiteSpace(Zone))
            throw InvalidConfig("'kahuna.zone' must not be blank when set (omit the key for zone-unaware placement)");

        if (PlacementPassIntervalMs is <= 0)
            throw InvalidConfig($"'kahuna.placement_pass_interval_ms' must be > 0, got {PlacementPassIntervalMs}");

        if (MaxReplicaMovesPerPass is <= 0)
            throw InvalidConfig($"'kahuna.max_replica_moves_per_pass' must be > 0, got {MaxReplicaMovesPerPass}");

        if (MaxConcurrentReplicaTransfers is <= 0)
            throw InvalidConfig($"'kahuna.max_concurrent_replica_transfers' must be > 0, got {MaxConcurrentReplicaTransfers}");

        if (MaxConcurrentReplicaRepairs is <= 0)
            throw InvalidConfig($"'kahuna.max_concurrent_replica_repairs' must be > 0, got {MaxConcurrentReplicaRepairs}");

        if (ReplicaCountDeadband is < 0)
            throw InvalidConfig($"'kahuna.replica_count_deadband' must be >= 0, got {ReplicaCountDeadband}");

        if (DecommissionDrainTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.decommission_drain_timeout_ms' must be > 0, got {DecommissionDrainTimeoutMs}");

        if (LeaderBalancerIntervalMs is <= 0)
            throw InvalidConfig($"'kahuna.leader_balancer_interval_ms' must be > 0, got {LeaderBalancerIntervalMs}");

        if (LeaderBalancerReportIntervalMs is <= 0)
            throw InvalidConfig($"'kahuna.leader_balancer_report_interval_ms' must be > 0, got {LeaderBalancerReportIntervalMs}");

        if (LeaderBalancerReportTtlMs is <= 0)
            throw InvalidConfig($"'kahuna.leader_balancer_report_ttl_ms' must be > 0, got {LeaderBalancerReportTtlMs}");

        if (MinLeaderStabilityMs is <= 0)
            throw InvalidConfig($"'kahuna.min_leader_stability_ms' must be > 0, got {MinLeaderStabilityMs}");

        // A TTL at or below the publishing cadence expires every report before the balancer can read
        // it, which silently disables balancing while looking configured. Cross-check the EFFECTIVE
        // pair so a one-sided override is caught (Kahuna defaults: report every 5 s, TTL 20 s).
        int effectiveReportInterval = LeaderBalancerReportIntervalMs ?? DefaultLeaderBalancerReportIntervalMs;
        int effectiveReportTtl = LeaderBalancerReportTtlMs ?? DefaultLeaderBalancerReportTtlMs;
        if (effectiveReportTtl <= effectiveReportInterval)
            throw InvalidConfig(
                $"effective 'kahuna.leader_balancer_report_ttl_ms' ({effectiveReportTtl}) must be > " +
                $"'kahuna.leader_balancer_report_interval_ms' ({effectiveReportInterval}), " +
                "or every load report expires before the balancer reads it");
    }

    // Match EmbeddedKahunaOptions defaults (PitrWindow 1h, BaseSnapshotInterval 30m) so the effective
    // cross-check above reflects what the node will actually run with when a key is left unset.
    private const int DefaultPitrWindowSeconds = 3600;
    private const int DefaultBaseSnapshotIntervalSeconds = 1800;

    // Kahuna's own RangeSplitMinRangeSize default, used to cross-check a threshold supplied without it.
    private const int DefaultRangeSplitMinRangeSize = 10;

    // Kahuna's load-branch defaults, used to cross-check a one-sided poll/window override and a
    // settle window supplied without the leader-stability window it must cover.
    private const int DefaultRangeSplitLoadWindowMs = 15_000;
    private const int DefaultRangeSplitLoadPollIntervalMs = 5_000;
    private const int DefaultRangeSplitSettleWindowMs = 10_000;

    // Kahuna's leader-balancer report defaults, used to cross-check a one-sided TTL/interval override.
    private const int DefaultLeaderBalancerReportIntervalMs = 5_000;
    private const int DefaultLeaderBalancerReportTtlMs = 20_000;
    private const int DefaultMinLeaderStabilityMs = 5_000;

    private static void ValidateStorage(string? value, string field)
    {
        if (value is null)
            return;

        if (value is not ("memory" or "sqlite" or "rocksdb"))
            throw InvalidConfig($"'{field}' must be 'memory', 'sqlite', or 'rocksdb', got '{value}'");
    }

    private static CamusDBException InvalidConfig(string message)
        => new(CamusDBErrorCodes.InvalidConfig, message);
}
