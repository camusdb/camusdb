
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
    }

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
