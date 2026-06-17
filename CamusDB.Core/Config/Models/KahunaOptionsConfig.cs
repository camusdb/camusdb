
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
/// Unset fields keep the mode-specific CamusDB baseline (cluster sqlite defaults in
/// <c>Program.cs</c>, standalone sqlite defaults in <see cref="Storage.Kv.EmbeddedKahunaOptionsBuilder"/>).
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
        "default_transaction_timeout_ms",
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
        "compact_every_operations",
    };

    /// <summary>Persistence backend: <c>memory</c>, <c>sqlite</c>, or <c>rocksdb</c>.</summary>
    public string? Storage { get; set; }

    public string? StorageRevision { get; set; }

    /// <summary>Raft WAL backend: <c>memory</c>, <c>sqlite</c>, or <c>rocksdb</c>.</summary>
    public string? WalStorage { get; set; }

    public string? WalRevision { get; set; }

    public bool? WalSyncWrites { get; set; }

    public int? DefaultTransactionTimeoutMs { get; set; }

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

    public int? CompactEveryOperations { get; set; }

    /// <summary>
    /// Validates allow-listed Kahuna fields. Called from <see cref="ConfigDefinition.Validate"/>.
    /// </summary>
    public void Validate()
    {
        ValidateStorage(Storage, "kahuna.storage");
        ValidateStorage(WalStorage, "kahuna.wal_storage");

        if (DefaultTransactionTimeoutMs is <= 0)
            throw InvalidConfig($"'kahuna.default_transaction_timeout_ms' must be > 0, got {DefaultTransactionTimeoutMs}");

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

        if (CompactEveryOperations is <= 0)
            throw InvalidConfig($"'kahuna.compact_every_operations' must be > 0, got {CompactEveryOperations}");
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
