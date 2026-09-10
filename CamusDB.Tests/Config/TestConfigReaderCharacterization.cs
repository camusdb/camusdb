
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Reflection;
using NUnit.Framework;

using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Characterization tests for the configuration surface: sample config round-trip,
/// validation failures, and Kahuna allow-list enforcement.
/// </summary>
[TestFixture]
public sealed class TestConfigReaderCharacterization
{
    [Test]
    public void ReadsShippedConfigYaml_WithoutValidationErrors()
    {
        string configPath = Path.GetFullPath(Path.Combine(
            TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", "CamusDB", "Config", "config.yml"));
        string yml = File.ReadAllText(configPath);

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.That(config.DataDir, Is.EqualTo("/tmp/camusdb/"));
        Assert.That(config.Mode, Is.EqualTo("standalone"));
    }

    [Test]
    public void RejectsUnknownRootKey()
    {
        // A typo on a real key (htttp_port) must fail loudly rather than silently leaving
        // http_port at its default — YamlDotNet would otherwise drop the unknown property.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("htttp_port: 6000"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("htttp_port"));
    }

    [Test]
    public void RejectsUnknownKahunaKey()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  mystery_option: 1"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("mystery_option"));
    }

    [Test]
    public void RejectsInvalidDefaultIsolationLevel()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("default_isolation_level: snapshot"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("default_isolation_level"));
    }

    [Test]
    public void RejectsInvalidDefaultTransactionLocking()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("default_transaction_locking: eventual"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("default_transaction_locking"));
    }

    [Test]
    public void RejectsInvalidDefaultTransactionPriority()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("default_transaction_priority: urgent"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("default_transaction_priority"));
    }

    [Test]
    public void RejectsReservedSlotsAtOrAboveTheSessionCeiling()
    {
        // A reserve >= the ceiling leaves ordinary work no capacity at all — every Normal
        // transaction would queue until aging promoted it. Reject rather than let a node
        // configure itself into that state.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(
                "kahuna:\n  max_concurrent_sessions: 2\n  transaction_priority_reserved_slots: 2"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("transaction_priority_reserved_slots"));
    }

    [Test]
    public void AcceptsAdmissionWaitBudgets()
    {
        ConfigDefinition config = new ConfigReader().Read(
            "transaction_admission_wait_ms: 2000\n" +
            "kahuna:\n  default_admission_wait_ms: 3000\n  max_admission_wait_ms: 20000");

        Assert.That(config.TransactionAdmissionWaitMs, Is.EqualTo(2_000));
        Assert.That(config.Kahuna.DefaultAdmissionWaitMs, Is.EqualTo(3_000));
        Assert.That(config.Kahuna.MaxAdmissionWaitMs, Is.EqualTo(20_000));
    }

    [Test]
    public void RejectsNegativeTransactionAdmissionWait()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("transaction_admission_wait_ms: -1"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("transaction_admission_wait_ms"));
    }

    [Test]
    public void RejectsAdmissionWaitMaximumBelowTheDefault()
    {
        // The maximum clamps every caller, including one that supplied nothing and received the
        // default — so a maximum below the default silently truncates the default it is paired with.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(
                "kahuna:\n  default_admission_wait_ms: 10000\n  max_admission_wait_ms: 5000"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("max_admission_wait_ms"));
    }

    [Test]
    public void RejectsNonPositiveDefaultAdmissionWait()
    {
        // Zero here is not "use the node default" — it is a budget of zero, which refuses admission
        // before a caller can ever be queued. Kahuna rejects it at startup; catch it at config load,
        // where the offending key can be named.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  default_admission_wait_ms: 0"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("default_admission_wait_ms"));
    }

    [Test]
    public void RejectsUnknownKahunaStorageBackend()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  storage: mysql"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("kahuna.storage"));
    }

    [Test]
    public void RejectsInvalidHttpPort()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("http_port: 0"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("http_port"));
    }

    [Test]
    public void ReadsKahunaOverrides()
    {
        ConfigDefinition config = new ConfigReader().Read(
            "kahuna:\n  storage: rocksdb\n  start_election_timeout_ms: 1000\n  end_election_timeout_ms: 3000");

        Assert.That(config.Kahuna.Storage, Is.EqualTo("rocksdb"));
        Assert.That(config.Kahuna.StartElectionTimeoutMs, Is.EqualTo(1000));
        Assert.That(config.Kahuna.EndElectionTimeoutMs, Is.EqualTo(3000));
    }

    [Test]
    public void ReadsRocksDbSharedMemoryOverrides()
    {
        // Exercises the real YamlDotNet UnderscoredNamingConvention binding: the underscored YAML keys
        // must map onto the C# properties (RocksdbSharedMemory, etc.). A mis-cased property name would
        // silently fail to bind here while the direct-construction builder tests still pass, so this is
        // the test that locks the yaml-key <-> property contract.
        ConfigDefinition config = new ConfigReader().Read(
            "kahuna:\n  rocksdb_shared_memory: false\n" +
            "  rocksdb_shared_memory_budget_mb: 512\n" +
            "  rocksdb_shared_memtable_budget_mb: 200");

        Assert.That(config.Kahuna.RocksdbSharedMemory, Is.False);
        Assert.That(config.Kahuna.RocksdbSharedMemoryBudgetMb, Is.EqualTo(512));
        Assert.That(config.Kahuna.RocksdbSharedMemtableBudgetMb, Is.EqualTo(200));
    }

    [Test]
    public void ReadsRocksDbDirectReadsOverride()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  rocksdb_direct_reads: true");

        Assert.That(config.Kahuna.RocksdbDirectReads, Is.True);
    }

    [Test]
    public void ReadsKeyValueWriteMaxInFlightBatchesOverride_AndRejectsOutOfRange()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  key_value_write_max_in_flight_batches_per_partition: 2");
        Assert.That(config.Kahuna.KeyValueWriteMaxInFlightBatchesPerPartition, Is.EqualTo(2));

        Assert.That(new ConfigReader().Read("kahuna:\n  key_value_write_max_in_flight_batches_per_partition: 1").Kahuna.KeyValueWriteMaxInFlightBatchesPerPartition, Is.EqualTo(1));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  key_value_write_max_in_flight_batches_per_partition: 0"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  key_value_write_max_in_flight_batches_per_partition: 65"));
    }

    [Test]
    public void ReadsKeyValueWriteLingerAndBatchItems_AndRejectsOutOfRange()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  key_value_write_linger_ms: 4\n  key_value_write_max_batch_items: 1024");
        Assert.That(config.Kahuna.KeyValueWriteLingerMs, Is.EqualTo(4));
        Assert.That(config.Kahuna.KeyValueWriteMaxBatchItems, Is.EqualTo(1024));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  key_value_write_linger_ms: -1"));
        Assert.That(new ConfigReader().Read("kahuna:\n  key_value_write_post_completion_hold_ms: 2").Kahuna.KeyValueWritePostCompletionHoldMs, Is.EqualTo(2));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  key_value_write_post_completion_hold_ms: -1"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  key_value_write_max_batch_items: 0"));
    }

    [Test]
    public void ReadsEveryWalShardTuningKey()
    {
        // All eight in one document, each at a non-default value, so a key that parses into the wrong
        // property — or not at all — cannot pass by coinciding with Kommander's default.
        ConfigDefinition config = new ConfigReader().Read(
            "kahuna:\n" +
            "  wal_shard_write_buffer_size_mb: 32\n" +
            "  wal_shard_min_write_buffer_number_to_merge: 3\n" +
            "  wal_shard_max_write_buffer_number: 6\n" +
            "  wal_shard_level0_file_num_compaction_trigger: 10\n" +
            "  wal_shard_level0_slowdown_writes_trigger: 30\n" +
            "  wal_shard_level0_stop_writes_trigger: 50\n" +
            "  wal_shard_max_bytes_for_level_base_mb: 2048\n" +
            "  wal_shard_universal_compaction: true");

        Assert.That(config.Kahuna.WalShardWriteBufferSizeMb, Is.EqualTo(32));
        Assert.That(config.Kahuna.WalShardMinWriteBufferNumberToMerge, Is.EqualTo(3));
        Assert.That(config.Kahuna.WalShardMaxWriteBufferNumber, Is.EqualTo(6));
        Assert.That(config.Kahuna.WalShardLevel0FileNumCompactionTrigger, Is.EqualTo(10));
        Assert.That(config.Kahuna.WalShardLevel0SlowdownWritesTrigger, Is.EqualTo(30));
        Assert.That(config.Kahuna.WalShardLevel0StopWritesTrigger, Is.EqualTo(50));
        Assert.That(config.Kahuna.WalShardMaxBytesForLevelBaseMb, Is.EqualTo(2048));
        Assert.That(config.Kahuna.WalShardUniversalCompaction, Is.True);
    }

    [Test]
    public void WalShardTuningKeysAreUnsetWhenAbsent()
    {
        // Unset has to stay null all the way down, because null is what Kahuna reads as "leave
        // Kommander's default for that field". A zero or a stand-in value here would silently retune
        // every node's Raft log.
        KahunaOptionsConfig kahuna = new ConfigReader().Read("kahuna:\n  storage: rocksdb").Kahuna;

        Assert.That(kahuna.WalShardWriteBufferSizeMb, Is.Null);
        Assert.That(kahuna.WalShardMinWriteBufferNumberToMerge, Is.Null);
        Assert.That(kahuna.WalShardMaxWriteBufferNumber, Is.Null);
        Assert.That(kahuna.WalShardLevel0FileNumCompactionTrigger, Is.Null);
        Assert.That(kahuna.WalShardLevel0SlowdownWritesTrigger, Is.Null);
        Assert.That(kahuna.WalShardLevel0StopWritesTrigger, Is.Null);
        Assert.That(kahuna.WalShardMaxBytesForLevelBaseMb, Is.Null);
        Assert.That(kahuna.WalShardUniversalCompaction, Is.Null);
    }

    [Test]
    public void RejectsWalShardTuningValuesOutOfRange()
    {
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  wal_shard_write_buffer_size_mb: 0"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  wal_shard_write_buffer_size_mb: 65537"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  wal_shard_max_bytes_for_level_base_mb: 0"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  wal_shard_min_write_buffer_number_to_merge: 0"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  wal_shard_max_write_buffer_number: 65"));
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("kahuna:\n  wal_shard_level0_stop_writes_trigger: 0"));

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  wal_shard_write_buffer_size_mb: -1"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("wal_shard_write_buffer_size_mb"));
    }

    [Test]
    public void RejectsWalShardMemtableCountsWithoutHeadroomForTheMergeQuorum()
    {
        // A flush claims the merge quorum of immutable memtables, so the writer needs one mutable
        // memtable above it or every rotation stalls the Raft log. Equality is the boundary case.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(
                "kahuna:\n  wal_shard_min_write_buffer_number_to_merge: 4\n  wal_shard_max_write_buffer_number: 4"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("wal_shard_max_write_buffer_number"));
        Assert.That(ex.Message, Does.Contain("wal_shard_min_write_buffer_number_to_merge"));

        // The one-sided override is the realistic mistake: against Kommander's default of 4 maximum
        // buffers, a merge count of 4 leaves no mutable memtable, and neither key looks wrong alone.
        Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  wal_shard_min_write_buffer_number_to_merge: 4"));

        // One above the merge count is the minimum Kommander accepts, so it must be accepted here.
        Assert.DoesNotThrow(
            () => new ConfigReader().Read(
                "kahuna:\n  wal_shard_min_write_buffer_number_to_merge: 4\n  wal_shard_max_write_buffer_number: 5"));
    }

    [Test]
    public void RejectsWalShardLevel0TriggersThatAreNotStrictlyIncreasing()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(
                "kahuna:\n" +
                "  wal_shard_level0_file_num_compaction_trigger: 40\n" +
                "  wal_shard_level0_slowdown_writes_trigger: 20\n" +
                "  wal_shard_level0_stop_writes_trigger: 50"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("wal_shard_level0_slowdown_writes_trigger"));

        // One-sided overrides, checked against Kommander's defaults (8 / 28 / 44) for the rest: a
        // compaction trigger above the unset slowdown, and a slowdown above the unset stop trigger.
        Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  wal_shard_level0_file_num_compaction_trigger: 30"));
        Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  wal_shard_level0_slowdown_writes_trigger: 45"));
    }

    [Test]
    public void AcceptsUniversalCompactionTogetherWithALevelBaseSize()
    {
        // RocksDB ignores level sizing under universal compaction, so the pair is inert rather than
        // contradictory. Documented as inert and deliberately NOT an error: rejecting it would force
        // an operator comparing the two layouts to edit two keys per arm instead of one.
        ConfigDefinition config = new ConfigReader().Read(
            "kahuna:\n  wal_shard_universal_compaction: true\n  wal_shard_max_bytes_for_level_base_mb: 2048");

        Assert.That(config.Kahuna.WalShardUniversalCompaction, Is.True);
        Assert.That(config.Kahuna.WalShardMaxBytesForLevelBaseMb, Is.EqualTo(2048));
    }

    [Test]
    public void ReadsAbandonedTransactionReaperOverrides()
    {
        // The reaper keys were shipped in the sample config.yml but were missing from the
        // allow-list, so enabling either one caused startup to reject the whole config as an
        // "unknown option". Reading them here locks in that they are accepted and bound.
        ConfigDefinition config = new ConfigReader().Read(
            "transaction_idle_timeout_ms: 120000\ntransaction_reaper_interval_ms: 15000");

        Assert.That(config.TransactionIdleTimeoutMs, Is.EqualTo(120000));
        Assert.That(config.TransactionReaperIntervalMs, Is.EqualTo(15000));
    }

    [Test]
    public void ReadsStatisticsAndAutoAnalyzeOverrides()
    {
        // These keys ship in the sample config.yml; reading them here locks in that they are accepted
        // and bound (a new tuning knob missing from the allow-list would reject startup).
        ConfigDefinition config = new ConfigReader().Read(
            "stats_analyze_sample_rows: 250000\n" +
            "stats_histogram_buckets: 64\n" +
            "auto_analyze_enabled: true\n" +
            "auto_analyze_check_interval_ms: 30000\n" +
            "auto_analyze_fraction_stale_rows: 0.10\n" +
            "auto_analyze_min_stale_rows: 250\n" +
            "auto_analyze_max_concurrent: 2\n" +
            "auto_analyze_max_rows_per_second: 20000\n" +
            "auto_analyze_histogram_sample_rows: 5000\n" +
            "auto_analyze_hll_precision: 12\n" +
            "auto_analyze_load_pause_threshold: 8\n" +
            "auto_analyze_ownership_check_rows: 200");

        Assert.That(config.StatsAnalyzeSampleRows, Is.EqualTo(250000));
        Assert.That(config.StatsHistogramBuckets, Is.EqualTo(64));
        Assert.That(config.AutoAnalyzeEnabled, Is.True);
        Assert.That(config.AutoAnalyzeCheckIntervalMs, Is.EqualTo(30000));
        Assert.That(config.AutoAnalyzeFractionStaleRows, Is.EqualTo(0.10).Within(1e-9));
        Assert.That(config.AutoAnalyzeMinStaleRows, Is.EqualTo(250));
        Assert.That(config.AutoAnalyzeMaxConcurrent, Is.EqualTo(2));
        Assert.That(config.AutoAnalyzeMaxRowsPerSecond, Is.EqualTo(20000));
        Assert.That(config.AutoAnalyzeHistogramSampleRows, Is.EqualTo(5000));
        Assert.That(config.AutoAnalyzeHllPrecision, Is.EqualTo(12));
        Assert.That(config.AutoAnalyzeLoadPauseThreshold, Is.EqualTo(8));
        Assert.That(config.AutoAnalyzeOwnershipCheckRows, Is.EqualTo(200));
    }

    [Test]
    public void RejectsInvalidAutoAnalyzeHllPrecision()
    {
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("auto_analyze_hll_precision: 3"));
    }

    [Test]
    public void ReadsSchemaAndTransactionLimitOverrides()
    {
        ConfigDefinition config = new ConfigReader().Read(
            "max_index_columns: 16\n" +
            "max_index_include_tuple_bytes: 8192\n" +
            "max_mutations_per_transaction: 50000\n" +
            "branch_snapshot_hold_lease_ms: 120000");

        Assert.That(config.MaxIndexColumns, Is.EqualTo(16));
        Assert.That(config.MaxIndexIncludeTupleBytes, Is.EqualTo(8192));
        Assert.That(config.MaxMutationsPerTransaction, Is.EqualTo(50000));
        Assert.That(config.BranchSnapshotHoldLeaseMs, Is.EqualTo(120000));
    }

    [Test]
    public void RejectsNonPositiveBranchSnapshotHoldLease()
    {
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("branch_snapshot_hold_lease_ms: 0"));
    }

    /// <summary>
    /// Guards the allow-list against drift: every settable <see cref="ConfigDefinition"/> property
    /// binds from an underscored root YAML key, so each one MUST have a matching entry in
    /// <see cref="ConfigReader.AllowedRootKeys"/> or the reader rejects a config that sets it.
    /// A new property added without updating the allow-list fails here instead of at a user's startup.
    /// </summary>
    [Test]
    public void EverySettablePropertyHasAnAllowedRootKey()
    {
        INamingConvention naming = UnderscoredNamingConvention.Instance;

        foreach (PropertyInfo prop in typeof(ConfigDefinition).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (prop.GetSetMethod() is null)
                continue;

            string yamlKey = naming.Apply(prop.Name);
            Assert.That(ConfigReader.AllowedRootKeys, Does.Contain(yamlKey),
                $"Property '{prop.Name}' maps to root key '{yamlKey}' which is missing from AllowedRootKeys");
        }
    }
}
