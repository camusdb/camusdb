
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
