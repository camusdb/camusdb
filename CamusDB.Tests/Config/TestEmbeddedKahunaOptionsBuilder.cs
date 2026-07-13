
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna;

namespace CamusDB.Tests.Config;

[TestFixture]
public sealed class TestEmbeddedKahunaOptionsBuilder
{
    [Test]
    public void EmptyKahunaSection_ReproducesClusterBaseline()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            NodeName = "node-a",
            RaftNodeId = 2,
            RaftHost = "10.0.0.1",
            RaftPort = 7071,
            InitialPartitions = 3,
        };

        EmbeddedKahunaOptions expected = EmbeddedKahunaOptionsBuilder.ClusterBaseline(config);
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config);

        Assert.That(built.NodeName, Is.EqualTo(expected.NodeName));
        Assert.That(built.NodeId, Is.EqualTo(expected.NodeId));
        Assert.That(built.Host, Is.EqualTo(expected.Host));
        Assert.That(built.Port, Is.EqualTo(expected.Port));
        Assert.That(built.InitialPartitions, Is.EqualTo(expected.InitialPartitions));
        Assert.That(built.Storage, Is.EqualTo("sqlite"));
        Assert.That(built.StorageRevision, Is.EqualTo("v1"));
        Assert.That(built.WalStorage, Is.EqualTo("sqlite"));
        Assert.That(built.WalRevision, Is.EqualTo("v1"));
        Assert.That(built.StartElectionTimeout, Is.EqualTo(2000));
        Assert.That(built.EndElectionTimeout, Is.EqualTo(4000));
        Assert.That(built.StoragePath, Is.EqualTo(Path.Combine("/data/camus", "kv")));
        Assert.That(built.WalPath, Is.EqualTo(Path.Combine("/data/camus", "wal")));
    }

    [Test]
    public void EmptyKahunaSection_ReproducesStandaloneBaseline()
    {
        string dataPath = "/tmp/db-one";
        EmbeddedKahunaOptions expected = EmbeddedKahunaOptionsBuilder.StandaloneBaseline(dataPath);
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone(dataPath, new KahunaOptionsConfig());

        Assert.That(built.Storage, Is.EqualTo(expected.Storage));
        Assert.That(built.StorageRevision, Is.EqualTo(expected.StorageRevision));
        Assert.That(built.WalStorage, Is.EqualTo(expected.WalStorage));
        Assert.That(built.WalRevision, Is.EqualTo(expected.WalRevision));
        Assert.That(built.InitialPartitions, Is.EqualTo(1));
    }

    [Test]
    public void KahunaStorageRocksdb_OverridesStandaloneBaseline()
    {
        KahunaOptionsConfig kahuna = new() { Storage = "rocksdb" };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/rocksdb-db", kahuna);

        Assert.That(built.Storage, Is.EqualTo("rocksdb"));
        Assert.That(built.StoragePath, Is.EqualTo(Path.Combine("/tmp/rocksdb-db", "kv")));
    }

    [Test]
    public void KahunaElectionTimeouts_OverrideClusterBaseline()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            Kahuna = new KahunaOptionsConfig
            {
                StartElectionTimeoutMs = 1500,
                EndElectionTimeoutMs = 3500,
            },
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config);

        Assert.That(built.StartElectionTimeout, Is.EqualTo(1500));
        Assert.That(built.EndElectionTimeout, Is.EqualTo(3500));
    }

    [Test]
    public void EvictionAndCompactionKnobs_OverrideStandaloneBaseline()
    {
        KahunaOptionsConfig kahuna = new()
        {
            MaxEntriesPerActor = 20_000,
            MaxBytesPerActor = 128L * 1024 * 1024,
            CacheEntryTtlMs = 90_000,
            CacheEntriesToRemove = 250,
            CollectionIntervalMs = 15_000,
            CompactEveryOperations = 500,
            CompactNumberEntries = 32,
            MaxEntriesPerCompaction = 2_000,
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/evict-db", kahuna);

        Assert.That(built.MaxEntriesPerActor, Is.EqualTo(20_000));
        Assert.That(built.MaxBytesPerActor, Is.EqualTo(128L * 1024 * 1024));
        Assert.That(built.CacheEntryTtl, Is.EqualTo(TimeSpan.FromMilliseconds(90_000)));
        Assert.That(built.CacheEntriesToRemove, Is.EqualTo(250));
        Assert.That(built.CollectionInterval, Is.EqualTo(TimeSpan.FromMilliseconds(15_000)));
        Assert.That(built.CompactEveryOperations, Is.EqualTo(500));
        Assert.That(built.CompactNumberEntries, Is.EqualTo(32));
        Assert.That(built.MaxEntriesPerCompaction, Is.EqualTo(2_000));
    }

    [Test]
    public void UnsetEvictionKnobs_KeepKahunaDefaults()
    {
        // An empty kahuna section must not touch the eviction/compaction knobs; they inherit
        // Kahuna's own EmbeddedKahunaOptions defaults untouched.
        EmbeddedKahunaOptions defaults = new();
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/defaults-db", new KahunaOptionsConfig());

        Assert.That(built.CacheEntryTtl, Is.EqualTo(defaults.CacheEntryTtl));
        Assert.That(built.CacheEntriesToRemove, Is.EqualTo(defaults.CacheEntriesToRemove));
        Assert.That(built.CollectionInterval, Is.EqualTo(defaults.CollectionInterval));
        Assert.That(built.CompactNumberEntries, Is.EqualTo(defaults.CompactNumberEntries));
        Assert.That(built.MaxEntriesPerCompaction, Is.EqualTo(defaults.MaxEntriesPerCompaction));
    }

    [Test]
    public void NonPositiveEvictionKnob_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { CacheEntryTtlMs = 0 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("cache_entry_ttl_ms"));
    }

    // ── RocksDB shared memory ────────────────────────────────────────────────

    [Test]
    public void AllBaselines_HaveSharedMemoryEnabledByDefault()
    {
        string dataPath = "/tmp/shared-mem-test";
        ConfigDefinition config = new() { DataDir = dataPath };

        EmbeddedKahunaOptions standalone = EmbeddedKahunaOptionsBuilder.StandaloneBaseline(dataPath);
        EmbeddedKahunaOptions standaloneRocksDb = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline(dataPath);
        EmbeddedKahunaOptions cluster = EmbeddedKahunaOptionsBuilder.ClusterBaseline(config);

        foreach (EmbeddedKahunaOptions opts in new[] { standalone, standaloneRocksDb, cluster })
        {
            Assert.That(opts.RocksDbSharedMemoryEnabled, Is.True);
            Assert.That(opts.RocksDbSharedMemoryBudgetMb, Is.EqualTo(320));
            Assert.That(opts.RocksDbSharedMemtableBudgetMb, Is.EqualTo(128));
        }
    }

    [Test]
    public void SharedMemoryOff_Override_DisablesFlag()
    {
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemory = false };
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-off", kahuna);

        Assert.That(built.RocksDbSharedMemoryEnabled, Is.False);
    }

    [Test]
    public void SharedMemoryBudgets_Override_FlowThrough()
    {
        KahunaOptionsConfig kahuna = new()
        {
            RocksdbSharedMemory = true,
            RocksdbSharedMemoryBudgetMb = 512,
            RocksdbSharedMemtableBudgetMb = 200,
        };
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-budget", kahuna);

        Assert.That(built.RocksDbSharedMemoryEnabled, Is.True);
        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.EqualTo(512));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.EqualTo(200));
    }

    [Test]
    public void UnsetSharedMemoryFields_KeepBaselineDefaults()
    {
        KahunaOptionsConfig kahuna = new();
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-defaults", kahuna);

        Assert.That(built.RocksDbSharedMemoryEnabled, Is.True);
        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.EqualTo(320));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.EqualTo(128));
    }

    [Test]
    public void ZeroSharedMemoryBudget_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { RocksdbSharedMemoryBudgetMb = 0 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memory_budget_mb"));
    }

    [Test]
    public void ZeroSharedMemtableBudget_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { RocksdbSharedMemtableBudgetMb = 0 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memtable_budget_mb"));
    }

    [Test]
    public void MemtableBudgetExceedsTotal_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig
            {
                RocksdbSharedMemoryBudgetMb = 128,
                RocksdbSharedMemtableBudgetMb = 256,
            }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memtable_budget_mb"));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memory_budget_mb"));
    }

    [Test]
    public void BudgetsOmitted_IsValid()
    {
        // When budgets are omitted entirely, Validate() must not throw; baseline defaults apply.
        Assert.DoesNotThrow(() => new KahunaOptionsConfig { RocksdbSharedMemory = true }.Validate());
    }

    [Test]
    public void LoweringOnlyTotalBudgetBelowMemtableDefault_IsRejectedAtBuild()
    {
        // Only the total budget is overridden (to below the baseline memtable default of 128). The raw
        // config passes KahunaOptionsConfig.Validate (memtable is null there), but after merging onto the
        // RocksDB baseline the effective pair is total=100 / memtable=128 — which Kahuna would reject with
        // a raw ArgumentOutOfRangeException at node startup. The builder must catch it as a clean config error.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemoryBudgetMb = 100 };

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-merge-total", kahuna))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memtable_budget_mb"));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memory_budget_mb"));
    }

    [Test]
    public void RaisingOnlyMemtableBudgetAboveTotalDefault_IsRejectedAtBuild()
    {
        // Symmetric to the above: only the memtable budget is overridden, past the baseline total default
        // of 320. Effective pair becomes total=320 / memtable=500 — rejected at build time.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemtableBudgetMb = 500 };

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-merge-memtable", kahuna))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
    }

    [Test]
    public void EffectiveBudgetInversion_OnNonRocksDbBaseline_IsNotRejected()
    {
        // The merge-time guard is scoped to the case where Kahuna actually builds the shared bundle
        // (sharing enabled AND both storages rocksdb). On the SQLite standalone baseline the budgets are
        // ignored, so an inverted effective pair (total=100 / memtable=128) must NOT be rejected —
        // rejecting it would be over-strict for a config Kahuna never acts on.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemoryBudgetMb = 100 };

        Assert.DoesNotThrow(() => EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/sm-merge-sqlite", kahuna));
    }

    [Test]
    public void SharingDisabled_WithInvertedEffectiveBudgets_IsNotRejected()
    {
        // Sharing explicitly off: the bundle is never built, so an inverted effective pair is harmless
        // and must not be rejected even on the RocksDB baseline.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemory = false, RocksdbSharedMemoryBudgetMb = 100 };

        Assert.DoesNotThrow(() => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-merge-off", kahuna));
    }
}
