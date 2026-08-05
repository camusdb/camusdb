
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

        EmbeddedKahunaOptions expected = EmbeddedKahunaOptionsBuilder.ClusterBaseline(config, CamusDBOptions.Default);
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.That(built.NodeName, Is.EqualTo(expected.NodeName));
        Assert.That(built.NodeId, Is.EqualTo(expected.NodeId));
        Assert.That(built.Host, Is.EqualTo(expected.Host));
        Assert.That(built.Port, Is.EqualTo(expected.Port));
        Assert.That(built.InitialPartitions, Is.EqualTo(expected.InitialPartitions));
        Assert.That(built.Storage, Is.EqualTo("rocksdb"));
        Assert.That(built.StorageRevision, Is.EqualTo("v1"));
        Assert.That(built.WalStorage, Is.EqualTo("rocksdb"));
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
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone(dataPath, new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(built.Storage, Is.EqualTo(expected.Storage));
        Assert.That(built.StorageRevision, Is.EqualTo(expected.StorageRevision));
        Assert.That(built.WalStorage, Is.EqualTo(expected.WalStorage));
        Assert.That(built.WalRevision, Is.EqualTo(expected.WalRevision));
        Assert.That(built.InitialPartitions, Is.EqualTo(1));
    }

    [Test]
    public void StandaloneRocksDbBaseline_EnablesSingleFsyncCommitByDefault()
    {
        // The single-fsync commit fast path is the recommended standalone default; Kahuna's own embedded
        // default is off, so CamusDB opts standalone in via the baseline. Group-commit linger stays opt-in.
        EmbeddedKahunaOptions built =
            EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/fsync-db", new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(built.RaftWalSingleFsyncCommit, Is.True);
        Assert.That(built.RaftWalGroupCommitLingerMs, Is.EqualTo(0));
    }

    [Test]
    public void WalGroupCommitKnobs_OverrideStandaloneBaseline()
    {
        KahunaOptionsConfig kahuna = new()
        {
            WalGroupCommitLingerMs = 2,
            WalSingleFsyncCommit = false,
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/gc-db", kahuna, CamusDBOptions.Default);

        Assert.That(built.RaftWalGroupCommitLingerMs, Is.EqualTo(2));
        // An explicit config value wins over the baseline default (which enables single-fsync).
        Assert.That(built.RaftWalSingleFsyncCommit, Is.False);
    }

    [Test]
    public void KahunaStorageRocksdb_OverridesStandaloneBaseline()
    {
        KahunaOptionsConfig kahuna = new() { Storage = "rocksdb" };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/rocksdb-db", kahuna, CamusDBOptions.Default);

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

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

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

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/evict-db", kahuna, CamusDBOptions.Default);

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
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/defaults-db", new KahunaOptionsConfig(), CamusDBOptions.Default);

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
        EmbeddedKahunaOptions cluster = EmbeddedKahunaOptionsBuilder.ClusterBaseline(config, CamusDBOptions.Default);

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
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-off", kahuna, CamusDBOptions.Default);

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
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-budget", kahuna, CamusDBOptions.Default);

        Assert.That(built.RocksDbSharedMemoryEnabled, Is.True);
        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.EqualTo(512));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.EqualTo(200));
    }

    [Test]
    public void UnsetSharedMemoryFields_SizeProportionallyToMachineMemory()
    {
        // Unset shared-memory knobs are sized against the machine (respecting container limits)
        // rather than left at the fixed baseline: block cache = 25% of RAM clamped to [320 MB, 8 GB],
        // memtable budget = a quarter of that clamped to [128 MB, 1 GB]. The expected values are
        // recomputed here from the same input the builder reads, so the assertion holds on any
        // machine — hardcoding the baseline is what made this test machine-dependent before.
        const long OneMb = 1024L * 1024;
        long totalRam = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        Assume.That(totalRam, Is.GreaterThan(0), "memory-proportional sizing needs a known RAM size");

        int expectedBlockCacheMb = (int)Math.Clamp(totalRam / 4 / OneMb, 320, 8192);
        int expectedMemtableMb   = (int)Math.Clamp(expectedBlockCacheMb / 4, 128, 1024);

        KahunaOptionsConfig kahuna = new();
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-defaults", kahuna, CamusDBOptions.Default);

        Assert.That(built.RocksDbSharedMemoryEnabled, Is.True);
        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.EqualTo(expectedBlockCacheMb));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.EqualTo(expectedMemtableMb));

        // The derived pair must never invert, or the build-time cross-field check would reject its
        // own defaults on a machine whose RAM lands near a clamp boundary.
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.LessThanOrEqualTo(built.RocksDbSharedMemoryBudgetMb));
    }

    [Test]
    public void UnsetSharedMemoryFields_StayWithinSizingClamps()
    {
        // Guards the clamp bounds independently of the machine this runs on: a very small container
        // must still get the 320/128 MB floor, and a very large host must not hand RocksDB more than
        // the 8 GB / 1 GB ceiling.
        KahunaOptionsConfig kahuna = new();
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-clamps", kahuna, CamusDBOptions.Default);

        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.InRange(320, 8192));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.InRange(128, 1024));
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
            () => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-merge-total", kahuna, CamusDBOptions.Default))!;
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
            () => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-merge-memtable", kahuna, CamusDBOptions.Default))!;
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

        Assert.DoesNotThrow(() => EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/sm-merge-sqlite", kahuna, CamusDBOptions.Default));
    }

    [Test]
    public void SharingDisabled_WithInvertedEffectiveBudgets_IsNotRejected()
    {
        // Sharing explicitly off: the bundle is never built, so an inverted effective pair is harmless
        // and must not be rejected even on the RocksDB baseline.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemory = false, RocksdbSharedMemoryBudgetMb = 100 };

        Assert.DoesNotThrow(() => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-merge-off", kahuna, CamusDBOptions.Default));
    }

    // ── Priority admission gate ──────────────────────────────────────────────────────────────────

    [Test]
    public void AdmissionGate_DefaultsToOff_WhenNoKnobsAreSet()
    {
        // The whole feature ships dark: with no configuration the ceiling stays at zero, so every
        // transaction is admitted immediately and priority is recorded but never gates.
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone(
            "/tmp/admission-default", new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(built.MaxConcurrentSessions, Is.EqualTo(0));
        Assert.That(built.TransactionPriorityReservedSlots, Is.EqualTo(0));
    }

    [Test]
    public void AdmissionGateKnobs_ReachTheNodeOptions()
    {
        KahunaOptionsConfig kahuna = new()
        {
            MaxConcurrentSessions             = 8,
            TransactionPriorityReservedSlots  = 2,
            TransactionPriorityAgingThreshold = 30_000,
            TransactionPriorityMaxQueued      = 512,
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone(
            "/tmp/admission-set", kahuna, CamusDBOptions.Default);

        Assert.That(built.MaxConcurrentSessions, Is.EqualTo(8));
        Assert.That(built.TransactionPriorityReservedSlots, Is.EqualTo(2));
        Assert.That(built.TransactionPriorityAgingThreshold, Is.EqualTo(30_000));
        Assert.That(built.TransactionPriorityMaxQueued, Is.EqualTo(512));
    }

    [Test]
    public void ScriptTransactionCeiling_IsNeverConfigured()
    {
        // MaxConcurrentTransactions gates Kahuna's script-transaction path, which CamusDB never uses
        // (every transaction goes through LocateAndStartTransaction). It is deliberately not exposed,
        // so it must stay at the node default no matter what the session ceiling is set to.
        KahunaOptionsConfig kahuna = new() { MaxConcurrentSessions = 4 };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone(
            "/tmp/admission-script", kahuna, CamusDBOptions.Default);

        Assert.That(built.MaxConcurrentTransactions, Is.EqualTo(0));
    }

    // ── Transaction-timeout composition (session Timeout vs node MaxTransactionTimeout) ──────────

    /// <summary>
    /// Runs <paramref name="body"/> with the global serializable-lifetime static pinned to
    /// <paramref name="lifetimeMs"/>, restoring it afterward. The option builder derives the node's
    /// MaxTransactionTimeout from this static, so these tests must set it deterministically rather than
    /// inherit whatever a prior test left behind.
    /// </summary>
    /// <summary>
    /// Options whose serializable-transaction lifetime is <paramref name="lifetimeMs"/>. The builder
    /// derives the node's MaxTransactionTimeout from it and takes it as an argument, so a case states
    /// the lifetime it wants instead of assigning a global and restoring it afterwards.
    /// </summary>
    private static CamusDBOptions WithSerializableLifetime(int lifetimeMs)
        => CamusDBOptions.Default with { MaxSerializableTransactionLifetimeMs = lifetimeMs };

    [Test]
    public void MaxTransactionTimeout_DerivedFromSerializableLifetime_WhenUnset()
    {
        // The engine passes MaxSerializableTransactionLifetimeMs as each session's Timeout; the node cap
        // must be lifted to admit it, or Kahuna clamps the session to its 300 s default and reaps a long
        // transaction early. With no explicit override the builder derives the cap from the lifetime.
        CamusDBOptions lifetimeOptions = WithSerializableLifetime(1_800_000);

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/txto-derive", new KahunaOptionsConfig(), lifetimeOptions);
        Assert.That(built.MaxTransactionTimeout, Is.EqualTo(1_800_000));
    }

    [Test]
    public void ExplicitMaxTransactionTimeout_AboveLifetime_IsNotLowered()
    {
        // An operator who pins a cap larger than the lifetime keeps it; the derive step only ever raises
        // an unset/too-small cap, never lowers an explicit one.
        CamusDBOptions lifetimeOptions = WithSerializableLifetime(600_000);

        KahunaOptionsConfig kahuna = new() { MaxTransactionTimeoutMs = 900_000 };
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/txto-explicit", kahuna, lifetimeOptions);
        Assert.That(built.MaxTransactionTimeout, Is.EqualTo(900_000));    }

    [Test]
    public void NonPositiveLifetime_LeavesNodeMaxTimeoutAtDefault()
    {
        // A disabled lifetime cap (<= 0) means "no engine-imposed maximum", so the node keeps Kahuna's
        // own default MaxTransactionTimeout untouched.
        CamusDBOptions lifetimeOptions = WithSerializableLifetime(0);

        EmbeddedKahunaOptions defaults = new();
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/txto-disabled", new KahunaOptionsConfig(), lifetimeOptions);
        Assert.That(built.MaxTransactionTimeout, Is.EqualTo(defaults.MaxTransactionTimeout));
    }

    [Test]
    public void NonPositiveMaxTransactionTimeout_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { MaxTransactionTimeoutMs = 0 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("max_transaction_timeout_ms"));
    }

    [Test]
    public void DefaultTimeoutAboveMaxTimeout_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { DefaultTransactionTimeoutMs = 10_000, MaxTransactionTimeoutMs = 5_000 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("default_transaction_timeout_ms"));
        Assert.That(ex.Message, Does.Contain("max_transaction_timeout_ms"));
    }

    // ── ConfigDefinition cross-field composition (timeout cap + range-lock TTL) ───────────────────

    [Test]
    public void DefaultConfig_PassesCrossFieldValidation()
    {
        // Sanity: the shipped defaults (150 s TTL vs 60 s interval, 1 h lifetime, no explicit cap) are
        // internally consistent and must validate.
        Assert.DoesNotThrow(() => new ConfigDefinition().Validate());
    }

    [Test]
    public void ExplicitKahunaMaxTimeout_BelowSerializableLifetime_IsRejected()
    {
        ConfigDefinition config = new()
        {
            MaxSerializableTransactionLifetimeMs = 3_600_000,
            Kahuna = new KahunaOptionsConfig { MaxTransactionTimeoutMs = 100_000 },
        };

        CamusDBException ex = Assert.Throws<CamusDBException>(() => config.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("max_transaction_timeout_ms"));
        Assert.That(ex.Message, Does.Contain("max_serializable_transaction_lifetime_ms"));
    }

    [Test]
    public void RangeLockTtl_BelowTwiceCollectionInterval_IsRejected()
    {
        // TTL 100 s but the coordinator renews on the (default 60 s) collection tick: 100 s < 2x60 s, so
        // the lock could lapse before its first renewal.
        ConfigDefinition config = new() { RangeLockExpiresMs = 100_000 };

        CamusDBException ex = Assert.Throws<CamusDBException>(() => config.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("range_lock_expires_ms"));
        Assert.That(ex.Message, Does.Contain("collection interval"));
    }

    [Test]
    public void RangeLockTtl_BelowTwiceExplicitCollectionInterval_IsRejected()
    {
        // A raised collection interval must be honored by the cross-check: 150 s TTL < 2x90 s = 180 s.
        ConfigDefinition config = new()
        {
            RangeLockExpiresMs = 150_000,
            Kahuna = new KahunaOptionsConfig { CollectionIntervalMs = 90_000 },
        };

        CamusDBException ex = Assert.Throws<CamusDBException>(() => config.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("range_lock_expires_ms"));
    }

    [Test]
    public void RangeLockTtl_DisabledWithNonPositive_IsAccepted()
    {
        // A non-positive TTL disables client-side expiry (server-side renewal owns the lease), so the
        // renewal-margin cross-check is skipped.
        ConfigDefinition config = new() { RangeLockExpiresMs = 0 };
        Assert.DoesNotThrow(() => config.Validate());
    }
}
