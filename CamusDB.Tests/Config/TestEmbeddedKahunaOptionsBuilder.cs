
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
using CamusDB.Core.Config;
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
        // rather than left at the fixed baseline: block cache = 10% of RAM, memtable budget = a
        // quarter of that, both with floors that yield below their proportional share (the exact
        // small-container values are pinned in TestMemoryProfile, which drives the sizing with
        // synthetic RAM sizes). The expected values are recomputed here from the same input the
        // builder reads, so the assertion holds on any machine — hardcoding the baseline is what
        // made this test machine-dependent before.
        const long OneMb = 1024L * 1024;
        long totalRam = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        Assume.That(totalRam, Is.GreaterThan(0), "memory-proportional sizing needs a known RAM size");

        int expectedBlockCacheMb = (int)(EmbeddedKahunaOptionsBuilder.ClampWithYieldingFloor(
            totalRam / 10, 320 * OneMb, 64 * OneMb, 2048 * OneMb) / OneMb);
        int expectedMemtableMb = (int)EmbeddedKahunaOptionsBuilder.ClampWithYieldingFloor(
            expectedBlockCacheMb / 4, 128, 16, 1024);

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
        // Guards the clamp bounds independently of the machine this runs on: even a very small
        // container keeps the degenerate 64/16 MB minimums, and a very large host must not hand
        // RocksDB more than the 2 GB / 1 GB ceiling — an unconfigured node usually shares a
        // developer workstation or a CI container with the rest of the toolchain, so the ceiling
        // matters more than the fraction. (The historic 320/128 MB floors yield below their
        // proportional share, so they are deliberately NOT lower bounds here.)
        KahunaOptionsConfig kahuna = new();
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/sm-clamps", kahuna, CamusDBOptions.Default);

        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.InRange(64, 2048));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.InRange(16, 1024));

        // The KV actor caches are the other half of the default footprint; their bounds are the
        // 1 MiB degenerate per-actor floor and the 2 GiB per-actor ceiling.
        Assert.That(built.MaxBytesPerActor, Is.InRange(1024L * 1024, 2048L * 1024 * 1024));
    }

    [Test]
    public void ActorCacheFloor_BoundsTheLayerNotEachActor()
    {
        // The per-actor byte budget used to carry a 64 MB floor, which the actor count multiplied:
        // with one actor per CPU, a many-core machine claimed 64 MB x cores however little RAM it
        // had — 1 GB of a 4 GB container, four times the share the layer was meant to take. The
        // floor now bounds the layer as a whole, so raising the actor count splits a fixed budget
        // instead of growing it.
        const long OneMb = 1024L * 1024;
        long totalRam = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        Assume.That(totalRam, Is.GreaterThan(0), "memory-proportional sizing needs a known RAM size");

        long expectedLayerBytes = Math.Max(totalRam / 16, 64L * OneMb);

        EmbeddedKahunaOptions few = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/actor-floor-few", new KahunaOptionsConfig { KeyValueWorkers = 4 }, CamusDBOptions.Default);

        EmbeddedKahunaOptions many = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/actor-floor-many", new KahunaOptionsConfig { KeyValueWorkers = 32 }, CamusDBOptions.Default);

        Assert.That(few.MaxBytesPerActor * 4, Is.LessThanOrEqualTo(expectedLayerBytes));

        // At 32 actors the layer may only exceed its budget through the 8 MB per-actor minimum, which
        // exists so an individual cache stays large enough to be worth having.
        Assert.That(many.MaxBytesPerActor * 32, Is.LessThanOrEqualTo(Math.Max(expectedLayerBytes, 32 * 8L * OneMb)));

        // More actors must never hand each actor more memory.
        Assert.That(many.MaxBytesPerActor, Is.LessThanOrEqualTo(few.MaxBytesPerActor));
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
    public void LoweringOnlyTotalBudget_ScalesTheMemtableDefaultWithIt()
    {
        // Only the total budget is overridden, to below the historic 128 MB memtable floor. The
        // derived memtable default is a quarter of the effective total with a floor that yields, so
        // it follows the operator's total down (100 -> 25) instead of staying pinned at 128 and
        // inverting the pair — an inversion Kahuna would reject with a raw
        // ArgumentOutOfRangeException at node startup.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemoryBudgetMb = 100 };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/sm-merge-total", kahuna, CamusDBOptions.Default);

        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.EqualTo(100));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.EqualTo(25));
    }

    [Test]
    public void RaisingOnlyMemtableBudgetAboveTotalDefault_IsRejectedAtBuild()
    {
        // Symmetric to the above: only the memtable budget is overridden, past anything the total can be
        // defaulted to. The check runs on the pair the node is actually built with — after the sizing
        // defaults — and the total's ceiling is 2 GB however large the machine is, so 4 GB of memtable
        // inside it is inverted on every machine this can run on.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemtableBudgetMb = 4096 };

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

    // ── Age at which a coordinator-unknown transaction's holdings may be released ─────────────────

    [Test]
    public void AbandonedReleaseAge_DerivesFromTheSessionCeilingPlusGrace()
    {
        // Unset (0) means derive. The ceiling is the session-timeout clamp the node will enforce — here
        // raised from the engine's serializable lifetime — and the grace covers the two windows in which
        // a reaped session's work can still land.
        CamusDBOptions options = CamusDBOptions.Default with { MaxSerializableTransactionLifetimeMs = 600_000 };

        Assert.That(
            KahunaSessionLifetime.AbandonedReleaseAgeMs(options),
            Is.EqualTo(600_000 + KahunaSessionLifetime.CoordinatorReclaimGraceMs));
    }

    [Test]
    public void AbandonedReleaseAge_MirrorsAnExplicitNodeClamp()
    {
        // With the engine cap disabled the node's own clamp is what bounds a session, so the mirror must
        // follow the configured value rather than Kahuna's default.
        CamusDBOptions options = CamusDBOptions.Default with
        {
            MaxSerializableTransactionLifetimeMs = 0,
            Kahuna = new KahunaOptionsConfig { MaxTransactionTimeoutMs = 900_000 },
        };

        Assert.That(
            KahunaSessionLifetime.AbandonedReleaseAgeMs(options),
            Is.EqualTo(900_000 + KahunaSessionLifetime.CoordinatorReclaimGraceMs));
    }

    [Test]
    public void AbandonedReleaseAge_HonoursAnExplicitAgeAndTheDisabledValue()
    {
        Assert.That(
            KahunaSessionLifetime.AbandonedReleaseAgeMs(
                CamusDBOptions.Default with { AbandonedTransactionReleaseAfterMs = 120_000 }),
            Is.EqualTo(120_000));

        Assert.That(
            KahunaSessionLifetime.AbandonedReleaseAgeMs(
                CamusDBOptions.Default with { AbandonedTransactionReleaseAfterMs = -1 }),
            Is.Null,
            "a negative age disables the release entirely");
    }

    [Test]
    public void AbandonedReleaseAge_BelowSerializableLifetime_IsRejected()
    {
        // A session may live for the whole serializable lifetime, so an age under it could release the
        // holdings of a transaction that is still running.
        ConfigDefinition config = new()
        {
            MaxSerializableTransactionLifetimeMs = 3_600_000,
            AbandonedTransactionReleaseAfterMs = 60_000,
        };

        CamusDBException ex = Assert.Throws<CamusDBException>(() => config.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("abandoned_transaction_release_after_ms"));
        Assert.That(ex.Message, Does.Contain("max_serializable_transaction_lifetime_ms"));
    }

    [Test]
    public void AbandonedReleaseAge_DeriveAndDisableAreAccepted()
    {
        Assert.DoesNotThrow(() => new ConfigDefinition { AbandonedTransactionReleaseAfterMs = 0 }.Validate());
        Assert.DoesNotThrow(() => new ConfigDefinition { AbandonedTransactionReleaseAfterMs = -1 }.Validate());
    }

    // ── Partition placement + leader balancing ───────────────────────────────

    [Test]
    public void PlacementAndBalancerKnobs_OverrideClusterBaseline()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            Kahuna = new KahunaOptionsConfig
            {
                ReplicationFactor = 3,
                Zone = "rack-a",
                EnablePlacementRebalancer = true,
                PlacementPassIntervalMs = 2_000,
                MaxReplicaMovesPerPass = 6,
                MaxConcurrentReplicaTransfers = 2,
                MaxConcurrentReplicaRepairs = 4,
                ReplicaCountDeadband = 0,
                DecommissionDrainTimeoutMs = 60_000,
                EnableLeaderBalancer = true,
                LeaderBalancerIntervalMs = 10_000,
                LeaderBalancerReportIntervalMs = 2_500,
                LeaderBalancerReportTtlMs = 12_000,
                MinLeaderStabilityMs = 3_000,
            },
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.That(built.ReplicationFactor, Is.EqualTo(3));
        Assert.That(built.Zone, Is.EqualTo("rack-a"));
        Assert.That(built.EnablePlacementRebalancer, Is.True);
        Assert.That(built.PlacementPassInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2_000)));
        Assert.That(built.MaxReplicaMovesPerPass, Is.EqualTo(6));
        Assert.That(built.MaxConcurrentReplicaTransfers, Is.EqualTo(2));
        Assert.That(built.MaxConcurrentReplicaRepairs, Is.EqualTo(4));
        Assert.That(built.ReplicaCountDeadband, Is.EqualTo(0));
        Assert.That(built.DecommissionDrainTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(60_000)));
        Assert.That(built.EnableLeaderBalancer, Is.True);
        Assert.That(built.LeaderBalancerInterval, Is.EqualTo(TimeSpan.FromMilliseconds(10_000)));
        Assert.That(built.LeaderBalancerReportInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2_500)));
        Assert.That(built.LeaderBalancerReportTtl, Is.EqualTo(TimeSpan.FromMilliseconds(12_000)));
        Assert.That(built.MinLeaderStability, Is.EqualTo(TimeSpan.FromMilliseconds(3_000)));
    }

    [Test]
    public void UnsetPlacementKnobs_KeepKahunaDefaults()
    {
        ConfigDefinition config = new() { DataDir = "/data/camus" };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.That(built.ReplicationFactor, Is.EqualTo(0), "full replication stays the default");
        Assert.That(built.Zone, Is.Null);
        Assert.That(built.EnablePlacementRebalancer, Is.False);
        Assert.That(built.PlacementPassInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(built.MaxReplicaMovesPerPass, Is.EqualTo(4));
        Assert.That(built.MaxConcurrentReplicaTransfers, Is.EqualTo(1));
        Assert.That(built.MaxConcurrentReplicaRepairs, Is.EqualTo(3));
        Assert.That(built.ReplicaCountDeadband, Is.EqualTo(1));
        Assert.That(built.DecommissionDrainTimeout, Is.EqualTo(TimeSpan.FromMinutes(2)));
        Assert.That(built.EnableLeaderBalancer, Is.False);
        Assert.That(built.LeaderBalancerInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void NegativeReplicationFactor_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { ReplicationFactor = -1 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("replication_factor"));
    }

    [Test]
    public void BlankZone_IsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { Zone = "  " }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("zone"));
    }

    [Test]
    public void ReportTtlAtOrBelowReportInterval_IsRejected()
    {
        // The cross-check compares the EFFECTIVE pair: a TTL lowered below the default 5 s report
        // interval must be caught even when the interval key itself is untouched.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { LeaderBalancerReportTtlMs = 4_000 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("leader_balancer_report_ttl_ms"));
    }

    [Test]
    public void JoinExisting_SeedsFlowFromPeers()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            Mode = "cluster",
            JoinExisting = true,
            Peers = ["10.0.0.1:7070", "10.0.0.2:7072"],
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.That(built.JoinExistingSeeds, Is.EqualTo(new[] { "10.0.0.1:7070", "10.0.0.2:7072" }));
    }

    [Test]
    public void JoinExistingOff_LeavesSeedsNull()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            Mode = "cluster",
            Peers = ["10.0.0.1:7070"],
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.That(built.JoinExistingSeeds, Is.Null, "an ordinary boot must never be mistaken for a join");
    }

    [Test]
    public void NonPositivePlacementKnobs_AreRejected()
    {
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { PlacementPassIntervalMs = 0 }.Validate());
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { MaxReplicaMovesPerPass = 0 }.Validate());
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { MaxConcurrentReplicaTransfers = 0 }.Validate());
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { MaxConcurrentReplicaRepairs = 0 }.Validate());
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { ReplicaCountDeadband = -1 }.Validate());
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { DecommissionDrainTimeoutMs = 0 }.Validate());
        Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { MinLeaderStabilityMs = 0 }.Validate());
    }

    [Test]
    public void LoadSplitKnobs_MapToEmbeddedOptions()
    {
        KahunaOptionsConfig kahuna = new()
        {
            RangeSplitLoadThreshold = 250.5,
            RangeSplitLoadMinQueueDepth = 16,
            RangeSplitLoadMinCommitWaitMs = 12.5,
            RangeSplitLoadWindowMs = 20_000,
            RangeSplitLoadPollIntervalMs = 2_000,
            RangeSplitLoadImbalanceMax = 0.9,
            RangeSplitSettleWindowMs = 30_000,
            RangeSplitIndivisibleCooldownMs = 60_000,
            RangeMergeMinSize = 7,
            RangeMoveSettleTimeoutMs = 4_000,
            EnableLoadReports = true,
        };

        Assert.DoesNotThrow(() => kahuna.Validate());

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb("/tmp/load-split", kahuna, CamusDBOptions.Default);

        Assert.That(built.RangeSplitLoadThreshold, Is.EqualTo(250.5));
        Assert.That(built.RangeSplitLoadMinQueueDepth, Is.EqualTo(16));
        Assert.That(built.RangeSplitLoadMinCommitWaitMs, Is.EqualTo(12.5));
        Assert.That(built.RangeSplitLoadWindow, Is.EqualTo(TimeSpan.FromMilliseconds(20_000)));
        Assert.That(built.RangeSplitLoadPollInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2_000)));
        Assert.That(built.RangeSplitLoadImbalanceMax, Is.EqualTo(0.9));
        Assert.That(built.RangeSplitSettleWindow, Is.EqualTo(TimeSpan.FromMilliseconds(30_000)));
        Assert.That(built.RangeSplitIndivisibleCooldown, Is.EqualTo(TimeSpan.FromMilliseconds(60_000)));
        Assert.That(built.RangeMergeMinSize, Is.EqualTo(7));
        Assert.That(built.RangeMoveSettleTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(4_000)));
        Assert.That(built.EnableLoadReports, Is.True);
    }

    [Test]
    public void UnsetLoadSplitKnobs_KeepKahunaDefaults()
    {
        // Every knob except the threshold inherits Kahuna's own default, so an operator who sets one
        // key does not silently re-tune the rest. The threshold itself is pinned to 0 in the CamusDB
        // baseline, so a deployment that asks for nothing gets no heat-based splitting.
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/load-split-defaults", new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(built.RangeSplitLoadThreshold, Is.EqualTo(0), "auto-split must never arrive unasked");
        Assert.That(built.RangeSplitLoadMinQueueDepth, Is.EqualTo(8));
        Assert.That(built.RangeSplitLoadMinCommitWaitMs, Is.EqualTo(0));
        Assert.That(built.RangeSplitLoadWindow, Is.EqualTo(TimeSpan.FromSeconds(15)));
        Assert.That(built.RangeSplitLoadPollInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(built.RangeSplitLoadImbalanceMax, Is.EqualTo(0.8));
        Assert.That(built.RangeSplitSettleWindow, Is.EqualTo(TimeSpan.FromSeconds(10)));
        Assert.That(built.RangeSplitIndivisibleCooldown, Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(built.RangeMergeMinSize, Is.EqualTo(10));
        Assert.That(built.RangeMoveSettleTimeout, Is.EqualTo(TimeSpan.FromSeconds(10)),
            "the drain wait must inherit Kahuna's own default; a shorter one refuses every attempt under load");
        Assert.That(built.EnableLoadReports, Is.False);
    }

    [Test]
    public void ClusterBaseline_PinsLoadSplitOff()
    {
        ConfigDefinition config = new() { DataDir = "/data/camus", Mode = "cluster", InitialPartitions = 3 };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.That(built.RangeSplitLoadThreshold, Is.EqualTo(0));
        Assert.That(built.RangeSplitThreshold, Is.EqualTo(0));
    }

    [Test]
    public void NegativeMoveSettleTimeout_IsRejected()
    {
        // 0 is legitimate — it means "do not wait, refuse an attempt that meets an unsettled intent".
        Assert.DoesNotThrow(() => new KahunaOptionsConfig { RangeMoveSettleTimeoutMs = 0 }.Validate());

        Assert.That(
            Assert.Throws<CamusDBException>(
                () => new KahunaOptionsConfig { RangeMoveSettleTimeoutMs = -1 }.Validate())!.Message,
            Does.Contain("range_move_settle_timeout_ms"));
    }

    [Test]
    public void ScanPageRetryBudget_MapsValidatesAndKeepsTheDefault()
    {
        // Mapping: a configured budget reaches the embedded options.
        KahunaOptionsConfig kahuna = new() { ScanPageRetryBudgetMs = 4_000 };
        Assert.DoesNotThrow(() => kahuna.Validate());
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/scan-budget", kahuna, CamusDBOptions.Default);
        Assert.That(built.ScanPageRetryBudgetMs, Is.EqualTo(4_000));

        // Unset keeps Kahuna's default, which must sit below the shipped 10 s client command deadline —
        // a budget at or above the deadline raises the named scan error into a call the client has
        // already cancelled, so the diagnosis never reaches a caller.
        EmbeddedKahunaOptions defaults = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/scan-budget-default", new KahunaOptionsConfig(), CamusDBOptions.Default);
        Assert.That(defaults.ScanPageRetryBudgetMs, Is.EqualTo(5_000));
        Assert.That(defaults.ScanPageRetryBudgetMs, Is.LessThan(10_000),
            "the budget must fire below the default client command deadline or it is unobservable");

        // A non-positive budget would restore the unbounded silent retry loop the setting rules out.
        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { ScanPageRetryBudgetMs = 0 }.Validate())!.Message,
            Does.Contain("scan_page_retry_budget_ms"));
        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { ScanPageRetryBudgetMs = -1 }.Validate())!.Message,
            Does.Contain("scan_page_retry_budget_ms"));
    }

    [Test]
    public void NegativeLoadSplitKnobs_AreRejected()
    {
        CamusDBException threshold = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { RangeSplitLoadThreshold = -1 }.Validate())!;
        Assert.That(threshold.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(threshold.Message, Does.Contain("range_split_load_threshold"));

        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeSplitLoadMinQueueDepth = -1 }.Validate())!.Message,
            Does.Contain("range_split_load_min_queue_depth"));

        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeSplitLoadMinCommitWaitMs = -0.5 }.Validate())!.Message,
            Does.Contain("range_split_load_min_commit_wait_ms"));

        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeSplitLoadWindowMs = 0 }.Validate())!.Message,
            Does.Contain("range_split_load_window_ms"));

        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeSplitLoadPollIntervalMs = 0 }.Validate())!.Message,
            Does.Contain("range_split_load_poll_interval_ms"));

        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeSplitIndivisibleCooldownMs = -1 }.Validate())!.Message,
            Does.Contain("range_split_indivisible_cooldown_ms"));

        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeMergeMinSize = -1 }.Validate())!.Message,
            Does.Contain("range_merge_min_size"));
    }

    [Test]
    public void ImbalanceCeilingOutsideItsBand_IsRejected()
    {
        // At or below 0.5 no split can ever be accepted, because no split leaves less than half the
        // writes on its heavier child. Above 1.0 no split is ever refused, so a single hot key would
        // split its range again and again.
        foreach (double outOfBand in new[] { 0.5, 0.25, 1.01 })
        {
            CamusDBException ex = Assert.Throws<CamusDBException>(
                () => new KahunaOptionsConfig { RangeSplitLoadImbalanceMax = outOfBand }.Validate())!;
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
            Assert.That(ex.Message, Does.Contain("range_split_load_imbalance_max"));
        }

        Assert.DoesNotThrow(() => new KahunaOptionsConfig { RangeSplitLoadImbalanceMax = 1.0 }.Validate());
        Assert.DoesNotThrow(() => new KahunaOptionsConfig { RangeSplitLoadImbalanceMax = 0.51 }.Validate());
    }

    [Test]
    public void PollIntervalAtOrAboveTheLoadWindow_IsRejected()
    {
        CamusDBException both = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig
            {
                RangeSplitLoadWindowMs = 10_000,
                RangeSplitLoadPollIntervalMs = 10_000,
            }.Validate())!;
        Assert.That(both.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(both.Message, Does.Contain("range_split_load_poll_interval_ms"));
        Assert.That(both.Message, Does.Contain("range_split_load_window_ms"));

        // The cross-check compares the EFFECTIVE pair, so a window lowered below the default 5000 ms
        // poll interval is caught with the interval key untouched.
        CamusDBException oneSided = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { RangeSplitLoadWindowMs = 3_000 }.Validate())!;
        Assert.That(oneSided.Message, Does.Contain("range_split_load_poll_interval_ms"));
    }

    [Test]
    public void SettleWindowShorterThanLeaderStability_IsRejected()
    {
        // Kahuna raises an ArgumentException from the node constructor for this pair. Catching it
        // here turns a startup crash into a named configuration error.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig
            {
                RangeSplitSettleWindowMs = 2_000,
                MinLeaderStabilityMs = 5_000,
            }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("range_split_settle_window_ms"));
        Assert.That(ex.Message, Does.Contain("min_leader_stability_ms"));

        // The default leader-stability window is 5000 ms, so a settle window below it must be caught
        // with the stability key untouched.
        Assert.That(
            Assert.Throws<CamusDBException>(() => new KahunaOptionsConfig { RangeSplitSettleWindowMs = 1_000 }.Validate())!.Message,
            Does.Contain("range_split_settle_window_ms"));

        // Equal is acceptable: Kahuna's own bound is >=, not >.
        Assert.DoesNotThrow(
            () => new KahunaOptionsConfig { RangeSplitSettleWindowMs = 5_000, MinLeaderStabilityMs = 5_000 }.Validate());
    }

    [Test]
    public void ZeroSettleWindow_IsRejected()
    {
        // Kahuna reads 0 as "no settle window". CamusDB does not accept it: a fresh child starts warm
        // from its inherited histogram, so an unsuppressed re-evaluation turns one hot range into a
        // split storm.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new KahunaOptionsConfig { RangeSplitSettleWindowMs = 0 }.Validate())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("range_split_settle_window_ms"));
    }
}
