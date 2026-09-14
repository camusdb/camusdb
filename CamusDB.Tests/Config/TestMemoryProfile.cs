/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers the <c>memory_profile</c> / <c>--memory-profile</c> setting end to end: how it is spelled in
/// YAML and on the command line, and what the embedded node is actually built with once it resolves.
///
/// <para>The assertions deliberately compare the dev profile against the prod one rather than pinning
/// absolute numbers everywhere: what the setting promises an operator is "smaller than the default and
/// independent of the machine", and a test that only pinned constants would pass on a machine where the
/// proportional sizing happened to land lower.</para>
/// </summary>
[TestFixture]
public sealed class TestMemoryProfile
{
    private const long OneMb = 1024L * 1024;

    private static CamusDBOptions Resolve(string yaml)
    {
        ConfigDefinition config = new ConfigReader().Read(yaml);
        return ConfigResolver.Resolve(config);
    }

    [Test]
    public void DefaultProfileIsProd()
    {
        Assert.That(CamusDBOptions.Default.MemoryProfile, Is.EqualTo(MemoryProfile.Prod));
        Assert.That(Resolve("").MemoryProfile, Is.EqualTo(MemoryProfile.Prod));
    }

    [Test]
    public void YamlSelectsDevProfile()
    {
        Assert.That(Resolve("memory_profile: dev").MemoryProfile, Is.EqualTo(MemoryProfile.Dev));
    }

    [Test]
    public void CliFlagOverridesYaml()
    {
        ConfigDefinition config = new ConfigReader().Read("memory_profile: dev");
        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides { MemoryProfile = "prod" });

        Assert.That(ConfigResolver.Resolve(config).MemoryProfile, Is.EqualTo(MemoryProfile.Prod));
        Assert.That(config.KeySources["memory_profile"], Is.EqualTo(ConfigValueSource.CommandLine));
    }

    [Test]
    public void UnknownProfileIsRejected()
    {
        // Rejected rather than silently treated as prod: a typo in the flag would otherwise leave the
        // operator with the large profile they were trying to avoid and no indication of it.
        CamusDBException fromYaml = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("memory_profile: small"))!;

        Assert.That(fromYaml.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(fromYaml.Message, Does.Contain("'prod' or 'dev'"));

        ConfigDefinition config = new ConfigReader().Read("");
        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides { MemoryProfile = "tiny" });

        CamusDBException fromCli = Assert.Throws<CamusDBException>(config.Validate)!;
        Assert.That(fromCli.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
    }

    [Test]
    public void DevProfileCapsTheCacheBudgets()
    {
        EmbeddedKahunaOptions dev = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/mem-profile-dev", new KahunaOptionsConfig(), CamusDBOptions.Default with { MemoryProfile = MemoryProfile.Dev });

        Assert.That(dev.RocksDbSharedMemoryBudgetMb, Is.EqualTo(64));
        Assert.That(dev.RocksDbSharedMemtableBudgetMb, Is.EqualTo(16));

        // The actor caches are budgeted as a layer and split across the actor count Kahuna will
        // actually run (the config leaves key_value_workers unset here, so the count is Kahuna's own
        // default, not the unset 0), so the aggregate is what the profile promises however many cores
        // the machine running this has.
        int actors = EmbeddedKahunaOptionsBuilder.EffectiveActorCount(dev);
        Assert.That(dev.MaxBytesPerActor * actors,
            Is.LessThanOrEqualTo(Math.Max(32 * OneMb, actors * OneMb)));
    }

    [Test]
    public void ProportionalFloorsHoldOnALargeMachine()
    {
        // 8 GiB: every proportional share clears its historic floor, so the sizing must be
        // byte-for-byte what the plain clamps always produced — the yielding floors are inert.
        EmbeddedKahunaOptions options = new() { KeyValueWorkers = 32 };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 8L * 1024 * OneMb);

        Assert.That(options.RocksDbSharedMemoryBudgetMb, Is.EqualTo(819));   // 10% of RAM
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(204)); // a quarter of the cache
        Assert.That(options.MaxBytesPerActor, Is.EqualTo(16 * OneMb));       // 512 MiB layer / 32 actors
        Assert.That(options.MaxEntriesPerActor, Is.EqualTo(32_768));         // bytes / 512
    }

    [Test]
    public void ProportionalFloorsYieldInsideASmallContainer()
    {
        // 1536 MiB: the historic floors (320 MB cache, 128 MB memtables, 8 MB x 32 actors) all
        // exceed their proportional shares. As plain clamps they claimed ~40% of the container in
        // caches and the node was OOM-killed under load; the percentages must govern instead.
        EmbeddedKahunaOptions options = new() { KeyValueWorkers = 32 };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 1536 * OneMb);

        Assert.That(options.RocksDbSharedMemoryBudgetMb, Is.EqualTo(153));  // 10%, not the 320 floor
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(38)); // a quarter, not the 128 floor
        Assert.That(options.MaxBytesPerActor, Is.EqualTo(3 * OneMb));       // 96 MiB layer / 32, not 8 MiB
        Assert.That(options.MaxEntriesPerActor, Is.EqualTo(6_144));         // derived, not the 10k floor

        // The invariant the shared-bundle builder enforces at boot must hold at every size.
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.LessThanOrEqualTo(options.RocksDbSharedMemoryBudgetMb));
    }

    [Test]
    public void ProportionalSizingStopsAtDegenerateFloors()
    {
        // 256 MiB: below every proportional share. The degenerate minimums keep each cache usable,
        // and the memtable sub-budget stays inside the cache it is charged to.
        EmbeddedKahunaOptions options = new() { KeyValueWorkers = 128 };
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 256 * OneMb);

        Assert.That(options.RocksDbSharedMemoryBudgetMb, Is.EqualTo(64));
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(16));
        Assert.That(options.MaxBytesPerActor, Is.EqualTo(OneMb));           // 64 MiB layer / 128 actors, floored
        Assert.That(options.MaxEntriesPerActor, Is.EqualTo(2_048));
    }

    [Test]
    public void HeapLimitNoLongerShrinksTheNativeRocksDbBudgets()
    {
        // The reference node: a 4,096 MiB container with DOTNET_GCHeapHardLimitPercent at 60%, so the
        // GC reports a 2,458 MiB budget. Sized from that figure the memtable budget was 61 MiB, below
        // the Raft log's 192 MiB flush unit, and every Raft-log flush was forced by the shared
        // WriteBufferManager. The native pair now comes from the container and the managed actor
        // caches from the heap.
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-heap-limit");
        options.KeyValueWorkers = 32;

        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            options, new KahunaOptionsConfig(), machineMemoryBytes: 4096 * OneMb, managedHeapBytes: 2458 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(options.RocksDbSharedMemoryBudgetMb, Is.EqualTo(409));   // 10% of the container
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(192)); // the flush unit, not 102 or 61
        Assert.That(options.MaxBytesPerActor, Is.EqualTo(2458 * OneMb / 16 / 32)); // 6.25% of the HEAP, split

        // The exact comparison Kommander's RocksDbWAL makes at open; below it, it logs the warning.
        Assert.That((long)options.RocksDbSharedMemtableBudgetMb * OneMb,
            Is.GreaterThanOrEqualTo(EmbeddedKahunaOptionsBuilder.RaftLogFlushUnitHeadroomBytes(options)));
    }

    [Test]
    public void ShippedDefaultsApplyTheFlushUnitFloor()
    {
        // The floor ships ON (EmbeddedKahunaOptionsBuilder.RaftWalFlushUnitFloorEnabled) since Kommander
        // 1.6.6 bounds the Raft-log write-ahead files. It was gated off on Kommander 1.6.5, where an
        // unstarved budget let the Raft-log RocksDB pin every .log file for the life of the process
        // (443 -> 2,709 MiB in ten minutes). With the gate on, the reference node's memtable budget is
        // the flush unit (192) rather than the proportional quarter (102), and a heap-limit sized input
        // yields half its 245 MiB cache (122) rather than 61 — Kommander's open-time warning is gone.
        Assert.That(EmbeddedKahunaOptionsBuilder.RaftWalFlushUnitFloorEnabled, Is.True);

        EmbeddedKahunaOptions reference = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-shipped-ref");
        reference.KeyValueWorkers = 32;
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            reference, new KahunaOptionsConfig(), machineMemoryBytes: 4096 * OneMb, managedHeapBytes: 2458 * OneMb);
        Assert.That(reference.RocksDbSharedMemoryBudgetMb, Is.EqualTo(409));
        Assert.That(reference.RocksDbSharedMemtableBudgetMb, Is.EqualTo(192));

        // The gate can still be turned off per call (the test seam), which restores the quarter.
        EmbeddedKahunaOptions gatedOff = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-shipped-off");
        gatedOff.KeyValueWorkers = 32;
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            gatedOff, new KahunaOptionsConfig(), machineMemoryBytes: 4096 * OneMb, managedHeapBytes: 2458 * OneMb, raftLogFlushUnitFloor: false);
        Assert.That(gatedOff.RocksDbSharedMemtableBudgetMb, Is.EqualTo(102));

        EmbeddedKahunaOptions heapOnly = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-shipped-heap");
        heapOnly.KeyValueWorkers = 32;
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(heapOnly, new KahunaOptionsConfig(), 2458 * OneMb);
        Assert.That(heapOnly.RocksDbSharedMemtableBudgetMb, Is.EqualTo(122));

        Assert.That(EmbeddedKahunaOptionsBuilder.RaftLogFlushUnitFloorMb(reference), Is.EqualTo(192));
    }

    [Test]
    public void AHeapSizedInputNoLongerProducesA61MiBMemtable()
    {
        // Even when the only size known is the 2,458 MiB heap figure (the GC fallback on a platform
        // with no readable machine size), the flush-unit floor lifts the memtable budget to half the
        // 245 MiB total instead of leaving it at a quarter.
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-heap-only");
        options.KeyValueWorkers = 32;

        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 2458 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(options.RocksDbSharedMemoryBudgetMb, Is.EqualTo(245));
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(122));
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.Not.EqualTo(61));
    }

    [Test]
    public void FlushUnitFloorYieldsToHalfTheTotalOnASmallNode()
    {
        // 1536 MiB: a 153 MiB total cannot hold the 192 MiB unit. The floor stops at half the total so
        // the block cache keeps a read share under write load; this node keeps a budget-forced flush
        // cadence, which the docs state.
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-small-floor");
        options.KeyValueWorkers = 32;

        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 1536 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(options.RocksDbSharedMemoryBudgetMb, Is.EqualTo(153));
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(76));
        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.LessThanOrEqualTo(options.RocksDbSharedMemoryBudgetMb));
    }

    [Test]
    public void FlushUnitFloorFollowsTheEffectiveShardTuning()
    {
        // The floor is Kommander's own formula over the tuning the WAL will run: write_buffer_size x
        // (merge + 1). A narrower unit lowers it; a wider one is still capped at half the total.
        EmbeddedKahunaOptions narrow = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-floor-narrow");
        narrow.RaftWalShardWriteBufferSizeMb = 32;
        narrow.RaftWalShardMinWriteBufferNumberToMerge = 3;
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(narrow, new KahunaOptionsConfig(), 4096 * OneMb, raftLogFlushUnitFloor: true);
        Assert.That(narrow.RocksDbSharedMemtableBudgetMb, Is.EqualTo(128)); // 32 x 4

        EmbeddedKahunaOptions wide = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-floor-wide");
        wide.RaftWalShardWriteBufferSizeMb = 128;
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(wide, new KahunaOptionsConfig(), 4096 * OneMb, raftLogFlushUnitFloor: true);
        Assert.That(wide.RocksDbSharedMemtableBudgetMb, Is.EqualTo(204));   // 384 wanted, half of 409 allowed

        // Where the proportional share already covers the unit, the floor is inert.
        EmbeddedKahunaOptions large = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-floor-large");
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(large, new KahunaOptionsConfig(), 16L * 1024 * OneMb, raftLogFlushUnitFloor: true);
        Assert.That(large.RocksDbSharedMemtableBudgetMb, Is.EqualTo(409));  // a quarter of 1638
    }

    [Test]
    public void FlushUnitFloorAppliesOnlyToASharedWriteBufferManager()
    {
        // With the WAL on SQLite, or with sharing off, Kahuna builds no shared bundle and the memtable
        // budget is ignored, so there is nothing to floor: the quarter-of-the-cache default stands.
        EmbeddedKahunaOptions sqlite = EmbeddedKahunaOptionsBuilder.StandaloneBaseline("/tmp/mem-floor-sqlite");
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(sqlite, new KahunaOptionsConfig(), 4096 * OneMb, raftLogFlushUnitFloor: true);
        Assert.That(sqlite.RocksDbSharedMemtableBudgetMb, Is.EqualTo(102));

        EmbeddedKahunaOptions unshared = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-floor-unshared");
        unshared.RocksDbSharedMemoryEnabled = false;
        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(unshared, new KahunaOptionsConfig(), 4096 * OneMb, raftLogFlushUnitFloor: true);
        Assert.That(unshared.RocksDbSharedMemtableBudgetMb, Is.EqualTo(102));
    }

    [Test]
    public void ExplicitMemtableBudgetBeatsTheFlushUnitFloor()
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.StandaloneRocksDbBaseline("/tmp/mem-floor-explicit");
        options.RocksDbSharedMemtableBudgetMb = 64;

        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(
            options, new KahunaOptionsConfig { RocksdbSharedMemtableBudgetMb = 64 }, 4096 * OneMb, raftLogFlushUnitFloor: true);

        Assert.That(options.RocksDbSharedMemtableBudgetMb, Is.EqualTo(64));
    }

    [Test]
    [NonParallelizable]
    public void ClusterBuildOnThisMachineCoversTheFlushUnitWhenTheTotalAllows()
    {
        // End to end through the real memory probe with the floor gated on: the built pair is
        // consistent, and wherever half the total can hold the flush unit, the memtable budget holds
        // it. The gate is a process-wide static (shipped on), hence NonParallelizable and restored in finally.
        EmbeddedKahunaOptionsBuilder.RaftWalFlushUnitFloorEnabled = true;
        try
        {
            EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(
                new ConfigDefinition { DataDir = "/tmp/mem-floor-cluster" }, CamusDBOptions.Default);

            long unit = EmbeddedKahunaOptionsBuilder.RaftLogFlushUnitHeadroomBytes(built);
            Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.LessThanOrEqualTo(built.RocksDbSharedMemoryBudgetMb));

            if (built.RocksDbSharedMemoryBudgetMb / 2 * OneMb >= unit)
                Assert.That((long)built.RocksDbSharedMemtableBudgetMb * OneMb, Is.GreaterThanOrEqualTo(unit));
        }
        finally
        {
            EmbeddedKahunaOptionsBuilder.RaftWalFlushUnitFloorEnabled = true;
        }
    }

    [Test]
    public void DevProfileKeepsItsFixedMemtableBudget()
    {
        // The dev profile promises a small fixed footprint on any machine, so the flush-unit floor does
        // not apply to it; its Raft-log flushes are budget-forced by design.
        EmbeddedKahunaOptions dev = EmbeddedKahunaOptionsBuilder.BuildCluster(
            new ConfigDefinition { DataDir = "/tmp/mem-floor-dev" }, CamusDBOptions.Default with { MemoryProfile = MemoryProfile.Dev });

        Assert.That(dev.RocksDbSharedMemtableBudgetMb, Is.EqualTo(16));
    }

    [Test]
    public void ActorSplitAnticipatesKahunasWorkerDefault()
    {
        // key_value_workers unset: Kahuna fills its 32+ default only after these options reach it.
        // The split must divide by that coming count — dividing by max(1, unset = 0) hands the whole
        // layer to every actor, multiplying the layer by the actor count at runtime.
        EmbeddedKahunaOptions options = new();
        int actors = EmbeddedKahunaOptionsBuilder.EffectiveActorCount(options);
        Assert.That(actors, Is.GreaterThanOrEqualTo(32));

        EmbeddedKahunaOptionsBuilder.ApplyMemoryProportionalDefaults(options, new KahunaOptionsConfig(), 8L * 1024 * OneMb);

        // 512 MiB layer split across the actors Kahuna will run; the aggregate stays near the layer
        // (per-actor floor rounding at most), never layer x actors.
        Assert.That(options.MaxBytesPerActor, Is.EqualTo(Math.Max(512 * OneMb / actors, OneMb)));
        Assert.That(options.MaxBytesPerActor * actors, Is.LessThanOrEqualTo(512 * OneMb + actors * OneMb));
    }

    [Test]
    public void DevProfileIsSmallerThanProd()
    {
        EmbeddedKahunaOptions prod = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/mem-profile-prod", new KahunaOptionsConfig(), CamusDBOptions.Default);

        EmbeddedKahunaOptions dev = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/mem-profile-dev-cmp", new KahunaOptionsConfig(), CamusDBOptions.Default with { MemoryProfile = MemoryProfile.Dev });

        Assert.That(dev.RocksDbSharedMemoryBudgetMb, Is.LessThan(prod.RocksDbSharedMemoryBudgetMb));
        Assert.That(dev.RocksDbSharedMemtableBudgetMb, Is.LessThan(prod.RocksDbSharedMemtableBudgetMb));
        Assert.That(dev.MaxBytesPerActor, Is.LessThan(prod.MaxBytesPerActor));
        Assert.That(dev.MaxEntriesPerActor, Is.LessThan(prod.MaxEntriesPerActor));

        // The pair the shared-bundle builder validates must stay consistent, or the node fails to boot
        // on the profile that is meant to be the easy one to run.
        Assert.That(dev.RocksDbSharedMemtableBudgetMb, Is.LessThanOrEqualTo(dev.RocksDbSharedMemoryBudgetMb));
    }

    [Test]
    public void ExplicitBudgetsBeatTheDevProfile()
    {
        KahunaOptionsConfig kahuna = new()
        {
            RocksdbSharedMemoryBudgetMb = 512,
            RocksdbSharedMemtableBudgetMb = 200,
            MaxBytesPerActor = 256 * OneMb,
            MaxEntriesPerActor = 50_000,
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/mem-profile-explicit", kahuna, CamusDBOptions.Default with { MemoryProfile = MemoryProfile.Dev });

        Assert.That(built.RocksDbSharedMemoryBudgetMb, Is.EqualTo(512));
        Assert.That(built.RocksDbSharedMemtableBudgetMb, Is.EqualTo(200));
        Assert.That(built.MaxBytesPerActor, Is.EqualTo(256 * OneMb));
        Assert.That(built.MaxEntriesPerActor, Is.EqualTo(50_000));
    }

    [Test]
    public void DevProfileWithOnlyAMemtableOverride_IsRejectedNotCrashedAtBoot()
    {
        // The memtable sub-budget is charged inside the total. Overriding only the memtable leaves the
        // total at the profile's 64 MiB, and an inverted pair makes Kahuna's shared-bundle builder throw
        // a raw ArgumentOutOfRangeException while the node is starting. The effective pair is checked
        // after the profile has been applied so this surfaces as a config error instead.
        KahunaOptionsConfig kahuna = new() { RocksdbSharedMemtableBudgetMb = 128 };

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
                "/tmp/mem-profile-inverted", kahuna, CamusDBOptions.Default with { MemoryProfile = MemoryProfile.Dev }))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("rocksdb_shared_memtable_budget_mb"));
    }

    [Test]
    public void ProfileIsReportedAsAVariable()
    {
        CamusDBOptions options = Resolve("memory_profile: dev");

        ConfigVariable variable = ConfigVariableCatalog.Describe(options).Single(v => v.Name == "memory_profile");

        // Rendered the way config.yml spells it, so a value read out of SHOW VARIABLES can be written
        // straight back into a file.
        Assert.That(variable.Value, Is.EqualTo("dev"));
        Assert.That(variable.Default, Is.EqualTo("prod"));
        Assert.That(variable.Mutability, Is.EqualTo(ConfigMutability.Restart));
        Assert.That(variable.Scope, Is.EqualTo(ConfigScope.Node));
    }
}
