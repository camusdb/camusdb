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

        // The actor caches are budgeted as a layer and split, so the total is what the profile promises
        // however many cores the machine running this has.
        Assert.That(dev.MaxBytesPerActor * Math.Max(1, dev.KeyValueWorkers),
            Is.LessThanOrEqualTo(Math.Max(32 * OneMb, dev.KeyValueWorkers * OneMb)));
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
