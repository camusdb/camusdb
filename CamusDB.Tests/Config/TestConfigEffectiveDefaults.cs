
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;

using NUnit.Framework;

using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers the settings whose default depends on the rest of the configuration. The distinction under
/// test throughout is "the operator set this" versus "nobody said anything" — reading the value alone
/// cannot tell them apart, so a default applied by value comparison would overwrite an explicit
/// choice that happens to equal the default.
/// </summary>
[NonParallelizable]
public class TestConfigEffectiveDefaults
{
    private string? savedCamusHome;

    [SetUp]
    public void SetUp()
    {
        savedCamusHome = Environment.GetEnvironmentVariable("CAMUS_HOME");
        Environment.SetEnvironmentVariable("CAMUS_HOME", Path.Combine(Path.GetTempPath(), "camusdb-defaults-test"));
    }

    [TearDown]
    public void TearDown() => Environment.SetEnvironmentVariable("CAMUS_HOME", savedCamusHome);

    [Test]
    public void TestUnsetDataDirFallsBackToUserDataLocation()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual(ConfigLocator.DefaultDataDirectory(), config.DataDir);
    }

    [Test]
    public void TestExplicitDataDirIsPreserved()
    {
        ConfigDefinition config = new ConfigReader().Read("data_dir: /var/lib/camusdb");

        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual("/var/lib/camusdb", config.DataDir);
    }

    [Test]
    public void TestDataDirFromCommandLineIsPreserved()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides { DataDir = "/data" });
        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual("/data", config.DataDir);
    }

    /// <summary>
    /// A standalone node has one disk and therefore one fsync target, so a single partition keeps
    /// every transaction single-participant and on the one-phase commit path. Defaulting a
    /// zero-configuration node to the cluster-shaped value would cost several times the write
    /// throughput for no benefit.
    /// </summary>
    [Test]
    public void TestStandaloneDefaultsToOnePartition()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual(1, config.InitialPartitions);
    }

    [Test]
    public void TestClusterDefaultsToThreePartitions()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: cluster");

        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual(3, config.InitialPartitions);
    }

    [Test]
    public void TestExplicitPartitionCountIsPreservedOnStandalone()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone\ninitial_partitions: 4");

        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual(4, config.InitialPartitions);
    }

    /// <summary>
    /// The container image passes its partition count as a flag rather than in YAML, so a
    /// command-line value must count as explicit exactly as a YAML key does.
    /// </summary>
    [Test]
    public void TestPartitionCountFromCommandLineIsPreserved()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides { InitialPartitions = 4 });
        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual(4, config.InitialPartitions);
    }

    /// <summary>
    /// An explicit value that coincides with the default must survive: this is the case a
    /// value-comparison default gets wrong, and the reason provided keys are tracked at all.
    /// </summary>
    [Test]
    public void TestExplicitValueEqualToTheDefaultIsStillTreatedAsProvided()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: cluster\ninitial_partitions: 3");

        Assert.IsTrue(config.ProvidedKeys.Contains("initial_partitions"));

        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.AreEqual(3, config.InitialPartitions);
    }
}
