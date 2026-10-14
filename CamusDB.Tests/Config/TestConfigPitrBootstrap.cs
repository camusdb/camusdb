/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// The <c>pitr_bootstrap_from</c> / <c>pitr_bootstrap_target_time_ms</c> keys that seed a joining node
/// from a backup chain: parsed from YAML and from the command line, and refused at config time when
/// the node could never run the bootstrap it asks for (no join, no backup catalog, a malformed id).
/// </summary>
[TestFixture]
public sealed class TestConfigPitrBootstrap
{
    private const string Leaf = "6f1c1b7e-3f53-4c8e-9d58-0d3a2a7f4b11";

    private const string JoiningNode = """
        mode: cluster
        peers: [10.0.0.1:7070, 10.0.0.2:7070]
        join_existing: true
        kahuna:
          backup_dir: /opt/camusdb/backups
        """;

    [Test]
    public void BootstrapKeys_ParsedFromYaml()
    {
        ConfigDefinition config = new ConfigReader().Read(
            JoiningNode + $"\npitr_bootstrap_from: {Leaf}\npitr_bootstrap_target_time_ms: 1750000000000");

        Assert.That(config.PitrBootstrapLeafBackupId, Is.EqualTo(Guid.Parse(Leaf)));
        Assert.That(config.PitrBootstrapTargetTimeMs, Is.EqualTo(1750000000000));
    }

    [Test]
    public void NoBootstrapByDefault()
    {
        ConfigDefinition config = new ConfigReader().Read(JoiningNode);

        Assert.That(config.PitrBootstrapLeafBackupId, Is.Null, "an ordinary join must never be mistaken for a seeded one");
        Assert.That(config.PitrBootstrapTargetTimeMs, Is.EqualTo(0));
    }

    [Test]
    public void CliOverridesReachTheDefinition()
    {
        ConfigDefinition config = new ConfigReader().Read(JoiningNode);

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides
        {
            PitrBootstrapFrom = Leaf,
            PitrBootstrapTargetTimeMs = 42,
        });
        config.Validate();

        Assert.That(config.PitrBootstrapLeafBackupId, Is.EqualTo(Guid.Parse(Leaf)));
        Assert.That(config.PitrBootstrapTargetTimeMs, Is.EqualTo(42));
        Assert.That(config.KeySources["pitr_bootstrap_from"], Is.EqualTo(ConfigValueSource.CommandLine));
        Assert.That(config.KeySources["pitr_bootstrap_target_time_ms"], Is.EqualTo(ConfigValueSource.CommandLine));
    }

    [Test]
    public void RejectedWithoutJoinExisting()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => new ConfigReader().Read($"""
            mode: cluster
            peers: [10.0.0.1:7070]
            pitr_bootstrap_from: {Leaf}
            kahuna:
              backup_dir: /opt/camusdb/backups
            """))!;

        StringAssert.Contains("join_existing", ex.Message);
    }

    [Test]
    public void RejectedWithoutBackupDir()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => new ConfigReader().Read($"""
            mode: cluster
            peers: [10.0.0.1:7070]
            join_existing: true
            pitr_bootstrap_from: {Leaf}
            """))!;

        StringAssert.Contains("kahuna.backup_dir", ex.Message);
    }

    [TestCase("not-a-guid")]
    [TestCase("00000000-0000-0000-0000-000000000000")]
    public void RejectedForAMalformedOrEmptyId(string id)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(JoiningNode + $"\npitr_bootstrap_from: \"{id}\""))!;

        StringAssert.Contains("pitr_bootstrap_from", ex.Message);
    }

    [Test]
    public void TargetTimeRejectedAlone()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(JoiningNode + "\npitr_bootstrap_target_time_ms: 1750000000000"))!;

        StringAssert.Contains("pitr_bootstrap_target_time_ms", ex.Message);
    }

    [Test]
    public void NegativeTargetTimeRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(JoiningNode + $"\npitr_bootstrap_from: {Leaf}\npitr_bootstrap_target_time_ms: -1"))!;

        StringAssert.Contains("pitr_bootstrap_target_time_ms", ex.Message);
    }
}
