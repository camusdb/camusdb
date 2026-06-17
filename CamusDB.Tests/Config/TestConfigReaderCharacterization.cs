
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
    public void RejectsHeartbeatNotLessThanExpires()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(
                "range_lock_expires_ms: 5000\nrange_lock_heartbeat_interval_ms: 5000"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("range_lock_heartbeat_interval_ms"));
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
}
