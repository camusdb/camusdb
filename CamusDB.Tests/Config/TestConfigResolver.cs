
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Config;

[TestFixture]
public sealed class TestConfigResolver
{
    [Test]
    public void YamlOnlyValueUsedWhenNoCliOverride()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: cluster\nhttp_port: 6001");

        Assert.That(config.Mode, Is.EqualTo("cluster"));
        Assert.That(config.HttpPort, Is.EqualTo(6001));
    }

    [Test]
    public void CliOverridesYaml_IncludingClusterToStandalone()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: cluster\nhttp_port: 6001");

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides
        {
            Mode = "standalone",
            HttpPort = 5095,
        });

        Assert.That(config.Mode, Is.EqualTo("standalone"));
        Assert.That(config.HttpPort, Is.EqualTo(5095));
        Assert.That(config.IsClusterMode, Is.False);
    }

    [Test]
    public void CliDoesNotOverrideYamlWhenFlagOmitted()
    {
        ConfigDefinition config = new ConfigReader().Read("raft_host: db-host\nraft_port: 8080");

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides
        {
            Mode = "standalone",
        });

        Assert.That(config.RaftHost, Is.EqualTo("db-host"));
        Assert.That(config.RaftPort, Is.EqualTo(8080));
    }

    [Test]
    public void ApplyToCamusDBConfig_SetsDefaultIsolationLevelFromYaml()
    {
        bool prevSharding = CamusDBConfig.KeyRangeShardingEnabled;
        int prevDeadline = CamusDBConfig.LockWaitDeadlineMs;
        CamusIsolationLevel prevIso = CamusDBConfig.DefaultIsolationLevel;

        try
        {
            ConfigDefinition config = new ConfigReader().Read("default_isolation_level: read_committed");
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.DefaultIsolationLevel, Is.EqualTo(CamusIsolationLevel.ReadCommitted));
        }
        finally
        {
            CamusDBConfig.KeyRangeShardingEnabled = prevSharding;
            CamusDBConfig.LockWaitDeadlineMs = prevDeadline;
            CamusDBConfig.DefaultIsolationLevel = prevIso;
        }
    }

    [Test]
    public void ApplyToCamusDBConfig_KeyRangeShardingFromYaml()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        string? prevEnv = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");

        try
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", null);

            ConfigDefinition config = new ConfigReader().Read("key_range_sharding: true");
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.KeyRangeShardingEnabled, Is.True);
        }
        finally
        {
            CamusDBConfig.KeyRangeShardingEnabled = prev;
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", prevEnv);
        }
    }

    [Test]
    public void ApplyToCamusDBConfig_EnvVarOverridesYamlKeyRangeSharding()
    {
        bool prev = CamusDBConfig.KeyRangeShardingEnabled;
        string? prevEnv = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");

        try
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", "1");

            ConfigDefinition config = new ConfigReader().Read("key_range_sharding: false");
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.KeyRangeShardingEnabled, Is.True);
        }
        finally
        {
            CamusDBConfig.KeyRangeShardingEnabled = prev;
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", prevEnv);
        }
    }

    [Test]
    public void ApplyToCamusDBConfig_LockKnobsRoundTrip()
    {
        int prevDeadline = CamusDBConfig.LockWaitDeadlineMs;
        int prevEscalation = CamusDBConfig.LockEscalationThreshold;

        try
        {
            string yml =
                "lock_wait_deadline_ms: 250\n" +
                "lock_escalation_threshold: 12\n" +
                "range_lock_expires_ms: 15000\n" +
                "range_lock_heartbeat_interval_ms: 4000";

            ConfigDefinition config = new ConfigReader().Read(yml);
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.LockWaitDeadlineMs, Is.EqualTo(250));
            Assert.That(CamusDBConfig.LockEscalationThreshold, Is.EqualTo(12));
            Assert.That(CamusDBConfig.RangeLockExpiresMs, Is.EqualTo(15_000));
            Assert.That(CamusDBConfig.RangeLockHeartbeatIntervalMs, Is.EqualTo(4000));
        }
        finally
        {
            CamusDBConfig.LockWaitDeadlineMs = prevDeadline;
            CamusDBConfig.LockEscalationThreshold = prevEscalation;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task ReadCommittedYamlDefault_MakesNewTransactionsReadCommitted()
    {
        CamusIsolationLevel saved = CamusDBConfig.DefaultIsolationLevel;

        try
        {
            ConfigDefinition config = new ConfigReader().Read("default_isolation_level: read_committed");
            ConfigResolver.ApplyToCamusDBConfig(config);

            EmbeddedKahuna node = new();
            await node.StartAsync(CancellationToken.None);
            await using (node)
            {
                KvTransactionsManager mgr = new(node.Kahuna);
                KvTransaction tx = await mgr.BeginAsync();
                Assert.That(tx.IsolationLevel, Is.EqualTo(CamusIsolationLevel.ReadCommitted));
                await mgr.RollbackAsync(tx);
            }
        }
        finally
        {
            CamusDBConfig.DefaultIsolationLevel = saved;
        }
    }
}
