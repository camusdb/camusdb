
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

using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.Config;

/// <summary>
/// Merge-precedence tests for YAML / CLI resolution and the process-wide knobs it applies.
///
/// <para>Non-parallelizable: applying a config rewrites <b>every</b> static from that one definition,
/// not just the keys the YAML mentions, so two fixtures applying configs at once clobber each other's
/// values mid-assertion.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
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
    public void ApplyToCamusDBConfig_SetsDefaultTransactionLockingFromYaml()
    {
        KeyValueTransactionLocking prevLocking = CamusDBConfig.DefaultTransactionLocking;

        try
        {
            ConfigDefinition config = new ConfigReader().Read("default_transaction_locking: optimistic");
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.DefaultTransactionLocking, Is.EqualTo(KeyValueTransactionLocking.Optimistic));
        }
        finally
        {
            CamusDBConfig.DefaultTransactionLocking = prevLocking;
        }
    }

    [Test]
    public void DefaultTransactionLocking_DefaultsToPessimisticWhenKeyAbsent()
    {
        // No default_transaction_locking key present → the ConfigDefinition default resolves to Pessimistic.
        ConfigDefinition config = new ConfigReader().Read("default_isolation_level: serializable");
        Assert.That(config.ParseDefaultTransactionLocking(), Is.EqualTo(KeyValueTransactionLocking.Pessimistic));
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
    public void ApplyToCamusDBConfig_RegexKnobsRoundTrip()
    {
        int prevTimeout = CamusDBConfig.RegexMatchTimeoutMs;
        int prevCache = CamusDBConfig.RegexCacheMaxEntries;

        try
        {
            string yml =
                "regex_match_timeout_ms: 500\n" +
                "regex_cache_max_entries: 64";

            ConfigDefinition config = new ConfigReader().Read(yml);
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.RegexMatchTimeoutMs, Is.EqualTo(500));
            Assert.That(CamusDBConfig.RegexCacheMaxEntries, Is.EqualTo(64));
        }
        finally
        {
            CamusDBConfig.RegexMatchTimeoutMs = prevTimeout;
            CamusDBConfig.RegexCacheMaxEntries = prevCache;
        }
    }

    [Test]
    public void RegexMatchTimeout_NonPositive_Rejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            new ConfigReader().Read("regex_match_timeout_ms: 0"))!;
        Assert.That(ex.Message, Does.Contain("regex_match_timeout_ms"));
    }

    [Test]
    public void ApplyToCamusDBConfig_LockKnobsRoundTrip()
    {
        int prevDeadline = CamusDBConfig.LockWaitDeadlineMs;
        int prevEscalation = CamusDBConfig.LockEscalationThreshold;

        try
        {
            // range_lock_expires_ms must clear the renewal-margin cross-check (>= 2x the 60 s default
            // collection interval), so use a valid value that still exercises the round-trip.
            string yml =
                "lock_wait_deadline_ms: 250\n" +
                "lock_escalation_threshold: 12\n" +
                "range_lock_expires_ms: 150000";

            ConfigDefinition config = new ConfigReader().Read(yml);
            ConfigResolver.ApplyToCamusDBConfig(config);

            Assert.That(CamusDBConfig.LockWaitDeadlineMs, Is.EqualTo(250));
            Assert.That(CamusDBConfig.LockEscalationThreshold, Is.EqualTo(12));
            Assert.That(CamusDBConfig.RangeLockExpiresMs, Is.EqualTo(150_000));
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
