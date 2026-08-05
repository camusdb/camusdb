
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
/// <para>Serial: two cases set and restore the <c>CAMUS_KEY_RANGE_SHARDING</c> environment variable to
/// prove it overrides the YAML. Environment variables are process-wide, so a concurrent fixture reading
/// one mid-assertion would see the other's value. Resolution itself is now pure — it returns options
/// rather than writing statics — so nothing else here needs isolating.</para>
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
    public void Resolve_SetsDefaultIsolationLevelFromYaml()
    {

            ConfigDefinition config = new ConfigReader().Read("default_isolation_level: read_committed");
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.DefaultIsolationLevel, Is.EqualTo(CamusIsolationLevel.ReadCommitted));
    }

    [Test]
    public void Resolve_SetsDefaultTransactionLockingFromYaml()
    {

            ConfigDefinition config = new ConfigReader().Read("default_transaction_locking: optimistic");
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.DefaultTransactionLocking, Is.EqualTo(KeyValueTransactionLocking.Optimistic));
    }

    [Test]
    public void Resolve_SetsDefaultTransactionPriorityFromYaml()
    {
        ConfigDefinition config = new ConfigReader().Read("default_transaction_priority: background");
        CamusDBOptions resolved = ConfigResolver.Resolve(config);

        Assert.That(resolved.DefaultTransactionPriority, Is.EqualTo(TransactionPriority.Background));
    }

    [Test]
    public void DefaultTransactionPriority_DefaultsToNormalWhenKeyAbsent()
    {
        ConfigDefinition config = new ConfigReader().Read("default_isolation_level: serializable");
        CamusDBOptions resolved = ConfigResolver.Resolve(config);

        Assert.That(resolved.DefaultTransactionPriority, Is.EqualTo(TransactionPriority.Normal));
    }

    [Test]
    public void DefaultTransactionLocking_DefaultsToPessimisticWhenKeyAbsent()
    {
        // No default_transaction_locking key present → the ConfigDefinition default resolves to Pessimistic.
        ConfigDefinition config = new ConfigReader().Read("default_isolation_level: serializable");
        Assert.That(config.ParseDefaultTransactionLocking(), Is.EqualTo(KeyValueTransactionLocking.Pessimistic));
    }

    [Test]
    public void Resolve_KeyRangeShardingFromYaml()
    {
        string? prevEnv = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");
        try
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", null);

            ConfigDefinition config = new ConfigReader().Read("key_range_sharding: true");
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.KeyRangeShardingEnabled, Is.True);
        }
        finally
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", prevEnv);
        }
    }

    [Test]
    public void Resolve_EnvVarOverridesYamlKeyRangeSharding()
    {
        string? prevEnv = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");
        try
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", "1");

            ConfigDefinition config = new ConfigReader().Read("key_range_sharding: false");
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.KeyRangeShardingEnabled, Is.True);
        }
        finally
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", prevEnv);
        }
    }

    [Test]
    public void Resolve_RegexKnobsRoundTrip()
    {

            string yml =
                "regex_match_timeout_ms: 500\n" +
                "regex_cache_max_entries: 64";

            ConfigDefinition config = new ConfigReader().Read(yml);
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.RegexMatchTimeoutMs, Is.EqualTo(500));
            Assert.That(resolved.RegexCacheMaxEntries, Is.EqualTo(64));
    }

    [Test]
    public void RegexMatchTimeout_NonPositive_Rejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            new ConfigReader().Read("regex_match_timeout_ms: 0"))!;
        Assert.That(ex.Message, Does.Contain("regex_match_timeout_ms"));
    }

    [Test]
    public void Resolve_LockKnobsRoundTrip()
    {

            // range_lock_expires_ms must clear the renewal-margin cross-check (>= 2x the 60 s default
            // collection interval), so use a valid value that still exercises the round-trip.
            string yml =
                "lock_wait_deadline_ms: 250\n" +
                "lock_escalation_threshold: 12\n" +
                "range_lock_expires_ms: 150000";

            ConfigDefinition config = new ConfigReader().Read(yml);
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.LockWaitDeadlineMs, Is.EqualTo(250));
            Assert.That(resolved.LockEscalationThreshold, Is.EqualTo(12));
            Assert.That(resolved.RangeLockExpiresMs, Is.EqualTo(150_000));
    }

    [Test]
    [NonParallelizable]
    public async Task ReadCommittedYamlDefault_MakesNewTransactionsReadCommitted()
    {
        ConfigDefinition config = new ConfigReader().Read("default_isolation_level: read_committed");
        CamusDBOptions resolved = ConfigResolver.Resolve(config);

        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await using (node)
        {
            // The manager is built from the resolved options — that is the property under test: what
            // the YAML says must reach a transaction begun with no explicit level.
            KvTransactionsManager mgr = new(node.Kahuna, resolved);
            KvTransaction tx = await mgr.BeginAsync();
            Assert.That(tx.IsolationLevel, Is.EqualTo(CamusIsolationLevel.ReadCommitted));
            await mgr.RollbackAsync(tx);
        }
    }
}
