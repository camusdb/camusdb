
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Config;

/// <summary>
/// Merges YAML, environment, and CLI into one resolved <see cref="ConfigDefinition"/> and applies
/// process-wide <see cref="CamusDBConfig"/> knobs. Precedence: CLI &gt; env &gt; YAML &gt; built-in default.
/// </summary>
public static class ConfigResolver
{
    /// <summary>
    /// Applies nullable CLI overrides on top of an already-validated YAML config.
    /// </summary>
    public static void ApplyCliOverrides(ConfigDefinition config, ConfigCliOverrides? cli)
    {
        if (cli is null)
            return;

        if (cli.Mode is not null)
            config.Mode = cli.Mode;

        if (cli.DataDir is not null)
            config.DataDir = cli.DataDir;

        if (cli.NodeName is not null)
            config.NodeName = cli.NodeName;

        if (cli.RaftNodeId is int raftNodeId)
            config.RaftNodeId = raftNodeId;

        if (cli.RaftHost is not null)
            config.RaftHost = cli.RaftHost;

        if (cli.RaftPort is int raftPort)
            config.RaftPort = raftPort;

        if (cli.InitialPartitions is int initialPartitions)
            config.InitialPartitions = initialPartitions;

        if (cli.Peers is { Count: > 0 })
            config.Peers = [.. cli.Peers];

        if (cli.HttpPeers is { Count: > 0 })
            config.HttpPeers = [.. cli.HttpPeers];

        if (cli.SchemaAckWaitTimeoutMs is int schemaAckWait)
            config.SchemaAckWaitTimeoutMs = schemaAckWait;

        if (cli.SchemaAckLiveNodeLeaseMs is int schemaAckLease)
            config.SchemaAckLiveNodeLeaseMs = schemaAckLease;

        if (cli.HttpPort is int httpPort)
            config.HttpPort = httpPort;

        if (cli.HttpsPort is int httpsPort)
            config.HttpsPort = httpsPort;

        if (cli.HttpsCertificate is not null)
            config.HttpsCertificate = cli.HttpsCertificate;

        if (cli.RaftCertificate is not null)
            config.RaftCertificate = cli.RaftCertificate;
    }

    /// <summary>
    /// Applies resolved config to process-wide static knobs. Call once at startup before the engine runs.
    /// </summary>
    public static void ApplyToCamusDBConfig(ConfigDefinition config)
    {
        if (!string.IsNullOrEmpty(config.DataDir))
            CamusDBConfig.DataDirectory = config.DataDir;

        CamusDBConfig.StatsFlushIntervalMs = config.StatsFlushIntervalMs;
        CamusDBConfig.SqlParserCacheTtlSeconds = config.SqlParserCacheTtlSeconds;
        CamusDBConfig.SqlParserCacheMaxEntries = config.SqlParserCacheMaxEntries;
        CamusDBConfig.SqlParserCacheSweepSeconds = config.SqlParserCacheSweepSeconds;

        CamusDBConfig.DefaultIsolationLevel = config.ParseDefaultIsolationLevel();
        CamusDBConfig.RangeLockExpiresMs = config.RangeLockExpiresMs;
        CamusDBConfig.RangeLockHeartbeatIntervalMs = config.RangeLockHeartbeatIntervalMs;
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = config.MaxSerializableTransactionLifetimeMs;
        CamusDBConfig.LockEscalationThreshold = config.LockEscalationThreshold;
        CamusDBConfig.LockWaitDeadlineMs = config.LockWaitDeadlineMs;

        bool keyRangeSharding = config.KeyRangeSharding;
        string? envSharding = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");
        if (!string.IsNullOrEmpty(envSharding))
        {
            keyRangeSharding =
                string.Equals(envSharding, "1", StringComparison.Ordinal) ||
                string.Equals(envSharding, "true", StringComparison.OrdinalIgnoreCase);
        }

        CamusDBConfig.KeyRangeShardingEnabled = keyRangeSharding;
        CamusDBConfig.Kahuna = config.Kahuna;
    }
}
