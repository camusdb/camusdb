
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

        if (cli.RequireTlsWhenAuthEnabled is bool requireTls)
            config.RequireTlsWhenAuthEnabled = requireTls;
    }

    /// <summary>
    /// Applies resolved config to process-wide static knobs. Call once at startup before the engine runs.
    /// </summary>
    public static void ApplyToCamusDBConfig(ConfigDefinition config)
    {
        if (!string.IsNullOrEmpty(config.DataDir))
            CamusDBConfig.DataDirectory = config.DataDir;

        CamusDBConfig.StatsFlushIntervalMs = config.StatsFlushIntervalMs;
        CamusDBConfig.StatsAnalyzeSampleRows = config.StatsAnalyzeSampleRows;
        CamusDBConfig.StatsHistogramBuckets = config.StatsHistogramBuckets;

        CamusDBConfig.AutoAnalyzeEnabled = config.AutoAnalyzeEnabled;
        CamusDBConfig.AutoAnalyzeCheckIntervalMs = config.AutoAnalyzeCheckIntervalMs;
        CamusDBConfig.AutoAnalyzeFractionStaleRows = config.AutoAnalyzeFractionStaleRows;
        CamusDBConfig.AutoAnalyzeMinStaleRows = config.AutoAnalyzeMinStaleRows;
        CamusDBConfig.AutoAnalyzeMaxConcurrent = config.AutoAnalyzeMaxConcurrent;
        CamusDBConfig.AutoAnalyzeMaxRowsPerSecond = config.AutoAnalyzeMaxRowsPerSecond;
        CamusDBConfig.AutoAnalyzeHistogramSampleRows = config.AutoAnalyzeHistogramSampleRows;
        CamusDBConfig.AutoAnalyzeHllPrecision = config.AutoAnalyzeHllPrecision;
        CamusDBConfig.AutoAnalyzeLoadPauseThreshold = config.AutoAnalyzeLoadPauseThreshold;
        CamusDBConfig.AutoAnalyzeOwnershipCheckRows = config.AutoAnalyzeOwnershipCheckRows;

        CamusDBConfig.SqlParserCacheTtlSeconds = config.SqlParserCacheTtlSeconds;
        CamusDBConfig.SqlParserCacheMaxEntries = config.SqlParserCacheMaxEntries;
        CamusDBConfig.SqlParserCacheSweepSeconds = config.SqlParserCacheSweepSeconds;

        CamusDBConfig.OrphanRetentionMs = config.OrphanRetentionMs;
        CamusDBConfig.OrphanReclaimIntervalMs = config.OrphanReclaimIntervalMs;

        CamusDBConfig.DefaultIsolationLevel = config.ParseDefaultIsolationLevel();
        CamusDBConfig.DefaultTransactionLocking = config.ParseDefaultTransactionLocking();
        CamusDBConfig.RangeLockExpiresMs = config.RangeLockExpiresMs;
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = config.MaxSerializableTransactionLifetimeMs;
        CamusDBConfig.TransactionIdleTimeoutMs = config.TransactionIdleTimeoutMs;
        CamusDBConfig.TransactionReaperIntervalMs = config.TransactionReaperIntervalMs;
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
        CamusDBConfig.ClusterPartitionCount = config.InitialPartitions;
        CamusDBConfig.GrpcBatchMaxInFlight = config.GrpcBatchMaxInFlight;
        CamusDBConfig.CostBasedAccessPathEnabled = config.CostBasedAccessPathEnabled;
        CamusDBConfig.CostBasedJoinOrderEnabled = config.CostBasedJoinOrderEnabled;
        CamusDBConfig.PlanCacheEnabled = config.PlanCacheEnabled;
        CamusDBConfig.PlanCacheMaxEntries = config.PlanCacheMaxEntries;

        CamusDBConfig.RegexMatchTimeoutMs = config.RegexMatchTimeoutMs;
        CamusDBConfig.RegexCacheMaxEntries = config.RegexCacheMaxEntries;

        CamusDBConfig.SpillEnabled = config.SpillEnabled;
        CamusDBConfig.SpillThresholdRows = config.SpillThresholdRows;
        CamusDBConfig.SpillMergeFanIn = config.SpillMergeFanIn;

        CamusDBConfig.MaxIdentifierLength = config.MaxIdentifierLength;
        CamusDBConfig.MaxColumnsPerTable = config.MaxColumnsPerTable;
        CamusDBConfig.MaxIndexesPerTable = config.MaxIndexesPerTable;
        CamusDBConfig.MaxTablesPerDatabase = config.MaxTablesPerDatabase;
        CamusDBConfig.MaxIndexColumns = config.MaxIndexColumns;
        CamusDBConfig.MaxIndexIncludeTupleBytes = config.MaxIndexIncludeTupleBytes;
        CamusDBConfig.MaxMutationsPerTransaction = config.MaxMutationsPerTransaction;
        CamusDBConfig.BranchSnapshotHoldLeaseMs = config.BranchSnapshotHoldLeaseMs;

        CamusDBConfig.QueryResultCacheEnabled = config.QueryResultCacheEnabled;
        CamusDBConfig.QueryResultCacheDefaultTtlMs = config.QueryResultCacheDefaultTtlMs;
        CamusDBConfig.QueryResultCacheMaxEntries = config.QueryResultCacheMaxEntries;
        CamusDBConfig.QueryResultCacheMaxBytes = config.QueryResultCacheMaxBytes;
        CamusDBConfig.QueryResultCacheMaxEntryBytes = config.QueryResultCacheMaxEntryBytes;
        CamusDBConfig.QueryResultCacheMaxEntryRows = config.QueryResultCacheMaxEntryRows;
        CamusDBConfig.QueryResultCacheMaxDeps = config.QueryResultCacheMaxDeps;
        CamusDBConfig.QueryResultCacheMaxPointDeps = config.QueryResultCacheMaxPointDeps;
        CamusDBConfig.QueryResultCacheMaxRanges = config.QueryResultCacheMaxRanges;
        CamusDBConfig.QueryResultCacheSingleFlightWaitMs = config.QueryResultCacheSingleflightWaitMs;
        CamusDBConfig.QueryResultCacheStrictValidationMaxKeys = config.QueryResultCacheStrictValidationMaxKeys;
        CamusDBConfig.QueryResultCacheSweepIntervalMs = config.QueryResultCacheSweepIntervalMs;

        // Transport-security policy for authenticated requests. Applied unconditionally: authentication
        // itself is switched on later from the environment, so this must already hold the operator's
        // choice by the time the first request is gated.
        CamusDBConfig.RequireTlsWhenAuthEnabled = config.RequireTlsWhenAuthEnabled;

        CamusDBConfig.Kahuna = config.Kahuna;
    }
}
