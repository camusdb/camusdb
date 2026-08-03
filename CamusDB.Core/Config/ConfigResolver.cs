
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
    /// Builds the immutable options instance for one engine from resolved configuration. Precedence
    /// (CLI &gt; env &gt; YAML &gt; built-in default) has already been applied to <paramref name="config"/>
    /// by <see cref="ApplyCliOverrides"/> and the reader; this method only maps the result.
    /// </summary>
    public static CamusDBOptions Resolve(ConfigDefinition config)
    {
        // Key-range sharding is the one knob the environment may override after YAML, so it is
        // computed before the initializer rather than mapped straight across.
        bool keyRangeSharding = config.KeyRangeSharding;
        string? envSharding = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");

        if (!string.IsNullOrEmpty(envSharding))
        {
            keyRangeSharding =
                string.Equals(envSharding, "1", StringComparison.Ordinal) ||
                string.Equals(envSharding, "true", StringComparison.OrdinalIgnoreCase);
        }

        return new CamusDBOptions
        {
        // An empty data_dir means "unset": keep the built-in default rather than
        // rooting the database at the current directory.
        DataDirectory = !string.IsNullOrEmpty(config.DataDir) ? config.DataDir : CamusDBOptions.Default.DataDirectory,

        StatsFlushIntervalMs = config.StatsFlushIntervalMs,
        StatsAnalyzeSampleRows = config.StatsAnalyzeSampleRows,
        StatsHistogramBuckets = config.StatsHistogramBuckets,

        AutoAnalyzeEnabled = config.AutoAnalyzeEnabled,
        AutoAnalyzeCheckIntervalMs = config.AutoAnalyzeCheckIntervalMs,
        AutoAnalyzeFractionStaleRows = config.AutoAnalyzeFractionStaleRows,
        AutoAnalyzeMinStaleRows = config.AutoAnalyzeMinStaleRows,
        AutoAnalyzeMaxConcurrent = config.AutoAnalyzeMaxConcurrent,
        AutoAnalyzeMaxRowsPerSecond = config.AutoAnalyzeMaxRowsPerSecond,
        AutoAnalyzeHistogramSampleRows = config.AutoAnalyzeHistogramSampleRows,
        AutoAnalyzeHllPrecision = config.AutoAnalyzeHllPrecision,
        AutoAnalyzeLoadPauseThreshold = config.AutoAnalyzeLoadPauseThreshold,
        AutoAnalyzeOwnershipCheckRows = config.AutoAnalyzeOwnershipCheckRows,

        SqlParserCacheTtlSeconds = config.SqlParserCacheTtlSeconds,
        SqlParserCacheMaxEntries = config.SqlParserCacheMaxEntries,
        SqlParserCacheSweepSeconds = config.SqlParserCacheSweepSeconds,

        OrphanRetentionMs = config.OrphanRetentionMs,
        OrphanReclaimIntervalMs = config.OrphanReclaimIntervalMs,

        EngineMetricsEnabled = config.EngineMetricsEnabled,

        DefaultIsolationLevel = config.ParseDefaultIsolationLevel(),
        DefaultTransactionLocking = config.ParseDefaultTransactionLocking(),
        RangeLockExpiresMs = config.RangeLockExpiresMs,
        MaxSerializableTransactionLifetimeMs = config.MaxSerializableTransactionLifetimeMs,
        TransactionIdleTimeoutMs = config.TransactionIdleTimeoutMs,
        TransactionReaperIntervalMs = config.TransactionReaperIntervalMs,
        PreparedStatementIdleTimeoutMs = config.PreparedStatementIdleTimeoutMs,
        PreparedStatementSweepIntervalMs = config.PreparedStatementSweepIntervalMs,
        GrpcMaxPreparedStatementsPerStream = config.GrpcMaxPreparedStatementsPerStream,
        RestMaxPreparedStatementsPerPrincipal = config.RestMaxPreparedStatementsPerPrincipal,
        RestMaxPreparedStatements = config.RestMaxPreparedStatements,
        MaxPreparedStatementBytes = config.MaxPreparedStatementBytes,
        RestMaxPreparedStatementBytes = config.RestMaxPreparedStatementBytes,
        RestMaxPreparedStatementBytesPerPrincipal = config.RestMaxPreparedStatementBytesPerPrincipal,
        GrpcMaxPreparedStatementBytesPerStream = config.GrpcMaxPreparedStatementBytesPerStream,
        LockEscalationThreshold = config.LockEscalationThreshold,
        LockWaitDeadlineMs = config.LockWaitDeadlineMs,

        KeyRangeShardingEnabled = keyRangeSharding,
        ClusterPartitionCount = config.InitialPartitions,
        GrpcBatchMaxInFlight = config.GrpcBatchMaxInFlight,
        CostBasedAccessPathEnabled = config.CostBasedAccessPathEnabled,
        CostBasedJoinOrderEnabled = config.CostBasedJoinOrderEnabled,
        PlanCacheEnabled = config.PlanCacheEnabled,
        PlanCacheMaxEntries = config.PlanCacheMaxEntries,

        RegexMatchTimeoutMs = config.RegexMatchTimeoutMs,
        RegexCacheMaxEntries = config.RegexCacheMaxEntries,

        SpillEnabled = config.SpillEnabled,
        SpillThresholdRows = config.SpillThresholdRows,
        SpillMergeFanIn = config.SpillMergeFanIn,

        MaxIdentifierLength = config.MaxIdentifierLength,
        MaxColumnsPerTable = config.MaxColumnsPerTable,
        MaxIndexesPerTable = config.MaxIndexesPerTable,
        MaxTablesPerDatabase = config.MaxTablesPerDatabase,
        MaxIndexColumns = config.MaxIndexColumns,
        MaxIndexIncludeTupleBytes = config.MaxIndexIncludeTupleBytes,
        MaxMutationsPerTransaction = config.MaxMutationsPerTransaction,
        BranchSnapshotHoldLeaseMs = config.BranchSnapshotHoldLeaseMs,

            // Mirror the effective Kahuna PITR retention window (seconds) into the process-wide config so
            // the restore window guard can reject a target time older than now - window without re-reading
            // the embedded node's options. Falls back to Kahuna's 1-hour default when the kahuna block
            // leaves it unset, matching EmbeddedKahunaOptions.PitrWindow.
        PitrWindowSeconds = config.Kahuna.PitrWindowSeconds ?? 3600,

        QueryResultCacheEnabled = config.QueryResultCacheEnabled,
        QueryResultCacheDefaultTtlMs = config.QueryResultCacheDefaultTtlMs,
        QueryResultCacheMaxEntries = config.QueryResultCacheMaxEntries,
        QueryResultCacheMaxBytes = config.QueryResultCacheMaxBytes,
        QueryResultCacheMaxEntryBytes = config.QueryResultCacheMaxEntryBytes,
        QueryResultCacheMaxEntryRows = config.QueryResultCacheMaxEntryRows,
        QueryResultCacheMaxDeps = config.QueryResultCacheMaxDeps,
        QueryResultCacheMaxPointDeps = config.QueryResultCacheMaxPointDeps,
        QueryResultCacheMaxRanges = config.QueryResultCacheMaxRanges,
        QueryResultCacheSingleFlightWaitMs = config.QueryResultCacheSingleflightWaitMs,
        QueryResultCacheStrictValidationMaxKeys = config.QueryResultCacheStrictValidationMaxKeys,
        QueryResultCacheSweepIntervalMs = config.QueryResultCacheSweepIntervalMs,

            // Transport-security policy for authenticated requests. Applied unconditionally: authentication
            // itself is switched on later from the environment, so this must already hold the operator's
            // choice by the time the first request is gated.
        RequireTlsWhenAuthEnabled = config.RequireTlsWhenAuthEnabled,

        Kahuna = config.Kahuna.Copy(),
        };
    }

    /// <summary>
    /// Resolves the configuration and also installs it as the process-wide ambient instance, returning
    /// what it installed so the caller can inject the same instance. Retained only while call sites are
    /// migrated to constructor-injected options; new code should call <see cref="Resolve"/> and pass
    /// the result explicitly rather than relying on the ambient value.
    /// </summary>
    public static CamusDBOptions ApplyToCamusDBConfig(ConfigDefinition config)
    {
        CamusDBOptions options = Resolve(config);

        CamusDBConfig.SetAmbient(options);

        return options;
    }
}
