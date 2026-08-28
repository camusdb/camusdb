
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
    ///
    /// <para>Every override that takes effect records itself as command-line sourced, overwriting the
    /// file provenance the reader recorded. Only the three keys that a context-dependent default
    /// consults are also added to <see cref="ConfigDefinition.ProvidedKeys"/> — that set answers a
    /// narrower question and is deliberately not widened here.</para>
    /// </summary>
    public static void ApplyCliOverrides(ConfigDefinition config, ConfigCliOverrides? cli)
    {
        if (cli is null)
            return;

        if (cli.Mode is not null)
        {
            config.Mode = cli.Mode;
            config.ProvidedKeys.Add("mode");
            config.RecordSource("mode", ConfigValueSource.CommandLine);
        }

        if (cli.MemoryProfile is not null)
        {
            config.MemoryProfile = cli.MemoryProfile;
            config.RecordSource("memory_profile", ConfigValueSource.CommandLine);
        }

        if (cli.DataDir is not null)
        {
            config.DataDir = cli.DataDir;
            config.ProvidedKeys.Add("data_dir");
            config.RecordSource("data_dir", ConfigValueSource.CommandLine);
        }

        if (cli.NodeName is not null)
        {
            config.NodeName = cli.NodeName;
            config.RecordSource("node_name", ConfigValueSource.CommandLine);
        }

        if (cli.RaftNodeId is int raftNodeId)
        {
            config.RaftNodeId = raftNodeId;
            config.RecordSource("raft_node_id", ConfigValueSource.CommandLine);
        }

        if (cli.RaftHost is not null)
        {
            config.RaftHost = cli.RaftHost;
            config.RecordSource("raft_host", ConfigValueSource.CommandLine);
        }

        if (cli.RaftPort is int raftPort)
        {
            config.RaftPort = raftPort;
            config.RecordSource("raft_port", ConfigValueSource.CommandLine);
        }

        if (cli.InitialPartitions is int initialPartitions)
        {
            config.InitialPartitions = initialPartitions;
            config.ProvidedKeys.Add("initial_partitions");
            config.RecordSource("initial_partitions", ConfigValueSource.CommandLine);
        }

        if (cli.Peers is { Count: > 0 })
        {
            config.Peers = [.. cli.Peers];
            config.RecordSource("peers", ConfigValueSource.CommandLine);
        }

        if (cli.HttpPeers is { Count: > 0 })
        {
            config.HttpPeers = [.. cli.HttpPeers];
            config.RecordSource("http_peers", ConfigValueSource.CommandLine);
        }

        if (cli.JoinExisting is bool joinExisting)
        {
            config.JoinExisting = joinExisting;
            config.RecordSource("join_existing", ConfigValueSource.CommandLine);
        }

        if (cli.SchemaAckWaitTimeoutMs is int schemaAckWait)
        {
            config.SchemaAckWaitTimeoutMs = schemaAckWait;
            config.RecordSource("schema_ack_wait_timeout_ms", ConfigValueSource.CommandLine);
        }

        if (cli.SchemaAckLiveNodeLeaseMs is int schemaAckLease)
        {
            config.SchemaAckLiveNodeLeaseMs = schemaAckLease;
            config.RecordSource("schema_ack_live_node_lease_ms", ConfigValueSource.CommandLine);
        }

        if (cli.HttpPort is int httpPort)
        {
            config.HttpPort = httpPort;
            config.RecordSource("http_port", ConfigValueSource.CommandLine);
        }

        if (cli.HttpsPort is int httpsPort)
        {
            config.HttpsPort = httpsPort;
            config.RecordSource("https_port", ConfigValueSource.CommandLine);
        }

        if (cli.HttpsCertificate is not null)
        {
            config.HttpsCertificate = cli.HttpsCertificate;
            config.RecordSource("https_certificate", ConfigValueSource.CommandLine);
        }

        if (cli.RaftCertificate is not null)
        {
            config.RaftCertificate = cli.RaftCertificate;
            config.RecordSource("raft_certificate", ConfigValueSource.CommandLine);
        }

        if (cli.RequireTlsWhenAuthEnabled is bool requireTls)
        {
            config.RequireTlsWhenAuthEnabled = requireTls;
            config.RecordSource("require_tls_when_auth_enabled", ConfigValueSource.CommandLine);
        }
    }

    /// <summary>
    /// Fills in the settings whose sensible default depends on the rest of the resolved
    /// configuration, for keys the operator left unset. Call once, after
    /// <see cref="ApplyCliOverrides"/> (so an explicit flag is visible) and before
    /// <see cref="ConfigDefinition.Validate"/>.
    /// <para>
    /// Both defaults exist because a node must be able to start with no configuration file at all.
    /// <c>data_dir</c> otherwise resolves relative to the process working directory, which for an
    /// installed tool means a different database depending on where the user was standing.
    /// <c>initial_partitions</c> otherwise defaults to the cluster-shaped value of 3 even on a
    /// standalone node, where a single partition makes every transaction single-participant and
    /// enables the one-phase commit fast path — worth several times the write throughput on one
    /// disk, since fanning out partitions buys nothing when they share a single fsync target.
    /// </para>
    /// </summary>
    public static void ApplyEffectiveDefaults(ConfigDefinition config)
    {
        if (!config.ProvidedKeys.Contains("data_dir") || string.IsNullOrWhiteSpace(config.DataDir))
            config.DataDir = ConfigLocator.DefaultDataDirectory();

        if (!config.ProvidedKeys.Contains("initial_partitions"))
            config.InitialPartitions = config.IsClusterMode ? 3 : 1;
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

            config.RecordSource("key_range_sharding", ConfigValueSource.Environment);
        }

        return new CamusDBOptions
        {
        // An empty data_dir means "unset": keep the built-in default rather than
        // rooting the database at the current directory.
        DataDirectory = !string.IsNullOrEmpty(config.DataDir) ? config.DataDir : CamusDBOptions.Default.DataDirectory,

        MemoryProfile = config.ParseMemoryProfile(),

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

        TtlEnabled = config.TtlEnabled,
        TtlDefaultJobCron = config.TtlDefaultJobCron,
        TtlDefaultSelectBatchSize = config.TtlDefaultSelectBatchSize,
        TtlDefaultDeleteBatchSize = config.TtlDefaultDeleteBatchSize,
        TtlDefaultSelectRateLimit = config.TtlDefaultSelectRateLimit,
        TtlDefaultDeleteRateLimit = config.TtlDefaultDeleteRateLimit,
        TtlSpansPerTable = config.TtlSpansPerTable,
        TtlMaxConcurrentSpansPerNode = config.TtlMaxConcurrentSpansPerNode,
        TtlLoadPauseThreshold = config.TtlLoadPauseThreshold,
        TtlSpanLeaseMs = config.TtlSpanLeaseMs,
        TtlSpanLeaseRenewIntervalMs = config.TtlSpanLeaseRenewIntervalMs,

        SqlParserCacheTtlSeconds = config.SqlParserCacheTtlSeconds,
        SqlParserCacheMaxEntries = config.SqlParserCacheMaxEntries,
        SqlParserCacheSweepSeconds = config.SqlParserCacheSweepSeconds,

        OrphanRetentionMs = config.OrphanRetentionMs,
        OrphanReclaimIntervalMs = config.OrphanReclaimIntervalMs,
        DatabaseIdleEvictionMs = config.DatabaseIdleEvictionMs,

        EngineMetricsEnabled = config.EngineMetricsEnabled,
        SlowQueryLogEnabled = config.SlowQueryLogEnabled,
        SlowQueryLogThresholdMs = config.SlowQueryLogThresholdMs,
        SlowQueryLogMaxEntries = config.SlowQueryLogMaxEntries,
        SlowQueryLogMaxSqlLength = config.SlowQueryLogMaxSqlLength,
        DashboardEnabled = config.DashboardEnabled,
        DashboardRefreshSeconds = config.DashboardRefreshSeconds,
        QueryTracingEnabled = config.QueryTracingEnabled,
        LockTracingEnabled = config.LockTracingEnabled,

        FenceLeaseMs = config.FenceLeaseMs,
        FenceLeaseRenewIntervalMs = config.FenceLeaseRenewIntervalMs,
        KeyspacePurgeBatchSize = config.KeyspacePurgeBatchSize,
        IndexScanFetchBatchSize = config.IndexScanFetchBatchSize,
        MaxQueryParallelism = config.MaxQueryParallelism,
        BroadcastJoinMaxBuildRows = config.BroadcastJoinMaxBuildRows,
        HashJoinMaxBuildRows = config.HashJoinMaxBuildRows,
        NetWeight = config.NetWeight,
        SlotBackedDecode = config.SlotBackedDecode,
        BorrowedDecode = config.ParseBorrowedDecode(),
        SpillMaxFrameBytes = config.SpillMaxFrameBytes,
        DefaultReadValidation = config.ParseDefaultReadValidation(),
        DefaultDecisionDurability = config.ParseDefaultDecisionDurability(),

        PasswordHashIterations = config.PasswordHashIterations,
        LoginKdfMaxConcurrency = config.LoginKdfMaxConcurrency,
        LoginMaxAttemptsPerMinute = config.LoginMaxAttemptsPerMinute,
        LoginRateLimitMaxEntries = config.LoginRateLimitMaxEntries,
        AuthenticationCacheTtl = TimeSpan.FromMilliseconds(config.AuthenticationCacheTtl),
        AuthenticationCacheMaxEntries = config.AuthenticationCacheMaxEntries,
        AccessTokenTtl = TimeSpan.FromMilliseconds(config.AccessTokenTtl),

        DefaultIsolationLevel = config.ParseDefaultIsolationLevel(),
        DefaultTransactionLocking = config.ParseDefaultTransactionLocking(),
        DefaultTransactionPriority = config.ParseDefaultTransactionPriority(),
        TransactionAdmissionWaitMs = config.TransactionAdmissionWaitMs,
        RangeLockExpiresMs = config.RangeLockExpiresMs,
        MaxSerializableTransactionLifetimeMs = config.MaxSerializableTransactionLifetimeMs,
        TransactionFinalizeRetryBudgetMs = config.TransactionFinalizeRetryBudgetMs,
        SequenceRetryBudgetMs = config.SequenceRetryBudgetMs,
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
        DistributedQueryExecutionEnabled = config.DistributedQueryExecution,
        ClusterPartitionCount = config.InitialPartitions,
        GrpcBatchMaxInFlight = config.GrpcBatchMaxInFlight,
        CostBasedAccessPathEnabled = config.CostBasedAccessPathEnabled,
        CostBasedJoinOrderEnabled = config.CostBasedJoinOrderEnabled,
        PlanCacheEnabled = config.PlanCacheEnabled,
        PlanCacheMaxEntries = config.PlanCacheMaxEntries,
        BoundQueryCacheEnabled = config.BoundQueryCacheEnabled,

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
        MaxViewExpansionDepth = config.MaxViewExpansionDepth,
        MinFreeDiskBytes = config.MinFreeDiskBytes,
        MaterializedViewRefreshChunkRows = config.MaterializedViewRefreshChunkRows,
        MaterializedViewRefreshEnabled = config.MaterializedViewRefreshEnabled,
        MaterializedViewRefreshTakeoverAttempts = config.MaterializedViewRefreshTakeoverAttempts,
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

            // Carried across so the resolved options can report where each value came from without
            // holding a reference back to the mutable definition. Snapshotted, not shared: the
            // definition is still writable after this point and the options record is not.
        ValueSources = new Dictionary<string, ConfigValueSource>(config.KeySources, StringComparer.OrdinalIgnoreCase),
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
