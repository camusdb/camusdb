/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Core;

/// <summary>
/// Transitional facade over the ambient <see cref="CamusDBOptions"/> instance, kept only while call
/// sites are migrated from process-wide statics to constructor-injected options. Every member here
/// reads (or rewrites) one field of a single shared instance — it holds no state of its own.
///
/// <para><b>Do not add members.</b> A new tunable is a property on <see cref="CamusDBOptions"/>, a
/// field on <c>ConfigDefinition</c>, an entry in <c>ConfigReader.AllowedRootKeys</c> and a mapping in
/// <c>ConfigResolver</c>. New code takes <see cref="CamusDBOptions"/> by constructor; this type is
/// being deleted.</para>
///
/// <para>The setters exist so that not-yet-migrated code (chiefly tests that toggle a knob around a
/// case) keeps compiling. They rewrite the shared instance under a compare-and-swap loop, so a
/// concurrent pair of writers cannot lose an update the way two independent statics could not — but
/// they are still process-wide, and that global visibility is exactly what the migration removes.</para>
/// </summary>
public static class CamusDBConfig
{
    private static CamusDBOptions ambient = CamusDBOptions.Default;

    // Per-async-context data-directory override, retained until callers pass options explicitly.
    // Setting it in a test's set-up affects only that test's async execution context, which is what
    // keeps concurrently-running fixtures from stamping on each other's temp directory.
    private static readonly AsyncLocal<string?> TestDataDirectoryOverride = new();

    /// <summary>
    /// The options instance every member here reads through. Migrated code should take this by
    /// constructor instead of reaching for the ambient value.
    ///
    /// <para>Public only so that not-yet-migrated callers outside this assembly — chiefly tests that
    /// still configure an engine by assigning to the statics above — can hand the very instance they
    /// configured to a component that now requires one. Passing this is a transitional step, not the
    /// destination: the point of the migration is that a test builds its own options.</para>
    /// </summary>
    public static CamusDBOptions Ambient => ambient;

    /// <summary>
    /// Installs the resolved configuration as the ambient instance. Called once at composition time
    /// (host startup) while migration is in progress; it exists so the facade and injected options
    /// cannot disagree. Public only because the host composes outside this assembly — it is not an
    /// extension point, and it disappears with the rest of this type.
    /// </summary>
    public static void SetAmbient(CamusDBOptions options)
        => Interlocked.Exchange(ref ambient, options);

    /// <summary>
    /// Rewrites one field of the shared instance. The compare-and-swap retry keeps two concurrent
    /// writers from losing an update: <c>with</c> copies the whole record, so a plain assignment would
    /// let the later writer discard the earlier writer's unrelated field.
    /// </summary>
    private static void Mutate(Func<CamusDBOptions, CamusDBOptions> update)
    {
        while (true)
        {
            CamusDBOptions current = ambient;

            if (ReferenceEquals(Interlocked.CompareExchange(ref ambient, update(current), current), current))
                return;
        }
    }

    /// <inheritdoc cref="CamusDBOptions.DataDirectory"/>
    public static string DataDirectory
    {
        get => TestDataDirectoryOverride.Value ?? ambient.DataDirectory;
        set => TestDataDirectoryOverride.Value = value;
    }

    /// <inheritdoc cref="CamusDBOptions.StatsFlushIntervalMs"/>
    public static int StatsFlushIntervalMs
    {
        get => ambient.StatsFlushIntervalMs;
        set => Mutate(o => o with { StatsFlushIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.StatsAnalyzeSampleRows"/>
    public static int StatsAnalyzeSampleRows
    {
        get => ambient.StatsAnalyzeSampleRows;
        set => Mutate(o => o with { StatsAnalyzeSampleRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.StatsHistogramBuckets"/>
    public static int StatsHistogramBuckets
    {
        get => ambient.StatsHistogramBuckets;
        set => Mutate(o => o with { StatsHistogramBuckets = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeEnabled"/>
    public static bool AutoAnalyzeEnabled
    {
        get => ambient.AutoAnalyzeEnabled;
        set => Mutate(o => o with { AutoAnalyzeEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeCheckIntervalMs"/>
    public static int AutoAnalyzeCheckIntervalMs
    {
        get => ambient.AutoAnalyzeCheckIntervalMs;
        set => Mutate(o => o with { AutoAnalyzeCheckIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeFractionStaleRows"/>
    public static double AutoAnalyzeFractionStaleRows
    {
        get => ambient.AutoAnalyzeFractionStaleRows;
        set => Mutate(o => o with { AutoAnalyzeFractionStaleRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeMinStaleRows"/>
    public static long AutoAnalyzeMinStaleRows
    {
        get => ambient.AutoAnalyzeMinStaleRows;
        set => Mutate(o => o with { AutoAnalyzeMinStaleRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeMaxConcurrent"/>
    public static int AutoAnalyzeMaxConcurrent
    {
        get => ambient.AutoAnalyzeMaxConcurrent;
        set => Mutate(o => o with { AutoAnalyzeMaxConcurrent = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeMaxRowsPerSecond"/>
    public static int AutoAnalyzeMaxRowsPerSecond
    {
        get => ambient.AutoAnalyzeMaxRowsPerSecond;
        set => Mutate(o => o with { AutoAnalyzeMaxRowsPerSecond = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeHistogramSampleRows"/>
    public static int AutoAnalyzeHistogramSampleRows
    {
        get => ambient.AutoAnalyzeHistogramSampleRows;
        set => Mutate(o => o with { AutoAnalyzeHistogramSampleRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeHllPrecision"/>
    public static int AutoAnalyzeHllPrecision
    {
        get => ambient.AutoAnalyzeHllPrecision;
        set => Mutate(o => o with { AutoAnalyzeHllPrecision = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeLoadPauseThreshold"/>
    public static int AutoAnalyzeLoadPauseThreshold
    {
        get => ambient.AutoAnalyzeLoadPauseThreshold;
        set => Mutate(o => o with { AutoAnalyzeLoadPauseThreshold = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AutoAnalyzeOwnershipCheckRows"/>
    public static int AutoAnalyzeOwnershipCheckRows
    {
        get => ambient.AutoAnalyzeOwnershipCheckRows;
        set => Mutate(o => o with { AutoAnalyzeOwnershipCheckRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.KeyspacePurgeBatchSize"/>
    public static int KeyspacePurgeBatchSize
    {
        get => ambient.KeyspacePurgeBatchSize;
        set => Mutate(o => o with { KeyspacePurgeBatchSize = value });
    }

    /// <inheritdoc cref="CamusDBOptions.OrphanRetentionMs"/>
    public static long OrphanRetentionMs
    {
        get => ambient.OrphanRetentionMs;
        set => Mutate(o => o with { OrphanRetentionMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.OrphanReclaimIntervalMs"/>
    public static int OrphanReclaimIntervalMs
    {
        get => ambient.OrphanReclaimIntervalMs;
        set => Mutate(o => o with { OrphanReclaimIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.FenceLeaseMs"/>
    public static int FenceLeaseMs
    {
        get => ambient.FenceLeaseMs;
        set => Mutate(o => o with { FenceLeaseMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.FenceLeaseRenewIntervalMs"/>
    public static int FenceLeaseRenewIntervalMs
    {
        get => ambient.FenceLeaseRenewIntervalMs;
        set => Mutate(o => o with { FenceLeaseRenewIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.BranchSnapshotHoldLeaseMs"/>
    public static int BranchSnapshotHoldLeaseMs
    {
        get => ambient.BranchSnapshotHoldLeaseMs;
        set => Mutate(o => o with { BranchSnapshotHoldLeaseMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.PitrWindowSeconds"/>
    public static int PitrWindowSeconds
    {
        get => ambient.PitrWindowSeconds;
        set => Mutate(o => o with { PitrWindowSeconds = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SqlParserCacheTtlSeconds"/>
    public static int SqlParserCacheTtlSeconds
    {
        get => ambient.SqlParserCacheTtlSeconds;
        set => Mutate(o => o with { SqlParserCacheTtlSeconds = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SqlParserCacheMaxEntries"/>
    public static int SqlParserCacheMaxEntries
    {
        get => ambient.SqlParserCacheMaxEntries;
        set => Mutate(o => o with { SqlParserCacheMaxEntries = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SqlParserCacheSweepSeconds"/>
    public static int SqlParserCacheSweepSeconds
    {
        get => ambient.SqlParserCacheSweepSeconds;
        set => Mutate(o => o with { SqlParserCacheSweepSeconds = value });
    }

    /// <inheritdoc cref="CamusDBOptions.HashJoinMaxBuildRows"/>
    public static int HashJoinMaxBuildRows
    {
        get => ambient.HashJoinMaxBuildRows;
        set => Mutate(o => o with { HashJoinMaxBuildRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.KeyRangeShardingEnabled"/>
    public static bool KeyRangeShardingEnabled
    {
        get => ambient.KeyRangeShardingEnabled;
        set => Mutate(o => o with { KeyRangeShardingEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.ClusterPartitionCount"/>
    public static int ClusterPartitionCount
    {
        get => ambient.ClusterPartitionCount;
        set => Mutate(o => o with { ClusterPartitionCount = value });
    }

    /// <inheritdoc cref="CamusDBOptions.NetWeight"/>
    public static double NetWeight
    {
        get => ambient.NetWeight;
        set => Mutate(o => o with { NetWeight = value });
    }

    /// <inheritdoc cref="CamusDBOptions.CostBasedAccessPathEnabled"/>
    public static bool CostBasedAccessPathEnabled
    {
        get => ambient.CostBasedAccessPathEnabled;
        set => Mutate(o => o with { CostBasedAccessPathEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.CostBasedJoinOrderEnabled"/>
    public static bool CostBasedJoinOrderEnabled
    {
        get => ambient.CostBasedJoinOrderEnabled;
        set => Mutate(o => o with { CostBasedJoinOrderEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.PlanCacheEnabled"/>
    public static bool PlanCacheEnabled
    {
        get => ambient.PlanCacheEnabled;
        set => Mutate(o => o with { PlanCacheEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.PlanCacheMaxEntries"/>
    public static int PlanCacheMaxEntries
    {
        get => ambient.PlanCacheMaxEntries;
        set => Mutate(o => o with { PlanCacheMaxEntries = value });
    }

    /// <inheritdoc cref="CamusDBOptions.DefaultIsolationLevel"/>
    public static CamusIsolationLevel DefaultIsolationLevel
    {
        get => ambient.DefaultIsolationLevel;
        set => Mutate(o => o with { DefaultIsolationLevel = value });
    }

    /// <inheritdoc cref="CamusDBOptions.DefaultTransactionLocking"/>
    public static global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking DefaultTransactionLocking
    {
        get => ambient.DefaultTransactionLocking;
        set => Mutate(o => o with { DefaultTransactionLocking = value });
    }

    /// <inheritdoc cref="CamusDBOptions.DefaultReadValidation"/>
    public static global::Kahuna.Shared.KeyValue.ReadValidation DefaultReadValidation
    {
        get => ambient.DefaultReadValidation;
        set => Mutate(o => o with { DefaultReadValidation = value });
    }

    /// <inheritdoc cref="CamusDBOptions.DefaultDecisionDurability"/>
    public static global::Kahuna.Shared.KeyValue.DecisionDurability DefaultDecisionDurability
    {
        get => ambient.DefaultDecisionDurability;
        set => Mutate(o => o with { DefaultDecisionDurability = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RangeLockExpiresMs"/>
    public static int RangeLockExpiresMs
    {
        get => ambient.RangeLockExpiresMs;
        set => Mutate(o => o with { RangeLockExpiresMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/>
    public static int MaxSerializableTransactionLifetimeMs
    {
        get => ambient.MaxSerializableTransactionLifetimeMs;
        set => Mutate(o => o with { MaxSerializableTransactionLifetimeMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.TransactionIdleTimeoutMs"/>
    public static int TransactionIdleTimeoutMs
    {
        get => ambient.TransactionIdleTimeoutMs;
        set => Mutate(o => o with { TransactionIdleTimeoutMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.TransactionReaperIntervalMs"/>
    public static int TransactionReaperIntervalMs
    {
        get => ambient.TransactionReaperIntervalMs;
        set => Mutate(o => o with { TransactionReaperIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.LockEscalationThreshold"/>
    public static int LockEscalationThreshold
    {
        get => ambient.LockEscalationThreshold;
        set => Mutate(o => o with { LockEscalationThreshold = value });
    }

    /// <inheritdoc cref="CamusDBOptions.GrpcBatchMaxInFlight"/>
    public static int GrpcBatchMaxInFlight
    {
        get => ambient.GrpcBatchMaxInFlight;
        set => Mutate(o => o with { GrpcBatchMaxInFlight = value });
    }

    /// <inheritdoc cref="CamusDBOptions.GrpcMaxPreparedStatementsPerStream"/>
    public static int GrpcMaxPreparedStatementsPerStream
    {
        get => ambient.GrpcMaxPreparedStatementsPerStream;
        set => Mutate(o => o with { GrpcMaxPreparedStatementsPerStream = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RestMaxPreparedStatementsPerPrincipal"/>
    public static int RestMaxPreparedStatementsPerPrincipal
    {
        get => ambient.RestMaxPreparedStatementsPerPrincipal;
        set => Mutate(o => o with { RestMaxPreparedStatementsPerPrincipal = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RestMaxPreparedStatements"/>
    public static int RestMaxPreparedStatements
    {
        get => ambient.RestMaxPreparedStatements;
        set => Mutate(o => o with { RestMaxPreparedStatements = value });
    }

    /// <inheritdoc cref="CamusDBOptions.PreparedStatementIdleTimeoutMs"/>
    public static int PreparedStatementIdleTimeoutMs
    {
        get => ambient.PreparedStatementIdleTimeoutMs;
        set => Mutate(o => o with { PreparedStatementIdleTimeoutMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.PreparedStatementSweepIntervalMs"/>
    public static int PreparedStatementSweepIntervalMs
    {
        get => ambient.PreparedStatementSweepIntervalMs;
        set => Mutate(o => o with { PreparedStatementSweepIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxPreparedStatementBytes"/>
    public static int MaxPreparedStatementBytes
    {
        get => ambient.MaxPreparedStatementBytes;
        set => Mutate(o => o with { MaxPreparedStatementBytes = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RestMaxPreparedStatementBytes"/>
    public static long RestMaxPreparedStatementBytes
    {
        get => ambient.RestMaxPreparedStatementBytes;
        set => Mutate(o => o with { RestMaxPreparedStatementBytes = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RestMaxPreparedStatementBytesPerPrincipal"/>
    public static long RestMaxPreparedStatementBytesPerPrincipal
    {
        get => ambient.RestMaxPreparedStatementBytesPerPrincipal;
        set => Mutate(o => o with { RestMaxPreparedStatementBytesPerPrincipal = value });
    }

    /// <inheritdoc cref="CamusDBOptions.GrpcMaxPreparedStatementBytesPerStream"/>
    public static long GrpcMaxPreparedStatementBytesPerStream
    {
        get => ambient.GrpcMaxPreparedStatementBytesPerStream;
        set => Mutate(o => o with { GrpcMaxPreparedStatementBytesPerStream = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxMutationsPerTransaction"/>
    public static int MaxMutationsPerTransaction
    {
        get => ambient.MaxMutationsPerTransaction;
        set => Mutate(o => o with { MaxMutationsPerTransaction = value });
    }

    /// <inheritdoc cref="CamusDBOptions.LockWaitDeadlineMs"/>
    public static int LockWaitDeadlineMs
    {
        get => ambient.LockWaitDeadlineMs;
        set => Mutate(o => o with { LockWaitDeadlineMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxIdentifierLength"/>
    public static int MaxIdentifierLength
    {
        get => ambient.MaxIdentifierLength;
        set => Mutate(o => o with { MaxIdentifierLength = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxColumnsPerTable"/>
    public static int MaxColumnsPerTable
    {
        get => ambient.MaxColumnsPerTable;
        set => Mutate(o => o with { MaxColumnsPerTable = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SpillEnabled"/>
    public static bool SpillEnabled
    {
        get => ambient.SpillEnabled;
        set => Mutate(o => o with { SpillEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SpillThresholdRows"/>
    public static int SpillThresholdRows
    {
        get => ambient.SpillThresholdRows;
        set => Mutate(o => o with { SpillThresholdRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SpillMergeFanIn"/>
    public static int SpillMergeFanIn
    {
        get => ambient.SpillMergeFanIn;
        set => Mutate(o => o with { SpillMergeFanIn = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SlotBackedDecode"/>
    public static bool SlotBackedDecode
    {
        get => ambient.SlotBackedDecode;
        set => Mutate(o => o with { SlotBackedDecode = value });
    }

    /// <inheritdoc cref="CamusDBOptions.BorrowedDecode"/>
    public static BorrowedDecodePolicy BorrowedDecode
    {
        get => ambient.BorrowedDecode;
        set => Mutate(o => o with { BorrowedDecode = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SpillMaxFrameBytes"/>
    public static int SpillMaxFrameBytes
    {
        get => ambient.SpillMaxFrameBytes;
        set => Mutate(o => o with { SpillMaxFrameBytes = value });
    }

    /// <inheritdoc cref="CamusDBOptions.ForceSpillThresholdRows"/>
    public static int? ForceSpillThresholdRows
    {
        get => ambient.ForceSpillThresholdRows;
        set => Mutate(o => o with { ForceSpillThresholdRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.SpillEffectiveThreshold"/>
    public static int SpillEffectiveThreshold => ambient.SpillEffectiveThreshold;

    /// <inheritdoc cref="CamusDBOptions.MaxIndexesPerTable"/>
    public static int MaxIndexesPerTable
    {
        get => ambient.MaxIndexesPerTable;
        set => Mutate(o => o with { MaxIndexesPerTable = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxIndexColumns"/>
    public static int MaxIndexColumns
    {
        get => ambient.MaxIndexColumns;
        set => Mutate(o => o with { MaxIndexColumns = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxIndexIncludeTupleBytes"/>
    public static int MaxIndexIncludeTupleBytes
    {
        get => ambient.MaxIndexIncludeTupleBytes;
        set => Mutate(o => o with { MaxIndexIncludeTupleBytes = value });
    }

    /// <inheritdoc cref="CamusDBOptions.MaxTablesPerDatabase"/>
    public static int MaxTablesPerDatabase
    {
        get => ambient.MaxTablesPerDatabase;
        set => Mutate(o => o with { MaxTablesPerDatabase = value });
    }

    /// <inheritdoc cref="CamusDBOptions.PasswordHashIterations"/>
    public static int PasswordHashIterations
    {
        get => ambient.PasswordHashIterations;
        set => Mutate(o => o with { PasswordHashIterations = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AuthenticationEnabled"/>
    public static bool AuthenticationEnabled
    {
        get => ambient.AuthenticationEnabled;
        set => Mutate(o => o with { AuthenticationEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.BootstrapSuperuser"/>
    public static string BootstrapSuperuser
    {
        get => ambient.BootstrapSuperuser;
        set => Mutate(o => o with { BootstrapSuperuser = value });
    }

    /// <inheritdoc cref="CamusDBOptions.BootstrapSuperuserPassword"/>
    public static string BootstrapSuperuserPassword
    {
        get => ambient.BootstrapSuperuserPassword;
        set => Mutate(o => o with { BootstrapSuperuserPassword = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AccessTokenServerKey"/>
    public static string AccessTokenServerKey
    {
        get => ambient.AccessTokenServerKey;
        set => Mutate(o => o with { AccessTokenServerKey = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AccessTokenTtl"/>
    public static TimeSpan AccessTokenTtl
    {
        get => ambient.AccessTokenTtl;
        set => Mutate(o => o with { AccessTokenTtl = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AuthenticationCacheTtl"/>
    public static TimeSpan AuthenticationCacheTtl
    {
        get => ambient.AuthenticationCacheTtl;
        set => Mutate(o => o with { AuthenticationCacheTtl = value });
    }

    /// <inheritdoc cref="CamusDBOptions.LoginKdfMaxConcurrency"/>
    public static int LoginKdfMaxConcurrency
    {
        get => ambient.LoginKdfMaxConcurrency;
        set => Mutate(o => o with { LoginKdfMaxConcurrency = value });
    }

    /// <inheritdoc cref="CamusDBOptions.LoginMaxAttemptsPerMinute"/>
    public static int LoginMaxAttemptsPerMinute
    {
        get => ambient.LoginMaxAttemptsPerMinute;
        set => Mutate(o => o with { LoginMaxAttemptsPerMinute = value });
    }

    /// <inheritdoc cref="CamusDBOptions.AuthenticationCacheMaxEntries"/>
    public static int AuthenticationCacheMaxEntries
    {
        get => ambient.AuthenticationCacheMaxEntries;
        set => Mutate(o => o with { AuthenticationCacheMaxEntries = value });
    }

    /// <inheritdoc cref="CamusDBOptions.LoginRateLimitMaxEntries"/>
    public static int LoginRateLimitMaxEntries
    {
        get => ambient.LoginRateLimitMaxEntries;
        set => Mutate(o => o with { LoginRateLimitMaxEntries = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RequireTlsWhenAuthEnabled"/>
    public static bool RequireTlsWhenAuthEnabled
    {
        get => ambient.RequireTlsWhenAuthEnabled;
        set => Mutate(o => o with { RequireTlsWhenAuthEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.NodeSecret"/>
    public static string NodeSecret
    {
        get => ambient.NodeSecret;
        set => Mutate(o => o with { NodeSecret = value });
    }

    /// <inheritdoc cref="CamusDBOptions.IndexScanFetchBatchSize"/>
    public static int IndexScanFetchBatchSize
    {
        get => ambient.IndexScanFetchBatchSize;
        set => Mutate(o => o with { IndexScanFetchBatchSize = value });
    }

    /// <inheritdoc cref="CamusDBOptions.LockTracingEnabled"/>
    public static bool LockTracingEnabled
    {
        get => ambient.LockTracingEnabled;
        set => Mutate(o => o with { LockTracingEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryTracingEnabled"/>
    public static bool QueryTracingEnabled
    {
        get => ambient.QueryTracingEnabled;
        set => Mutate(o => o with { QueryTracingEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RegexMatchTimeoutMs"/>
    public static int RegexMatchTimeoutMs
    {
        get => ambient.RegexMatchTimeoutMs;
        set => Mutate(o => o with { RegexMatchTimeoutMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.RegexCacheMaxEntries"/>
    public static int RegexCacheMaxEntries
    {
        get => ambient.RegexCacheMaxEntries;
        set => Mutate(o => o with { RegexCacheMaxEntries = value });
    }

    /// <inheritdoc cref="CamusDBOptions.Kahuna"/>
    public static Config.Models.KahunaOptionsConfig Kahuna
    {
        get => ambient.Kahuna;
        set => Mutate(o => o with { Kahuna = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheEnabled"/>
    public static bool QueryResultCacheEnabled
    {
        get => ambient.QueryResultCacheEnabled;
        set => Mutate(o => o with { QueryResultCacheEnabled = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheDefaultTtlMs"/>
    public static int QueryResultCacheDefaultTtlMs
    {
        get => ambient.QueryResultCacheDefaultTtlMs;
        set => Mutate(o => o with { QueryResultCacheDefaultTtlMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxEntries"/>
    public static int QueryResultCacheMaxEntries
    {
        get => ambient.QueryResultCacheMaxEntries;
        set => Mutate(o => o with { QueryResultCacheMaxEntries = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxBytes"/>
    public static long QueryResultCacheMaxBytes
    {
        get => ambient.QueryResultCacheMaxBytes;
        set => Mutate(o => o with { QueryResultCacheMaxBytes = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxEntryBytes"/>
    public static long QueryResultCacheMaxEntryBytes
    {
        get => ambient.QueryResultCacheMaxEntryBytes;
        set => Mutate(o => o with { QueryResultCacheMaxEntryBytes = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxEntryRows"/>
    public static int QueryResultCacheMaxEntryRows
    {
        get => ambient.QueryResultCacheMaxEntryRows;
        set => Mutate(o => o with { QueryResultCacheMaxEntryRows = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxDeps"/>
    public static int QueryResultCacheMaxDeps
    {
        get => ambient.QueryResultCacheMaxDeps;
        set => Mutate(o => o with { QueryResultCacheMaxDeps = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxPointDeps"/>
    public static int QueryResultCacheMaxPointDeps
    {
        get => ambient.QueryResultCacheMaxPointDeps;
        set => Mutate(o => o with { QueryResultCacheMaxPointDeps = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheMaxRanges"/>
    public static int QueryResultCacheMaxRanges
    {
        get => ambient.QueryResultCacheMaxRanges;
        set => Mutate(o => o with { QueryResultCacheMaxRanges = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheSingleFlightWaitMs"/>
    public static int QueryResultCacheSingleFlightWaitMs
    {
        get => ambient.QueryResultCacheSingleFlightWaitMs;
        set => Mutate(o => o with { QueryResultCacheSingleFlightWaitMs = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheStrictValidationMaxKeys"/>
    public static int QueryResultCacheStrictValidationMaxKeys
    {
        get => ambient.QueryResultCacheStrictValidationMaxKeys;
        set => Mutate(o => o with { QueryResultCacheStrictValidationMaxKeys = value });
    }

    /// <inheritdoc cref="CamusDBOptions.QueryResultCacheSweepIntervalMs"/>
    public static int QueryResultCacheSweepIntervalMs
    {
        get => ambient.QueryResultCacheSweepIntervalMs;
        set => Mutate(o => o with { QueryResultCacheSweepIntervalMs = value });
    }

    /// <inheritdoc cref="CamusDBConstants.PrimaryKeyInternalName"/>
    public const string PrimaryKeyInternalName = CamusDBConstants.PrimaryKeyInternalName;

    /// <inheritdoc cref="CamusDBConstants.DefaultStringMaxLength"/>
    public const int DefaultStringMaxLength = CamusDBConstants.DefaultStringMaxLength;

    /// <inheritdoc cref="CamusDBConstants.DefaultBytesMaxLength"/>
    public const int DefaultBytesMaxLength = CamusDBConstants.DefaultBytesMaxLength;

    /// <inheritdoc cref="CamusDBConstants.MaxCommentLength"/>
    public const int MaxCommentLength = CamusDBConstants.MaxCommentLength;

    /// <inheritdoc cref="CamusDBConstants.MaxPasswordBytes"/>
    public const int MaxPasswordBytes = CamusDBConstants.MaxPasswordBytes;
}

/// <summary>
/// The three decode-backing policies for <see cref="CamusDBOptions.BorrowedDecode"/>: let the scanner
/// choose per query (<see cref="Adaptive"/>), or force one path globally for A/B measurement and as a
/// kill-switch (<see cref="ForceBorrowed"/> / <see cref="ForceEager"/>).
/// </summary>
public enum BorrowedDecodePolicy
{
    /// <summary>Scanner opts into borrowed decode for filtered, non-row-retaining scans; eager otherwise. Production default.</summary>
    Adaptive,

    /// <summary>Never use borrowed decode — the eager kill-switch / A/B baseline.</summary>
    ForceEager,

    /// <summary>Always use borrowed decode, on every scan — the A/B measurement mode.</summary>
    ForceBorrowed,
}
