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
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Config;

/// <summary>
/// Pins every built-in configuration default to the value it had before configuration moved from
/// process-wide statics onto <see cref="CamusDBOptions"/>. The move was meant to change *where* a
/// value lives, never *what* it is, and a mistyped literal among a hundred properties would
/// otherwise surface as an unexplained behavior change far from here — a silently smaller cache, a
/// different isolation level — rather than as a failing assertion.
///
/// <para>A deliberate default change should update the expected value here in the same commit; that
/// edit is the record that the change was intended.</para>
/// </summary>
public class TestConfigurationDefaults
{
    /// <summary>
    /// The data directory must be a real path on the shared defaults. It is the one default computed
    /// by an expression rather than written as a literal, so it is also the one that a static
    /// initialization-order mistake can silently leave null — and a null here does not fail here, it
    /// fails much later inside an unrelated <c>Path.Combine</c>.
    /// </summary>
    [Test]
    public void DefaultDataDirectoryIsResolved()
    {
        Assert.IsNotNull(CamusDBOptions.Default.DataDirectory);
        Assert.AreEqual(Path.GetFullPath("Data"), CamusDBOptions.Default.DataDirectory);
        Assert.IsTrue(Path.IsPathRooted(CamusDBOptions.Default.DataDirectory));
    }

    [Test]
    public void DefaultOptionsMatchDocumentedValues()
    {
        Assert.AreEqual(5000, CamusDBOptions.Default.StatsFlushIntervalMs, nameof(CamusDBOptions.Default.StatsFlushIntervalMs));
        Assert.AreEqual(100_000, CamusDBOptions.Default.StatsAnalyzeSampleRows, nameof(CamusDBOptions.Default.StatsAnalyzeSampleRows));
        Assert.AreEqual(100, CamusDBOptions.Default.StatsHistogramBuckets, nameof(CamusDBOptions.Default.StatsHistogramBuckets));
        Assert.AreEqual(true, CamusDBOptions.Default.AutoAnalyzeEnabled, nameof(CamusDBOptions.Default.AutoAnalyzeEnabled));
        Assert.AreEqual(60_000, CamusDBOptions.Default.AutoAnalyzeCheckIntervalMs, nameof(CamusDBOptions.Default.AutoAnalyzeCheckIntervalMs));
        Assert.AreEqual(0.20, CamusDBOptions.Default.AutoAnalyzeFractionStaleRows, nameof(CamusDBOptions.Default.AutoAnalyzeFractionStaleRows));
        Assert.AreEqual(500, CamusDBOptions.Default.AutoAnalyzeMinStaleRows, nameof(CamusDBOptions.Default.AutoAnalyzeMinStaleRows));
        Assert.AreEqual(1, CamusDBOptions.Default.AutoAnalyzeMaxConcurrent, nameof(CamusDBOptions.Default.AutoAnalyzeMaxConcurrent));
        Assert.AreEqual(50_000, CamusDBOptions.Default.AutoAnalyzeMaxRowsPerSecond, nameof(CamusDBOptions.Default.AutoAnalyzeMaxRowsPerSecond));
        Assert.AreEqual(10_000, CamusDBOptions.Default.AutoAnalyzeHistogramSampleRows, nameof(CamusDBOptions.Default.AutoAnalyzeHistogramSampleRows));
        Assert.AreEqual(11, CamusDBOptions.Default.AutoAnalyzeHllPrecision, nameof(CamusDBOptions.Default.AutoAnalyzeHllPrecision));
        Assert.AreEqual(16, CamusDBOptions.Default.AutoAnalyzeLoadPauseThreshold, nameof(CamusDBOptions.Default.AutoAnalyzeLoadPauseThreshold));
        Assert.AreEqual(1000, CamusDBOptions.Default.AutoAnalyzeOwnershipCheckRows, nameof(CamusDBOptions.Default.AutoAnalyzeOwnershipCheckRows));
        Assert.AreEqual(true, CamusDBOptions.Default.TtlEnabled, nameof(CamusDBOptions.Default.TtlEnabled));
        Assert.AreEqual(512, CamusDBOptions.Default.KeyspacePurgeBatchSize, nameof(CamusDBOptions.Default.KeyspacePurgeBatchSize));
        Assert.AreEqual(7L * 24 * 60 * 60 * 1000, CamusDBOptions.Default.OrphanRetentionMs, nameof(CamusDBOptions.Default.OrphanRetentionMs));
        Assert.AreEqual(5 * 60 * 1000, CamusDBOptions.Default.OrphanReclaimIntervalMs, nameof(CamusDBOptions.Default.OrphanReclaimIntervalMs));
        Assert.AreEqual(30_000, CamusDBOptions.Default.FenceLeaseMs, nameof(CamusDBOptions.Default.FenceLeaseMs));
        Assert.AreEqual(10_000, CamusDBOptions.Default.FenceLeaseRenewIntervalMs, nameof(CamusDBOptions.Default.FenceLeaseRenewIntervalMs));
        Assert.AreEqual(300_000, CamusDBOptions.Default.BranchSnapshotHoldLeaseMs, nameof(CamusDBOptions.Default.BranchSnapshotHoldLeaseMs));
        Assert.AreEqual(3600, CamusDBOptions.Default.PitrWindowSeconds, nameof(CamusDBOptions.Default.PitrWindowSeconds));
        Assert.AreEqual(300, CamusDBOptions.Default.SqlParserCacheTtlSeconds, nameof(CamusDBOptions.Default.SqlParserCacheTtlSeconds));
        Assert.AreEqual(2048, CamusDBOptions.Default.SqlParserCacheMaxEntries, nameof(CamusDBOptions.Default.SqlParserCacheMaxEntries));
        Assert.AreEqual(60, CamusDBOptions.Default.SqlParserCacheSweepSeconds, nameof(CamusDBOptions.Default.SqlParserCacheSweepSeconds));
        Assert.AreEqual(1_000_000, CamusDBOptions.Default.HashJoinMaxBuildRows, nameof(CamusDBOptions.Default.HashJoinMaxBuildRows));
        Assert.AreEqual("~pk", CamusDBConstants.PrimaryKeyInternalName, nameof(CamusDBConstants.PrimaryKeyInternalName));
        Assert.IsFalse(CamusDBOptions.Default.KeyRangeShardingEnabled, nameof(CamusDBOptions.Default.KeyRangeShardingEnabled));
        Assert.AreEqual(1, CamusDBOptions.Default.ClusterPartitionCount, nameof(CamusDBOptions.Default.ClusterPartitionCount));
        Assert.AreEqual(0.01, CamusDBOptions.Default.NetWeight, nameof(CamusDBOptions.Default.NetWeight));
        Assert.AreEqual(true, CamusDBOptions.Default.CostBasedAccessPathEnabled, nameof(CamusDBOptions.Default.CostBasedAccessPathEnabled));
        Assert.AreEqual(true, CamusDBOptions.Default.CostBasedJoinOrderEnabled, nameof(CamusDBOptions.Default.CostBasedJoinOrderEnabled));
        Assert.AreEqual(false, CamusDBOptions.Default.PlanCacheEnabled, nameof(CamusDBOptions.Default.PlanCacheEnabled));
        Assert.AreEqual(512, CamusDBOptions.Default.PlanCacheMaxEntries, nameof(CamusDBOptions.Default.PlanCacheMaxEntries));
        Assert.AreEqual(CamusIsolationLevel.Serializable, CamusDBOptions.Default.DefaultIsolationLevel, nameof(CamusDBOptions.Default.DefaultIsolationLevel));
        Assert.AreEqual(global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Pessimistic, CamusDBOptions.Default.DefaultTransactionLocking, nameof(CamusDBOptions.Default.DefaultTransactionLocking));
        Assert.AreEqual(global::Kahuna.Shared.KeyValue.ReadValidation.None, CamusDBOptions.Default.DefaultReadValidation, nameof(CamusDBOptions.Default.DefaultReadValidation));
        Assert.AreEqual(global::Kahuna.Shared.KeyValue.DecisionDurability.BestEffort, CamusDBOptions.Default.DefaultDecisionDurability, nameof(CamusDBOptions.Default.DefaultDecisionDurability));
        Assert.AreEqual(150_000, CamusDBOptions.Default.RangeLockExpiresMs, nameof(CamusDBOptions.Default.RangeLockExpiresMs));
        Assert.AreEqual(3_600_000, CamusDBOptions.Default.MaxSerializableTransactionLifetimeMs, nameof(CamusDBOptions.Default.MaxSerializableTransactionLifetimeMs));
        Assert.AreEqual(300_000, CamusDBOptions.Default.TransactionIdleTimeoutMs, nameof(CamusDBOptions.Default.TransactionIdleTimeoutMs));
        Assert.AreEqual(30_000, CamusDBOptions.Default.TransactionReaperIntervalMs, nameof(CamusDBOptions.Default.TransactionReaperIntervalMs));
        Assert.AreEqual(50, CamusDBOptions.Default.LockEscalationThreshold, nameof(CamusDBOptions.Default.LockEscalationThreshold));
        Assert.AreEqual(64, CamusDBOptions.Default.GrpcBatchMaxInFlight, nameof(CamusDBOptions.Default.GrpcBatchMaxInFlight));
        Assert.AreEqual(512, CamusDBOptions.Default.GrpcMaxPreparedStatementsPerStream, nameof(CamusDBOptions.Default.GrpcMaxPreparedStatementsPerStream));
        Assert.AreEqual(512, CamusDBOptions.Default.RestMaxPreparedStatementsPerPrincipal, nameof(CamusDBOptions.Default.RestMaxPreparedStatementsPerPrincipal));
        Assert.AreEqual(8192, CamusDBOptions.Default.RestMaxPreparedStatements, nameof(CamusDBOptions.Default.RestMaxPreparedStatements));
        Assert.AreEqual(600_000, CamusDBOptions.Default.PreparedStatementIdleTimeoutMs, nameof(CamusDBOptions.Default.PreparedStatementIdleTimeoutMs));
        Assert.AreEqual(60_000, CamusDBOptions.Default.PreparedStatementSweepIntervalMs, nameof(CamusDBOptions.Default.PreparedStatementSweepIntervalMs));
        Assert.AreEqual(65_536, CamusDBOptions.Default.MaxPreparedStatementBytes, nameof(CamusDBOptions.Default.MaxPreparedStatementBytes));
        Assert.AreEqual(64L * 1024 * 1024, CamusDBOptions.Default.RestMaxPreparedStatementBytes, nameof(CamusDBOptions.Default.RestMaxPreparedStatementBytes));
        Assert.AreEqual(8L * 1024 * 1024, CamusDBOptions.Default.RestMaxPreparedStatementBytesPerPrincipal, nameof(CamusDBOptions.Default.RestMaxPreparedStatementBytesPerPrincipal));
        Assert.AreEqual(8L * 1024 * 1024, CamusDBOptions.Default.GrpcMaxPreparedStatementBytesPerStream, nameof(CamusDBOptions.Default.GrpcMaxPreparedStatementBytesPerStream));
        Assert.AreEqual(20_000, CamusDBOptions.Default.MaxMutationsPerTransaction, nameof(CamusDBOptions.Default.MaxMutationsPerTransaction));
        Assert.AreEqual(500, CamusDBOptions.Default.LockWaitDeadlineMs, nameof(CamusDBOptions.Default.LockWaitDeadlineMs));
        Assert.AreEqual(64, CamusDBOptions.Default.MaxIdentifierLength, nameof(CamusDBOptions.Default.MaxIdentifierLength));
        Assert.AreEqual(512, CamusDBOptions.Default.MaxColumnsPerTable, nameof(CamusDBOptions.Default.MaxColumnsPerTable));
        Assert.AreEqual(false, CamusDBOptions.Default.SpillEnabled, nameof(CamusDBOptions.Default.SpillEnabled));
        Assert.AreEqual(500_000, CamusDBOptions.Default.SpillThresholdRows, nameof(CamusDBOptions.Default.SpillThresholdRows));
        Assert.AreEqual(16, CamusDBOptions.Default.SpillMergeFanIn, nameof(CamusDBOptions.Default.SpillMergeFanIn));
        Assert.AreEqual(true, CamusDBOptions.Default.SlotBackedDecode, nameof(CamusDBOptions.Default.SlotBackedDecode));
        Assert.AreEqual(BorrowedDecodePolicy.Adaptive, CamusDBOptions.Default.BorrowedDecode, nameof(CamusDBOptions.Default.BorrowedDecode));
        Assert.AreEqual(256 * 1024 * 1024, CamusDBOptions.Default.SpillMaxFrameBytes, nameof(CamusDBOptions.Default.SpillMaxFrameBytes));
        Assert.IsNull(CamusDBOptions.Default.ForceSpillThresholdRows, nameof(CamusDBOptions.Default.ForceSpillThresholdRows));
        Assert.AreEqual(64, CamusDBOptions.Default.MaxIndexesPerTable, nameof(CamusDBOptions.Default.MaxIndexesPerTable));
        Assert.AreEqual(32, CamusDBOptions.Default.MaxIndexColumns, nameof(CamusDBOptions.Default.MaxIndexColumns));
        Assert.AreEqual(4096, CamusDBOptions.Default.MaxIndexIncludeTupleBytes, nameof(CamusDBOptions.Default.MaxIndexIncludeTupleBytes));
        Assert.AreEqual(10_000, CamusDBOptions.Default.MaxTablesPerDatabase, nameof(CamusDBOptions.Default.MaxTablesPerDatabase));
        Assert.AreEqual(2_621_440, CamusDBConstants.DefaultStringMaxLength, nameof(CamusDBConstants.DefaultStringMaxLength));
        Assert.AreEqual(10_485_760, CamusDBConstants.DefaultBytesMaxLength, nameof(CamusDBConstants.DefaultBytesMaxLength));
        Assert.AreEqual(65_535, CamusDBConstants.MaxCommentLength, nameof(CamusDBConstants.MaxCommentLength));
        Assert.AreEqual(600_000, CamusDBOptions.Default.PasswordHashIterations, nameof(CamusDBOptions.Default.PasswordHashIterations));
        Assert.AreEqual(1024, CamusDBConstants.MaxPasswordBytes, nameof(CamusDBConstants.MaxPasswordBytes));
        Assert.AreEqual(false, CamusDBOptions.Default.AuthenticationEnabled, nameof(CamusDBOptions.Default.AuthenticationEnabled));
        Assert.AreEqual("", CamusDBOptions.Default.BootstrapSuperuser, nameof(CamusDBOptions.Default.BootstrapSuperuser));
        Assert.AreEqual("", CamusDBOptions.Default.BootstrapSuperuserPassword, nameof(CamusDBOptions.Default.BootstrapSuperuserPassword));
        Assert.AreEqual("", CamusDBOptions.Default.AccessTokenServerKey, nameof(CamusDBOptions.Default.AccessTokenServerKey));
        Assert.AreEqual(TimeSpan.FromMinutes(15), CamusDBOptions.Default.AccessTokenTtl, nameof(CamusDBOptions.Default.AccessTokenTtl));
        Assert.AreEqual(TimeSpan.FromSeconds(1), CamusDBOptions.Default.AuthenticationCacheTtl, nameof(CamusDBOptions.Default.AuthenticationCacheTtl));
        Assert.AreEqual(8, CamusDBOptions.Default.LoginKdfMaxConcurrency, nameof(CamusDBOptions.Default.LoginKdfMaxConcurrency));
        Assert.AreEqual(20, CamusDBOptions.Default.LoginMaxAttemptsPerMinute, nameof(CamusDBOptions.Default.LoginMaxAttemptsPerMinute));
        Assert.AreEqual(10_000, CamusDBOptions.Default.AuthenticationCacheMaxEntries, nameof(CamusDBOptions.Default.AuthenticationCacheMaxEntries));
        Assert.AreEqual(100_000, CamusDBOptions.Default.LoginRateLimitMaxEntries, nameof(CamusDBOptions.Default.LoginRateLimitMaxEntries));
        Assert.AreEqual(true, CamusDBOptions.Default.RequireTlsWhenAuthEnabled, nameof(CamusDBOptions.Default.RequireTlsWhenAuthEnabled));
        Assert.AreEqual("", CamusDBOptions.Default.NodeSecret, nameof(CamusDBOptions.Default.NodeSecret));
        Assert.AreEqual(64, CamusDBOptions.Default.IndexScanFetchBatchSize, nameof(CamusDBOptions.Default.IndexScanFetchBatchSize));
        Assert.AreEqual(false, CamusDBOptions.Default.LockTracingEnabled, nameof(CamusDBOptions.Default.LockTracingEnabled));
        Assert.AreEqual(false, CamusDBOptions.Default.QueryTracingEnabled, nameof(CamusDBOptions.Default.QueryTracingEnabled));
        Assert.AreEqual(250, CamusDBOptions.Default.RegexMatchTimeoutMs, nameof(CamusDBOptions.Default.RegexMatchTimeoutMs));
        Assert.AreEqual(1024, CamusDBOptions.Default.RegexCacheMaxEntries, nameof(CamusDBOptions.Default.RegexCacheMaxEntries));
        Assert.AreEqual(true, CamusDBOptions.Default.QueryResultCacheEnabled, nameof(CamusDBOptions.Default.QueryResultCacheEnabled));
        Assert.AreEqual(5_000, CamusDBOptions.Default.QueryResultCacheDefaultTtlMs, nameof(CamusDBOptions.Default.QueryResultCacheDefaultTtlMs));
        Assert.AreEqual(1_024, CamusDBOptions.Default.QueryResultCacheMaxEntries, nameof(CamusDBOptions.Default.QueryResultCacheMaxEntries));
        Assert.AreEqual(64 * 1024 * 1024, CamusDBOptions.Default.QueryResultCacheMaxBytes, nameof(CamusDBOptions.Default.QueryResultCacheMaxBytes));
        Assert.AreEqual(1 * 1024 * 1024, CamusDBOptions.Default.QueryResultCacheMaxEntryBytes, nameof(CamusDBOptions.Default.QueryResultCacheMaxEntryBytes));
        Assert.AreEqual(10_000, CamusDBOptions.Default.QueryResultCacheMaxEntryRows, nameof(CamusDBOptions.Default.QueryResultCacheMaxEntryRows));
        Assert.AreEqual(4_096, CamusDBOptions.Default.QueryResultCacheMaxDeps, nameof(CamusDBOptions.Default.QueryResultCacheMaxDeps));
        Assert.AreEqual(2_048, CamusDBOptions.Default.QueryResultCacheMaxPointDeps, nameof(CamusDBOptions.Default.QueryResultCacheMaxPointDeps));
        Assert.AreEqual(256, CamusDBOptions.Default.QueryResultCacheMaxRanges, nameof(CamusDBOptions.Default.QueryResultCacheMaxRanges));
        Assert.AreEqual(250, CamusDBOptions.Default.QueryResultCacheSingleFlightWaitMs, nameof(CamusDBOptions.Default.QueryResultCacheSingleFlightWaitMs));
        Assert.AreEqual(10_000, CamusDBOptions.Default.QueryResultCacheStrictValidationMaxKeys, nameof(CamusDBOptions.Default.QueryResultCacheStrictValidationMaxKeys));
        Assert.AreEqual(10_000, CamusDBOptions.Default.QueryResultCacheSweepIntervalMs, nameof(CamusDBOptions.Default.QueryResultCacheSweepIntervalMs));
    }

    /// <summary>
    /// A <c>with</c> override must produce an independent instance: the shared
    /// <see cref="CamusDBOptions.Default"/> keeps its value, which is the property that lets two
    /// differently-configured engines (or tests) coexist instead of overwriting each other.
    /// </summary>
    [Test]
    public void OverridingAnOptionLeavesTheDefaultsUntouched()
    {
        bool originalSpill = CamusDBOptions.Default.SpillEnabled;

        CamusDBOptions overridden = CamusDBOptions.Default with { SpillEnabled = !originalSpill };

        Assert.AreEqual(!originalSpill, overridden.SpillEnabled);
        Assert.AreEqual(originalSpill, CamusDBOptions.Default.SpillEnabled);
        Assert.AreNotSame(CamusDBOptions.Default, overridden);
    }

    /// <summary>
    /// The spill threshold is derived from two other options rather than stored, so an override of
    /// either must be reflected immediately. A stored copy would go stale the moment a caller wrote
    /// <c>with { ForceSpillThresholdRows = … }</c> and would silently keep the old threshold.
    /// </summary>
    [Test]
    public void EffectiveSpillThresholdFollowsItsInputs()
    {
        CamusDBOptions options = CamusDBOptions.Default with { SpillThresholdRows = 1234 };
        Assert.AreEqual(1234, options.SpillEffectiveThreshold);

        options = options with { ForceSpillThresholdRows = 7 };
        Assert.AreEqual(7, options.SpillEffectiveThreshold);

        options = options with { ForceSpillThresholdRows = null };
        Assert.AreEqual(1234, options.SpillEffectiveThreshold);
    }
}
