
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Integration tests for automatic background <c>ANALYZE</c>: staleness detection, the throttled
/// lock-free background collector, the disable switch, and load-based backoff. These drive the real
/// stack (CommandExecutor → AutoAnalyzeScheduler → TableAnalyzer.AnalyzeBackgroundAsync →
/// StatisticsManager) against an in-memory Kahuna node.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestAutoAnalyze : BaseTest
{
    // Config is process-global static; snapshot and restore around each test so tuning one test's
    // thresholds can't leak into others.

    [SetUp]
    public void SnapshotConfig()
    {
    }

    [TearDown]
    public void RestoreConfig()
    {
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Auto-analyze configuration for a case. The scheduler and analyzer fix these when the engine is
    /// built, so a case states what it needs here rather than assigning process-wide values around it.
    /// The returned value is also what the case asserts against, so the thresholds under test and the
    /// thresholds being enforced cannot drift apart.
    /// </summary>
    private CamusDBOptions AutoAnalyze(
        bool enabled = true,
        double fractionStaleRows = 0.0,
        long minStaleRows = 5,
        int? loadPauseThreshold = null,
        int? maxRowsPerSecond = null,
        int? histogramSampleRows = null) =>
        Options with
        {
            AutoAnalyzeEnabled = enabled,
            AutoAnalyzeFractionStaleRows = fractionStaleRows,
            AutoAnalyzeMinStaleRows = minStaleRows,
            AutoAnalyzeLoadPauseThreshold = loadPauseThreshold ?? Options.AutoAnalyzeLoadPauseThreshold,
            AutoAnalyzeMaxRowsPerSecond = maxRowsPerSecond ?? Options.AutoAnalyzeMaxRowsPerSecond,
            AutoAnalyzeHistogramSampleRows = histogramSampleRows ?? Options.AutoAnalyzeHistogramSampleRows,
        };

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupRobotsTable(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",      new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task InsertRobotsAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname,
        int count, int baseYear = 2000)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(baseYear + i)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    private static async Task RunManualAnalyzeAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string tableName)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: txn, database: dbname, sql: $"ANALYZE {tableName}", parameters: null));
        await foreach (QueryResultRow _ in cursor) { }
        await database.Transactions.CommitAsync(txn);
    }

    // ── Tests ────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A table whose mutations since the last ANALYZE cross the staleness threshold is picked up by a
    /// background sweep, which rebuilds its statistics (exact row count over the full table) and
    /// resets the staleness counter.
    /// </summary>
    [Test]
    public async Task StaleTableIsBackgroundAnalyzedAndResetsStaleness()
    {
        // fractionStaleRows 0.0: any churn past the floor counts as stale.
        CamusDBOptions options = AutoAnalyze(enabled: true, fractionStaleRows: 0.0, minStaleRows: 5);

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        // Establish a fresh baseline: manual ANALYZE loads stats and clears the mutation counter.
        await InsertRobotsAsync(executor, database, dbname, 10);
        await RunManualAnalyzeAsync(executor, database, dbname, "robots");
        Assert.AreEqual(0, executor.Statistics.GetMutationsSinceAnalyze(database, table),
            "Manual ANALYZE must reset the mutation counter");

        // Now churn past the threshold.
        await InsertRobotsAsync(executor, database, dbname, 20, baseYear: 3000);
        Assert.IsTrue(
            executor.Statistics.IsStale(database, table, options.AutoAnalyzeFractionStaleRows, options.AutoAnalyzeMinStaleRows),
            "Table with 20 mutations over threshold must be stale");

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();

        Assert.GreaterOrEqual(analyzed, 1, "Background sweep must analyze at least the stale robots table");

        long? rowCount = executor.Statistics.GetRowCountEstimate(database, table);
        Assert.AreEqual(30, rowCount, "Background ANALYZE must record the exact full-table row count (10 + 20)");

        Assert.AreEqual(0, executor.Statistics.GetMutationsSinceAnalyze(database, table),
            "Background ANALYZE must reset the mutation counter");
        Assert.IsFalse(
            executor.Statistics.IsStale(database, table, options.AutoAnalyzeFractionStaleRows, options.AutoAnalyzeMinStaleRows),
            "Table must no longer be stale after a background ANALYZE");

        // NDV was rebuilt for the indexed 'year' column.
        Assert.IsNotNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "Background ANALYZE must populate column NDV");
    }

    /// <summary>The master switch fully disables the feature: a stale table is left untouched.</summary>
    [Test]
    public async Task DisabledFlagSkipsSweep()
    {
        CamusDBOptions options = AutoAnalyze(enabled: false, fractionStaleRows: 0.0, minStaleRows: 1);


        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 10);

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();

        Assert.AreEqual(0, analyzed, "No table may be analyzed while AutoAnalyzeEnabled is false");
        Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "Statistics must not be built while auto-analyze is disabled");
    }

    /// <summary>Under foreground load above the pause threshold, the sweep backs off and analyzes nothing.</summary>
    [Test]
    public async Task LoadBackoffSkipsSweep()
    {
        CamusDBOptions options = AutoAnalyze(enabled: true, fractionStaleRows: 0.0, minStaleRows: 5, loadPauseThreshold: 4);


        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        await InsertRobotsAsync(executor, database, dbname, 10);
        await RunManualAnalyzeAsync(executor, database, dbname, "robots");
        await InsertRobotsAsync(executor, database, dbname, 20, baseYear: 3000);

        // Simulate a busy node: report more in-flight foreground work than the threshold allows.
        executor.ForegroundLoadProbe = () => 999;

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();

        Assert.AreEqual(0, analyzed, "Sweep must back off entirely when foreground load exceeds the threshold");
        Assert.IsTrue(
            executor.Statistics.IsStale(database, table, options.AutoAnalyzeFractionStaleRows, options.AutoAnalyzeMinStaleRows),
            "Table stays stale after a load-skipped sweep (retried when quieter)");
    }

    /// <summary>
    /// Publishing a completed ANALYZE must preserve row/index deltas from writes that committed after
    /// the scan snapshot but before publication (the delta-merge fix). The scan reports its snapshot
    /// counts against a captured baseline; concurrent inserts must survive rather than be clobbered.
    /// </summary>
    [Test]
    public async Task PublishPreservesConcurrentCommittedDeltas()
    {
        CamusDBOptions options = AutoAnalyze();

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        await InsertRobotsAsync(executor, database, dbname, 10);
        await RunManualAnalyzeAsync(executor, database, dbname, "robots");

        // Baseline as of "scan start": 10 rows, index has 10 entries.
        StatisticsManager.AnalyzeBaseline baseline =
            await executor.Statistics.CaptureAnalyzeBaselineAsync(database, table);
        Assert.AreEqual(10, baseline.RowCount);

        // A write commits AFTER the scan snapshot but BEFORE publication.
        await InsertRobotsAsync(executor, database, dbname, 5, baseYear: 5000);

        // Publish the scan's snapshot view (it saw 10 rows; baseline was 10).
        await executor.Statistics.PublishAsync(
            database, table,
            scannedRowCount: 10, scanComplete: true,
            scannedMinMax: new Dictionary<string, ColumnMinMax>(),
            scannedIndexCounts: new Dictionary<string, long> { { "year_idx", 10 } },
            histograms: new Dictionary<string, ColumnHistogram>(),
            columnNdv: new Dictionary<string, long>(),
            keyNdv: null,
            baseline,
            analyzedAt: default);

        Assert.AreEqual(15, executor.Statistics.GetRowCountEstimate(database, table),
            "Row count must be scanned (10) + concurrent inserts (5), not clobbered back to 10");
        Assert.AreEqual(15, executor.Statistics.GetIndexEntryCount(database, table, "year_idx"),
            "Index entry count must preserve the concurrent inserts");
    }

    /// <summary>
    /// Cluster-visible discovery must not depend on a warm local cache: after the entry is evicted, a
    /// stale table is still found via the registry + persisted-staleness path and analyzed.
    /// </summary>
    [Test]
    public async Task DiscoversStaleTableFromPersistedStateAfterEviction()
    {
        CamusDBOptions options = AutoAnalyze(enabled: true, fractionStaleRows: 0.0, minStaleRows: 5);


        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        await InsertRobotsAsync(executor, database, dbname, 10);
        await RunManualAnalyzeAsync(executor, database, dbname, "robots");
        await InsertRobotsAsync(executor, database, dbname, 20, baseYear: 3000);

        // Persist the tracked mutation counter, then drop the in-memory cache so discovery must reload
        // staleness from KV (the cross-node path).
        await executor.Statistics.FlushAsync(database, table);
        executor.Statistics.EvictForTesting(database, table);

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();

        Assert.GreaterOrEqual(analyzed, 1, "Discovery must find the stale table from persisted state alone");
        Assert.AreEqual(30, executor.Statistics.GetRowCountEstimate(database, table));
    }

    /// <summary>
    /// A load surge that arrives after a background scan has started must cancel it at the next batch
    /// boundary, persisting nothing (the table stays stale for a later retry).
    /// </summary>
    [Test]
    public async Task BackgroundAnalyzeCancelsMidScanUnderLoad()
    {
        CamusDBOptions options = AutoAnalyze();

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        // More than the analyzer's mid-scan check interval (1000 rows) so the pause callback is hit.
        await InsertRobotsAsync(executor, database, dbname, 1100);

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await executor.RunBackgroundAnalyzeForTestsAsync(
                database, table, shouldPause: () => true, CancellationToken.None),
            "A persistent load surge must cancel the scan mid-flight");

        Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "A cancelled background analyze must not publish any statistics");
    }

    /// <summary>
    /// Publication is a single atomic transaction: if the flush fails after staging the value but
    /// before commit, the persisted stats stay entirely at the prior generation — no partial write, no
    /// mixed old/new fields. A never-analyzed table whose failed background analyze must leave no
    /// histograms/NDV in KV.
    /// </summary>
    [Test]
    public async Task PartialPublishFailureLeavesPersistedStatsUnchanged()
    {
        CamusDBOptions options = AutoAnalyze();

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 20);

        // Inject a flush fault so the analyze's publish transaction rolls back after staging.
        executor.Statistics.FailFlushForTesting = true;
        try
        {
            // The scan completes; only the publish flush faults. FlushAsync swallows the fault, so the
            // call returns, but nothing is committed to KV.
            await executor.RunBackgroundAnalyzeForTestsAsync(database, table, shouldPause: null, CancellationToken.None);
        }
        finally
        {
            executor.Statistics.FailFlushForTesting = false;
        }

        // Drop the in-memory (optimistically-updated) generation and reload only what reached KV.
        executor.Statistics.EvictForTesting(database, table);
        await executor.Statistics.LoadByIdAsync(database, table.Id);

        Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "A failed publish must persist no NDV (atomic rollback, not a partial write)");
        Assert.IsNull(executor.Statistics.GetColumnHistogram(database, table, "year"),
            "A failed publish must persist no histogram (atomic rollback, not a partial write)");
    }

    /// <summary>
    /// The per-table publish fence guarantees exactly-once during failover: when another owner holds
    /// the analyze fence, this node's background analyze must abort cleanly at publish time — throwing
    /// and persisting nothing — rather than double-writing the generation.
    /// </summary>
    [Test]
    public async Task FenceHeldElsewhereAbortsPublishWithoutPersisting()
    {
        CamusDBOptions options = AutoAnalyze();

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 10);

        // Simulate another node holding the per-table analyze fence.
        KvTransaction holderTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        string fenceKey = $"{database.Id}/meta/analyze:{table.Id}";
        (KeyValueResponseType lockType, _, _, _) = await database.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
            holderTx.TransactionId, fenceKey, 0, KeyValueDurability.Persistent, CancellationToken.None,
            coordinatorKey: holderTx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.AreEqual(KeyValueResponseType.Locked, lockType, "Test setup must hold the analyze fence");

        try
        {
            Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await executor.RunBackgroundAnalyzeForTestsAsync(database, table, shouldPause: null, CancellationToken.None),
                "Background analyze must abort when another owner holds the analyze fence");

            Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "year"),
                "A fenced-out analyze must not publish any statistics");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(holderTx);
        }
    }

    /// <summary>
    /// The background scan must honor the rows/second throttle: analyzing more rows than the cap
    /// allows per second must take a proportional amount of wall-clock time, not run flat out.
    /// </summary>
    [Test]
    public async Task BackgroundAnalyzeRespectsRowRateThrottle()
    {
        // maxRowsPerSecond 500: deliberately slow, so the throttle is observable.
        CamusDBOptions options = AutoAnalyze(enabled: true, fractionStaleRows: 0.0, minStaleRows: 5, maxRowsPerSecond: 500);

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 600);

        var sw = Stopwatch.StartNew();
        await executor.RunBackgroundAnalyzeForTestsAsync(database, table, shouldPause: null, CancellationToken.None);
        sw.Stop();

        // 600 rows at 500 rows/s ≈ 1.2s. Assert the throttle clearly engaged (well above instant),
        // with a generous lower bound so the test is not timing-flaky.
        Assert.Greater(sw.ElapsedMilliseconds, 700,
            $"Throttle must pace the scan to ~{options.AutoAnalyzeMaxRowsPerSecond} rows/s (took {sw.ElapsedMilliseconds} ms)");
    }

    /// <summary>
    /// A foreground writer must run concurrently with a background analyze without blocking or
    /// aborting — the background scan takes a lock-free snapshot. The write committing during the
    /// scan must also be preserved by the delta-merge at publication (row count reflects it).
    /// </summary>
    [Test]
    public async Task ForegroundWriteProceedsAndIsPreservedDuringBackgroundAnalyze()
    {
        // maxRowsPerSecond 2000: slow enough that the scan overlaps the write.
        CamusDBOptions options = AutoAnalyze(enabled: true, fractionStaleRows: 0.0, minStaleRows: 5, maxRowsPerSecond: 2000);

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 3000);

        // Start the background analyze (~1.5s at 2000 rows/s) and, while it scans, commit a foreground
        // write. It must neither block nor throw.
        Task analyzeTask = executor.RunBackgroundAnalyzeForTestsAsync(database, table, shouldPause: null, CancellationToken.None);
        await InsertRobotsAsync(executor, database, dbname, 5, baseYear: 9000);

        await analyzeTask; // completes without deadlock/abort

        Assert.AreEqual(3005, executor.Statistics.GetRowCountEstimate(database, table),
            "Row count must be scanned (3000) plus the write that committed during the scan (5)");
    }

    /// <summary>
    /// On a table larger than the reservoir sample cap, the background collector still records the
    /// exact row count but builds its histogram from at most the sampled number of values — proving
    /// peak memory is bounded by the sample size, not the table size.
    /// </summary>
    [Test]
    public async Task BackgroundAnalyzeSamplesLargeTable()
    {
        // histogramSampleRows 50: a tiny cap so sampling is observable.
        CamusDBOptions options = AutoAnalyze(enabled: true, fractionStaleRows: 0.0, minStaleRows: 5, histogramSampleRows: 50);

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(options);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        await InsertRobotsAsync(executor, database, dbname, 200);

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();
        Assert.GreaterOrEqual(analyzed, 1);

        Assert.AreEqual(200, executor.Statistics.GetRowCountEstimate(database, table),
            "Row count must be exact even when the histogram is sampled");

        ColumnHistogram? histogram = executor.Statistics.GetColumnHistogram(database, table, "year");
        Assert.IsNotNull(histogram, "Background ANALYZE must build a histogram for the indexed column");
        Assert.LessOrEqual(histogram!.TotalRows, 50,
            "Histogram must be built from the bounded sample (<= AutoAnalyzeHistogramSampleRows), not all 200 rows");
    }
}
