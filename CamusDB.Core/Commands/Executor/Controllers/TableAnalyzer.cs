
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Implements <c>ANALYZE TABLE</c>: full-scan (or sampled) statistics collection.
///
/// For tables with row count ≤ <see cref="CamusDBOptions.StatsAnalyzeSampleRows"/> the entire
/// table is scanned; for larger tables the first N rows in storage order are used as the
/// sample. v1 uses exact scan-based counting; HyperLogLog/reservoir sampling are deferred.
///
/// One pass builds all statistics simultaneously:
///   • row count
///   • per-column min/max (all indexed columns)
///   • per-column NDV (approximate — exact when within sample limit)
///   • per-column equi-depth histograms
///   • per-composite-index-prefix key-tuple NDV
///   • per-index entry counts
///
/// The scan always runs under ANALYZE's own read-only snapshot rather than the caller's
/// transaction, so published statistics describe committed data only. That is also why a caller
/// with uncommitted writes is rejected up front (<see cref="CamusDBErrorCodes.AnalyzeRequiresNoPendingWrites"/>):
/// the snapshot would block on the caller's own unresolved write intents, which only the blocked
/// caller could resolve.
/// </summary>
internal sealed class TableAnalyzer
{
    private readonly StatisticsManager statistics;

    /// <summary>Configuration for this engine; injected, never ambient.</summary>
    private readonly CamusDBOptions options;

    public TableAnalyzer(StatisticsManager statistics, CamusDBOptions options)
    {
        this.statistics = statistics;
        this.options = options;
    }

    internal async Task<QueryResultRow> AnalyzeAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction callerTxn)
    {
        // The scan below runs under its own snapshot, which cannot see past the caller's unresolved
        // write intents: the storage layer makes a snapshot read wait for an intent whose commit
        // timestamp is undetermined, and nothing but this caller can resolve those intents — it is
        // blocked here. Refuse instead of stalling until the transaction is reaped.
        if (callerTxn.HasPendingWrites)
            throw new CamusDBException(
                CamusDBErrorCodes.AnalyzeRequiresNoPendingWrites,
                $"ANALYZE cannot run on transaction {callerTxn.UniqueId} because it has uncommitted writes; " +
                "commit or roll back before collecting statistics"
            );

        int sampleLimit = options.StatsAnalyzeSampleRows;
        int bucketCount = options.StatsHistogramBuckets;
        if (bucketCount < 1) bucketCount = 1;

        // Capture the live counter baseline BEFORE pinning the scan snapshot (below), mirroring
        // AnalyzeBackgroundAsync. The reverse order has a systematic under-count: a row committed
        // between snapshot-pin and baseline-capture is inside the baseline but invisible to the
        // scan, so PublishAsync's `scanned − baseline` correction subtracts it from the live count.
        StatisticsManager.AnalyzeBaseline baseline =
            await statistics.CaptureAnalyzeBaselineAsync(database, table).ConfigureAwait(false);

        // Collect indexed columns and composite index key-column groups.
        List<string> indexedColumns = GetIndexedColumns(table);
        List<(string indexName, string[] keyColumns)> readableIndexes = GetReadableIndexes(table);

        // Per-column accumulators.
        var distinctSets   = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var minMax         = new Dictionary<string, ColumnMinMax>(StringComparer.Ordinal);
        var valueLists     = new Dictionary<string, List<ScalarBound>>(StringComparer.Ordinal);
        // Per-index entry counts (non-null rows).
        var indexCounts    = new Dictionary<string, long>(StringComparer.Ordinal);
        // Per-composite-key-tuple distinct sets.
        var keyDistinct    = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (string col in indexedColumns)
        {
            distinctSets[col] = new HashSet<string>(StringComparer.Ordinal);
            valueLists[col]   = [];
        }

        foreach ((string indexName, string[] keyCols) in readableIndexes)
        {
            // Initialize a distinct-set for every composite prefix of length 2..N.
            // A WHERE col1=… AND col2=… query needs KeyNdv["col1,col2"] even when the index
            // has a third column — ANALYZE must emit all prefix lengths so the optimizer can look them up.
            for (int len = 2; len <= keyCols.Length; len++)
            {
                string sig = StatisticsManager.KeyTupleSignature(keyCols[..len]);
                keyDistinct.TryAdd(sig, new HashSet<string>(StringComparer.Ordinal));
            }

            indexCounts[indexName] = 0;
        }

        long rowCount   = 0;
        bool isSampled  = false;
        long? limit     = sampleLimit > 0 ? sampleLimit : null;

        // Request one row past the sample limit so the sentinel can set isSampled=true;
        // the sentinel is detected below and not counted toward rowCount.
        long? scanLimit = limit.HasValue ? limit.Value + 1 : null;

        // Scan under our own lock-free read-only snapshot (not the caller's transaction), like
        // the background path: statistics must reflect committed data — a scan inside a user
        // transaction would fold that transaction's uncommitted writes into globally published
        // stats — and the snapshot must be pinned AFTER the baseline capture above. The reads
        // are never folded into any validation set, so ANALYZE cannot abort foreground work.
        KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote: false).ConfigureAwait(false);
        Kommander.Time.HLCTimestamp analyzedAt = tx.ReadTimestamp;

        try
        {
            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(tx, maxRows: scanLimit))
            {
                if (limit.HasValue && rowCount >= limit.Value)
                {
                    isSampled = true;
                    break;
                }

                Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data.Span);
                rowCount++;

                foreach (string col in indexedColumns)
                {
                    if (!row.TryGetValue(col, out ColumnValue? val) || val.Type is ColumnType.Null or ColumnType.Bool)
                        continue;

                    ScalarBound bound = ScalarBound.FromColumnValue(val);
                    UpdateMinMax(minMax, col, bound);
                    distinctSets[col].Add(BoundKey(bound));

                    if (IsOrderable(val.Type))
                        valueLists[col].Add(bound);
                }

                // Index entry counts and composite-key NDV.
                foreach ((string indexName, string[] keyCols) in readableIndexes)
                {
                    // An index entry exists only when the first key column is non-null.
                    if (!row.TryGetValue(keyCols[0], out ColumnValue? firstVal) || firstVal.Type == ColumnType.Null)
                        continue;

                    indexCounts[indexName]++;

                    // Accumulate distinct tuple keys for every prefix length 2..N,
                    // mirroring the initialization above.
                    for (int len = 2; len <= keyCols.Length; len++)
                    {
                        string sig = StatisticsManager.KeyTupleSignature(keyCols[..len]);
                        string tupleKey = BuildTupleKey(row, keyCols[..len]);
                        keyDistinct[sig].Add(tupleKey);
                    }
                }
            }
        }
        finally
        {
            // A read-only snapshot holds no locks; always finalize so nothing leaks.
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        // --- Build result dicts ---

        var columnNdv = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string col, HashSet<string> set) in distinctSets)
            columnNdv[col] = set.Count;

        var keyNdv = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string sig, HashSet<string> set) in keyDistinct)
            keyNdv[sig] = set.Count;

        var histograms = new Dictionary<string, ColumnHistogram>(StringComparer.Ordinal);
        foreach ((string col, List<ScalarBound> values) in valueLists)
        {
            if (values.Count == 0) continue;
            histograms[col] = BuildEquiDepthHistogram(values, bucketCount);
        }

        // --- Persist ---

        // One atomic generation: merges any DML that committed during the scan, resets staleness, and
        // writes the whole blob in a single transaction. A sampled scan's row count is a sample, not
        // the true size, so it must not correct the live tracked count (scanComplete: !isSampled).
        await statistics.PublishAsync(
            database, table,
            rowCount, scanComplete: !isSampled,
            minMax, indexCounts, histograms, columnNdv, keyNdv.Count > 0 ? keyNdv : null,
            baseline, analyzedAt).ConfigureAwait(false);

        string status = isSampled
            ? $"sampled {rowCount} rows (table larger than {options.StatsAnalyzeSampleRows})"
            : $"analyzed {rowCount} rows";

        return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "table",   new ColumnValue(ColumnType.String, table.Name) },
            { "status",  new ColumnValue(ColumnType.String, status) },
            { "rows",    new ColumnValue(ColumnType.Integer64, rowCount) },
            { "columns", new ColumnValue(ColumnType.Integer64, (long)columnNdv.Count) },
        });
    }

    /// <summary>
    /// Bounded, throttled, lock-free background variant of <see cref="AnalyzeAsync"/> used by the
    /// automatic-analyze scheduler. Produces the same <see cref="Statistics.Models.TableStatistics"/>
    /// shape but with three hard guarantees that keep it from spiking resources or interfering with
    /// foreground work:
    ///
    /// <list type="bullet">
    ///   <item><b>Constant peak memory.</b> Histograms are built from a fixed-size
    ///   <see cref="ReservoirSampler{T}"/> and NDV from fixed-size <see cref="HyperLogLog"/> sketches,
    ///   so memory does not scale with table size (unlike the manual path's full distinct-sets).
    ///   Row and index-entry counts stay exact (running integers, O(1) memory).</item>
    ///   <item><b>Throttled scan.</b> Rows are paced to <see cref="CamusDBOptions.AutoAnalyzeMaxRowsPerSecond"/>
    ///   with an inter-batch delay and cooperative yield, bounding CPU/IO.</item>
    ///   <item><b>Zero lock contention.</b> It opens its <em>own</em> read-only snapshot transaction
    ///   (no range locks, no read-set folding), so it can neither block nor abort a foreground
    ///   query/writer, and is never itself aborted. It must not reuse a caller's ambient transaction —
    ///   that is why this is a separate method, not a flag on <see cref="AnalyzeAsync"/>.</item>
    /// </list>
    ///
    /// <para><b>All-or-nothing.</b> Statistics are accumulated in bounded memory and persisted only
    /// after a complete scan. If <paramref name="cancellationToken"/> fires, a load surge trips
    /// <paramref name="shouldPause"/>, or leadership is lost (<paramref name="stillOwner"/> returns
    /// false), the scan aborts by throwing and nothing is persisted, so the table stays marked stale
    /// and is retried later — making the whole operation idempotent and safe to resume.</para>
    /// </summary>
    /// <param name="analyzedAt">HLC timestamp recorded as the table's last-analyzed marker.</param>
    /// <param name="stillOwner">Re-checked at each batch boundary and immediately before publishing;
    /// returning false (this node is no longer the elected owner) aborts the scan without persisting,
    /// so a former leader can never publish after losing its lease. Null disables the check (tests).</param>
    /// <param name="shouldPause">Re-checked at each batch boundary; returning true (foreground load
    /// surged) aborts the scan so it retries when the node is quieter. Null disables the check.</param>
    internal async Task AnalyzeBackgroundAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        Kommander.Time.HLCTimestamp analyzedAt,
        Func<CancellationToken, ValueTask<bool>>? stillOwner,
        Func<bool>? shouldPause,
        CancellationToken cancellationToken)
    {
        // Rows between successive leadership/load re-checks. Amortizes the (async) leadership probe
        // while still noticing a surge or a lost lease within a bounded number of rows mid-scan.
        int checkEveryRows = Math.Max(1, options.AutoAnalyzeOwnershipCheckRows);

        // Honor a per-table opt-out that landed on this node's descriptor after discovery selected it
        // (discovery already filters on the fresh meta blob; this covers the disable-during-sweep race).
        if (!table.Schema.AutoStatsCollectionEnabled)
            return;

        int bucketCount = options.StatsHistogramBuckets;
        if (bucketCount < 1) bucketCount = 1;

        int sampleCapacity = options.AutoAnalyzeHistogramSampleRows;
        if (sampleCapacity < 1) sampleCapacity = 1;

        int hllPrecision = options.AutoAnalyzeHllPrecision;
        if (hllPrecision is < 4 or > 16) hllPrecision = 11;

        int maxRowsPerSecond = options.AutoAnalyzeMaxRowsPerSecond;

        List<string> indexedColumns = GetIndexedColumns(table);
        List<(string indexName, string[] keyColumns)> readableIndexes = GetReadableIndexes(table);

        // Bounded accumulators — memory is fixed regardless of how many rows/distinct values exist.
        var reservoirSeed = SeedFor(database, table, analyzedAt);
        var minMax        = new Dictionary<string, ColumnMinMax>(StringComparer.Ordinal);
        var columnHll     = new Dictionary<string, HyperLogLog>(StringComparer.Ordinal);
        var valueSamples  = new Dictionary<string, ReservoirSampler<ScalarBound>>(StringComparer.Ordinal);
        var keyHll        = new Dictionary<string, HyperLogLog>(StringComparer.Ordinal);
        var indexCounts   = new Dictionary<string, long>(StringComparer.Ordinal);

        int colSeed = 0;
        foreach (string col in indexedColumns)
        {
            columnHll[col]    = new HyperLogLog(hllPrecision);
            // Vary each column's reservoir seed so columns don't share an identical retention pattern.
            valueSamples[col] = new ReservoirSampler<ScalarBound>(sampleCapacity, reservoirSeed + (ulong)(++colSeed) * 0x100000001B3UL);
        }

        foreach ((string indexName, string[] keyCols) in readableIndexes)
        {
            for (int len = 2; len <= keyCols.Length; len++)
            {
                string sig = StatisticsManager.KeyTupleSignature(keyCols[..len]);
                keyHll.TryAdd(sig, new HyperLogLog(hllPrecision));
            }
            indexCounts[indexName] = 0;
        }

        // Capture the live counter baseline before pinning the scan snapshot so DML that commits
        // during the (throttled) scan is preserved at publication rather than clobbered.
        StatisticsManager.AnalyzeBaseline baseline =
            await statistics.CaptureAnalyzeBaselineAsync(database, table).ConfigureAwait(false);
        long rowCount = 0;

        // Open our own lock-free read-only snapshot. Under the serializable default this pins a
        // single HLC across every scan page (consistent), takes no range locks, and does not fold
        // reads into any transaction's validation set — so it cannot interfere with foreground work.
        KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        try
        {
            long throttleStartTicks = Environment.TickCount64;

            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in
                table.Store.ScanRows(tx, maxRows: null, afterRowId: null, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data.Span);
                rowCount++;

                foreach (string col in indexedColumns)
                {
                    if (!row.TryGetValue(col, out ColumnValue? val) || val.Type is ColumnType.Null or ColumnType.Bool)
                        continue;

                    ScalarBound bound = ScalarBound.FromColumnValue(val);
                    UpdateMinMax(minMax, col, bound);
                    columnHll[col].Add(BoundKey(bound));

                    if (IsOrderable(val.Type))
                        valueSamples[col].Add(bound);
                }

                foreach ((string indexName, string[] keyCols) in readableIndexes)
                {
                    if (!row.TryGetValue(keyCols[0], out ColumnValue? firstVal) || firstVal.Type == ColumnType.Null)
                        continue;

                    indexCounts[indexName]++;

                    for (int len = 2; len <= keyCols.Length; len++)
                    {
                        string sig = StatisticsManager.KeyTupleSignature(keyCols[..len]);
                        keyHll[sig].Add(BuildTupleKey(row, keyCols[..len]));
                    }
                }

                // At each batch boundary, bail out (without persisting) if the node lost leadership, the
                // foreground load surged, or the table was opted out of automatic collection mid-scan.
                // Cheap-and-lock-free reads, so abandoning here loses nothing.
                if (rowCount % checkEveryRows == 0)
                {
                    if (shouldPause is not null && shouldPause())
                        throw new OperationCanceledException("Auto-analyze paused: foreground load surge");
                    if (stillOwner is not null && !await stillOwner(cancellationToken).ConfigureAwait(false))
                        throw new OperationCanceledException("Auto-analyze aborted: leadership lost");
                    if (!table.Schema.AutoStatsCollectionEnabled)
                        throw new OperationCanceledException("Auto-analyze aborted: table opted out mid-scan");
                }

                await ThrottleAsync(rowCount, throttleStartTicks, maxRowsPerSecond, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            // A read-only snapshot holds no locks; finalizing it is a cheap no-op (or a bare rollback
            // for a promoted identity). Always finalize so nothing leaks even on cancellation.
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        // Reaching here means the scan completed without cancellation — safe to build and persist.
        var columnNdv = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string col, HyperLogLog hll) in columnHll)
            columnNdv[col] = hll.Estimate();

        var keyNdv = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string sig, HyperLogLog hll) in keyHll)
            keyNdv[sig] = hll.Estimate();

        var histograms = new Dictionary<string, ColumnHistogram>(StringComparer.Ordinal);
        foreach ((string col, ReservoirSampler<ScalarBound> sampler) in valueSamples)
        {
            if (sampler.Items.Count == 0) continue;
            // Build the histogram over the bounded sample. Bucket boundaries and cumulative
            // fractions are sample-invariant; the estimator scales them by the exact RowCount below.
            histograms[col] = BuildEquiDepthHistogram([.. sampler.Items], bucketCount);
        }

        // Publish under a cluster-visible per-table fence so that, during a failover, exactly one node
        // writes the new generation. The scan itself was lock-free (no foreground interference); the
        // fence is taken only for the brief publish. An exclusive lock on the analyze key excludes every
        // other node, and re-confirming ownership *under* the lock guarantees a former leader that lost
        // its lease mid-scan aborts here instead of publishing. A node that cannot take the fence
        // (another node holds it) or is no longer the owner throws — nothing is persisted, so the table
        // stays stale and is retried by the current owner.
        // Background priority: publishing refreshed statistics is maintenance and should yield to
        // user traffic at the door. Note the analyze *scan* is not gated at all — it runs on a
        // zero-identity snapshot with no coordinator session — so this brief publish is the only
        // part of auto-analyze the admission gate ever sees. What actually bounds the scan's impact
        // is its row-rate cap and foreground-load backoff, not this tag.
        KvTransaction fenceTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            priority: TransactionPriority.Background).ConfigureAwait(false);
        try
        {
            string fenceKey = $"{database.Id}/meta/analyze:{table.Id}";
            (KeyValueResponseType lockType, _, _, _) = await database.Kahuna.Kahuna.LocateAndTryAcquireExclusiveLock(
                fenceTx.TransactionId, fenceKey, 0, KeyValueDurability.Persistent, cancellationToken,
                coordinatorKey: fenceTx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()).ConfigureAwait(false);

            if (lockType != KeyValueResponseType.Locked)
                throw new OperationCanceledException("Auto-analyze fence held by another node");

            if (stillOwner is not null && !await stillOwner(cancellationToken).ConfigureAwait(false))
                throw new OperationCanceledException("Auto-analyze aborted before publish: leadership lost");

            // Final opt-out fence: a SET (... = false) that committed during the scan must prevent
            // publication, so the scheduler never publishes statistics for a now-disabled table.
            if (!table.Schema.AutoStatsCollectionEnabled)
                throw new OperationCanceledException("Auto-analyze aborted before publish: table opted out");

            // One atomic generation that merges any DML committed during the scan and resets staleness.
            await statistics.PublishAsync(
                database, table,
                rowCount, scanComplete: true,
                minMax, indexCounts, histograms, columnNdv, keyNdv.Count > 0 ? keyNdv : null,
                baseline, analyzedAt).ConfigureAwait(false);

            await database.Transactions.CommitAsync(fenceTx).ConfigureAwait(false); // releases the fence
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(fenceTx).ConfigureAwait(false);
            throw;
        }
    }

    // Paces the scan to the configured rows/second. Checked every row but only actually delays when
    // the scan has run ahead of its rate budget, so steady-state overhead is one comparison per row.
    private static async Task ThrottleAsync(long rowsProcessed, long startTicks, int maxRowsPerSecond, CancellationToken cancellationToken)
    {
        if (maxRowsPerSecond <= 0)
            return;

        // Time this many rows *should* have taken at the cap, minus how long they actually took.
        double targetMs = rowsProcessed * 1000.0 / maxRowsPerSecond;
        double elapsedMs = Environment.TickCount64 - startTicks;
        double aheadMs = targetMs - elapsedMs;

        // Only sleep once we're at least a few ms ahead, to avoid thousands of sub-millisecond delays.
        if (aheadMs >= 5.0)
            await Task.Delay((int)Math.Min(aheadMs, 1000), cancellationToken).ConfigureAwait(false);
    }

    // Stable per-(table, run) seed so a reservoir sample is reproducible for a given snapshot.
    private static ulong SeedFor(DatabaseDescriptor database, TableDescriptor table, Kommander.Time.HLCTimestamp analyzedAt)
    {
        ulong seed = 1469598103934665603UL;
        foreach (char c in database.Id) seed = (seed ^ c) * 1099511628211UL;
        foreach (char c in (table.Id ?? "")) seed = (seed ^ c) * 1099511628211UL;
        seed ^= (ulong)analyzedAt.L * 0x9E3779B97F4A7C15UL;
        seed ^= (ulong)analyzedAt.C;
        return seed;
    }

    // --- Private helpers ---

    private static List<string> GetIndexedColumns(TableDescriptor table)
    {
        var cols = new HashSet<string>(StringComparer.Ordinal);
        if (table.Indexes is null) return [];
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadable(kv.Value)) continue;
            if (kv.Value.Columns is { Length: > 0 })
                cols.Add(kv.Value.Columns[0]);
        }
        return [.. cols];
    }

    private static List<(string indexName, string[] keyColumns)> GetReadableIndexes(TableDescriptor table)
    {
        var result = new List<(string, string[])>();
        if (table.Indexes is null) return result;
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadable(kv.Value)) continue;
            if (kv.Value.Columns is { Length: > 0 })
                result.Add((kv.Key, kv.Value.Columns));
        }
        return result;
    }

    private static void UpdateMinMax(Dictionary<string, ColumnMinMax> minMax, string col, ScalarBound bound)
    {
        if (!minMax.TryGetValue(col, out ColumnMinMax? mm))
        {
            minMax[col] = new ColumnMinMax { Min = bound, Max = bound };
            return;
        }
        if (mm.Min is null || bound.CompareTo(mm.Min) < 0) mm.Min = bound;
        if (mm.Max is null || bound.CompareTo(mm.Max) > 0) mm.Max = bound;
    }

    private static string BoundKey(ScalarBound b) => b.Type switch
    {
        ColumnType.Integer64 => b.LongValue.ToString(),
        ColumnType.Float64   => b.FloatValue.ToString("R"),
        ColumnType.Float32   => b.FloatValue.ToString("R"),
        ColumnType.String    => b.StrValue ?? "",
        ColumnType.Id        => b.StrValue ?? "",
        ColumnType.Date      => b.LongValue.ToString(),
        ColumnType.DateTime  => b.LongValue.ToString(),
        _                    => b.Type + ":" + b.LongValue,
    };

    private static bool IsOrderable(ColumnType t) => t is
        ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32 or
        ColumnType.String or ColumnType.Id or ColumnType.Date or ColumnType.DateTime;

    private static string BuildTupleKey(Dictionary<string, ColumnValue> row, string[] keyCols)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < keyCols.Length; i++)
        {
            if (i > 0) sb.Append('\x1F'); // unit-separator, not in SQL identifiers
            if (row.TryGetValue(keyCols[i], out ColumnValue? v) && v.Type != ColumnType.Null)
                sb.Append(BoundKey(ScalarBound.FromColumnValue(v)));
        }
        return sb.ToString();
    }

    private static ColumnHistogram BuildEquiDepthHistogram(List<ScalarBound> values, int bucketCount)
    {
        values.Sort((a, b) => a.CompareTo(b));

        long total = values.Count;
        int bucketsActual = Math.Min(bucketCount, (int)total);
        int bucketSize = (int)Math.Ceiling((double)total / bucketsActual);

        var buckets = new List<ColumnHistogramBucket>(bucketsActual);

        // Iterate by bucket index so the last bucket always ends at total-1, even when total
        // is not a multiple of bucketSize. The previous index-stepping loop stopped short of
        // the tail, leaving a partial run unrepresented and the last UpperBound below the max.
        for (int k = 0; k < bucketsActual; k++)
        {
            int start = k * bucketSize;
            if (start > (int)total - 1) break;
            int end   = Math.Min((k + 1) * bucketSize - 1, (int)total - 1);

            var distinct = new HashSet<string>(StringComparer.Ordinal);
            for (int j = start; j <= end; j++)
                distinct.Add(BoundKey(values[j]));

            buckets.Add(new ColumnHistogramBucket
            {
                UpperBound       = values[end],
                CumulativeRows   = end + 1,
                DistinctInBucket = distinct.Count,
            });
        }

        // Guarantee the last bucket's CumulativeRows equals TotalRows in case rounding
        // caused end+1 to be one short of total.
        if (buckets.Count > 0)
            buckets[^1].CumulativeRows = total;

        // The observed minimum is the first bucket's lower boundary — without it the first
        // bucket cannot be interpolated (values inside it would estimate ~0 rows).
        return new ColumnHistogram
        {
            Buckets   = buckets,
            TotalRows = total,
            MinValue  = total > 0 ? values[0] : null,
        };
    }
}
