
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// Processes one claimed span of one TTL run: read a batch of rows within the span's bounds, keep the
/// expired ones, delete them, checkpoint, repeat until the span is exhausted.
///
/// <para><b>Read and delete are separate transactions, and that is a requirement rather than a
/// tuning choice.</b> One transaction spanning the whole scan would hold locks for its entire duration
/// and blow past <c>MaxMutationsPerTransaction</c> on any table worth expiring. Small, short
/// transactions are also the only thing that actually protects foreground latency: the admission
/// priority below orders who gets in the door, but once admitted a <c>Background</c> delete holds
/// ordinary exclusive locks and blocks a foreground writer exactly as a <c>Normal</c> one would.</para>
///
/// <para><b>Checkpoint after commit, never before.</b> A checkpoint that ran ahead of its deletes would
/// permanently skip rows on the next resume. One that lags merely re-scans a batch whose rows are
/// already gone — wasted work, not lost data. The asymmetry decides the ordering.</para>
///
/// <para><b>Ownership and load are re-checked between batches, not only at span start.</b> A lease can
/// lapse during a long stall and a load surge can arrive at any time; a sweep that only checked at the
/// start would keep deleting through both.</para>
/// </summary>
internal sealed class TtlSpanSweeper
{
    private readonly TtlSpanCoordinator coordinator;
    private readonly RowDeleter rowDeleter;
    private readonly Func<int> foregroundLoad;

    /// <summary>
    /// Configuration for this engine; injected, never ambient. Swapped by <see cref="ApplyOptions"/>:
    /// per-use reads (TTL settings resolution, the load-pause threshold) pick the new snapshot up at
    /// their next read, so a runtime change is honored without a restart.
    /// </summary>
    private CamusDBOptions options;

    private readonly ILogger<ICamusDB> logger;

    /// <summary>
    /// Test-only fault injection: when set and it returns true for a chunk, that chunk's delete
    /// transaction is failed as though its commit had not resolved.
    ///
    /// <para>Exists because the property that matters here — the checkpoint never advancing past a row
    /// whose delete did not land — is only observable when a delete actually fails, and a real failure
    /// (a partition, a lost coordinator, an exceeded mutation budget) cannot be produced reliably from a
    /// test. Nothing in production reads this; it is null on every non-test path.</para>
    /// </summary>
    internal Func<IReadOnlyList<ObjectIdValue>, bool>? DeleteChunkFaultInjector { get; set; }

    public TtlSpanSweeper(
        TtlSpanCoordinator coordinator,
        RowDeleter rowDeleter,
        Func<int> foregroundLoad,
        CamusDBOptions options,
        ILogger<ICamusDB> logger)
    {
        this.coordinator = coordinator;
        this.rowDeleter = rowDeleter;
        this.foregroundLoad = foregroundLoad;
        this.options = options;
        this.logger = logger;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic; each read
    /// site pins the field once per use, so a span batch in flight keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Sweeps one span to completion (or until it loses the span, is cancelled, or is paused by load).
    /// Returns the rows deleted and the rows spared by the delete-time re-check.
    /// </summary>
    public async Task<(long deleted, long skipped, long failed)> SweepSpanAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TtlRunManifest manifest,
        TtlSettings ttl,
        int spanIndex,
        long claimToken,
        TtlRateLimiter selectLimiter,
        TtlRateLimiter deleteLimiter,
        CancellationToken ct)
    {
        (ObjectIdValue? spanStart, ObjectIdValue? spanEnd) = TtlSpanCoordinator.SpanBounds(spanIndex, manifest.SpanCount);

        // `persisted` is the exact record currently stored for this span, or null if there is none. It is
        // the compare operand for every checkpoint write, so it must be the object that was actually
        // written — not a mutated copy — or the compare-and-set can never match.
        TtlSpanCheckpoint? persisted = await coordinator.ReadCheckpointAsync(
            database.Id, manifest.TableId, spanIndex, manifest.RunId, ct).ConfigureAwait(false);

        if (persisted?.Done == true)
            return (0, 0, 0);

        // Two independent lower bounds. spanStart is the inclusive first row id of this span and never
        // changes; resumeAfter is the exclusive "already handled up to here" mark and advances as the
        // sweep progresses. Folding the resume mark into the span start would drop the row sitting
        // exactly on the span boundary, which no worker would then ever visit.
        ObjectIdValue? resumeAfter = persisted is { LastRowIdHex.Length: 24 }
            ? ObjectId.ToValue(persisted.LastRowIdHex)
            : null;

        // The predicate comes from the MANIFEST, not from the live settings: a run means one thing for
        // its whole life, and re-reading the configuration here is what would let a mid-run ALTER change
        // the answer for spans that had not started yet.
        long cutoffEpochMs = TtlExpiryPredicate.CutoffEpochMs(manifest.Horizon, manifest.GraceMs);
        string expirationColumn = manifest.ExpirationColumn;

        // Counters carried by the checkpoint are cumulative for the span across every attempt, so a
        // resumed span reports the whole span's work rather than restarting its tally. The persisted
        // totals are recomputed from this baseline each write instead of being incremented in place —
        // incrementing a field that is also re-read on resume double-counts the moment anything retries.
        long baseDeleted = persisted?.RowsDeleted ?? 0;
        long baseSkipped = persisted?.RowsSkipped ?? 0;
        long baseFailed = persisted?.RowsFailed ?? 0;
        string lastPersistedRowIdHex = persisted?.LastRowIdHex ?? "";

        long totalDeleted = 0;
        long totalSkipped = 0;
        long totalFailed = 0;
        ObjectIdValue? advanceTo;

        while (!ct.IsCancellationRequested)
        {
            // Between-batch re-checks. Losing the span matters most: another worker has reclaimed it and
            // is deleting the same rows, so continuing would double the work and the counters.
            if (!coordinator.StillOwnsSpan(database.Id, manifest.TableId, spanIndex, claimToken))
            {
                logger.LogWarning(
                    "TTL span {Span} of table {Table} was lost mid-sweep; stopping so its new owner can continue",
                    spanIndex, manifest.TableName);
                break;
            }

            if (LoadExceeded())
                break;

            // Operator intent must take effect promptly. Pausing or resetting TTL is something a person
            // does because the sweep is causing a problem right now; honouring it only at the next run
            // would leave batches deleting for however long the current span takes.
            TtlSettings live = TtlSettings.Resolve(table.Schema.Settings, options);
            if (!live.IsActive)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "TTL was paused or cleared on table {Table} mid-span; stopping after the current batch",
                        manifest.TableName);
                break;
            }

            (List<ObjectIdValue> candidates, ObjectIdValue? lastScanned, bool exhausted) =
                await ScanBatchAsync(database, table, ttl, expirationColumn, cutoffEpochMs, spanStart, resumeAfter, spanEnd, selectLimiter, ct)
                    .ConfigureAwait(false);

            bool deleteFailed = false;

            if (candidates.Count > 0)
            {
                DeleteOutcome outcome = await DeleteBatchAsync(
                    database, table, ttl, expirationColumn, candidates, cutoffEpochMs, ct).ConfigureAwait(false);

                totalDeleted += outcome.Deleted;
                totalSkipped += outcome.Skipped;
                totalFailed += outcome.Failed;
                deleteFailed = outcome.Failed_Unresolved;

                // The checkpoint may only advance to a row that is provably handled. When a delete chunk
                // did not commit, that is the last row of the last chunk that did — not the last row
                // scanned. Advancing to the latter would step over candidates whose deletes never
                // landed, and nothing in this run would look at them again.
                advanceTo = deleteFailed ? outcome.LastSafeRowId : lastScanned;

                await deleteLimiter.ThrottleAsync(outcome.Deleted + outcome.Skipped, ct).ConfigureAwait(false);
            }
            else
            {
                advanceTo = lastScanned;
            }

            // Checkpoint only after the deletes for this batch have committed (see the class note).
            if (advanceTo is not null)
            {
                resumeAfter = advanceTo;
                lastPersistedRowIdHex = advanceTo.Value.ToString();
            }

            TtlSpanCheckpoint next = new()
            {
                RunId = manifest.RunId,
                LastRowIdHex = lastPersistedRowIdHex,
                // A span with an unresolved delete is not finished, whatever the scan reported.
                Done = exhausted && !deleteFailed,
                RowsDeleted = baseDeleted + totalDeleted,
                RowsSkipped = baseSkipped + totalSkipped,
                RowsFailed = baseFailed + totalFailed,
            };

            if (!await coordinator.TryWriteCheckpointAsync(
                    database.Id, manifest.TableId, spanIndex, claimToken, persisted, next, ct).ConfigureAwait(false))
            {
                // The write was refused: this worker no longer owns the span, or a newer owner has
                // already advanced it. Continuing would delete rows a second worker is also deleting and
                // report their work twice, so the only correct move is to stop and let the owner finish.
                logger.LogWarning(
                    "TTL span {Span} of table {Table} declined a checkpoint write; another worker owns it",
                    spanIndex, manifest.TableName);
                break;
            }

            persisted = next;

            // End the attempt after an unresolved delete rather than retrying in place: the checkpoint
            // now sits just before the failed rows, so the next tick — or whichever worker next claims
            // this span — resumes exactly there. Spinning here would hammer a contended row while the
            // rest of the run waits.
            if (deleteFailed || exhausted)
                break;
        }

        return (totalDeleted, totalSkipped, totalFailed);
    }

    /// <summary>
    /// Reads one bounded batch and returns the row ids that look expired, the last row id examined
    /// (the next resume point), and whether the span is exhausted.
    ///
    /// <para>The resume point is the last row <em>examined</em>, not the last row matched: resuming from
    /// the last match would re-scan every non-expiring row between them on each batch, which on a table
    /// that is mostly live rows never advances.</para>
    /// </summary>
    private async Task<(List<ObjectIdValue> candidates, ObjectIdValue? lastScanned, bool exhausted)> ScanBatchAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TtlSettings ttl,
        string expirationColumn,
        long cutoffEpochMs,
        ObjectIdValue? spanStart,
        ObjectIdValue? resumeAfter,
        ObjectIdValue? spanEnd,
        TtlRateLimiter selectLimiter,
        CancellationToken ct)
    {
        List<ObjectIdValue> candidates = [];
        ObjectIdValue? lastScanned = null;
        int scanned = 0;

        // The candidate scan only reads the expiry column, so narrow the per-row decode to it —
        // a wide row's other columns are never materialized. The delete phase re-reads and fully
        // re-checks each candidate under the mutation lock, so this projection cannot lose data.
        HashSet<string> requiredColumns = new(StringComparer.OrdinalIgnoreCase) { expirationColumn };
        RowEncoder.DictionaryDecodeState decodeState = new();

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly,
            priority: TransactionPriority.Background).ConfigureAwait(false);

        try
        {
            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                tx, maxRows: ttl.SelectBatchSize, afterRowId: resumeAfter,
                cancellationToken: ct, untilRowId: spanEnd, fromRowId: spanStart).ConfigureAwait(false))
            {
                scanned++;
                lastScanned = rowId;

                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema, tx.TransactionId, rowId, data,
                    requiredColumns: requiredColumns,
                    visibilitySchemaVersion: table.Schema.Version,
                    decodeState: decodeState).ConfigureAwait(false);

                if (TtlExpiryPredicate.IsExpired(row, expirationColumn, cutoffEpochMs))
                    candidates.Add(rowId);
            }

            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        await selectLimiter.ThrottleAsync(scanned, ct).ConfigureAwait(false);

        // Fewer rows than asked for means the scan reached the span's end.
        return (candidates, lastScanned, scanned < ttl.SelectBatchSize);
    }

    /// <summary>
    /// What one batch of deletes actually achieved. <see cref="LastSafeRowId"/> is the point up to which
    /// every row is provably dealt with — the checkpoint may not pass it.
    /// </summary>
    private readonly record struct DeleteOutcome(
        int Deleted,
        int Skipped,
        int Failed,
        ObjectIdValue? LastSafeRowId,
        bool Failed_Unresolved);

    /// <summary>
    /// Deletes the expired candidates in transactions of at most <c>ttl_delete_batch_size</c> rows,
    /// re-asserting the predicate on each row under its mutation lock.
    ///
    /// <para><b>Stops at the first chunk that does not commit, and says where.</b> Candidates arrive in
    /// ascending row-id order, so a failed chunk marks the exact point past which nothing can be claimed
    /// as handled. Continuing on to later chunks would leave a hole: the sweep would report progress
    /// covering rows it never deleted, the checkpoint would advance past them, and they would not be
    /// reconsidered until an entirely new run — silently retaining data the user believes expired.</para>
    ///
    /// <para><b>A failed commit is an unknown outcome, not a failed delete.</b> The transaction may have
    /// committed remotely and failed on the way back. The rows are therefore treated as neither deleted
    /// nor safe, and the query cache is invalidated anyway — invalidating when nothing changed costs a
    /// re-computation, while not invalidating when the delete did land serves a row that no longer
    /// exists.</para>
    /// </summary>
    private async Task<DeleteOutcome> DeleteBatchAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TtlSettings ttl,
        string expirationColumn,
        List<ObjectIdValue> candidates,
        long cutoffEpochMs,
        CancellationToken ct)
    {
        int deleted = 0;
        int skipped = 0;
        ObjectIdValue? lastSafeRowId = null;

        for (int offset = 0; offset < candidates.Count; offset += ttl.DeleteBatchSize)
        {
            int take = Math.Min(ttl.DeleteBatchSize, candidates.Count - offset);
            List<ObjectIdValue> slice = candidates.GetRange(offset, take);

            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                priority: TransactionPriority.Background).ConfigureAwait(false);

            try
            {
                if (DeleteChunkFaultInjector?.Invoke(slice) == true)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, "Injected TTL delete failure");

                (int batchDeleted, int batchSkipped) = await rowDeleter.DeleteExpiredRowsAsync(
                    table, tx, slice, expirationColumn, cutoffEpochMs, ct).ConfigureAwait(false);

                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

                deleted += batchDeleted;
                skipped += batchSkipped;
                lastSafeRowId = slice[^1];

                // The deletes are ordinary writes, so cached results that depend on this table must be
                // invalidated exactly as a user DELETE would invalidate them.
                if (batchDeleted > 0)
                    database.Cache?.InvalidateByTableId(database.Id, table.Id);
            }
            catch (Exception ex)
            {
                // Conservative: the commit may have landed before the failure surfaced, so anything
                // cached for this table could already be stale.
                database.Cache?.InvalidateByTableId(database.Id, table.Id);

                logger.LogWarning(ex,
                    "TTL delete batch failed on table {Table}; ending this attempt at the last confirmed row so nothing is skipped",
                    table.Name);

                return new DeleteOutcome(deleted, skipped, slice.Count, lastSafeRowId, true);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }

        return new DeleteOutcome(deleted, skipped, 0, lastSafeRowId, false);
    }

    private bool LoadExceeded()
    {
        int threshold = options.TtlLoadPauseThreshold;
        if (threshold <= 0)
            return false; // load-based backoff disabled

        return foregroundLoad() > threshold;
    }
}

/// <summary>
/// Paces work to a rows-per-second budget that is shared by every caller holding the same instance.
///
/// <para><b>Shared state, not shared code.</b> A limiter that merely slept for "however long my own work
/// should have taken" enforces the rate per caller, so running <c>N</c> spans concurrently issues
/// <c>N ×</c> the configured rate — a table capped at 100 deletes/second would emit 800 on eight
/// concurrent spans. A rate limit that scales with concurrency is not a rate limit. So this keeps one
/// monotonic "next free instant": callers atomically reserve their slice of it and wait for their turn,
/// which makes the aggregate rate across all of them the configured one.</para>
///
/// <para>The reservation is a compare-and-swap loop rather than a lock: callers only ever move the
/// cursor forward, so a losing racer simply re-reads and takes the next slot. Timing uses
/// <see cref="Stopwatch"/> ticks — a monotonic source, unaffected by wall-clock adjustment.</para>
///
/// <para><b>Zero means unlimited, not stopped.</b> That is CockroachDB's convention for these
/// parameters and the one users will expect; reading it as "no rows per second" would silently disable
/// the sweep on a table configured with the documented default.</para>
/// </summary>
internal sealed class TtlRateLimiter
{
    private readonly int rowsPerSecond;
    private long nextFreeTicks;

    public TtlRateLimiter(int rowsPerSecond)
    {
        this.rowsPerSecond = rowsPerSecond;
        nextFreeTicks = Stopwatch.GetTimestamp();
    }

    public async Task ThrottleAsync(long rowsProcessed, CancellationToken ct)
    {
        if (rowsPerSecond <= 0 || rowsProcessed <= 0)
            return;

        long costTicks = (long)((double)rowsProcessed / rowsPerSecond * Stopwatch.Frequency);
        if (costTicks <= 0)
            return;

        long now = Stopwatch.GetTimestamp();
        long start;

        while (true)
        {
            long current = Interlocked.Read(ref nextFreeTicks);

            // A cursor left behind by an idle period must not bank credit: reserving from `now` rather
            // than from the stale value stops a burst equal to however long the sweep was quiet.
            start = Math.Max(current, now);

            if (Interlocked.CompareExchange(ref nextFreeTicks, start + costTicks, current) == current)
                break;
        }

        long waitTicks = start - now;
        if (waitTicks <= 0)
            return;

        int delayMs = (int)Math.Min(int.MaxValue, waitTicks * 1000L / Stopwatch.Frequency);
        if (delayMs > 0)
            await Task.Delay(delayMs, ct).ConfigureAwait(false);
    }
}
