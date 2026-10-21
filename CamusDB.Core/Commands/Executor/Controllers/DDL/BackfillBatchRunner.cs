/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Runs one scanned batch of an online backfill against concurrent writers until every row of the
/// batch is done. Both backfills, the fill of an added column (<see cref="TableColumnAdder"/>) and
/// the build of an added index (<see cref="IndexBackfiller"/>), lock the rows of a batch before they
/// read them, so a batch competes with foreground writers for its row locks. Under sustained writes
/// a batch of hundreds of rows loses that race often. A lost attempt commits nothing and costs one
/// lock round trip, but a backfill that gives up after a fixed number of losses fails the whole
/// schema change on a busy table, and the coordinator then drops the index or takes the column back.
///
/// <para><b>A batch is never given up.</b> A loss is retried after an exponential, jittered delay
/// (<see cref="BaseDelayMs"/> doubling up to <see cref="MaxDelayMs"/>). Only a retryable code is a
/// loss (<see cref="SerializableRetryHelper.IsRetryable"/>): a lock another transaction holds, a
/// commit the coordinator refused, a transaction past its lifetime. Any other error, and a
/// cancellation, end the backfill as before. A writer holds a row lock for one transaction at most,
/// and Kahuna expires an abandoned lock with its lease, so every row is free again within a bounded
/// time: a retried batch waits for the writers that hold its rows, and for nothing else.</para>
///
/// <para><b>A lost batch is halved, a won batch is doubled.</b> A loss means that at least one row of
/// the batch was held at that instant. Halving separates the busy rows from the rest, so the free
/// rows are written while the busy ones wait, and a lost attempt wastes less work the longer the
/// contention lasts. A win doubles the batch again, up to the size it was scanned at, so a quiet
/// stretch of the table is crossed at full speed. The parts run in row order, and the call returns
/// only when every row is done, so the caller's resume checkpoint, the last row id of the scanned
/// batch, keeps its meaning: every row up to it is done.</para>
///
/// <para>A loss is logged at Debug. Every <see cref="LossesPerWarning"/> consecutive losses are
/// logged at Warning with the last refusal, because by then the backfill waits on a lock that is
/// held longer than a statement, such as an open explicit transaction.</para>
///
/// <para><b>The row-id scan is retried the same way.</b> Each batch starts with a read-only scan of
/// the next row ids (<see cref="ScanRowIdsAsync"/>). Kahuna can refuse a page of that scan under
/// concurrent writes, and the store reports the refusal with the retryable code. The scan keeps
/// nothing and commits nothing, so it is simply run again after the same delay; a refusal that
/// escaped here used to fail the whole schema change on the busiest tables, the ones an online
/// build exists for.</para>
/// </summary>
internal static class BackfillBatchRunner
{
    private const int BaseDelayMs = 10;

    private const int MaxDelayMs = 250;

    /// <summary>Consecutive losses of one batch between two Warning log lines.</summary>
    internal const int LossesPerWarning = 16;

    /// <summary>
    /// One attempt over a batch: begin a transaction, lock and read the rows, write, commit, and
    /// return the number of rows written. An attempt that fails must have rolled its transaction back.
    /// </summary>
    internal delegate Task<int> BatchAttempt(IReadOnlyList<ObjectIdValue> rowIds, CancellationToken cancellationToken);

    /// <summary>
    /// Test-only seam: observes every lost attempt with the rows it covered and the refusal, so a test
    /// under concurrent writers can tell what the batches lost to. Null in production; a test clears it
    /// after use.
    /// </summary>
    internal static Action<int, CamusDBException>? TestObserveLoss;

    /// <summary>
    /// Test-only seam: the number of upcoming row-id scans to refuse with a retryable code before they
    /// run, so a test can show that a refused scan is retried rather than failing the backfill. Zero in
    /// production; each refusal decrements it.
    /// </summary>
    internal static int TestScanRefusals;

    /// <summary>
    /// Runs <paramref name="attempt"/> over <paramref name="rowIds"/> until every row is done, as the
    /// class summary describes. <paramref name="element"/> names the element under construction in
    /// log lines, for example <c>index 'acct_balance' of table 'accounts'</c>.
    /// </summary>
    internal static async Task<int> RunAsync(
        IReadOnlyList<ObjectIdValue> rowIds,
        BatchAttempt attempt,
        ILogger logger,
        string element,
        CancellationToken cancellationToken)
    {
        int written = 0;
        int start = 0;
        int size = rowIds.Count;
        int consecutiveLosses = 0;

        while (start < rowIds.Count)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int count = Math.Min(size, rowIds.Count - start);
            IReadOnlyList<ObjectIdValue> part = count == rowIds.Count ? rowIds : new RowIdSlice(rowIds, start, count);

            try
            {
                written += await attempt(part, cancellationToken).ConfigureAwait(false);
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
            {
                consecutiveLosses++;
                TestObserveLoss?.Invoke(count, ex);

                int delayMs = DelayFor(consecutiveLosses);

                Log.LogBackfillBatchLost(logger, element, count, consecutiveLosses, delayMs, ex.Message);

                if (consecutiveLosses % LossesPerWarning == 0)
                    Log.LogBackfillBatchWaiting(logger, element, consecutiveLosses, count, ex.Message);

                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);

                size = Math.Max(1, count / 2);
                continue;
            }

            start += count;
            consecutiveLosses = 0;
            size = Math.Min(rowIds.Count, size * 2);
        }

        return written;
    }

    /// <summary>
    /// The next row ids in row-id order after <paramref name="afterRowId"/>, at most
    /// <paramref name="maxRows"/>. Read without a lock in a read-only transaction: only the ids are
    /// kept, and the batch reads each row again under its lock, so a row that changed or disappeared
    /// after this scan is seen as it is then. A scan Kahuna refuses with a retryable code is run again
    /// after the delay of a lost batch, as the class summary describes; any other error ends the
    /// backfill.
    /// </summary>
    internal static async Task<List<ObjectIdValue>> ScanRowIdsAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        ObjectIdValue? afterRowId,
        int maxRows,
        ILogger logger,
        string element,
        CancellationToken cancellationToken)
    {
        int consecutiveLosses = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await ScanRowIdsOnceAsync(database, table, afterRowId, maxRows, cancellationToken).ConfigureAwait(false);
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
            {
                consecutiveLosses++;

                int delayMs = DelayFor(consecutiveLosses);

                Log.LogBackfillScanLost(logger, element, consecutiveLosses, delayMs, ex.Message);

                if (consecutiveLosses % LossesPerWarning == 0)
                    Log.LogBackfillScanWaiting(logger, element, consecutiveLosses, ex.Message);

                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>One attempt of <see cref="ScanRowIdsAsync"/>.</summary>
    private static async Task<List<ObjectIdValue>> ScanRowIdsOnceAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        ObjectIdValue? afterRowId,
        int maxRows,
        CancellationToken cancellationToken)
    {
        if (TestScanRefusals > 0 && Interlocked.Decrement(ref TestScanRefusals) >= 0)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, "Row-id scan refused by the test seam");

        List<ObjectIdValue> rowIds = new(maxRows);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            // The scan keeps no cell, so no out-of-line value is fetched for it.
            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(
                tx, maxRows: maxRows, afterRowId: afterRowId, cancellationToken: cancellationToken, largeValues: LargeValueFetch.Raw).ConfigureAwait(false))
            {
                rowIds.Add(rowId);
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return rowIds;
    }

    /// <summary>
    /// <c>min(BaseDelay × 2^(losses-1), MaxDelay)</c> with ±25 % jitter, so two backfills that lost
    /// to the same writer do not collide again at the same instant.
    /// </summary>
    private static int DelayFor(int consecutiveLosses)
    {
        int shift = Math.Min(consecutiveLosses - 1, 10);
        int delayMs = Math.Min(BaseDelayMs << shift, MaxDelayMs);
        int jitter = (int)(delayMs * 0.25 * (Random.Shared.NextDouble() * 2 - 1));
        return Math.Max(1, delayMs + jitter);
    }

    /// <summary>
    /// A window over a scanned batch, so a halved batch is passed on without copying the ids.
    /// </summary>
    private sealed class RowIdSlice : IReadOnlyList<ObjectIdValue>
    {
        private readonly IReadOnlyList<ObjectIdValue> source;

        private readonly int offset;

        public RowIdSlice(IReadOnlyList<ObjectIdValue> source, int offset, int count)
        {
            this.source = source;
            this.offset = offset;
            Count = count;
        }

        public int Count { get; }

        public ObjectIdValue this[int index]
        {
            get
            {
                ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)Count);
                return source[offset + index];
            }
        }

        public IEnumerator<ObjectIdValue> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
                yield return source[offset + i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
