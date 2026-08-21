/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>
/// A bank transfer: move a fixed amount from one account row to another in a single transaction, so
/// the two <c>version + 1</c> writes commit atomically or not at all. Unlike <see cref="WriteOperation"/>
/// the two rows are drawn from the <b>whole</b> keyspace, not the worker's own shard, so different
/// workers contend for the same rows — real write/write conflict, which the operation absorbs with a
/// bounded retry from a fresh BEGIN rather than surfacing (a lock conflict here is expected, not a bug).
///
/// <para>The point is the invariant: because every transfer conserves total balance and a transaction
/// is atomic, <c>SUM(balance)</c> must be unchanged after the run — even across faults and
/// indeterminate commits, since an atomic transfer either applies both legs or neither. A changed sum
/// is therefore direct evidence of an atomicity violation, a far stronger signal than the version
/// accounting alone. The two rows are always touched in ascending-index order so concurrent transfers
/// acquire locks in a canonical order and cannot deadlock in a cycle.</para>
/// </summary>
public sealed class TransferOperation : IWriteOperation
{
    private const long TransferAmount = 1;

    private readonly CamusTransactionOptions _options;
    private readonly ConnectionSet _connections;
    private readonly Dataset _dataset;
    private readonly long _rows;
    private readonly int _maxRetries;
    private long _committedRows;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;

    public TransferOperation(
        ConnectionSet connections, Dataset dataset, long rows,
        CamusLocking locking = CamusLocking.Optimistic,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted,
        int maxRetries = 10)
    {
        _connections = connections;
        _dataset = dataset;
        _rows = Math.Max(2, rows);
        _maxRetries = Math.Max(1, maxRetries);
        _options = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
        };
    }

    public long CommittedRows => System.Threading.Interlocked.Read(ref _committedRows);

    public long IndeterminateTxns => System.Threading.Interlocked.Read(ref _indeterminateTxns);

    public long RetryAttempts => System.Threading.Interlocked.Read(ref _retryAttempts);

    public long RetriedTxns => System.Threading.Interlocked.Read(ref _retriedTxns);

    public long MaxAttemptsUsed => System.Threading.Interlocked.Read(ref _maxAttemptsUsed);

    public async Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct)
    {
        // 'from' is the worker's own row; 'to' is derived across the whole keyspace so transfers from
        // different workers collide. Ascending order gives a canonical lock order (deadlock-free).
        long fromIndex = ((baseRowIndex % _rows) + _rows) % _rows;
        long toIndex = SecondRow(baseRowIndex, fromIndex);
        (long lowIndex, long highIndex) = fromIndex < toIndex ? (fromIndex, toIndex) : (toIndex, fromIndex);

        // The debit/credit sign is fixed to the original from/to, independent of lock order.
        long lowDelta = lowIndex == fromIndex ? -TransferAmount : TransferAmount;
        long highDelta = -lowDelta;

        OperationResult lastConflict = OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, "CADB0502");

        for (int attempt = 1; attempt <= _maxRetries; attempt++)
        {
            (bool done, OperationResult result) = await TryOnceAsync(lowIndex, highIndex, lowDelta, highDelta, ct)
                .ConfigureAwait(false);

            if (done)
            {
                RecordAttempts(attempt);
                return result;
            }

            // Retryable conflict: back off briefly and try the whole transfer again from BEGIN.
            lastConflict = result;
            await Task.Delay(Math.Min(attempt, 10), ct).ConfigureAwait(false);
        }

        // Exhausted the retry budget still conflicting — surface it like the baseline write would.
        RecordAttempts(_maxRetries);
        return lastConflict;
    }

    private async Task<(bool Done, OperationResult Result)> TryOnceAsync(
        long lowIndex, long highIndex, long lowDelta, long highDelta, CancellationToken ct)
    {
        CamusConnection conn = _connections.NextWrite();
        Timing t = new() { Ts = Stopwatch.GetTimestamp() };
        double beginMs, commitMs = 0;

        CamusTransaction tx;
        try
        {
            tx = await conn.BeginTransactionAsync(_options, ct).ConfigureAwait(false);
            beginMs = t.Tick();
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex);
            // A begin failure never touched a row; retry if it was a conflict, else surface it.
            return status == OperationStatus.Conflict
                ? (false, OperationResult.Failure(OperationKind.Write, status, code))
                : (true, OperationResult.Failure(OperationKind.Write, status, code));
        }

        bool commitSubmitted = false;
        try
        {
            await ApplyLegAsync(conn, tx, lowIndex, lowDelta, t, ct).ConfigureAwait(false);
            await ApplyLegAsync(conn, tx, highIndex, highDelta, t, ct).ConfigureAwait(false);

            commitSubmitted = true;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            commitMs = t.Tick();

            System.Threading.Interlocked.Add(ref _committedRows, 2);
            return (true, new OperationResult(OperationKind.Write, OperationStatus.Ok, null, beginMs, t.ReadMs, t.UpdateMs, commitMs));
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex, commitSubmitted);

            if (status == OperationStatus.Indeterminate)
            {
                // The atomic commit may already have applied both legs or neither; either keeps the sum
                // conserved. Leave it for the server's reaper and carry the ambiguity into reconciliation.
                System.Threading.Interlocked.Increment(ref _indeterminateTxns);
                return (true, OperationResult.Failure(OperationKind.Write, status, code));
            }

            try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
            catch { /* abandoned; the server reaper resolves it */ }

            // A conflict is retried by the caller; any other definite abort is surfaced now.
            return status == OperationStatus.Conflict
                ? (false, OperationResult.Failure(OperationKind.Write, status, code))
                : (true, OperationResult.Failure(OperationKind.Write, status, code));
        }
    }

    private async Task ApplyLegAsync(
        CamusConnection conn, CamusTransaction tx, long rowIndex, long delta, Timing t, CancellationToken ct)
    {
        (string id, _, _, _) = _dataset.RowFor(rowIndex);

        long balance;
        using (CamusCommand read = conn.CreateSelectCommand(
            "SELECT balance, version FROM " + Dataset.TableName + " WHERE id = @id"))
        {
            read.Transaction = tx;
            read.Parameters.Add("@id", ColumnType.Id, id);
            using CamusDataReader reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                throw new InvalidOperationException("transfer target row missing: " + id);
            balance = reader.GetInt64(0);
        }
        t.ReadMs += t.Tick();

        using (CamusCommand update = conn.CreateCamusCommand(
            "UPDATE " + Dataset.TableName + " SET balance = @b, version = version + 1 WHERE id = @id"))
        {
            update.Transaction = tx;
            update.Parameters.Add("@b", ColumnType.Integer64, balance + delta);
            update.Parameters.Add("@id", ColumnType.Id, id);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        t.UpdateMs += t.Tick();
    }

    /// <summary>Mutable per-attempt timing, so the async leg helper can accumulate span times without
    /// ref parameters (which async methods forbid).</summary>
    private sealed class Timing
    {
        public long Ts;
        public double ReadMs;
        public double UpdateMs;

        /// <summary>Milliseconds since the last tick; advances the clock.</summary>
        public double Tick()
        {
            long now = Stopwatch.GetTimestamp();
            double ms = Stopwatch.GetElapsedTime(Ts, now).TotalMilliseconds;
            Ts = now;
            return ms;
        }
    }

    /// <summary>A second row index across the whole keyspace, derived deterministically from the base
    /// and guaranteed distinct from <paramref name="fromIndex"/>.</summary>
    /// <summary>
    /// Records how many attempts one transfer consumed. Only the re-runs count as retries, so a
    /// first-try success adds nothing. A conflict absorbed here never reaches the conflict counter,
    /// which is why the contention signal has to be captured at this point.
    /// </summary>
    private void RecordAttempts(int attempts)
    {
        if (attempts > 1)
        {
            System.Threading.Interlocked.Add(ref _retryAttempts, attempts - 1);
            System.Threading.Interlocked.Increment(ref _retriedTxns);
        }

        // Compare-and-swap loop: several workers can finish concurrently, so a plain read-then-write
        // would lose the higher value in a race.
        long observed = System.Threading.Interlocked.Read(ref _maxAttemptsUsed);
        while (attempts > observed)
        {
            long previous = System.Threading.Interlocked.CompareExchange(ref _maxAttemptsUsed, attempts, observed);
            if (previous == observed)
                break;
            observed = previous;
        }
    }

    private long SecondRow(long baseRowIndex, long fromIndex)
    {
        // Multiplicative hash spread across the keyspace; nudge off any self-collision.
        ulong mixed = unchecked((ulong)baseRowIndex * 0x9E3779B97F4A7C15UL + 0xD1B54A32D192ED03UL);
        long to = (long)(mixed % (ulong)_rows);
        if (to == fromIndex)
            to = (to + 1) % _rows;
        return to;
    }
}
