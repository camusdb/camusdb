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
/// <para>On a multi-table dataset the operation can be asked to make every transfer <b>cross-table</b>:
/// the second row is drawn from a different table than the first, so each transaction writes to two
/// key spaces at once and commits across two partitions. That is the shape that keeps every partition
/// loaded, and — under key-range routing — keeps every table's range hot enough to be a split
/// candidate. The conservation invariant is unchanged; it just sums over all the tables.</para>
///
/// <para>The point is the invariant: because every transfer conserves total balance and a transaction
/// is atomic, <c>SUM(balance)</c> must be unchanged after the run — even across faults and
/// indeterminate commits, since an atomic transfer either applies both legs or neither. A changed sum
/// is therefore direct evidence of an atomicity violation, a far stronger signal than the version
/// accounting alone. The two rows are always touched in ascending-index order so concurrent transfers
/// acquire locks in a canonical order and cannot deadlock in a cycle.</para>
///
/// <para><b>Two wire shapes.</b> The sequential transfer awaits each statement: six client round trips
/// (<c>BEGIN</c>, two reads, two updates, <c>COMMIT</c>). The pipelined transfer (<c>--pipeline</c>)
/// sends the same six statements as two <see cref="CamusPipeline"/> exchanges — <c>[BEGIN, SELECT,
/// SELECT]</c>, then <c>[UPDATE, UPDATE, COMMIT]</c> — because the updates depend on the values read.
/// The ledger semantics are identical; what changes is where a failure is seen. An update that matched
/// no row is a tripwire for a lost-row bug class: the sequential path refuses to commit on it, while
/// the pipelined path can only see it after the conditional commit has committed, and then reports the
/// transfer as an error so reconciliation and the operator both learn of it.</para>
/// </summary>
public sealed class TransferOperation : IWriteOperation
{
    private const long TransferAmount = 1;

    /// <summary>Rows each transfer increments <c>version</c> on: the debited row and the credited row.</summary>
    public const int RowsPerTransfer = 2;

    private readonly CamusTransactionOptions _options;
    private readonly ConnectionSet _connections;
    private readonly Dataset _dataset;
    private readonly long _rows;
    private readonly bool _crossTable;
    private readonly bool _pipeline;
    private readonly int _maxRetries;
    private readonly Metrics.TransferLedger? _ledger;
    private long _committedRows;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;

    public TransferOperation(
        ConnectionSet connections, Dataset dataset, long rows,
        CamusLocking locking = CamusLocking.Optimistic,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted,
        int maxRetries = 10,
        Metrics.TransferLedger? ledger = null,
        bool crossTable = false,
        bool pipeline = false)
    {
        _connections = connections;
        _dataset = dataset;
        _rows = Math.Max(2, rows);
        // Asking for cross-table transfers on a single-table dataset would leave no second table to
        // aim at, so the flag only takes effect when there is one.
        _crossTable = crossTable && dataset.Tables > 1;
        _maxRetries = Math.Max(1, maxRetries);
        _ledger = ledger;
        _pipeline = pipeline;
        _options = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
            // The pipelined transfer puts BEGIN inside its first frame instead of spending a round trip on it.
            DeferBegin = pipeline,
        };
    }

    /// <summary>Round trips the sequential transfer costs: BEGIN, two reads, two updates, COMMIT.</summary>
    private const int SequentialExchanges = 6;

    public long CommittedRows => System.Threading.Interlocked.Read(ref _committedRows);

    public long IndeterminateTxns => System.Threading.Interlocked.Read(ref _indeterminateTxns);

    public int RowsPerTransaction => RowsPerTransfer;

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
            (bool done, OperationResult result) = await TryOnceAsync(lowIndex, highIndex, lowDelta, highDelta, attempt, ct)
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
        _ledger?.Record(lowIndex, highIndex, lowDelta, _maxRetries, Metrics.TransferOutcome.ConflictFinal, lastConflict.ErrorCode);
        RecordAttempts(_maxRetries);
        return lastConflict;
    }

    private Task<(bool Done, OperationResult Result)> TryOnceAsync(
        long lowIndex, long highIndex, long lowDelta, long highDelta, int attempt, CancellationToken ct)
        => _pipeline
            ? TryOncePipelinedAsync(lowIndex, highIndex, lowDelta, highDelta, attempt, ct)
            : TryOnceSequentialAsync(lowIndex, highIndex, lowDelta, highDelta, attempt, ct);

    private async Task<(bool Done, OperationResult Result)> TryOnceSequentialAsync(
        long lowIndex, long highIndex, long lowDelta, long highDelta, int attempt, CancellationToken ct)
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
                ? (false, OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex)))
                : (true, OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex)));
        }

        bool commitSubmitted = false;
        try
        {
            await ApplyLegAsync(conn, tx, lowIndex, lowDelta, t, ct).ConfigureAwait(false);
            await ApplyLegAsync(conn, tx, highIndex, highDelta, t, ct).ConfigureAwait(false);

            commitSubmitted = true;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            commitMs = t.Tick();

            System.Threading.Interlocked.Add(ref _committedRows, RowsPerTransfer);
            _ledger?.Record(lowIndex, highIndex, lowDelta, attempt, Metrics.TransferOutcome.Committed, null);
            return (true, new OperationResult(
                OperationKind.Write, OperationStatus.Ok, null, beginMs, t.ReadMs, t.UpdateMs, commitMs, Exchanges: SequentialExchanges));
        }
        catch (Exception ex)
        {
            return await FailedAttemptAsync(tx, ex, commitSubmitted, lowIndex, highIndex, lowDelta, attempt, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The transfer as two pipelined exchanges. The first carries <c>BEGIN</c> and both reads, so the
    /// transaction begins and reads in one round trip; the second carries both updates and a commit
    /// that happens only if both updates succeeded. Stage timings are per frame: the read stage is the
    /// first exchange, the commit stage the second; begin and update, which have no exchange of their
    /// own here, read as zero.
    /// </summary>
    private async Task<(bool Done, OperationResult Result)> TryOncePipelinedAsync(
        long lowIndex, long highIndex, long lowDelta, long highDelta, int attempt, CancellationToken ct)
    {
        CamusConnection conn = _connections.NextWrite();
        Timing t = new() { Ts = Stopwatch.GetTimestamp() };

        // No BEGIN is sent here: the options defer it into the first pipeline send.
        CamusTransaction tx = await conn.BeginTransactionAsync(_options, ct).ConfigureAwait(false);
        CamusPipeline pipeline = tx.CreatePipeline();

        bool commitSubmitted = false;
        try
        {
            (string lowId, string lowTable) = Target(lowIndex);
            (string highId, string highTable) = Target(highIndex);

            Task<CamusDataReader> readLow = pipeline.QueueReader(ReadCommand(conn, lowTable, lowId));
            Task<CamusDataReader> readHigh = pipeline.QueueReader(ReadCommand(conn, highTable, highId));
            await pipeline.SendAsync(ct).ConfigureAwait(false);
            t.ReadMs += t.Tick();

            long lowBalance = await BalanceOfAsync(readLow, lowId, ct).ConfigureAwait(false);
            long highBalance = await BalanceOfAsync(readHigh, highId, ct).ConfigureAwait(false);

            Task<int> updateLow = pipeline.QueueNonQuery(UpdateCommand(conn, lowTable, lowId, lowBalance + lowDelta));
            Task<int> updateHigh = pipeline.QueueNonQuery(UpdateCommand(conn, highTable, highId, highBalance + highDelta));

            commitSubmitted = true;
            await pipeline.CommitAsync(ct).ConfigureAwait(false);
            double commitMs = t.Tick();

            System.Threading.Interlocked.Add(ref _committedRows, RowsPerTransfer);
            int modifiedLow = await updateLow.ConfigureAwait(false);
            int modifiedHigh = await updateHigh.ConfigureAwait(false);

            // The tripwire, seen after the fact: the reads proved both rows exist, so an update that
            // matched none means the engine lost a row inside the transaction. The commit has already
            // happened, so the transfer is reported as an error rather than a success; reconciliation
            // will show the drift it caused, and this says which transfer caused it.
            if (modifiedLow != 1 || modifiedHigh != 1)
            {
                _ledger?.Record(lowIndex, highIndex, lowDelta, attempt, Metrics.TransferOutcome.Error, "PARTIAL_TRANSFER");
                return (true, OperationResult.Failure(OperationKind.Write, OperationStatus.InternalError, "PARTIAL_TRANSFER",
                    $"pipelined transfer committed with UPDATE matches {modifiedLow}/{modifiedHigh} for existing ids {lowId}/{highId}"));
            }

            _ledger?.Record(lowIndex, highIndex, lowDelta, attempt, Metrics.TransferOutcome.Committed, null);
            return (true, new OperationResult(
                OperationKind.Write, OperationStatus.Ok, null, 0, t.ReadMs, 0, commitMs, Exchanges: pipeline.Exchanges));
        }
        catch (Exception ex)
        {
            // A BEGIN that failed inside the first frame never touched a row and is retried if it was
            // a conflict, as the sequential path retries a failed BeginTransaction.
            return await FailedAttemptAsync(tx, ex, commitSubmitted, lowIndex, highIndex, lowDelta, attempt, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Classifies a failed attempt, rolls the transaction back when the outcome is definite, and
    /// records it: indeterminate commits are carried into reconciliation, conflicts are retried by the
    /// caller, anything else is surfaced. Shared by both wire shapes, whose failure semantics are the same.
    /// </summary>
    private async Task<(bool Done, OperationResult Result)> FailedAttemptAsync(
        CamusTransaction tx, Exception ex, bool commitSubmitted,
        long lowIndex, long highIndex, long lowDelta, int attempt, CancellationToken ct)
    {
        (OperationStatus status, string code) = ErrorClassifier.Classify(ex, commitSubmitted);

        if (status == OperationStatus.Indeterminate)
        {
            // The atomic commit may already have applied both legs or neither; either keeps the sum
            // conserved. Leave it for the server's reaper and carry the ambiguity into reconciliation.
            System.Threading.Interlocked.Increment(ref _indeterminateTxns);
            _ledger?.Record(lowIndex, highIndex, lowDelta, attempt, Metrics.TransferOutcome.Indeterminate, code);
            return (true, OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex)));
        }

        try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
        catch { /* abandoned; the server reaper resolves it */ }

        // A conflict is retried by the caller; any other definite abort is surfaced now.
        _ledger?.Record(lowIndex, highIndex, lowDelta, attempt,
            status == OperationStatus.Conflict ? Metrics.TransferOutcome.ConflictRetry : Metrics.TransferOutcome.Error,
            code);
        return status == OperationStatus.Conflict
            ? (false, OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex)))
            : (true, OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex)));
    }

    private (string Id, string Table) Target(long rowIndex)
    {
        (string id, _, _, _) = _dataset.RowFor(rowIndex);
        return (id, _dataset.TableOf(rowIndex));
    }

    private static CamusCommand ReadCommand(CamusConnection conn, string table, string id)
    {
        CamusCommand read = conn.CreateSelectCommand($"SELECT balance, version FROM {table} WHERE id = @id");
        read.Parameters.Add("@id", ColumnType.Id, id);
        return read;
    }

    private static CamusCommand UpdateCommand(CamusConnection conn, string table, string id, long balance)
    {
        CamusCommand update = conn.CreateCamusCommand($"UPDATE {table} SET balance = @b, version = version + 1 WHERE id = @id");
        update.Parameters.Add("@b", ColumnType.Integer64, balance);
        update.Parameters.Add("@id", ColumnType.Id, id);
        return update;
    }

    private static async Task<long> BalanceOfAsync(Task<CamusDataReader> read, string id, CancellationToken ct)
    {
        using CamusDataReader reader = await read.ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            throw new InvalidOperationException("transfer target row missing: " + id);
        return reader.GetInt64(0);
    }

    private async Task ApplyLegAsync(
        CamusConnection conn, CamusTransaction tx, long rowIndex, long delta, Timing t, CancellationToken ct)
    {
        (string id, _, _, _) = _dataset.RowFor(rowIndex);
        string table = _dataset.TableOf(rowIndex);

        long balance;
        using (CamusCommand read = conn.CreateSelectCommand(
            $"SELECT balance, version FROM {table} WHERE id = @id"))
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
            $"UPDATE {table} SET balance = @b, version = version + 1 WHERE id = @id"))
        {
            update.Transaction = tx;
            update.Parameters.Add("@b", ColumnType.Integer64, balance + delta);
            update.Parameters.Add("@id", ColumnType.Id, id);
            int modified = await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            // Tripwire for the run-K atomicity bug class: the SELECT above just proved the row exists,
            // so an UPDATE matching 0 rows means the engine lost the row mid-transaction (an exhausted
            // transient read converted into a miss, before that conversion was fixed). Committing the
            // other leg anyway is exactly a silent SUM(balance) drift; fail the transfer loudly instead.
            if (modified != 1)
                throw new InvalidOperationException(
                    $"transfer UPDATE matched {modified} rows for existing id {id} — refusing to commit a partial transfer");
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

        if (_crossTable)
        {
            // Land in a table other than the source's. The +1 makes the offset non-zero before the
            // modulo, so the source table can never be selected, and the row inside the chosen table
            // comes from the untouched high bits of the same hash.
            int tables = _dataset.Tables;
            int fromTable = _dataset.TableIndexOf(fromIndex);
            int toTable = (int)((fromTable + 1 + (long)(mixed % (ulong)(tables - 1))) % tables);
            long crossed = _dataset.RowIndexInTable(toTable, mixed >> 20);
            // Distinct tables means distinct rows, so no self-collision is possible here.
            return crossed;
        }

        long to = (long)(mixed % (ulong)_rows);
        if (to == fromIndex)
            to = (to + 1) % _rows;
        return to;
    }
}
