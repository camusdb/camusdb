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
/// One read/write transaction under the run's configured isolation/locking (defaults: ReadCommitted,
/// Optimistic): BEGIN, read each target row's balance+version, then UPDATE it with a bounded balance change and
/// <c>version = version + 1</c>, and COMMIT — success is counted only after the commit returns. All
/// target rows come from the worker's own <see cref="WorkerShard"/>, so two workers never touch the
/// same row and the baseline is structurally conflict-free. The begin/read/update/commit spans are
/// timed separately so the report can attribute where a write spends time (the prior investigation
/// found commit — durable WAL — dominates). A conflict here is not retried into a better number; it is
/// classified and surfaced so the run can be invalidated per the non-conflicting contract.
/// </summary>
public sealed class WriteOperation
{
    private readonly CamusTransactionOptions _options;
    private readonly ConnectionSet _connections;
    private readonly Dataset _dataset;
    private readonly int _writesPerTransaction;
    private long _committedRows;
    private long _indeterminateTxns;

    public WriteOperation(
        ConnectionSet connections, Dataset dataset, int writesPerTransaction,
        CamusLocking locking = CamusLocking.Optimistic,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted)
    {
        _connections = connections;
        _dataset = dataset;
        _writesPerTransaction = Math.Max(1, writesPerTransaction);
        _options = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
        };
    }

    /// <summary>
    /// Total rows durably committed by this operation across the whole run — warm-up <em>and</em>
    /// measured windows. Reconciliation compares this to the persisted <c>SUM(version)</c>, because the
    /// database accumulates version increments from warm-up writes too, while the reported throughput
    /// counts only the measured window. Comparing the persisted sum to measured writes alone would
    /// always over-count by the warm-up writes.
    /// </summary>
    public long CommittedRows => System.Threading.Interlocked.Read(ref _committedRows);

    /// <summary>
    /// Transactions whose commit round trip failed without a server verdict, across the whole run
    /// (warm-up <em>and</em> measured windows, mirroring <see cref="CommittedRows"/>). Each may have
    /// durably committed all of its row writes or none; reconciliation widens its expected
    /// <c>SUM(version)</c> delta by up to this count × writes-per-transaction to admit both outcomes.
    /// </summary>
    public long IndeterminateTxns => System.Threading.Interlocked.Read(ref _indeterminateTxns);

    public async Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct)
    {
        // Derive distinct in-shard target rows for a multi-row transaction from the base row.
        long baseOrdinal = shard.OrdinalOf(baseRowIndex);
        int count = (int)Math.Min(_writesPerTransaction, shard.OwnedCount);
        long[] rowIndexes = new long[count];
        for (int k = 0; k < count; k++)
        {
            long ordinal = (baseOrdinal + k) % shard.OwnedCount;
            rowIndexes[k] = shard.RowIndexAt(ordinal);
        }

        CamusConnection conn = _connections.NextWrite();

        double beginMs = 0, readMs = 0, updateMs = 0, commitMs = 0;
        long ts = Stopwatch.GetTimestamp();

        CamusTransaction tx;
        try
        {
            tx = await conn.BeginTransactionAsync(_options, ct).ConfigureAwait(false);
            beginMs = Elapsed(ref ts);
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex);
            return OperationResult.Failure(OperationKind.Write, status, code);
        }

        // False until control reaches the commit call itself. A SELECT/UPDATE failure — even one thrown
        // after the row work — is a definite abort because no commit was ever requested; only an
        // exception from the commit round trip leaves the durable outcome unknown.
        bool commitSubmitted = false;
        try
        {
            foreach (long rowIndex in rowIndexes)
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
                        throw new InvalidOperationException("write target row missing: " + id);
                    balance = reader.GetInt64(0);
                }
                readMs += Elapsed(ref ts);

                long newBalance = balance + BoundedDelta(rowIndex);
                using (CamusCommand update = conn.CreateCamusCommand(
                    "UPDATE " + Dataset.TableName + " SET balance = @b, version = version + 1 WHERE id = @id"))
                {
                    update.Transaction = tx;
                    update.Parameters.Add("@b", ColumnType.Integer64, newBalance);
                    update.Parameters.Add("@id", ColumnType.Id, id);
                    await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                updateMs += Elapsed(ref ts);
            }

            commitSubmitted = true;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            commitMs = Elapsed(ref ts);
            System.Threading.Interlocked.Add(ref _committedRows, count);
            return new OperationResult(OperationKind.Write, OperationStatus.Ok, null, beginMs, readMs, updateMs, commitMs);
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex, commitSubmitted);
            if (status == OperationStatus.Indeterminate)
            {
                // The commit may already have durably applied; a rollback attempt would either be
                // rejected or race the in-flight commit, so the transaction is left for the server's
                // reaper/finalizer and the ambiguity is carried into reconciliation instead.
                System.Threading.Interlocked.Increment(ref _indeterminateTxns);
                return OperationResult.Failure(OperationKind.Write, status, code);
            }

            try
            {
                await tx.RollbackAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                // A rollback failure does not change the operation's classification; the transaction
                // is abandoned and the server's reaper/finalizer resolves it.
            }
            return OperationResult.Failure(OperationKind.Write, status, code);
        }
    }

    /// <summary>A deterministic, bounded balance change that keeps balances from drifting unboundedly.</summary>
    private static long BoundedDelta(long rowIndex) => (rowIndex % 200) - 100;

    private static double Elapsed(ref long ts)
    {
        long now = Stopwatch.GetTimestamp();
        double ms = Stopwatch.GetElapsedTime(ts, now).TotalMilliseconds;
        ts = now;
        return ms;
    }
}
