/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Elle;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>
/// Elle's list-append workload over SQL. Each transaction is a short list of steps on a few contended
/// keys; a step either appends a unique integer to a list or reads the whole list. Every attempt is
/// written to an <see cref="ElleHistory"/>, and Elle later searches that history for dependency cycles:
/// lost updates, write skew, read skew, dirty and aborted reads, and the rest of the G 0 – G 2 family.
///
/// <para>This is the check that the invariant workloads cannot make. A conserved sum proves that no
/// write was lost or duplicated, but two transactions that each read what the other overwrites leave
/// every sum intact. Elle sees that as a cycle.</para>
///
/// <para>A list is one row of <see cref="TableName"/>, its elements joined with commas. An append is
/// <c>UPDATE … SET v = concat(v, ',x')</c>, so the engine does the read-modify-write itself, under
/// its own locks; an UPDATE that matches no row falls back to an INSERT that starts the list. Two
/// transactions that both start the same list collide on the primary key, and one aborts — a definite
/// failure, which Elle handles. Keys carry a per-run tag, so lists left by an earlier run in the same
/// database are never read as this run's.</para>
///
/// <para>Outcomes map onto Jepsen's: a commit that returns is <c>:ok</c>, any definite abort is
/// <c>:fail</c>, and a commit that was sent with no verdict back is <c>:info</c>. A read-only
/// transaction has no effects to be unsure about, so it is never <c>:info</c>.</para>
///
/// <para>The seeded dataset is not touched: <see cref="CommittedRows"/> stays 0, and the dataset
/// reconciliation still runs and still proves the seeded rows were left alone.</para>
/// </summary>
public sealed class AppendOperation : IWriteOperation, IReadOperation
{
    public const string TableName = "elle_append";

    private const string ReadSql = $"SELECT v FROM {TableName} WHERE k = @k";
    private const string UpdateSql = $"UPDATE {TableName} SET v = concat(v, @x) WHERE k = @k";
    private const string InsertSql = $"INSERT INTO {TableName} (k, v) VALUES (@k, @x)";

    private readonly ConnectionSet _connections;
    private readonly AppendKeySpace _keys;
    private readonly ElleHistory _history;
    private readonly string _runTag;
    private readonly int _maxRetries;
    private readonly CamusTransactionOptions _readWrite;
    private readonly CamusTransactionOptions _readOnly;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;

    public AppendOperation(
        ConnectionSet connections, AppendKeySpace keys, ElleHistory history, string runTag,
        CamusLocking locking, CamusIsolationLevel isolationLevel, int maxRetries = 10)
    {
        _connections = connections;
        _keys = keys;
        _history = history;
        _runTag = runTag;
        _maxRetries = Math.Max(1, maxRetries);
        _readWrite = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
        };
        _readOnly = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadOnly,
            Locking = locking,
        };
    }

    /// <summary>The DDL for the list table. The key is a string so it can carry the run tag.</summary>
    public static string CreateTableSql => $"CREATE TABLE {TableName} (k STRING PRIMARY KEY, v STRING NOT NULL)";

    /// <summary>Always 0: the append shape writes no row of the seeded dataset.</summary>
    public long CommittedRows => 0;

    public long IndeterminateTxns => Interlocked.Read(ref _indeterminateTxns);

    public long RetryAttempts => Interlocked.Read(ref _retryAttempts);

    public long RetriedTxns => Interlocked.Read(ref _retriedTxns);

    public long MaxAttemptsUsed => Interlocked.Read(ref _maxAttemptsUsed);

    /// <summary>
    /// Creates the list table if it is absent, then waits until every connection of the set can see
    /// it. A just-created table reaches the other nodes with a delay, and an operation that hit a node
    /// without it would fail as a domain error that says nothing about the engine under test.
    ///
    /// <para>The pools hand out connections round-robin, so one full round of consecutive successes on
    /// a pool has touched each of its connections once. A failure starts the round again.</para>
    /// </summary>
    public static async Task EnsureTableAsync(
        CamusConnection setup, ConnectionSet connections, TimeSpan budget, CancellationToken ct)
    {
        await Dataset.EnsureDdlAsync(setup, CreateTableSql, $"table {TableName}", ct).ConfigureAwait(false);

        Stopwatch waited = Stopwatch.StartNew();
        int poolSize = Math.Max(1, connections.ConnectionCount / 2);
        foreach (Func<CamusConnection> next in new Func<CamusConnection>[] { connections.NextWrite, connections.NextRead })
        {
            int consecutive = 0;
            while (consecutive < poolSize)
            {
                try
                {
                    using CamusCommand probe = next().CreateSelectCommand(ReadSql);
                    probe.Parameters.Add("@k", ColumnType.String, "probe");
                    using CamusDataReader reader = await probe.ExecuteReaderAsync(ct).ConfigureAwait(false);
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                    }
                    consecutive++;
                }
                catch (Exception ex) when (Dataset.IsMissingTable(ex) || ErrorClassifier.IsRetryableForIdempotentRead(ex))
                {
                    if (waited.Elapsed > budget)
                        throw new InvalidOperationException(
                            $"table {TableName} was not visible on every connection after {budget.TotalSeconds:F0}s: {ex.Message}", ex);
                    consecutive = 0;
                    await Task.Delay(250, ct).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>A read-write transaction. A conflict is retried from a fresh BEGIN with a fresh
    /// transaction; each attempt is its own operation in the history.</summary>
    public async Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct)
    {
        OperationResult last = OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, "CADB0502");
        for (int attempt = 1; attempt <= _maxRetries; attempt++)
        {
            (bool done, OperationResult result) = await TryOnceAsync(readOnly: false, ct).ConfigureAwait(false);
            if (done)
            {
                RecordAttempts(attempt);
                return result;
            }

            last = result;
            await Task.Delay(Math.Min(attempt, 10), ct).ConfigureAwait(false);
        }

        RecordAttempts(_maxRetries);
        return last;
    }

    /// <summary>A read-only transaction. Not retried: a failed read has no effect on the history.</summary>
    public async Task<OperationResult> ExecuteAsync(long rowIndex, CancellationToken ct)
    {
        (_, OperationResult result) = await TryOnceAsync(readOnly: true, ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>
    /// One attempt. <c>Done</c> is false only for a retryable conflict of a read-write attempt; every
    /// other outcome is final.
    /// </summary>
    private async Task<(bool Done, OperationResult Result)> TryOnceAsync(bool readOnly, CancellationToken ct)
    {
        OperationKind kind = readOnly ? OperationKind.Read : OperationKind.Write;
        ElleMicroOp[] txn = _keys.NextTxn(readOnly);
        ElleMicroOp[] observed = (ElleMicroOp[])txn.Clone();
        ElleInvocation invocation = _history.Invoke(txn);

        CamusConnection conn = readOnly ? _connections.NextRead() : _connections.NextWrite();
        long ts = Stopwatch.GetTimestamp();
        double beginMs = 0, readMs = 0, updateMs = 0, commitMs = 0;
        bool commitSubmitted = false;
        CamusTransaction? tx = null;

        try
        {
            tx = await conn.BeginTransactionAsync(readOnly ? _readOnly : _readWrite, ct).ConfigureAwait(false);
            beginMs = Lap(ref ts);

            for (int i = 0; i < txn.Length; i++)
            {
                if (txn[i].IsAppend)
                {
                    await AppendAsync(conn, tx, txn[i], ct).ConfigureAwait(false);
                    updateMs += Lap(ref ts);
                }
                else
                {
                    observed[i] = txn[i].WithRead(await ReadListAsync(conn, tx, txn[i].Key, ct).ConfigureAwait(false));
                    readMs += Lap(ref ts);
                }
            }

            commitSubmitted = true;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            commitMs = Lap(ref ts);

            _history.Complete(invocation, ElleOutcome.Ok, observed);
            return (true, new OperationResult(kind, OperationStatus.Ok, null, beginMs, readMs, updateMs, commitMs));
        }
        catch (Exception ex)
        {
            // A read-only transaction cannot have applied anything, so a lost commit reply is not an
            // ambiguity for it.
            (OperationStatus status, string code) = readOnly
                ? ErrorClassifier.Classify(ex)
                : ErrorClassifier.Classify(ex, commitSubmitted);
            string message = ErrorClassifier.MessageOf(ex);

            if (status == OperationStatus.Indeterminate)
            {
                Interlocked.Increment(ref _indeterminateTxns);
                _history.Complete(invocation, ElleOutcome.Info);
                return (true, OperationResult.Failure(kind, status, code, message));
            }

            if (tx is not null)
            {
                try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                catch { /* abandoned; the server reaper resolves it */ }
            }

            _history.Complete(invocation, ElleOutcome.Fail);
            bool retry = !readOnly && status == OperationStatus.Conflict;
            return (!retry, OperationResult.Failure(kind, status, code, message));
        }
    }

    private async Task AppendAsync(CamusConnection conn, CamusTransaction tx, ElleMicroOp step, CancellationToken ct)
    {
        string key = SqlKey(step.Key);
        string value = step.Value.ToString(CultureInfo.InvariantCulture);

        using (CamusCommand update = conn.CreateCamusCommand(UpdateSql))
        {
            update.Transaction = tx;
            update.Parameters.Add("@x", ColumnType.String, "," + value);
            update.Parameters.Add("@k", ColumnType.String, key);
            int modified = await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (modified == 1)
                return;

            // A primary-key UPDATE cannot match two rows. Refuse to commit rather than let a broken
            // statement put a value into the history that the table does not hold.
            if (modified != 0)
                throw new InvalidOperationException($"append UPDATE matched {modified} rows for key {key}");
        }

        using CamusCommand insert = conn.CreateCamusCommand(InsertSql);
        insert.Transaction = tx;
        insert.Parameters.Add("@k", ColumnType.String, key);
        insert.Parameters.Add("@x", ColumnType.String, value);
        await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<long>?> ReadListAsync(
        CamusConnection conn, CamusTransaction tx, long key, CancellationToken ct)
    {
        using CamusCommand read = conn.CreateSelectCommand(ReadSql);
        read.Transaction = tx;
        read.Parameters.Add("@k", ColumnType.String, SqlKey(key));
        using CamusDataReader reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        IReadOnlyList<long> list = ParseList(reader.GetString(0), key);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
            throw new InvalidOperationException($"list read returned more than one row for key {key}");
        return list;
    }

    /// <summary>
    /// Parses a stored list. A value that is not a comma-separated list of integers means the engine
    /// stored or returned something no append wrote; that is thrown as an internal error rather than
    /// recorded, because Elle cannot express it.
    /// </summary>
    public static IReadOnlyList<long> ParseList(string stored, long key)
    {
        string[] parts = stored.Split(',');
        long[] values = new long[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            if (!long.TryParse(parts[i], NumberStyles.None, CultureInfo.InvariantCulture, out values[i]))
                throw new InvalidOperationException($"list for key {key} is not a list of integers: '{Truncate(stored)}'");
        }
        return values;
    }

    private string SqlKey(long key) => _runTag + ":" + key.ToString(CultureInfo.InvariantCulture);

    private static string Truncate(string s) => s.Length <= 120 ? s : s[..120] + "…";

    private static double Lap(ref long ts)
    {
        long now = Stopwatch.GetTimestamp();
        double ms = Stopwatch.GetElapsedTime(ts, now).TotalMilliseconds;
        ts = now;
        return ms;
    }

    /// <summary>Same accounting as <see cref="TransferOperation"/>: only the re-runs are retries.</summary>
    private void RecordAttempts(int attempts)
    {
        if (attempts > 1)
        {
            Interlocked.Add(ref _retryAttempts, attempts - 1);
            Interlocked.Increment(ref _retriedTxns);
        }

        long observed = Interlocked.Read(ref _maxAttemptsUsed);
        while (attempts > observed)
        {
            long previous = Interlocked.CompareExchange(ref _maxAttemptsUsed, attempts, observed);
            if (previous == observed)
                break;
            observed = previous;
        }
    }
}
