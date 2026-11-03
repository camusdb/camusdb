/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Elle;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>Which predicate a group read uses, and so which plan serves it.</summary>
public enum PredicateRead
{
    /// <summary><c>WHERE grp = g</c> on a column with a secondary index.</summary>
    Index,

    /// <summary><c>WHERE grp2 = g</c> on a column with no index: a table scan with a filter.</summary>
    Scan,

    /// <summary><c>WHERE k BETWEEN lo AND hi</c>: a range scan of the primary key.</summary>
    Range,
}

/// <summary>
/// Predicate reads and membership-changing writes over SQL, recorded for Elle's rw-register checker.
/// A transaction reads whole groups of rows by a predicate and writes single rows into groups; see
/// <see cref="PredicateKeySpace"/> for how a group read becomes a set of register reads that lets the
/// point-key checker see phantoms and write skew.
///
/// <para>Inside an explicit or pipelined transaction, every group read also runs <c>COUNT(*)</c> with
/// the same predicate. Under serializable isolation both statements see one snapshot, so the count
/// must equal the rows selected; a difference is an intra-transaction inconsistency that the harness
/// grades by the arm's isolation level, because a Read Committed statement is allowed its own
/// snapshot. An autocommit group read is one statement, so it has no count to compare.</para>
///
/// <para>Rows are never deleted. A write is a blind <c>UPSERT</c>: an insert when the key is absent
/// (the membership change), an overwrite when it is present.</para>
/// </summary>
public sealed class PredicateOperation : IWriteOperation, IReadOperation
{
    public const string TableName = "elle_pred";

    private readonly ConnectionSet _connections;
    private readonly PredicateKeySpace _keys;
    private readonly ElleHistory _history;
    private readonly ElleWriteLedger _ledger;
    private readonly PredicateRead _read;
    private readonly ElleTxnMode _mode;
    private readonly long _keyBase;
    private readonly int _maxRetries;
    private readonly CamusTransactionOptions _readWrite;
    private readonly CamusTransactionOptions _readOnly;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;
    private long _countMismatches;
    private long _groupReads;

    public PredicateOperation(
        ConnectionSet connections, PredicateKeySpace keys, ElleHistory history, ElleWriteLedger ledger,
        PredicateRead read, ElleTxnMode mode, long keyBase,
        CamusLocking locking, CamusIsolationLevel isolationLevel, int maxRetries = 10)
    {
        _connections = connections;
        _keys = keys;
        _history = history;
        _ledger = ledger;
        _read = read;
        _mode = mode;
        _keyBase = keyBase;
        _maxRetries = Math.Max(1, maxRetries);
        _readWrite = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
            DeferBegin = mode == ElleTxnMode.Pipeline,
        };
        _readOnly = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadOnly,
            Locking = locking,
            DeferBegin = mode == ElleTxnMode.Pipeline,
        };
    }

    public static IReadOnlyList<string> Ddl =>
    [
        $"CREATE TABLE {TableName} (k INT64 PRIMARY KEY, grp INT64 NOT NULL, grp2 INT64 NOT NULL, v INT64 NOT NULL)",
        $"CREATE INDEX {TableName}_grp ON {TableName} (grp)",
    ];

    public static bool TryParseRead(string value, out PredicateRead read)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "index": read = PredicateRead.Index; return true;
            case "scan": read = PredicateRead.Scan; return true;
            case "range": read = PredicateRead.Range; return true;
            default: read = PredicateRead.Index; return false;
        }
    }

    public long CommittedRows => 0;

    public long IndeterminateTxns => Interlocked.Read(ref _indeterminateTxns);

    public int RowsPerTransaction => 0;

    public long RetryAttempts => Interlocked.Read(ref _retryAttempts);

    public long RetriedTxns => Interlocked.Read(ref _retriedTxns);

    public long MaxAttemptsUsed => Interlocked.Read(ref _maxAttemptsUsed);

    /// <summary>Group reads whose <c>COUNT(*)</c> disagreed with the rows the same transaction selected.</summary>
    public long CountMismatches => Interlocked.Read(ref _countMismatches);

    /// <summary>Group reads that ran inside a transaction with a count to compare against.</summary>
    public long GroupReadsWithCount => Interlocked.Read(ref _groupReads);

    public async Task EnsureTableAsync(CamusConnection setup, TimeSpan budget, CancellationToken ct)
    {
        foreach (string ddl in Ddl)
            await Dataset.EnsureDdlAsync(setup, ddl, $"table {TableName}", ct).ConfigureAwait(false);

        Stopwatch waited = Stopwatch.StartNew();
        int poolSize = Math.Max(1, _connections.ConnectionCount / 2);
        foreach (Func<CamusConnection> next in new Func<CamusConnection>[] { _connections.NextWrite, _connections.NextRead })
        {
            int consecutive = 0;
            while (consecutive < poolSize)
            {
                try
                {
                    using CamusCommand probe = GroupCommand(next(), null, PredicateRead.Index, -1, count: true);
                    await FinalReader.PointAsync(probe, ct).ConfigureAwait(false);
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

    public async Task<OperationResult> ExecuteAsync(long rowIndex, CancellationToken ct)
    {
        (_, OperationResult result) = await TryOnceAsync(readOnly: true, ct).ConfigureAwait(false);
        return result;
    }

    private async Task<(bool Done, OperationResult Result)> TryOnceAsync(bool readOnly, CancellationToken ct)
    {
        OperationKind kind = readOnly ? OperationKind.Read : OperationKind.Write;
        PredicateStep[] steps = _keys.NextTxn(readOnly);
        ElleMicroOp[] txn = _keys.ToMicroOps(steps);
        ElleMicroOp[] observed = (ElleMicroOp[])txn.Clone();
        long invokeTicks = Stopwatch.GetTimestamp();
        ElleInvocation invocation = _history.Invoke(txn);

        CamusConnection conn = readOnly ? _connections.NextRead() : _connections.NextWrite();
        long ts = Stopwatch.GetTimestamp();
        double beginMs = 0, readMs = 0, updateMs = 0, commitMs = 0;
        int exchanges = 0;
        bool commitSubmitted = false;
        CamusTransaction? tx = null;

        try
        {
            switch (_mode)
            {
                case ElleTxnMode.Autocommit:
                {
                    int at = 0;
                    foreach (PredicateStep step in steps)
                    {
                        if (step.IsGroupRead)
                        {
                            using CamusCommand select = GroupCommand(conn, null, _read, step.Group, count: false);
                            Dictionary<long, long> rows = await RowsOfAsync(await select.ExecuteReaderAsync(ct).ConfigureAwait(false), step.Group, ct).ConfigureAwait(false);
                            Observe(observed, at, step.Group, rows);
                            at += _keys.GroupSize;
                            readMs += Lap(ref ts);
                        }
                        else
                        {
                            commitSubmitted = true;
                            using CamusCommand upsert = UpsertCommand(conn, null, step);
                            await upsert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                            at++;
                            updateMs += Lap(ref ts);
                        }
                    }
                    break;
                }

                case ElleTxnMode.Pipeline:
                {
                    tx = await conn.BeginTransactionAsync(readOnly ? _readOnly : _readWrite, ct).ConfigureAwait(false);
                    CamusPipeline pipeline = tx.CreatePipeline();
                    List<(int At, long Group, Task<CamusDataReader> Rows, Task<CamusDataReader> Count)> reads = [];
                    List<Task<int>> writes = [];
                    int at = 0;
                    foreach (PredicateStep step in steps)
                    {
                        if (step.IsGroupRead)
                        {
                            Task<CamusDataReader> rows = pipeline.QueueReader(GroupCommand(conn, tx, _read, step.Group, count: false));
                            Task<CamusDataReader> count = pipeline.QueueReader(GroupCommand(conn, tx, _read, step.Group, count: true));
                            reads.Add((at, step.Group, rows, count));
                            at += _keys.GroupSize;
                        }
                        else
                        {
                            writes.Add(pipeline.QueueNonQuery(UpsertCommand(conn, tx, step)));
                            at++;
                        }
                    }

                    commitSubmitted = true;
                    await pipeline.CommitAsync(ct).ConfigureAwait(false);
                    commitMs = Lap(ref ts);
                    exchanges = pipeline.Exchanges;

                    foreach ((int readAt, long group, Task<CamusDataReader> rowsTask, Task<CamusDataReader> countTask) in reads)
                    {
                        Dictionary<long, long> rows = await RowsOfAsync(await rowsTask.ConfigureAwait(false), group, ct).ConfigureAwait(false);
                        long count = await ScalarOfAsync(await countTask.ConfigureAwait(false), ct).ConfigureAwait(false);
                        Observe(observed, readAt, group, rows);
                        CompareCount(rows.Count, count);
                    }
                    foreach (Task<int> write in writes)
                        await write.ConfigureAwait(false);
                    break;
                }

                default:
                {
                    tx = await conn.BeginTransactionAsync(readOnly ? _readOnly : _readWrite, ct).ConfigureAwait(false);
                    beginMs = Lap(ref ts);
                    int at = 0;
                    foreach (PredicateStep step in steps)
                    {
                        if (step.IsGroupRead)
                        {
                            Dictionary<long, long> rows;
                            using (CamusCommand select = GroupCommand(conn, tx, _read, step.Group, count: false))
                                rows = await RowsOfAsync(await select.ExecuteReaderAsync(ct).ConfigureAwait(false), step.Group, ct).ConfigureAwait(false);
                            long count;
                            using (CamusCommand counter = GroupCommand(conn, tx, _read, step.Group, count: true))
                                count = await ScalarOfAsync(await counter.ExecuteReaderAsync(ct).ConfigureAwait(false), ct).ConfigureAwait(false);
                            Observe(observed, at, step.Group, rows);
                            CompareCount(rows.Count, count);
                            at += _keys.GroupSize;
                            readMs += Lap(ref ts);
                        }
                        else
                        {
                            using CamusCommand upsert = UpsertCommand(conn, tx, step);
                            await upsert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                            at++;
                            updateMs += Lap(ref ts);
                        }
                    }

                    commitSubmitted = true;
                    await tx.CommitAsync(ct).ConfigureAwait(false);
                    commitMs = Lap(ref ts);
                    break;
                }
            }

            long completeTicks = Stopwatch.GetTimestamp();
            _history.Complete(invocation, ElleOutcome.Ok, observed);
            _ledger.RecordOk(txn, invokeTicks, completeTicks);
            return (true, new OperationResult(kind, OperationStatus.Ok, null, beginMs, readMs, updateMs, commitMs, Exchanges: exchanges));
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code, bool retry) = AppendOperation.ClassifyFailure(ex, readOnly, commitSubmitted);
            string message = ErrorClassifier.MessageOf(ex);

            if (status == OperationStatus.Indeterminate)
            {
                Interlocked.Increment(ref _indeterminateTxns);
                _history.Complete(invocation, ElleOutcome.Info);
                _ledger.RecordInfo(txn);
                return (true, OperationResult.Failure(kind, status, code, message));
            }

            if (tx is not null)
            {
                try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                catch { /* abandoned; the server reaper resolves it */ }
            }

            _history.Complete(invocation, ElleOutcome.Fail);
            _ledger.RecordFail(txn);
            if (status == OperationStatus.Transient)
                await TransientBackoff.PauseAsync(ct).ConfigureAwait(false);
            return (!retry, OperationResult.Failure(kind, status, code, message));
        }
    }

    /// <summary>Fills the group's register reads from the rows a predicate returned: the value for a
    /// present key, <c>nil</c> for an absent one.</summary>
    private void Observe(ElleMicroOp[] observed, int at, long group, Dictionary<long, long> rows)
    {
        long first = _keys.FirstKeyOf(group);
        for (int i = 0; i < _keys.GroupSize; i++)
        {
            long key = first + i;
            observed[at + i] = observed[at + i].WithRegisterRead(rows.TryGetValue(key, out long v) ? v : null);
        }
    }

    private void CompareCount(int rows, long count)
    {
        Interlocked.Increment(ref _groupReads);
        if (rows != count)
            Interlocked.Increment(ref _countMismatches);
    }

    /// <summary>
    /// Reads the <c>(k, v)</c> rows of a group. A row outside the group's key universe, or a key that
    /// appears twice, is a broken plan or a duplicate primary key: the history cannot express either,
    /// so the attempt fails as an internal error instead of being recorded.
    /// </summary>
    private async Task<Dictionary<long, long>> RowsOfAsync(CamusDataReader reader, long group, CancellationToken ct)
    {
        using (reader)
        {
            Dictionary<long, long> rows = [];
            long first = _keyBase + _keys.FirstKeyOf(group);
            long last = _keyBase + _keys.LastKeyOf(group);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                long sqlKey = reader.GetInt64(0);
                if (sqlKey < first || sqlKey > last)
                    throw new InvalidOperationException($"predicate read of group {group} by {_read} returned key {sqlKey} outside the group");
                long key = sqlKey - _keyBase;
                if (!rows.TryAdd(key, reader.GetInt64(1)))
                    throw new InvalidOperationException($"predicate read of group {group} by {_read} returned key {key} twice");
            }
            return rows;
        }
    }

    private static async Task<long> ScalarOfAsync(CamusDataReader reader, CancellationToken ct)
    {
        using (reader)
        {
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                throw new InvalidOperationException("COUNT(*) returned no row");
            return reader.GetInt64(0);
        }
    }

    /// <summary>The group's predicate, as a row select or as a count. Group -1 is the setup probe.</summary>
    private CamusCommand GroupCommand(CamusConnection conn, CamusTransaction? tx, PredicateRead read, long group, bool count)
    {
        string projection = count ? "COUNT(*)" : "k, v";
        string predicate = read switch
        {
            PredicateRead.Scan => "grp2 = @g",
            PredicateRead.Range => "k BETWEEN @lo AND @hi",
            _ => "grp = @g",
        };
        CamusCommand command = conn.CreateSelectCommand($"SELECT {projection} FROM {TableName} WHERE {predicate}");
        command.Transaction = tx;
        if (read == PredicateRead.Range)
        {
            command.Parameters.Add("@lo", ColumnType.Integer64, _keyBase + _keys.FirstKeyOf(group));
            command.Parameters.Add("@hi", ColumnType.Integer64, _keyBase + _keys.LastKeyOf(group));
        }
        else
        {
            command.Parameters.Add("@g", ColumnType.Integer64, _keyBase + group);
        }
        return command;
    }

    private CamusCommand UpsertCommand(CamusConnection conn, CamusTransaction? tx, PredicateStep step)
    {
        CamusCommand upsert = conn.CreateCamusCommand($"UPSERT INTO {TableName} (k, grp, grp2, v) VALUES (@k, @g, @g2, @v)");
        upsert.Transaction = tx;
        upsert.Parameters.Add("@k", ColumnType.Integer64, _keyBase + step.Key);
        upsert.Parameters.Add("@g", ColumnType.Integer64, _keyBase + step.Group);
        upsert.Parameters.Add("@g2", ColumnType.Integer64, _keyBase + step.Group);
        upsert.Parameters.Add("@v", ColumnType.Integer64, step.Value);
        return upsert;
    }

    /// <summary>Groups the scan path (a table scan per group) is read for in the final check.</summary>
    private const int ScannedGroupsInFinalCheck = 200;

    /// <summary>
    /// Reads every group the run used by the primary-key range and by the index, and the first groups
    /// by the unindexed scan too, and compares the three. Then counts the rows in the run's key range.
    /// The register judge sees one final per key with the index (and scan) reads as alternates, so a
    /// row the index lost or kept after its row changed is a path mismatch.
    /// </summary>
    public async Task<QuiescentResult> ReadFinalAsync(TimeSpan budget, CancellationToken ct)
    {
        long groupsUsed = _keys.GroupsUsed;
        long[] groups = new long[groupsUsed];
        for (long g = 0; g < groupsUsed; g++)
            groups[g] = g;

        try
        {
            List<string> extra = [];
            long countMismatches = 0;
            IReadOnlyList<List<FinalRegister>> perGroup = await FinalReader.ReadAllAsync(
                _connections, groups, async (conn, group, token) =>
                {
                    Dictionary<long, long> byRange = await ReadGroupAsync(conn, PredicateRead.Range, group, token).ConfigureAwait(false);
                    Dictionary<long, long> byIndex = await ReadGroupAsync(conn, PredicateRead.Index, group, token).ConfigureAwait(false);
                    Dictionary<long, long>? byScan = group < ScannedGroupsInFinalCheck
                        ? await ReadGroupAsync(conn, PredicateRead.Scan, group, token).ConfigureAwait(false)
                        : null;
                    long count;
                    using (CamusCommand counter = GroupCommand(conn, null, PredicateRead.Index, group, count: true))
                        count = await ScalarOfAsync(await counter.ExecuteReaderAsync(token).ConfigureAwait(false), token).ConfigureAwait(false);
                    if (count != byRange.Count)
                    {
                        Interlocked.Increment(ref countMismatches);
                        lock (extra)
                            extra.Add($"group {group}: COUNT(*) by index says {count} row(s), the primary-key range read returned {byRange.Count}");
                    }

                    List<FinalRegister> finals = [];
                    long first = _keys.FirstKeyOf(group);
                    for (int i = 0; i < _keys.GroupSize; i++)
                    {
                        long key = first + i;
                        long? value = byRange.TryGetValue(key, out long v) ? v : null;
                        List<FinalAlternate> alternates =
                        [
                            new FinalAlternate("index", byIndex.TryGetValue(key, out long iv) ? iv : null, byIndex.ContainsKey(key) ? 1 : 0),
                        ];
                        if (byScan is not null)
                            alternates.Add(new FinalAlternate("scan", byScan.TryGetValue(key, out long sv) ? sv : null, byScan.ContainsKey(key) ? 1 : 0));
                        finals.Add(new FinalRegister(key, value, value is null ? 0 : 1, alternates));
                    }
                    return finals;
                }, budget, ct).ConfigureAwait(false);

            List<FinalRegister> all = perGroup.SelectMany(g => g).ToList();
            long rowsInRange = await FinalReader.ScalarAsync(_connections, async (conn, token) =>
            {
                using CamusCommand count = conn.CreateSelectCommand($"SELECT COUNT(*) FROM {TableName} WHERE k >= @lo AND k < @hi");
                count.Parameters.Add("@lo", ColumnType.Integer64, _keyBase);
                count.Parameters.Add("@hi", ColumnType.Integer64, _keyBase + RegisterOperation.KeyRangeSpan);
                (long? value, _) = await FinalReader.PointAsync(count, token).ConfigureAwait(false);
                return value ?? 0;
            }, budget, ct).ConfigureAwait(false);

            long present = all.Count(f => f.Rows > 0);
            if (rowsInRange != present)
                extra.Add(
                    $"the table holds {rowsInRange:N0} row(s) in this run's key range, but {present:N0} of the run's keys are present; " +
                    (rowsInRange > present ? "an extra row exists that no group read reaches" : "a counted row is missing"));

            // COUNT(*) beside the SELECT must agree under one snapshot per transaction. Read committed
            // gives each statement its own snapshot, so a disagreement there is a measurement, not a
            // defect; it still travels in the result for the report.
            bool oneSnapshot = _readWrite.IsolationLevel == CamusIsolationLevel.Serializable;
            return _ledger.JudgeRegisters(all, CountMismatches, extra, countMismatchesFail: oneSnapshot);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            return QuiescentResult.Unavailable($"{ex.GetType().Name}: {ex.Message}", ElleModel.RwRegister, _ledger);
        }
    }

    private async Task<Dictionary<long, long>> ReadGroupAsync(CamusConnection conn, PredicateRead read, long group, CancellationToken ct)
    {
        using CamusCommand select = GroupCommand(conn, null, read, group, count: false);
        return await RowsOfAsync(await select.ExecuteReaderAsync(ct).ConfigureAwait(false), group, ct).ConfigureAwait(false);
    }

    private static double Lap(ref long ts)
    {
        long now = Stopwatch.GetTimestamp();
        double ms = Stopwatch.GetElapsedTime(ts, now).TotalMilliseconds;
        ts = now;
        return ms;
    }

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
