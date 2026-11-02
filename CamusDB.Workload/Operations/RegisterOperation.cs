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

/// <summary>The SQL type of the register key, each on a table of its own.</summary>
public enum RegisterKeyType
{
    Int64,
    String,
    Numeric,
}

/// <summary>Which access path a register read and write take.</summary>
public enum RegisterAccess
{
    /// <summary>By the primary key column.</summary>
    Pk,

    /// <summary>By an equal copy of the key in a column with no index: a scan with a filter.</summary>
    Secondary,

    /// <summary>By an equal copy of the key in a column with a secondary index.</summary>
    Index,

    /// <summary>Through a second table: the register row is found by joining a reference row.</summary>
    Join,
}

/// <summary>
/// Elle's rw-register workload over SQL. Each transaction is a short list of steps on a few contended
/// keys; a step either blindly writes a unique integer to a register or reads the register. Every
/// attempt is written to an <see cref="ElleHistory"/> for the rw-register checker, and every write to
/// an <see cref="ElleWriteLedger"/> for the end-of-run check of the table.
///
/// <para>A register is one row. The shape reaches the point-read and point-write paths that
/// list-append does not: the list shape always reads the whole value and appends through a
/// read-modify-write in the engine, so a lost update on a blind write, a point read served from the
/// wrong version, or a secondary index that lags its row is invisible to it. Here a write is a blind
/// <c>UPSERT</c> on the primary-key and join paths, and an <c>UPDATE</c> located by the secondary
/// column, with an <c>INSERT</c> when it matched nothing, on the others.</para>
///
/// <para>Keys carry a per-run base (integers) or tag (strings), so rows left by an earlier run in the
/// same database are never read as this run's: a value this run never wrote would look like a
/// garbage read. Every written value is unique across the run.</para>
///
/// <para>Outcomes map onto Jepsen's as in <see cref="AppendOperation"/>: a commit that returns is
/// <c>:ok</c>, a definite abort is <c>:fail</c>, a commit sent with no verdict back is <c>:info</c>.</para>
/// </summary>
public sealed class RegisterOperation : IWriteOperation, IReadOperation
{
    private readonly ConnectionSet _connections;
    private readonly RegisterKeySpace _keys;
    private readonly ElleHistory _history;
    private readonly ElleWriteLedger _ledger;
    private readonly RegisterKeyType _keyType;
    private readonly RegisterAccess _access;
    private readonly ElleTxnMode _mode;
    private readonly long _keyBase;
    private readonly string _runTag;
    private readonly int _maxRetries;
    private readonly CamusTransactionOptions _readWrite;
    private readonly CamusTransactionOptions _readOnly;
    private readonly string _table;
    private readonly string _refTable;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;

    public RegisterOperation(
        ConnectionSet connections, RegisterKeySpace keys, ElleHistory history, ElleWriteLedger ledger,
        RegisterKeyType keyType, RegisterAccess access, ElleTxnMode mode, long keyBase, string runTag,
        CamusLocking locking, CamusIsolationLevel isolationLevel, int maxRetries = 10)
    {
        if (mode == ElleTxnMode.Pipeline && access is RegisterAccess.Secondary or RegisterAccess.Index)
            throw new ArgumentException(
                "the pipelined mode sends every statement before any answer returns, so a write located by a secondary " +
                "column, which needs the UPDATE's row count before it can INSERT, cannot be pipelined; use access pk or join");

        _connections = connections;
        _keys = keys;
        _history = history;
        _ledger = ledger;
        _keyType = keyType;
        _access = access;
        _mode = mode;
        _keyBase = keyBase;
        _runTag = runTag;
        _maxRetries = Math.Max(1, maxRetries);
        _table = TableNameFor(keyType);
        _refTable = RefTableNameFor(keyType);
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

    public static string TableNameFor(RegisterKeyType keyType) => keyType switch
    {
        RegisterKeyType.String => "elle_rw_str",
        RegisterKeyType.Numeric => "elle_rw_num",
        _ => "elle_rw_i64",
    };

    public static string RefTableNameFor(RegisterKeyType keyType) => TableNameFor(keyType) + "_ref";

    private static string SqlTypeOf(RegisterKeyType keyType) => keyType switch
    {
        RegisterKeyType.String => "STRING",
        RegisterKeyType.Numeric => "NUMERIC",
        _ => "INT64",
    };

    /// <summary>The DDL for one key type: the register table with its primary key, an unindexed and
    /// an indexed copy of the key, and the reference table the join path goes through.</summary>
    public static IReadOnlyList<string> DdlFor(RegisterKeyType keyType)
    {
        string t = TableNameFor(keyType);
        string type = SqlTypeOf(keyType);
        return
        [
            $"CREATE TABLE {t} (k {type} PRIMARY KEY, k2 {type} NOT NULL, k3 {type} NOT NULL, v INT64 NOT NULL)",
            $"CREATE INDEX {t}_k3 ON {t} (k3)",
            $"CREATE TABLE {RefTableNameFor(keyType)} (k {type} PRIMARY KEY, target {type} NOT NULL)",
        ];
    }

    public static bool TryParseKeyType(string value, out RegisterKeyType keyType)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "int64": keyType = RegisterKeyType.Int64; return true;
            case "string": keyType = RegisterKeyType.String; return true;
            case "numeric": keyType = RegisterKeyType.Numeric; return true;
            default: keyType = RegisterKeyType.Int64; return false;
        }
    }

    public static bool TryParseAccess(string value, out RegisterAccess access)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "pk": access = RegisterAccess.Pk; return true;
            case "secondary": access = RegisterAccess.Secondary; return true;
            case "index": access = RegisterAccess.Index; return true;
            case "join": access = RegisterAccess.Join; return true;
            default: access = RegisterAccess.Pk; return false;
        }
    }

    /// <summary>Always 0: the register shape writes no row of the seeded dataset.</summary>
    public long CommittedRows => 0;

    public long IndeterminateTxns => Interlocked.Read(ref _indeterminateTxns);

    public int RowsPerTransaction => 0;

    public long RetryAttempts => Interlocked.Read(ref _retryAttempts);

    public long RetriedTxns => Interlocked.Read(ref _retriedTxns);

    public long MaxAttemptsUsed => Interlocked.Read(ref _maxAttemptsUsed);

    /// <summary>Creates the tables if absent and waits until every connection can see them, as
    /// <see cref="AppendOperation.EnsureTableAsync"/> does.</summary>
    public async Task EnsureTablesAsync(CamusConnection setup, TimeSpan budget, CancellationToken ct)
    {
        foreach (string ddl in DdlFor(_keyType))
            await Dataset.EnsureDdlAsync(setup, ddl, $"register table of {_keyType}", ct).ConfigureAwait(false);

        Stopwatch waited = Stopwatch.StartNew();
        int poolSize = Math.Max(1, _connections.ConnectionCount / 2);
        foreach (Func<CamusConnection> next in new Func<CamusConnection>[] { _connections.NextWrite, _connections.NextRead })
        {
            int consecutive = 0;
            while (consecutive < poolSize)
            {
                try
                {
                    CamusConnection conn = next();
                    using CamusCommand probe = ReadCommand(conn, null, RegisterAccess.Join, -1);
                    await FinalReader.PointAsync(probe, ct).ConfigureAwait(false);
                    consecutive++;
                }
                catch (Exception ex) when (Dataset.IsMissingTable(ex) || ErrorClassifier.IsRetryableForIdempotentRead(ex))
                {
                    if (waited.Elapsed > budget)
                        throw new InvalidOperationException(
                            $"the register tables were not visible on every connection after {budget.TotalSeconds:F0}s: {ex.Message}", ex);
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
        ElleMicroOp[] txn = _keys.NextTxn(readOnly);
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
                    for (int i = 0; i < txn.Length; i++)
                    {
                        if (txn[i].IsWrite)
                        {
                            // The statement commits on its own, so once it is sent a lost reply is a lost verdict.
                            commitSubmitted = true;
                            await WriteAsync(conn, null, txn[i], ct).ConfigureAwait(false);
                            updateMs += Lap(ref ts);
                        }
                        else
                        {
                            observed[i] = txn[i].WithRegisterRead(await ReadAsync(conn, null, txn[i].Key, ct).ConfigureAwait(false));
                            readMs += Lap(ref ts);
                        }
                    }
                    break;

                case ElleTxnMode.Pipeline:
                {
                    tx = await conn.BeginTransactionAsync(readOnly ? _readOnly : _readWrite, ct).ConfigureAwait(false);
                    CamusPipeline pipeline = tx.CreatePipeline();
                    Task<CamusDataReader>?[] readers = new Task<CamusDataReader>?[txn.Length];
                    List<Task<int>> writes = [];
                    for (int i = 0; i < txn.Length; i++)
                    {
                        if (txn[i].IsWrite)
                        {
                            writes.Add(pipeline.QueueNonQuery(UpsertCommand(conn, tx, txn[i])));
                            if (_access == RegisterAccess.Join)
                                writes.Add(pipeline.QueueNonQuery(UpsertRefCommand(conn, tx, txn[i].Key)));
                        }
                        else
                        {
                            readers[i] = pipeline.QueueReader(ReadCommand(conn, tx, _access, txn[i].Key));
                        }
                    }

                    commitSubmitted = true;
                    await pipeline.CommitAsync(ct).ConfigureAwait(false);
                    commitMs = Lap(ref ts);
                    exchanges = pipeline.Exchanges;

                    for (int i = 0; i < txn.Length; i++)
                    {
                        if (readers[i] is Task<CamusDataReader> pending)
                            observed[i] = txn[i].WithRegisterRead(await ValueOfAsync(pending, txn[i].Key, ct).ConfigureAwait(false));
                    }
                    foreach (Task<int> write in writes)
                        await write.ConfigureAwait(false);
                    break;
                }

                default:
                    tx = await conn.BeginTransactionAsync(readOnly ? _readOnly : _readWrite, ct).ConfigureAwait(false);
                    beginMs = Lap(ref ts);
                    for (int i = 0; i < txn.Length; i++)
                    {
                        if (txn[i].IsWrite)
                        {
                            await WriteAsync(conn, tx, txn[i], ct).ConfigureAwait(false);
                            updateMs += Lap(ref ts);
                        }
                        else
                        {
                            observed[i] = txn[i].WithRegisterRead(await ReadAsync(conn, tx, txn[i].Key, ct).ConfigureAwait(false));
                            readMs += Lap(ref ts);
                        }
                    }

                    commitSubmitted = true;
                    await tx.CommitAsync(ct).ConfigureAwait(false);
                    commitMs = Lap(ref ts);
                    break;
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

    /// <summary>One write on the explicit or autocommit path: a blind <c>UPSERT</c> by primary key, or,
    /// on the secondary paths, an <c>UPDATE</c> located by the copy column with an <c>INSERT</c> when it
    /// matched no row.</summary>
    private async Task WriteAsync(CamusConnection conn, CamusTransaction? tx, ElleMicroOp op, CancellationToken ct)
    {
        if (_access is RegisterAccess.Pk or RegisterAccess.Join)
        {
            using (CamusCommand upsert = UpsertCommand(conn, tx, op))
                await upsert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (_access == RegisterAccess.Join)
            {
                using CamusCommand reference = UpsertRefCommand(conn, tx, op.Key);
                await reference.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            return;
        }

        string column = _access == RegisterAccess.Index ? "k3" : "k2";
        using (CamusCommand update = conn.CreateCamusCommand($"UPDATE {_table} SET v = @v WHERE {column} = @k"))
        {
            update.Transaction = tx;
            update.Parameters.Add("@v", ColumnType.Integer64, op.Value);
            AddKey(update, "@k", op.Key);
            int modified = await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (modified == 1)
                return;

            // A copy of the primary key matches at most one row. Refuse to commit rather than let a
            // broken statement put a value into the history that one row does not hold.
            if (modified != 0)
                throw new InvalidOperationException($"register UPDATE matched {modified} rows for key {op.Key}");
        }

        using CamusCommand insert = conn.CreateCamusCommand($"INSERT INTO {_table} (k, k2, k3, v) VALUES (@k, @k2, @k3, @v)");
        insert.Transaction = tx;
        AddKey(insert, "@k", op.Key);
        AddKey(insert, "@k2", op.Key);
        AddKey(insert, "@k3", op.Key);
        insert.Parameters.Add("@v", ColumnType.Integer64, op.Value);
        await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private CamusCommand UpsertCommand(CamusConnection conn, CamusTransaction? tx, ElleMicroOp op)
    {
        CamusCommand upsert = conn.CreateCamusCommand($"UPSERT INTO {_table} (k, k2, k3, v) VALUES (@k, @k2, @k3, @v)");
        upsert.Transaction = tx;
        AddKey(upsert, "@k", op.Key);
        AddKey(upsert, "@k2", op.Key);
        AddKey(upsert, "@k3", op.Key);
        upsert.Parameters.Add("@v", ColumnType.Integer64, op.Value);
        return upsert;
    }

    private CamusCommand UpsertRefCommand(CamusConnection conn, CamusTransaction? tx, long key)
    {
        CamusCommand upsert = conn.CreateCamusCommand($"UPSERT INTO {_refTable} (k, target) VALUES (@k, @target)");
        upsert.Transaction = tx;
        AddKey(upsert, "@k", key);
        AddKey(upsert, "@target", key);
        return upsert;
    }

    private async Task<long?> ReadAsync(CamusConnection conn, CamusTransaction? tx, long key, CancellationToken ct)
    {
        using CamusCommand read = ReadCommand(conn, tx, _access, key);
        (long? value, int rows) = await FinalReader.PointAsync(read, ct).ConfigureAwait(false);
        if (rows > 1)
            throw new InvalidOperationException($"register read by {_access} returned {rows} rows for key {key}");
        return value;
    }

    private static async Task<long?> ValueOfAsync(Task<CamusDataReader> pending, long key, CancellationToken ct)
    {
        using CamusDataReader reader = await pending.ConfigureAwait(false);
        long? value = null;
        int rows = 0;
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (rows == 0)
                value = reader.GetInt64(0);
            rows++;
        }
        if (rows > 1)
            throw new InvalidOperationException($"pipelined register read returned {rows} rows for key {key}");
        return value;
    }

    /// <summary>The read of one key by the given path. Key -1 is the setup probe, a key no run writes.</summary>
    private CamusCommand ReadCommand(CamusConnection conn, CamusTransaction? tx, RegisterAccess access, long key)
    {
        string sql = access switch
        {
            RegisterAccess.Secondary => $"SELECT v FROM {_table} WHERE k2 = @k",
            RegisterAccess.Index => $"SELECT v FROM {_table} WHERE k3 = @k",
            RegisterAccess.Join => $"SELECT r.v FROM {_refTable} j JOIN {_table} r ON r.k = j.target WHERE j.k = @k",
            _ => $"SELECT v FROM {_table} WHERE k = @k",
        };
        CamusCommand read = conn.CreateSelectCommand(sql);
        read.Transaction = tx;
        AddKey(read, "@k", key);
        return read;
    }

    private void AddKey(CamusCommand command, string name, long key)
    {
        switch (_keyType)
        {
            case RegisterKeyType.String:
                command.Parameters.Add(name, ColumnType.String, _runTag + ":" + key.ToString(CultureInfo.InvariantCulture));
                break;
            case RegisterKeyType.Numeric:
                command.Parameters.Add(name, ColumnType.Numeric, (decimal)(_keyBase + key));
                break;
            default:
                command.Parameters.Add(name, ColumnType.Integer64, _keyBase + key);
                break;
        }
    }

    /// <summary>
    /// Reads every key the ledger knows, by primary key and by the paths that must agree with it, and
    /// counts the rows in this run's key range, so an extra row (a duplicate primary key, a row no
    /// write produced) is found even though no point read asks for it.
    ///
    /// <para>The indexed copy is read for every key on every arm, because every table has that index.
    /// The join path is read on the join arm, where every write also wrote the reference row. The
    /// unindexed copy is a table scan per key, so it is read only when the run touched few keys.</para>
    /// </summary>
    public async Task<QuiescentResult> ReadFinalAsync(TimeSpan budget, CancellationToken ct)
    {
        long[] keys = _ledger.TouchedKeys();
        Array.Sort(keys);
        bool readSecondary = _access == RegisterAccess.Secondary && keys.Length <= 5_000;

        try
        {
            IReadOnlyList<FinalRegister> finals = await FinalReader.ReadAllAsync(
                _connections, keys, async (conn, key, token) =>
                {
                    (long? value, int rows) = await Point(conn, RegisterAccess.Pk, key, token).ConfigureAwait(false);
                    List<FinalAlternate> alternates = [];
                    (long? byIndex, int indexRows) = await Point(conn, RegisterAccess.Index, key, token).ConfigureAwait(false);
                    alternates.Add(new FinalAlternate("index", byIndex, indexRows));
                    if (_access == RegisterAccess.Join)
                    {
                        (long? byJoin, int joinRows) = await Point(conn, RegisterAccess.Join, key, token).ConfigureAwait(false);
                        alternates.Add(new FinalAlternate("join", byJoin, joinRows));
                    }
                    if (readSecondary)
                    {
                        (long? bySecondary, int secondaryRows) = await Point(conn, RegisterAccess.Secondary, key, token).ConfigureAwait(false);
                        alternates.Add(new FinalAlternate("secondary", bySecondary, secondaryRows));
                    }
                    return new FinalRegister(key, value, rows, alternates);
                }, budget, ct).ConfigureAwait(false);

            long rowsInRange = await FinalReader.ScalarAsync(_connections, CountRunRowsAsync, budget, ct).ConfigureAwait(false);
            long present = finals.Count(f => f.Rows > 0);
            List<string> extra = [];
            if (rowsInRange != present)
                extra.Add(
                    $"the table holds {rowsInRange:N0} row(s) in this run's key range, but {present:N0} of the run's keys are present; " +
                    (rowsInRange > present ? "an extra row exists that no point read reaches" : "a counted row is missing"));

            return _ledger.JudgeRegisters(finals, 0, extra);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            return QuiescentResult.Unavailable($"{ex.GetType().Name}: {ex.Message}", ElleModel.RwRegister, _ledger);
        }
    }

    private async Task<(long? Value, int Rows)> Point(CamusConnection conn, RegisterAccess access, long key, CancellationToken ct)
    {
        using CamusCommand read = ReadCommand(conn, null, access, key);
        return await FinalReader.PointAsync(read, ct).ConfigureAwait(false);
    }

    /// <summary>Rows whose key this run could have written: the key range above the run base, or the
    /// run tag's prefix for string keys (the character after ':' bounds the prefix).</summary>
    private async Task<long> CountRunRowsAsync(CamusConnection conn, CancellationToken ct)
    {
        using CamusCommand count = conn.CreateSelectCommand($"SELECT COUNT(*) FROM {_table} WHERE k >= @lo AND k < @hi");
        switch (_keyType)
        {
            case RegisterKeyType.String:
                count.Parameters.Add("@lo", ColumnType.String, _runTag + ":");
                count.Parameters.Add("@hi", ColumnType.String, _runTag + ";");
                break;
            case RegisterKeyType.Numeric:
                count.Parameters.Add("@lo", ColumnType.Numeric, (decimal)_keyBase);
                count.Parameters.Add("@hi", ColumnType.Numeric, (decimal)(_keyBase + KeyRangeSpan));
                break;
            default:
                count.Parameters.Add("@lo", ColumnType.Integer64, _keyBase);
                count.Parameters.Add("@hi", ColumnType.Integer64, _keyBase + KeyRangeSpan);
                break;
        }
        (long? value, _) = await FinalReader.PointAsync(count, ct).ConfigureAwait(false);
        return value ?? 0;
    }

    /// <summary>Keys one run may use above its base: 2^24, more than a million transactions need.</summary>
    public const long KeyRangeSpan = 1L << 24;

    /// <summary>A per-run key base for the integer and numeric key types: the second of the run's start,
    /// shifted so that the runs of two years never share a key. Two runs that start in the same second
    /// against one database would collide; the harness starts one run at a time.</summary>
    public static long KeyBaseFor(DateTimeOffset startedAt) => (startedAt.ToUnixTimeSeconds() % (1L << 30)) << 24;

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
