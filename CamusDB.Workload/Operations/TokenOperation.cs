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
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>
/// The bank transfer for deletes. A fixed number of tokens, one per slot, live in a table of their own;
/// a move deletes a slot's row and inserts its successor in one transaction, so every slot holds exactly
/// one row at every committed state, and the table exactly <c>--tokens</c> rows.
///
/// <para>A move reads the slot's row through the slot index, deletes it by primary key (the delete must
/// match the row just read; a racing move that got there first sends this one back to BEGIN), and
/// inserts the successor at the next generation. The successor's key is one of three, so the delete
/// paths that reuse a key are all exercised: a key never used before (<see cref="TokenMoveKind.Fresh"/>),
/// the key just deleted in the same transaction (<see cref="TokenMoveKind.Same"/>), or the key the
/// previous move of the slot deleted (<see cref="TokenMoveKind.Back"/>), which inserts a key whose
/// delete an earlier transaction committed.</para>
///
/// <para>The slot index is deliberately not unique. Two moves that read the same generation and both
/// commit would leave two rows in the slot (with a fresh key each), and nothing but the engine's conflict
/// detection on the deleted row stands in the way; a unique index would turn that double spend into a
/// constraint error and hide it.</para>
///
/// <para>A read is a serializable read-only transaction, CamusDB's snapshot (pinned to one timestamp
/// across every key), whatever isolation the moves run at: at read committed a statement reads the latest
/// committed version of each key, so an index lookup and its row fetch can straddle a move and legitimately
/// see none or two. A snapshot sees a move entirely or not at all, so it must find exactly one row. The
/// moves themselves are fine at read committed: under optimistic locking a commit fails when a row it
/// deleted changed, which is the protection against a double spend this workload tests. The final state is judged by
/// <see cref="TokenLedger.Judge"/>. The seeded dataset is not touched.</para>
/// </summary>
public sealed class TokenOperation : IWriteOperation, IReadOperation
{
    public const string TableName = "workload_tokens";

    public const string IndexName = "workload_tokens_slot";

    private const string LookupSql = $"SELECT id, gen, prev_id FROM {TableName} WHERE slot = @slot";
    private const string DeleteSql = $"DELETE FROM {TableName} WHERE id = @id";
    private const string InsertSql =
        $"INSERT INTO {TableName} (id, slot, gen, prev_id, moved_by) VALUES (@id, @slot, @gen, @prev, @by)";

    private readonly ConnectionSet _connections;
    private readonly TokenLedger _ledger;
    private readonly long _tokens;
    private readonly int _maxRetries;
    private readonly CamusTransactionOptions _readWrite;
    private readonly CamusTransactionOptions _readOnly;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;

    public TokenOperation(
        ConnectionSet connections, TokenLedger ledger, CamusLocking locking, CamusIsolationLevel isolationLevel, int maxRetries = 10)
    {
        _connections = connections;
        _ledger = ledger;
        _tokens = ledger.Tokens;
        _maxRetries = Math.Max(1, maxRetries);
        _readWrite = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
        };
        _readOnly = SnapshotRead;
    }

    /// <summary>A serializable read-only transaction: CamusDB's lock-free snapshot.</summary>
    public static CamusTransactionOptions SnapshotRead => new()
    {
        IsolationLevel = CamusIsolationLevel.Serializable,
        Mode = CamusTransactionMode.ReadOnly,
    };

    public static string CreateTableSql =>
        $"CREATE TABLE {TableName} (id STRING PRIMARY KEY, slot INT64 NOT NULL, gen INT64 NOT NULL, " +
        "prev_id STRING NOT NULL, moved_by INT64 NOT NULL)";

    public static string CreateIndexSql => $"CREATE INDEX {IndexName} ON {TableName} (slot)";

    /// <summary>The key of a slot's seeded row.</summary>
    public static string SeedId(long slot) => string.Create(CultureInfo.InvariantCulture, $"s{slot}.g0");

    /// <summary>A key no other move uses: the slot, the generation it creates and a random suffix, so two
    /// moves of one generation never collide on the key and a double spend shows as two rows.</summary>
    public static string FreshId(long slot, long gen) =>
        string.Create(CultureInfo.InvariantCulture, $"s{slot}.g{gen}.{Guid.NewGuid().ToString("N")[..8]}");

    /// <summary>The slot and the move kind a write aims at, spread over every slot from the scheduler's row
    /// index so that workers contend for the same tokens.</summary>
    public static (long Slot, TokenMoveKind Kind) Aim(long rowIndex, long tokens)
    {
        ulong mixed = unchecked((ulong)rowIndex * 0x9E3779B97F4A7C15UL);
        mixed ^= mixed >> 29;
        return ((long)(mixed % (ulong)tokens), (TokenMoveKind)((mixed >> 40) % 3));
    }

    /// <summary>Always 0: a move writes no row of the seeded dataset.</summary>
    public long CommittedRows => 0;

    public long IndeterminateTxns => Interlocked.Read(ref _indeterminateTxns);

    /// <summary>Always 0: a move increments no seeded row's version.</summary>
    public int RowsPerTransaction => 0;

    public long RetryAttempts => Interlocked.Read(ref _retryAttempts);

    public long RetriedTxns => Interlocked.Read(ref _retriedTxns);

    public long MaxAttemptsUsed => Interlocked.Read(ref _maxAttemptsUsed);

    /// <summary>
    /// Creates the token table and its slot index, waits until every connection can read it, and seeds one
    /// row per slot. Refuses a table that already holds rows: the verdict counts the table, and rows a
    /// previous run left behind would make it count something else.
    /// </summary>
    public static async Task EnsureSeededAsync(
        CamusConnection setup, ConnectionSet connections, long tokens, int batch, TimeSpan budget, CancellationToken ct)
    {
        await Dataset.EnsureDdlAsync(setup, CreateTableSql, $"table {TableName}", ct).ConfigureAwait(false);
        await Dataset.EnsureDdlAsync(setup, CreateIndexSql, $"index {IndexName}", ct).ConfigureAwait(false);
        await WaitVisibleAsync(connections, budget, ct).ConfigureAwait(false);

        long existing = await CountAsync(setup, ct).ConfigureAwait(false);
        if (existing != 0)
            throw new InvalidOperationException(
                $"table {TableName} already holds {existing} row(s); the token workload needs it empty (use a fresh database)");

        batch = Math.Max(1, batch);
        for (long start = 0; start < tokens; start += batch)
        {
            long end = Math.Min(start + batch, tokens);
            Stopwatch waited = Stopwatch.StartNew();
            for (int attempt = 1; ; attempt++)
            {
                CamusTransaction? tx = null;
                try
                {
                    tx = await setup.BeginTransactionAsync(ct).ConfigureAwait(false);
                    for (long slot = start; slot < end; slot++)
                        await InsertAsync(setup, tx, SeedId(slot), slot, 0, "", -1, ct).ConfigureAwait(false);
                    await tx.CommitAsync(ct).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    if (tx is not null)
                    {
                        try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                        catch { /* retried from a new transaction regardless */ }
                    }

                    // An indeterminate commit of an earlier attempt may have landed: a duplicate seed key
                    // means the batch is in. The seeded rows are judged at the end with everything else.
                    if (ErrorClassifier.IsDuplicateKey(ex))
                        break;

                    OperationStatus status = ErrorClassifier.Classify(ex).Status;
                    bool retryable = status is OperationStatus.Conflict or OperationStatus.Transient || Dataset.IsMissingTable(ex);
                    if (!retryable || waited.Elapsed > budget)
                        throw;
                    await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
                }
            }
        }

        long seeded = await CountAsync(setup, ct).ConfigureAwait(false);
        if (seeded != tokens)
            throw new InvalidOperationException($"seeded {seeded} token(s), expected {tokens}");
    }

    private static async Task WaitVisibleAsync(ConnectionSet connections, TimeSpan budget, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        int poolSize = Math.Max(1, connections.ConnectionCount / 2);
        foreach (Func<CamusConnection> next in new Func<CamusConnection>[] { connections.NextWrite, connections.NextRead })
        {
            int consecutive = 0;
            while (consecutive < poolSize)
            {
                try
                {
                    using CamusCommand probe = next().CreateSelectCommand(LookupSql);
                    probe.Parameters.Add("@slot", ColumnType.Integer64, -1L);
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

    private static async Task<long> CountAsync(CamusConnection conn, CancellationToken ct)
    {
        using CamusCommand count = conn.CreateCamusCommand($"SELECT COUNT(*) FROM {TableName}");
        using CamusDataReader reader = await count.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await reader.ReadAsync(ct).ConfigureAwait(false) && !reader.IsDBNull(0) ? reader.GetInt64(0) : 0;
    }

    /// <summary>One move, retried from BEGIN on a conflict, a racing move, or a key collision.</summary>
    public async Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct)
    {
        (long slot, TokenMoveKind kind) = Aim(baseRowIndex, _tokens);
        OperationResult last = OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, "CADB0502");

        for (int attempt = 1; attempt <= _maxRetries; attempt++)
        {
            (bool done, OperationResult result, long fromGen) = await TryMoveAsync(slot, kind, shard.WorkerIndex, ct).ConfigureAwait(false);
            if (done)
            {
                _ledger.RecordMove(slot, fromGen, kind, attempt, Outcome(result), result.ErrorCode);
                RecordAttempts(attempt);
                return result;
            }

            _ledger.RecordMove(slot, fromGen, kind, attempt, TokenMoveOutcome.ConflictRetry, result.ErrorCode);
            last = result;
            await Task.Delay(Math.Min(attempt, 10), ct).ConfigureAwait(false);
        }

        _ledger.RecordMove(slot, -1, kind, _maxRetries, TokenMoveOutcome.ConflictFinal, last.ErrorCode);
        RecordAttempts(_maxRetries);
        return last;
    }

    private static TokenMoveOutcome Outcome(OperationResult result) => result.Status switch
    {
        OperationStatus.Ok => TokenMoveOutcome.Committed,
        OperationStatus.Indeterminate => TokenMoveOutcome.Indeterminate,
        _ => TokenMoveOutcome.Error,
    };

    /// <summary>
    /// One attempt. <c>Done</c> is false when the attempt should be retried: a conflict, a slot the lookup
    /// did not find exactly once (a read-committed statement can straddle a racing move), a delete that a
    /// racing move won, or a successor key that collided.
    /// </summary>
    private async Task<(bool Done, OperationResult Result, long FromGen)> TryMoveAsync(
        long slot, TokenMoveKind kind, int worker, CancellationToken ct)
    {
        CamusConnection conn = _connections.NextWrite();
        long ts = Stopwatch.GetTimestamp();
        double beginMs = 0, readMs = 0, updateMs = 0, commitMs;
        bool commitSubmitted = false;
        long fromGen = -1;
        CamusTransaction? tx = null;

        try
        {
            tx = await conn.BeginTransactionAsync(_readWrite, ct).ConfigureAwait(false);
            beginMs = Lap(ref ts);

            List<(string Id, long Gen, string PrevId)> found = await LookupAsync(conn, tx, slot, ct).ConfigureAwait(false);
            readMs = Lap(ref ts);
            if (found.Count != 1)
                return await RetryAsync(tx, "TOKEN_LOOKUP", $"slot {slot} lookup returned {found.Count} row(s)", fromGen, ct).ConfigureAwait(false);

            (string oldId, long gen, string prevId) = found[0];
            fromGen = gen;

            using (CamusCommand delete = conn.CreateCamusCommand(DeleteSql))
            {
                delete.Transaction = tx;
                delete.Parameters.Add("@id", ColumnType.String, oldId);
                int deleted = await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                if (deleted != 1)
                    return await RetryAsync(tx, "TOKEN_DELETE_RACE", $"delete of {oldId} matched {deleted} row(s)", fromGen, ct).ConfigureAwait(false);
            }

            string newId = kind switch
            {
                TokenMoveKind.Same => oldId,
                TokenMoveKind.Back when prevId.Length > 0 => prevId,
                _ => FreshId(slot, gen + 1),
            };
            await InsertAsync(conn, tx, newId, slot, gen + 1, oldId, worker, ct).ConfigureAwait(false);
            updateMs = Lap(ref ts);

            commitSubmitted = true;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            commitMs = Lap(ref ts);
            return (true, new OperationResult(OperationKind.Write, OperationStatus.Ok, null, beginMs, readMs, updateMs, commitMs), fromGen);
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex, commitSubmitted);
            string message = ErrorClassifier.MessageOf(ex);

            if (status == OperationStatus.Indeterminate)
            {
                Interlocked.Increment(ref _indeterminateTxns);
                return (true, OperationResult.Failure(OperationKind.Write, status, code, message), fromGen);
            }

            if (tx is not null)
            {
                try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                catch { /* abandoned; the server reaper resolves it */ }
            }

            // A successor key that collided (at the INSERT or, under optimistic locking, at the commit) was
            // taken by a racing move of the same slot, or (for a Back key) is a row whose committed delete
            // came back. The server answered, so this attempt applied nothing, and the final check finds a
            // resurrected row on its own.
            bool retry = status == OperationStatus.Conflict || ErrorClassifier.IsDuplicateKey(ex);
            return (!retry, OperationResult.Failure(OperationKind.Write, retry ? OperationStatus.Conflict : status, code, message), fromGen);
        }
    }

    private static async Task<(bool Done, OperationResult Result, long FromGen)> RetryAsync(
        CamusTransaction tx, string code, string message, long fromGen, CancellationToken ct)
    {
        try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
        catch { /* abandoned; the server reaper resolves it */ }
        return (false, OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, code, message), fromGen);
    }

    /// <summary>A read-only lookup of one slot. Not retried: a failed read changes nothing.</summary>
    public async Task<OperationResult> ExecuteAsync(long rowIndex, CancellationToken ct)
    {
        (long slot, _) = Aim(rowIndex, _tokens);
        CamusConnection conn = _connections.NextRead();
        long ts = Stopwatch.GetTimestamp();
        CamusTransaction? tx = null;
        try
        {
            tx = await conn.BeginTransactionAsync(_readOnly, ct).ConfigureAwait(false);
            double beginMs = Lap(ref ts);
            List<(string Id, long Gen, string PrevId)> found = await LookupAsync(conn, tx, slot, ct).ConfigureAwait(false);
            double readMs = Lap(ref ts);
            await tx.CommitAsync(ct).ConfigureAwait(false);
            double commitMs = Lap(ref ts);

            _ledger.RecordRead(slot, found.Select(f => (f.Id, f.Gen)).ToList());
            if (found.Count != 1)
                return OperationResult.Failure(OperationKind.Read, OperationStatus.InternalError, "TOKEN_READ",
                    $"read-only lookup of slot {slot} returned {found.Count} row(s)");
            return new OperationResult(OperationKind.Read, OperationStatus.Ok, null, beginMs, readMs, 0, commitMs);
        }
        catch (Exception ex)
        {
            if (tx is not null)
            {
                try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                catch { /* abandoned; the server reaper resolves it */ }
            }

            (OperationStatus status, string code) = ErrorClassifier.Classify(ex);
            return OperationResult.Failure(OperationKind.Read, status, code, ErrorClassifier.MessageOf(ex));
        }
    }

    private static async Task<List<(string Id, long Gen, string PrevId)>> LookupAsync(
        CamusConnection conn, CamusTransaction tx, long slot, CancellationToken ct)
    {
        using CamusCommand read = conn.CreateSelectCommand(LookupSql);
        read.Transaction = tx;
        read.Parameters.Add("@slot", ColumnType.Integer64, slot);
        using CamusDataReader reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        List<(string, long, string)> rows = [];
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            rows.Add((reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? "" : reader.GetString(2)));
        return rows;
    }

    private static async Task InsertAsync(
        CamusConnection conn, CamusTransaction tx, string id, long slot, long gen, string prevId, long movedBy, CancellationToken ct)
    {
        using CamusCommand insert = conn.CreateCamusCommand(InsertSql);
        insert.Transaction = tx;
        insert.Parameters.Add("@id", ColumnType.String, id);
        insert.Parameters.Add("@slot", ColumnType.Integer64, slot);
        insert.Parameters.Add("@gen", ColumnType.Integer64, gen);
        insert.Parameters.Add("@prev", ColumnType.String, prevId);
        insert.Parameters.Add("@by", ColumnType.Integer64, movedBy);
        await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the final table twice, through the primary key and through the slot index, for
    /// <see cref="TokenLedger.Judge"/>. Retries a transport failure within <paramref name="budget"/>, as
    /// the other reconciliation reads do.
    /// </summary>
    public static async Task<(IReadOnlyList<TokenRow> ByPrimaryKey, IReadOnlyList<string> IdsByIndex)> ReadFinalAsync(
        CamusConnection conn, TimeSpan budget, CancellationToken ct)
    {
        IReadOnlyList<TokenRow> rows = await ReadWithRetryAsync(conn,
            $"SELECT id, slot, gen, prev_id FROM {TableName}@{{FORCE_INDEX=pk}}",
            r => new TokenRow(r.GetString(0), r.GetInt64(1), r.GetInt64(2), r.IsDBNull(3) ? "" : r.GetString(3)),
            budget, ct).ConfigureAwait(false);
        IReadOnlyList<string> ids = await ReadWithRetryAsync(conn,
            $"SELECT id FROM {TableName}@{{FORCE_INDEX={IndexName}}}", r => r.GetString(0), budget, ct).ConfigureAwait(false);
        return (rows, ids);
    }

    private static async Task<IReadOnlyList<T>> ReadWithRetryAsync<T>(
        CamusConnection conn, string sql, Func<CamusDataReader, T> map, TimeSpan budget, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand(sql);
                using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                List<T> rows = [];
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    rows.Add(map(reader));
                return rows;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && ErrorClassifier.IsRetryableForIdempotentRead(ex) && waited.Elapsed < budget)
            {
                await Task.Delay(Math.Min(200 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

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
