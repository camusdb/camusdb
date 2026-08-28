/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using CamusDB.Client;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Util;

namespace CamusDB.Workload.Workload;

/// <summary>
/// Owns the deterministic account-like dataset the workload runs against: the DDL, the seeded rows,
/// and the fingerprint that ties a <c>run</c> to the exact data an <c>init</c> produced. Everything is
/// a pure function of <c>(seed, rows, payloadBytes, tables)</c> — ids come from <see cref="RowIdFactory"/>,
/// and balances/payloads from the same seed — so seeding is idempotent and a run can verify it is
/// pointed at the data it expects without trusting scan order. Seeding always happens outside the
/// measured interval.
///
/// <para>The dataset can span more than one table. One table is the historical shape and keeps the
/// historical name. Several tables exist because of how CamusDB places data: each table (and each of
/// its secondary indexes) is its own key space, so a single-table dataset lives on one partition under
/// hash routing, and on one range under key-range routing. A dataset spread over many tables occupies
/// every partition, which is what a run that wants all partitions loaded — and, under key-range
/// routing, wants ranges large and hot enough to split — needs.</para>
///
/// <para>Rows are assigned to tables in contiguous blocks of the row index, so a table holds an
/// ordered, contiguous span of ids (<see cref="RowIdFactory"/> encodes the row index in the id's low
/// bytes). That gives a range splitter a key space it can bisect, and keeps a seeding batch inside one
/// table. Every table gets at least one row as long as <c>rows &gt;= tables</c>.</para>
/// </summary>
public sealed class Dataset
{
    /// <summary>Name of the single table, and the prefix every table name in a multi-table dataset
    /// starts with.</summary>
    public const string TableName = "workload_accounts";

    public const string IndexName = "workload_accounts_owner";

    private readonly ulong _seed;
    private readonly long _rows;
    private readonly int _payloadBytes;
    private readonly long _ownerBuckets;
    private readonly int _tables;
    private readonly string[] _tableNames;

    public Dataset(ulong seed, long rows, int payloadBytes, int tables = 1)
    {
        _seed = seed;
        _rows = rows;
        _payloadBytes = payloadBytes;
        // A table with no rows would be created and never touched, and its empty key space cannot
        // split or serve a read, so the table count never exceeds the row count.
        _tables = Math.Max(1, rows > 0 && tables > rows ? (int)rows : tables);
        // Give the owner secondary index realistic, bounded cardinality (~100 rows per owner value).
        _ownerBuckets = Math.Max(1, rows / 100);
        _tableNames = BuildTableNames(_tables);
    }

    public long Rows => _rows;

    /// <summary>Number of tables the rows are spread over; 1 is the historical single-table shape.</summary>
    public int Tables => _tables;

    /// <summary>Every table in the dataset, in row-index order.</summary>
    public IReadOnlyList<string> TableNames => _tableNames;

    /// <summary>Stable identity of this dataset shape; recorded at init and re-checked at run start.</summary>
    public string Fingerprint()
    {
        // A single-table dataset hashes exactly the material it always has, so a fingerprint recorded
        // by an earlier run of the same shape still matches. The table count only enters the material
        // when it changes the schema.
        List<string> material =
        [
            CreateTableSqlFor(_tableNames[0]), CreateIndexSqlFor(_tableNames[0]),
            $"seed={_seed}", $"rows={_rows}", $"payloadBytes={_payloadBytes}", $"ownerBuckets={_ownerBuckets}",
        ];
        if (_tables > 1)
            material.Add($"tables={_tables}");

        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', material)));
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>Deterministic values for one row index. Ids are unique across the whole dataset, so a
    /// row index identifies a row without naming its table.</summary>
    public (string Id, long Owner, long Balance, string Payload) RowFor(long index)
    {
        string id = RowIdFactory.ForRow(_seed, index);
        long owner = index % _ownerBuckets;
        DeterministicRandom rng = new(_seed ^ (0xD1B54A32D192ED03UL * (ulong)(index + 1)));
        long balance = 1_000_000 + rng.NextLong(1_000_000);
        string payload = MakePayload(rng);
        return (id, owner, balance, payload);
    }

    /// <summary>The table that holds a row index.</summary>
    public string TableOf(long index) => _tableNames[TableIndexOf(index)];

    /// <summary>
    /// The position of a row index in <see cref="TableNames"/>. Blocks are cut by exact division, so
    /// table <c>t</c> starts at <see cref="TableRowStart"/> and no table is left empty when there are
    /// at least as many rows as tables.
    /// </summary>
    public int TableIndexOf(long index)
    {
        if (_tables == 1 || _rows <= 0)
            return 0;

        long clamped = Math.Clamp(index, 0, _rows - 1);
        return (int)(clamped * _tables / _rows);
    }

    /// <summary>First row index held by a table; <c>TableRowStart(Tables)</c> is the row count, so the
    /// count of a table is the difference between two consecutive starts.</summary>
    public long TableRowStart(int tableIndex)
    {
        int clamped = Math.Clamp(tableIndex, 0, _tables);
        return (clamped * _rows + _tables - 1) / _tables;
    }

    /// <summary>How many rows a table holds.</summary>
    public long TableRowCount(int tableIndex) => TableRowStart(tableIndex + 1) - TableRowStart(tableIndex);

    /// <summary>
    /// A row index inside a named table, chosen by an arbitrary offset. Used to aim an operation at a
    /// specific table — the caller supplies the spread, this maps it into the table's block.
    /// </summary>
    public long RowIndexInTable(int tableIndex, ulong offset)
    {
        int clamped = Math.Clamp(tableIndex, 0, _tables - 1);
        long count = TableRowCount(clamped);
        if (count <= 0)
            return TableRowStart(clamped);
        return TableRowStart(clamped) + (long)(offset % (ulong)count);
    }

    private static string[] BuildTableNames(int tables)
    {
        if (tables == 1)
            return [TableName];

        string[] names = new string[tables];
        for (int i = 0; i < tables; i++)
            names[i] = $"{TableName}_{i:D2}";
        return names;
    }

    private static string CreateTableSqlFor(string table) =>
        $"CREATE TABLE {table} (" +
        "id OID PRIMARY KEY, " +
        "owner INT64 NOT NULL, " +
        "balance INT64 NOT NULL, " +
        "version INT64 NOT NULL, " +
        "payload STRING NOT NULL)";

    private static string CreateIndexSqlFor(string table) => $"CREATE INDEX {table}_owner ON {table} (owner)";

    private string MakePayload(DeterministicRandom rng)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        char[] buffer = new char[_payloadBytes];
        for (int i = 0; i < buffer.Length; i++)
            buffer[i] = alphabet[(int)rng.NextLong(alphabet.Length)];
        return new string(buffer);
    }

    /// <summary>
    /// Creates the database (if missing), every table, and its index. Safe to call repeatedly — an
    /// already-existing table is treated as success so <c>init</c> and <c>--init-if-missing</c> are
    /// idempotent.
    /// </summary>
    public async Task EnsureSchemaAsync(CamusConnection conn, CancellationToken ct)
    {
        await conn.CreateDatabaseAsync(ifNotExists: true, cancellationToken: ct).ConfigureAwait(false);
        foreach (string table in _tableNames)
        {
            await EnsureDdlAsync(conn, CreateTableSqlFor(table), $"table {table}", ct).ConfigureAwait(false);
            await EnsureDdlAsync(conn, CreateIndexSqlFor(table), $"index on {table}", ct).ConfigureAwait(false);
        }
    }

    /// <summary>True when the tables already hold the expected row count (so seeding can be skipped).
    ///
    /// <para>Each count carries the same wall-clock patience as a seeding batch. This runs at the
    /// start of both verbs, including <c>run</c>, where it is the first thing to touch the cluster
    /// after setup — exactly when a burst of unanswered inter-node requests is most likely. Without
    /// the retry a burst here fails the whole run before the measured window opens, and reports it as
    /// a bare cancellation with no indication of which step gave up.</para></summary>
    public async Task<bool> IsSeededAsync(CamusConnection conn, CancellationToken ct)
    {
        long existing = 0;
        foreach (string table in _tableNames)
        {
            try
            {
                existing += await CountRowsAsync(conn, table, ct).ConfigureAwait(false);
            }
            catch (CamusException e) when (IsMissingTable(e))
            {
                // Table not found on the node this round-robined to. Either it was never created, or a
                // just-issued CREATE TABLE has not propagated to this node yet. Both mean "not fully
                // seeded", so report that and let the seeding path (which retries the propagation lag per
                // batch) proceed rather than crash the whole init.
                return false;
            }
        }

        return existing >= _rows;
    }

    /// <summary>
    /// Counts one table, riding out a transport transient for <see cref="SeedTransientBudget"/>.
    /// A missing table is left to the caller: that means something different (propagation lag) and
    /// has its own handling.
    /// </summary>
    private static async Task<long> CountRowsAsync(CamusConnection conn, string table, CancellationToken ct)
    {
        long startedAt = Stopwatch.GetTimestamp();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand count = conn.CreateCamusCommand($"SELECT COUNT(*) FROM {table}");
                using CamusDataReader reader = await count.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                    return 0;
                return reader.GetInt64(0);
            }
            catch (Exception ex) when (ex is not CamusException camus || !IsMissingTable(camus))
            {
                bool retryable = ErrorClassifier.Classify(ex).Status is OperationStatus.Transient or OperationStatus.Conflict;
                if (!retryable || Stopwatch.GetElapsedTime(startedAt) >= SeedTransientBudget || ct.IsCancellationRequested)
                    throw;

                await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// True when an exception is CamusDB's "table doesn't exist" (CADB0011), reported during setup as
    /// pure catalog-propagation lag. The gRPC client does not always carry the server error code across
    /// the wire — <see cref="CamusException.Code"/> can arrive empty — so the message is the reliable
    /// signal, with the code checked first when it is present.
    /// </summary>
    private static bool IsMissingTable(Exception ex)
    {
        if (ex is not CamusException camus)
            return false;
        if (camus.Code == "CADB0011")
            return true;
        return camus.Message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            && camus.Message.Contains("Table", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Seeds all rows in batched transactions. Idempotent when already seeded.</summary>
    public async Task SeedAsync(CamusConnection conn, int batchSize, CancellationToken ct)
    {
        if (await IsSeededAsync(conn, ct).ConfigureAwait(false))
            return;

        if (batchSize < 1)
            batchSize = 1;

        for (long start = 0; start < _rows; start += batchSize)
        {
            long end = Math.Min(start + batchSize, _rows);

            // Retry each batch from a fresh BEGIN on a retryable abort. Seeding runs right after the
            // cluster comes up, while the Raft leader balancer and placement rebalancer may still be
            // moving leadership; a batch commit can then hit a transient TransactionMustRetry / lock
            // conflict / Kahuna-aborted response. Without this, one momentary election during setup
            // fails the whole seed — and, under a chaos scenario, aborts the run before it starts.
            // Two independent retry budgets. A conflict / transient abort gets the tight budget: it
            // signals contention that either clears in a few tries or is a real problem. Table-not-found
            // (CADB0011) gets a far more patient budget on its own counter, because it is pure one-time
            // DDL-propagation lag: on a multi-node cluster the seeding INSERT can route to a node the
            // CREATE TABLE has not reached yet, and — with the placement rebalancer actively moving the
            // catalog partition at bootstrap — that window can outlast a few seconds. Seeding is setup,
            // so waiting a minute for the table to become visible everywhere is the right trade.
            int retryAttempt = 0;
            int propagationAttempt = 0;
            long transientSince = 0;
            while (true)
            {
                // BEGIN belongs inside the try. It is a round trip like any other, and during a burst
                // of unanswered inter-node requests it is usually the first call to time out — so
                // leaving it outside meant a failed BEGIN bypassed every retry below and killed the
                // whole seed, no matter how patient those retries were.
                CamusTransaction? tx = null;
                try
                {
                    tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);

                    for (long i = start; i < end; i++)
                    {
                        (string id, long owner, long balance, string payload) = RowFor(i);
                        using CamusCommand insert = conn.CreateCamusCommand(
                            $"INSERT INTO {TableOf(i)} (id, owner, balance, version, payload) " +
                            "VALUES (@id, @owner, @balance, 0, @payload)");
                        insert.Transaction = tx;
                        insert.Parameters.Add("@id", ColumnType.Id, id);
                        insert.Parameters.Add("@owner", ColumnType.Integer64, owner);
                        insert.Parameters.Add("@balance", ColumnType.Integer64, balance);
                        insert.Parameters.Add("@payload", ColumnType.String, payload);
                        await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }
                    await tx.CommitAsync(ct).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex)
                {
                    // Best-effort rollback: an already-aborted transaction may itself refuse the
                    // rollback, and a BEGIN that never returned leaves nothing to roll back at all.
                    if (tx is not null)
                    {
                        try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                        catch { /* the batch is retried from a new transaction regardless */ }
                    }

                    // A duplicate on a seed id means this batch is already committed. Seed ids are
                    // deterministic, only this seeder writes them, and each batch commits in one
                    // transaction — so a collision can only come from a batch whose earlier commit
                    // was reported indeterminate (the outcome was lost in transit but the commit
                    // landed) or from a batch a prior seed pass completed. The rows are present;
                    // move to the next batch instead of failing the whole init. The end-of-run
                    // reconciliation still verifies the row count, so a wrong skip cannot pass.
                    if (IsDuplicateSeedRow(ex))
                        break;

                    // The ct.IsCancellationRequested guard keeps the workload's own SIGINT (also a
                    // canceled task) from being retried.
                    OperationStatus status = ErrorClassifier.Classify(ex).Status;
                    bool tablePropagating = IsMissingTable(ex);
                    bool retryable = status is OperationStatus.Conflict or OperationStatus.Transient || tablePropagating;

                    // Three budgets, because the three causes clear on different timescales.
                    //
                    // A transport transient gets a WALL-CLOCK budget, not an attempt count. Measured
                    // during seeding: the inter-node batcher produces bursts of unanswered requests
                    // that last over a minute, and they occur on every run — a seed that survived one
                    // logged 1,892 of them, more than a seed that failed. An attempt count cannot
                    // express "outlast a burst" when each failed attempt costs whatever the client's
                    // request timeout happens to be, so twenty tight attempts were a coin flip on
                    // whether setup completed at all. Seeding is setup: waiting costs wall clock and
                    // nothing else.
                    //
                    // A conflict keeps the tight count-based budget: contention during seeding either
                    // clears in a few tries or is a real problem worth surfacing.
                    bool budgetLeft;
                    if (tablePropagating)
                    {
                        budgetLeft = ++propagationAttempt < MaxSeedPropagationAttempts;
                    }
                    else if (status == OperationStatus.Transient)
                    {
                        if (transientSince == 0)
                            transientSince = Stopwatch.GetTimestamp();
                        budgetLeft = Stopwatch.GetElapsedTime(transientSince) < SeedTransientBudget;
                        retryAttempt++;
                    }
                    else
                    {
                        budgetLeft = ++retryAttempt < MaxSeedBatchAttempts;
                    }

                    if (!retryable || !budgetLeft || ct.IsCancellationRequested)
                        throw;

                    int delayMs = tablePropagating || status == OperationStatus.Transient
                        ? Math.Min(250 * retryAttempt, 2000)
                        : Math.Min(50 * retryAttempt, 500);
                    await Task.Delay(delayMs, ct).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// True when an exception is CamusDB's duplicate-unique-key error (CADB0300) for one of this
    /// dataset's tables. Mirrors <see cref="IsMissingTable"/>: the gRPC client does not always carry the
    /// server error code across the wire, so the message is checked when the code is absent. Every table
    /// name starts with <see cref="TableName"/>, so the prefix identifies all of them.
    /// </summary>
    private static bool IsDuplicateSeedRow(Exception ex)
    {
        if (ex is not CamusException camus)
            return false;
        if (camus.Code == "CADB0300")
            return true;
        return camus.Message.Contains("Duplicate entry", StringComparison.OrdinalIgnoreCase)
            && camus.Message.Contains(TableName, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Bounded retries per seeding batch for a conflict before a persistent failure is surfaced.</summary>
    private const int MaxSeedBatchAttempts = 20;

    /// <summary>
    /// How long a seeding batch keeps retrying a transport transient before giving up. Wall clock,
    /// not attempts: each failed attempt costs an unpredictable amount of time (whatever the client
    /// request timeout is), so only a clock can express "ride out a burst of unanswered inter-node
    /// requests". Sized from observation, not guesswork: bursts of about seventy seconds and of four
    /// minutes have both been recorded, the latter exhausting a two-minute budget and failing an
    /// otherwise healthy run. Five minutes covers what has been seen, with margin.
    ///
    /// <para>This is compensation, not a cure. The bursts are a property of the store under bulk
    /// writes; raising this number does not stop them, it only keeps setup alive while they last. If
    /// one ever outlasts this budget, investigate the burst rather than raising the number again.</para>
    /// </summary>
    private static readonly TimeSpan SeedTransientBudget = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Patient retry budget for table-not-found (DDL propagation lag) during seeding: 120 tries at a
    /// steady 500&#160;ms is roughly a minute, enough to outlast catalog propagation on a large cluster
    /// whose placement rebalancer is still moving the catalog partition at bootstrap.
    /// </summary>
    private const int MaxSeedPropagationAttempts = 120;

    /// <summary>
    /// Runs one setup DDL statement, treating "the object is already there" as success and anything
    /// else as a failure worth reporting — after a bounded retry for the failures that clear on their
    /// own.
    ///
    /// <para>This used to swallow every <see cref="CamusException"/> as a benign already-exists race.
    /// That hid the case that matters: a <c>CREATE TABLE</c> refused because the DDL could not reach
    /// the schema leader (CADB0099) left the workload believing the schema was ready, and the run
    /// then failed a minute later with "table doesn't exist" — naming a symptom two steps removed
    /// from the cause. A multi-table dataset issues one statement per table plus one per index, so
    /// every extra table multiplies the exposure.</para>
    ///
    /// <para>The retry exists because a DDL forward failing right after cluster start is usually
    /// transient: the schema leader is being elected or is moving. The budget matches the seeding
    /// propagation budget for the same reason — setup can afford to wait a minute, and a run that
    /// starts on a half-created schema is worse than one that waits.</para>
    /// </summary>
    private static async Task EnsureDdlAsync(CamusConnection conn, string sql, string what, CancellationToken ct)
    {
        int attempt = 0;
        while (true)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand(sql);
                await cmd.ExecuteDDLAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                // Idempotent setup: the object is already there, from a prior init or a concurrent one.
                if (IsAlreadyExists(ex))
                    return;

                attempt++;

                OperationStatus status = ErrorClassifier.Classify(ex).Status;
                bool retryable = status is OperationStatus.Conflict or OperationStatus.Transient
                    || IsDdlForwardingFailure(ex)
                    || IsTransportTransient(ex);

                if (!retryable || attempt >= MaxSchemaAttempts || ct.IsCancellationRequested)
                    throw new InvalidOperationException(
                        $"could not create {what} after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}", ex);

                await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// True when a DDL failed because the object already exists. A table reports CADB0013 and a
    /// database CADB0012; a duplicate index arrives as CADB0400 with an "already exists" message, so
    /// the message is checked too — as elsewhere, the gRPC client does not always carry the code.
    /// </summary>
    private static bool IsAlreadyExists(Exception ex)
    {
        if (ex is not CamusException camus)
            return false;
        if (camus.Code is "CADB0012" or "CADB0013")
            return true;
        return camus.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// True when a DDL could not be forwarded to the schema leader (CADB0099). Distinct from a
    /// refused statement: nothing was applied, and the same statement usually succeeds once the
    /// schema leader settles.
    /// </summary>
    private static bool IsDdlForwardingFailure(Exception ex)
    {
        if (ex is not CamusException camus)
            return false;
        if (camus.Code == "CADB0099")
            return true;
        return camus.Message.Contains("forward DDL", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// True when a DDL failed on the transport rather than on its merits: the call exceeded its
    /// deadline, or the node was unavailable. These arrive as a <see cref="CamusException"/> whose
    /// <see cref="CamusException.Code"/> is empty — the gRPC status never becomes a CamusDB error code
    /// — so <see cref="ErrorClassifier"/> reads them as a domain error, which for a setup statement is
    /// the wrong verdict: nothing about the statement is wrong, the cluster was busy. A node still
    /// electing its schema leader answers exactly this way, and the very same statement succeeds a
    /// second later.
    /// </summary>
    private static bool IsTransportTransient(Exception ex)
    {
        if (ex is not CamusException camus)
            return false;

        return camus.Message.Contains("DeadlineExceeded", StringComparison.OrdinalIgnoreCase)
            || camus.Message.Contains("Unavailable", StringComparison.OrdinalIgnoreCase)
            || camus.Message.Contains("deadline", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Attempts per setup DDL statement before it is reported as a failure: 40 tries backing off to
    /// 2&#160;s is roughly a minute, matching <see cref="MaxSeedPropagationAttempts"/>.
    /// </summary>
    private const int MaxSchemaAttempts = 40;
}
