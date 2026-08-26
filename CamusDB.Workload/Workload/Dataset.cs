/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Text;
using CamusDB.Client;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Util;

namespace CamusDB.Workload.Workload;

/// <summary>
/// Owns the deterministic account-like dataset the workload runs against: the DDL, the seeded rows,
/// and the fingerprint that ties a <c>run</c> to the exact data an <c>init</c> produced. Everything is
/// a pure function of <c>(seed, rows, payloadBytes)</c> — ids come from <see cref="RowIdFactory"/>,
/// and balances/payloads from the same seed — so seeding is idempotent and a run can verify it is
/// pointed at the data it expects without trusting scan order. Seeding always happens outside the
/// measured interval.
/// </summary>
public sealed class Dataset
{
    public const string TableName = "workload_accounts";
    public const string IndexName = "workload_accounts_owner";

    private static readonly string CreateTableSql =
        $"CREATE TABLE {TableName} (" +
        "id OID PRIMARY KEY, " +
        "owner INT64 NOT NULL, " +
        "balance INT64 NOT NULL, " +
        "version INT64 NOT NULL, " +
        "payload STRING NOT NULL)";

    private static readonly string CreateIndexSql =
        $"CREATE INDEX {IndexName} ON {TableName} (owner)";

    private readonly ulong _seed;
    private readonly long _rows;
    private readonly int _payloadBytes;
    private readonly long _ownerBuckets;

    public Dataset(ulong seed, long rows, int payloadBytes)
    {
        _seed = seed;
        _rows = rows;
        _payloadBytes = payloadBytes;
        // Give the owner secondary index realistic, bounded cardinality (~100 rows per owner value).
        _ownerBuckets = Math.Max(1, rows / 100);
    }

    public long Rows => _rows;

    /// <summary>Stable identity of this dataset shape; recorded at init and re-checked at run start.</summary>
    public string Fingerprint()
    {
        string material = string.Join('\n',
            CreateTableSql, CreateIndexSql,
            $"seed={_seed}", $"rows={_rows}", $"payloadBytes={_payloadBytes}", $"ownerBuckets={_ownerBuckets}");
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(material));
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>Deterministic values for one row index.</summary>
    public (string Id, long Owner, long Balance, string Payload) RowFor(long index)
    {
        string id = RowIdFactory.ForRow(_seed, index);
        long owner = index % _ownerBuckets;
        DeterministicRandom rng = new(_seed ^ (0xD1B54A32D192ED03UL * (ulong)(index + 1)));
        long balance = 1_000_000 + rng.NextLong(1_000_000);
        string payload = MakePayload(rng);
        return (id, owner, balance, payload);
    }

    private string MakePayload(DeterministicRandom rng)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        char[] buffer = new char[_payloadBytes];
        for (int i = 0; i < buffer.Length; i++)
            buffer[i] = alphabet[(int)rng.NextLong(alphabet.Length)];
        return new string(buffer);
    }

    /// <summary>
    /// Creates the database (if missing), table, and index. Safe to call repeatedly — an
    /// already-existing table is treated as success so <c>init</c> and <c>--init-if-missing</c> are
    /// idempotent.
    /// </summary>
    public async Task EnsureSchemaAsync(CamusConnection conn, CancellationToken ct)
    {
        await conn.CreateDatabaseAsync(ifNotExists: true, cancellationToken: ct).ConfigureAwait(false);
        await TryDdlAsync(conn, CreateTableSql, ct).ConfigureAwait(false);
        await TryDdlAsync(conn, CreateIndexSql, ct).ConfigureAwait(false);
    }

    /// <summary>True when the table already holds the expected row count (so seeding can be skipped).</summary>
    public async Task<bool> IsSeededAsync(CamusConnection conn, CancellationToken ct)
    {
        try
        {
            using CamusCommand count = conn.CreateCamusCommand($"SELECT COUNT(*) FROM {TableName}");
            using CamusDataReader reader = await count.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return false;
            long existing = reader.GetInt64(0);
            return existing >= _rows;
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
            while (true)
            {
                CamusTransaction tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);
                try
                {
                    for (long i = start; i < end; i++)
                    {
                        (string id, long owner, long balance, string payload) = RowFor(i);
                        using CamusCommand insert = conn.CreateCamusCommand(
                            $"INSERT INTO {TableName} (id, owner, balance, version, payload) " +
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
                    // Best-effort rollback: an already-aborted transaction may itself refuse the rollback.
                    try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
                    catch { /* the batch is retried from a new transaction regardless */ }

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
                    bool budgetLeft = tablePropagating
                        ? ++propagationAttempt < MaxSeedPropagationAttempts
                        : ++retryAttempt < MaxSeedBatchAttempts;
                    if (!retryable || !budgetLeft || ct.IsCancellationRequested)
                        throw;

                    int delayMs = tablePropagating ? 500 : Math.Min(50 * retryAttempt, 500);
                    await Task.Delay(delayMs, ct).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// True when an exception is CamusDB's duplicate-unique-key error (CADB0300) for this table's
    /// primary key. Mirrors <see cref="IsMissingTable"/>: the gRPC client does not always carry the
    /// server error code across the wire, so the message is checked when the code is absent.
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

    /// <summary>Bounded retries per seeding batch for a conflict / transient abort before a persistent failure is surfaced.</summary>
    private const int MaxSeedBatchAttempts = 20;

    /// <summary>
    /// Patient retry budget for table-not-found (DDL propagation lag) during seeding: 120 tries at a
    /// steady 500&#160;ms is roughly a minute, enough to outlast catalog propagation on a large cluster
    /// whose placement rebalancer is still moving the catalog partition at bootstrap.
    /// </summary>
    private const int MaxSeedPropagationAttempts = 120;

    private static async Task TryDdlAsync(CamusConnection conn, string sql, CancellationToken ct)
    {
        try
        {
            using CamusCommand cmd = conn.CreateCamusCommand(sql);
            await cmd.ExecuteDDLAsync(ct).ConfigureAwait(false);
        }
        catch (CamusException)
        {
            // Object already exists (or an equivalent benign DDL race) — schema setup is idempotent.
        }
    }
}
