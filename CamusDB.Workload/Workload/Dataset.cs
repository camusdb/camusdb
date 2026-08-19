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
        using CamusCommand count = conn.CreateCamusCommand($"SELECT COUNT(*) FROM {TableName}");
        using CamusDataReader reader = await count.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return false;
        long existing = reader.GetInt64(0);
        return existing >= _rows;
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
            for (int attempt = 1; ; attempt++)
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

                    // Retry a retryable server verdict (conflict) OR a transient transport failure — a
                    // client request timeout / cancellation is common while a just-started cluster is
                    // still electing leaders. The ct.IsCancellationRequested guard keeps the workload's
                    // own SIGINT (which also surfaces as a canceled task) from being retried.
                    OperationStatus status = ErrorClassifier.Classify(ex).Status;
                    bool retryable = status is OperationStatus.Conflict or OperationStatus.Transient;
                    if (!retryable || attempt >= MaxSeedBatchAttempts || ct.IsCancellationRequested)
                        throw;

                    await Task.Delay(Math.Min(50 * attempt, 500), ct).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>Bounded retries per seeding batch before a persistent failure is surfaced.</summary>
    private const int MaxSeedBatchAttempts = 20;

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
