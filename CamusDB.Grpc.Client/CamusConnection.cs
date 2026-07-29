
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Net.Client;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client;

/// <summary>
/// Entry point for the multiplexing gRPC client. Owns the batcher (a pool of long-lived
/// <c>BatchExecute</c> streams) and issues autocommit statements and transactions over it, so many
/// concurrent callers share the streams. DDL is the one thing that does not batch — it goes over the
/// unary <c>ExecuteDdl</c> RPC.
/// </summary>
public sealed class CamusConnection : IAsyncDisposable
{
    private readonly GrpcBatcher batcher;
    private readonly CamusSql.CamusSqlClient? unaryClient;
    private readonly GrpcChannel? ownedChannel;

    /// <summary>
    /// Connects to <paramref name="address"/> (e.g. <c>https://host:port</c>, or <c>http://…</c> for
    /// plaintext h2c in dev). The channel's TLS trust policy and keep-alive tuning come from
    /// <paramref name="options"/>; the pool multiplexes several long-lived <c>BatchExecute</c> streams
    /// over it.
    /// </summary>
    public static CamusConnection Connect(string address, CamusGrpcOptions? options = null)
    {
        options ??= new CamusGrpcOptions();
        GrpcChannel channel = ChannelFactory.Create(address, options);
        CamusSql.CamusSqlClient client = new(channel);
        GrpcBatcher batcher = new(options, id => new GrpcBatchTransport(id, client));
        return new CamusConnection(batcher, client, channel);
    }

    internal CamusConnection(GrpcBatcher batcher, CamusSql.CamusSqlClient? unaryClient = null, GrpcChannel? ownedChannel = null)
    {
        this.batcher      = batcher;
        this.unaryClient  = unaryClient;
        this.ownedChannel = ownedChannel;
    }

    // ─── Autocommit ───────────────────────────────────────────────────────────

    public Task<QueryResult> ExecuteQueryAsync(string database, string sql, CancellationToken cancellationToken = default)
        => batcher.EnqueueQueryAsync(new SqlRequest { Database = database, Sql = sql }, slotIndex: null, cancellationToken);

    public Task<NonQueryResult> ExecuteNonQueryAsync(string database, string sql, CancellationToken cancellationToken = default)
        => batcher.EnqueueNonQueryAsync(new SqlRequest { Database = database, Sql = sql }, slotIndex: null, cancellationToken);

    // ─── Prepared statements ──────────────────────────────────────────────────

    /// <summary>
    /// Registers a statement so later executions send only its values, not the SQL and not the
    /// parameter names.
    ///
    /// <para>The registration is performed eagerly on one stream so the caller learns the parameter
    /// order immediately and a malformed statement fails here rather than at some later execution.
    /// The other streams in the pool register lazily, on first use — see
    /// <see cref="CamusPreparedStatement"/> for why a statement cannot simply hold one handle.</para>
    /// </summary>
    public async Task<CamusPreparedStatement> PrepareAsync(
        string database, string sql, CancellationToken cancellationToken = default)
    {
        int slot = batcher.ReserveSlot();
        PreparedStatementKey key = new(database, sql);
        PreparedSlotEntry entry = await batcher
            .EnsurePreparedAsync(slot, key, cancellationToken).ConfigureAwait(false);

        return new CamusPreparedStatement(batcher, key, entry.ParameterNames);
    }

    /// <summary>DDL over the unary RPC (not batchable). Requires a real connection.</summary>
    public async Task<DdlReply> ExecuteDdlAsync(string database, string sql, CancellationToken cancellationToken = default)
    {
        if (unaryClient is null)
            throw new InvalidOperationException("This connection has no unary client for DDL");
        return await unaryClient.ExecuteDdlAsync(
            new SqlRequest { Database = database, Sql = sql }, cancellationToken: cancellationToken).ResponseAsync.ConfigureAwait(false);
    }

    // ─── Transactions ─────────────────────────────────────────────────────────

    /// <summary>
    /// Begins a transaction over the batch stream. Reserves a stream slot up front and pins every op of
    /// the returned session to it, so START/statements/COMMIT land in the same server-side ordering
    /// chain. The START op itself carries no handle yet, hence the slot is chosen here rather than hashed.
    /// </summary>
    public async Task<CamusTransactionSession> BeginTransactionAsync(
        string database,
        IsolationLevel isolation = IsolationLevel.Unspecified,
        TransactionMode mode = TransactionMode.Unspecified,
        LockingMode locking = LockingMode.Unspecified,
        CancellationToken cancellationToken = default)
    {
        int slot = batcher.ReserveSlot();
        SqlRequest request = new()
        {
            Database         = database,
            IsolationLevel   = isolation,
            TransactionMode  = mode,
            Locking          = locking,
        };
        TxnHandle handle = await batcher.EnqueueStartAsync(request, slot, cancellationToken).ConfigureAwait(false);
        return new CamusTransactionSession(batcher, database, handle, slot);
    }

    public async ValueTask DisposeAsync()
    {
        await batcher.DisposeAsync().ConfigureAwait(false);
        ownedChannel?.Dispose();
    }
}
