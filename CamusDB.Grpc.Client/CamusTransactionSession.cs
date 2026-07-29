
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client;

/// <summary>
/// An explicit transaction opened over the batch stream. Every op is pinned to the one stream slot the
/// session reserved at BEGIN (so the server's per-stream ordering chain sees them together) and carries
/// the transaction handle plus the latest causal token, which the session threads forward for
/// read-your-writes. Issue the transaction's statements sequentially (await each) so no two same-handle
/// ops are ever in flight at once.
/// </summary>
public sealed class CamusTransactionSession
{
    private readonly GrpcBatcher batcher;
    private readonly string database;
    private readonly int slot;
    private readonly TxnHandle handle;
    private CausalToken token;
    private bool finalized;

    internal CamusTransactionSession(GrpcBatcher batcher, string database, TxnHandle handle, int slot)
    {
        this.batcher  = batcher;
        this.database = database;
        this.handle   = handle;
        this.slot     = slot;
    }

    /// <summary>The latest causal token observed by this session (advances on every reply).</summary>
    public CausalToken Token => token;

    public async Task<QueryResult> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        QueryResult result = await batcher.EnqueueQueryAsync(BuildRequest(sql), slot, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        return result;
    }

    public async Task<NonQueryResult> ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        NonQueryResult result = await batcher.EnqueueNonQueryAsync(BuildRequest(sql), slot, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        return result;
    }

    /// <summary>
    /// Runs a prepared statement inside this transaction. The statement registers itself on the
    /// session's reserved slot on first use, so the execution stays on the one stream the server
    /// orders this transaction's ops on.
    /// </summary>
    public async Task<QueryResult> ExecuteQueryAsync(
        CamusPreparedStatement statement, IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        QueryResult result = await statement
            .ExecuteQueryAsync(slot, ResumeHandle(), values, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        return result;
    }

    /// <inheritdoc cref="ExecuteQueryAsync(CamusPreparedStatement, IReadOnlyList{object?}, CancellationToken)"/>
    public async Task<NonQueryResult> ExecuteNonQueryAsync(
        CamusPreparedStatement statement, IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        NonQueryResult result = await statement
            .ExecuteNonQueryAsync(slot, ResumeHandle(), values, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        return result;
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureLive();
        CausalToken t = await batcher.EnqueueCommitAsync(BuildRequest(""), slot, cancellationToken).ConfigureAwait(false);
        Advance(t);
        finalized = true;
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        EnsureLive();
        await batcher.EnqueueRollbackAsync(BuildRequest(""), slot, cancellationToken).ConfigureAwait(false);
        finalized = true;
    }

    private void EnsureLive()
    {
        if (finalized)
            throw new InvalidOperationException("Transaction has already been committed or rolled back");
    }

    private void Advance(CausalToken next)
    {
        if (!next.IsEmpty)
            token = next;
    }

    /// <summary>
    /// Builds a request that resumes this transaction: the handle carries the session's latest causal
    /// token (all three of N/L/C, per the protocol) so the server continues from what the session has
    /// already observed.
    /// </summary>
    private SqlRequest BuildRequest(string sql)
        => new() { Database = database, Sql = sql, TxnHandle = ResumeHandle() };

    /// <summary>
    /// The handle to resume this transaction with, carrying the session's latest causal token (all
    /// three of N/L/C, per the protocol) so the server continues from what the session has already
    /// observed. A prepared execution needs the same handle without the inline database and SQL,
    /// which the statement id already stands for.
    /// </summary>
    private TxnHandle ResumeHandle() => new()
    {
        TxnIdPt      = handle.TxnIdPt,
        TxnIdCounter = handle.TxnIdCounter,
        CausalTokenN = token.N,
        CausalTokenL = token.L,
        CausalTokenC = token.C,
    };
}
