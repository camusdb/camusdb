/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc.Client.Batching;
using CamusDB.Grpc.Client.Routing;

namespace CamusDB.Grpc.Client;

/// <summary>
/// An explicit transaction opened over the batch stream. Every op is pinned to the one stream slot the
/// session reserved at START (so the server's per-stream ordering chain sees them together) and carries
/// the transaction handle plus the latest causal token, which the session threads forward for
/// read-your-writes. Issue the transaction's statements sequentially (await each) so no two same-handle
/// ops are ever in flight at once.
///
/// <para><b>Deferred start under routing.</b> When the owning connection negotiates routing and no
/// explicit affinity was given, <c>BeginTransactionAsync</c> sends nothing: the session holds the
/// START request and picks its endpoint at the <em>first statement</em>, using that statement's
/// learned route. The server anchors the transaction's coordinator session to the first table the
/// transaction touches, so starting on that table's leader is what makes every registration and the
/// commit local. From the first statement on the session is pinned exactly as an eager one — advice
/// learned from later statements informs the <em>next</em> transaction, never this one. A session
/// finalized before any statement sends START and then the finalize on a rotation endpoint, so the
/// server sees the same lifecycle it always did. A START that fails surfaces at the first statement
/// (same exception type as an eager START); every later statement rethrows it, and rollback is then a
/// no-op because nothing was started.</para>
///
/// <para>With routing off, or with an explicit affinity, START is sent inside
/// <c>BeginTransactionAsync</c> and this class behaves as it did before deferred start existed.</para>
/// </summary>
public sealed class CamusTransactionSession
{
    /// <summary>The connection that owns the session; null for a session built over a bare batcher (test seam).</summary>
    private readonly CamusConnection? owner;

    /// <summary>The START request a deferred session still has to send; null once started or for eager sessions.</summary>
    private readonly SqlRequest? startRequest;

    /// <summary>True when statements ask the server for routing metadata so the connection can learn from them.</summary>
    private readonly bool negotiate;

    private readonly string database;

    private GrpcBatcher? batcher;
    private int slot;
    private TxnHandle? handle;
    private CausalToken token;
    private bool finalized;

    /// <summary>Guards <see cref="startTask"/> so a deferred START is sent exactly once.</summary>
    private readonly object startSync = new();

    /// <summary>The single in-flight or completed deferred START; every caller awaits this same task.</summary>
    private Task? startTask;

    /// <summary>An eager session: START already succeeded on <paramref name="batcher"/> / <paramref name="slot"/>.</summary>
    internal CamusTransactionSession(
        GrpcBatcher batcher, string database, TxnHandle handle, int slot,
        CamusConnection? owner = null, bool negotiate = false)
    {
        this.batcher   = batcher;
        this.database  = database;
        this.handle    = handle;
        this.slot      = slot;
        this.owner     = owner;
        this.negotiate = negotiate;
    }

    /// <summary>A deferred session: START is sent at the first statement, on that statement's learned endpoint.</summary>
    internal CamusTransactionSession(CamusConnection owner, string database, SqlRequest startRequest)
    {
        this.owner        = owner;
        this.database     = database;
        this.startRequest = startRequest;
        negotiate         = true;
    }

    /// <summary>The latest causal token observed by this session (advances on every reply).</summary>
    public CausalToken Token => token;

    /// <summary>
    /// True once the server acknowledged START. False for a deferred session that has not yet run a
    /// statement, so a caller can tell that no server-side transaction exists yet.
    /// </summary>
    public bool IsStarted => handle is not null;

    public async Task<QueryResult> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        StatementRouteKey key = new(database, sql, RouteOpKind.Query);
        await EnsureStartedAsync(key, cancellationToken).ConfigureAwait(false);
        long observed = owner?.ObserveRouteRevision(key) ?? 0;
        QueryResult result = await batcher!.EnqueueQueryAsync(BuildRequest(sql), slot, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        owner?.LearnFromResult(key, result.Routing, observed);
        return result;
    }

    public async Task<NonQueryResult> ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        StatementRouteKey key = new(database, sql, RouteOpKind.NonQuery);
        await EnsureStartedAsync(key, cancellationToken).ConfigureAwait(false);
        long observed = owner?.ObserveRouteRevision(key) ?? 0;
        NonQueryResult result = await batcher!.EnqueueNonQueryAsync(BuildRequest(sql), slot, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        owner?.LearnFromResult(key, result.Routing, observed);
        return result;
    }

    /// <summary>
    /// Runs a prepared statement inside this transaction. The statement registers itself on the
    /// session's reserved slot on first use, so the execution stays on the one stream the server
    /// orders this transaction's ops on. As the first statement of a deferred session it also chooses
    /// where the transaction starts, through its own learned route.
    /// </summary>
    public async Task<QueryResult> ExecuteQueryAsync(
        CamusPreparedStatement statement, IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        await EnsureStartedAsync(statement.QueryRouteKey, cancellationToken).ConfigureAwait(false);
        long observed = owner?.ObserveRouteRevision(statement.QueryRouteKey) ?? 0;
        QueryResult result = await statement
            .ExecuteQueryAsync(batcher!, slot, ResumeHandle(), values, negotiate, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        owner?.LearnFromResult(statement.QueryRouteKey, result.Routing, observed);
        return result;
    }

    /// <inheritdoc cref="ExecuteQueryAsync(CamusPreparedStatement, IReadOnlyList{object?}, CancellationToken)"/>
    public async Task<NonQueryResult> ExecuteNonQueryAsync(
        CamusPreparedStatement statement, IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        EnsureLive();
        await EnsureStartedAsync(statement.NonQueryRouteKey, cancellationToken).ConfigureAwait(false);
        long observed = owner?.ObserveRouteRevision(statement.NonQueryRouteKey) ?? 0;
        NonQueryResult result = await statement
            .ExecuteNonQueryAsync(batcher!, slot, ResumeHandle(), values, negotiate, cancellationToken).ConfigureAwait(false);
        Advance(result.Token);
        owner?.LearnFromResult(statement.NonQueryRouteKey, result.Routing, observed);
        return result;
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureLive();
        // A session that never ran a statement still starts — on a rotation endpoint, since no
        // statement named a route — so the server sees START then COMMIT exactly as before.
        await EnsureStartedAsync(null, cancellationToken).ConfigureAwait(false);
        CausalToken t = await batcher!.EnqueueCommitAsync(BuildRequest(""), slot, cancellationToken).ConfigureAwait(false);
        Advance(t);
        finalized = true;
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        EnsureLive();

        // A deferred START that failed left nothing on the server to roll back; the failure already
        // surfaced at the statement that triggered it, so rollback completes quietly.
        if (handle is null && startTask is { IsFaulted: true } or { IsCanceled: true })
        {
            finalized = true;
            return;
        }

        await EnsureStartedAsync(null, cancellationToken).ConfigureAwait(false);
        await batcher!.EnqueueRollbackAsync(BuildRequest(""), slot, cancellationToken).ConfigureAwait(false);
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
    /// Sends the deferred START once, on the endpoint <paramref name="key"/>'s learned route names
    /// (rotation when the key is null or cold). Concurrent first statements all await the same START;
    /// a failed START stays failed, so every later statement rethrows rather than starting a second
    /// transaction the caller does not know about.
    /// </summary>
    private Task EnsureStartedAsync(StatementRouteKey? key, CancellationToken cancellationToken)
    {
        if (handle is not null)
            return Task.CompletedTask;

        lock (startSync)
            return startTask ??= StartAsync(key, cancellationToken);
    }

    private async Task StartAsync(StatementRouteKey? key, CancellationToken cancellationToken)
    {
        RoutedEndpoint endpoint = key is { } routeKey
            ? owner!.SelectEndpoint(routeKey, out _)
            : owner!.NextRotation(owner.Clock());

        int reserved = endpoint.Batcher.ReserveSlot();
        try
        {
            TxnHandle started = await endpoint.Batcher
                .EnqueueStartAsync(startRequest!, reserved, cancellationToken).ConfigureAwait(false);

            // Seat the batcher and slot before the handle: IsStarted (handle non-null) is the fast
            // path every statement checks without the lock.
            batcher = endpoint.Batcher;
            slot = reserved;
            handle = started;
        }
        catch (Exception ex) when (CamusConnection.IsTransportFailure(ex))
        {
            owner.NoteTransportFailure(endpoint);
            throw;
        }
    }

    /// <summary>
    /// Builds a request that resumes this transaction: the handle carries the session's latest causal
    /// token (all three of N/L/C, per the protocol) so the server continues from what the session has
    /// already observed. Routing metadata is negotiated when the owning connection learns routes: the
    /// advice describes the statement's table and feeds the next transaction's start.
    /// </summary>
    private SqlRequest BuildRequest(string sql)
    {
        SqlRequest request = new() { Database = database, Sql = sql, TxnHandle = ResumeHandle() };
        if (negotiate)
            request.RoutingAcceptVersion = RoutingWire.AcceptVersion;
        return request;
    }

    /// <summary>
    /// The handle to resume this transaction with, carrying the session's latest causal token (all
    /// three of N/L/C, per the protocol) so the server continues from what the session has already
    /// observed. A prepared execution needs the same handle without the inline database and SQL,
    /// which the statement id already stands for.
    /// </summary>
    private TxnHandle ResumeHandle() => new()
    {
        TxnIdPt      = handle!.TxnIdPt,
        TxnIdCounter = handle.TxnIdCounter,
        CausalTokenN = token.N,
        CausalTokenL = token.L,
        CausalTokenC = token.C,
    };
}
