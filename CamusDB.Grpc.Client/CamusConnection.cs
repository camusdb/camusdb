
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Net.Client;
using CamusDB.Grpc.Client.Batching;
using CamusDB.Grpc.Client.Routing;

namespace CamusDB.Grpc.Client;

/// <summary>
/// Entry point for the multiplexing gRPC client. Owns a set of endpoints — each with its own pool
/// of long-lived <c>BatchExecute</c> streams — and issues autocommit statements and transactions
/// over them, so many concurrent callers share the streams. DDL is the one thing that does not
/// batch — it goes over the unary <c>ExecuteDdl</c> RPC of the first endpoint.
///
/// <para><b>Routing chooses an endpoint before enqueueing, never after.</b> A duplex stream cannot
/// redirect an individual request, so learned statement routing is purely a selection bias applied
/// here: learned advice picks the endpoint for future unpinned work, a transaction stays pinned to
/// the endpoint and stream it started on, and route updates never drain queues or cancel work
/// already enqueued. With routing off (the default) a multi-endpoint connection simply rotates,
/// and a single-endpoint connection behaves exactly as before this feature existed.</para>
///
/// <para><b>Routing adds no retry.</b> A failed operation surfaces its failure unchanged; the only
/// routing consequence is a bounded cooldown that biases <em>future</em> unpinned selections away
/// from the failed endpoint. Resending DML or COMMIT elsewhere is the caller's decision under the
/// existing retry taxonomy, because a timeout after dispatch may hide a committed write.</para>
/// </summary>
public sealed class CamusConnection : IAsyncDisposable
{
    private readonly RoutedEndpoint[] endpoints;

    /// <summary>Server node identity → configured endpoint; the only way advice becomes a destination.</summary>
    private readonly Dictionary<string, RoutedEndpoint> byNodeId;

    private readonly StatementRouteCache? routeCache;

    /// <summary>True when requests ask the server for routing metadata (and replies are learned from).</summary>
    private readonly bool negotiate;

    private readonly long maxHintAgeMs;

    private readonly long cooldownMs;

    private readonly CamusSql.CamusSqlClient? unaryClient;

    private int endpointRotation = -1;

    /// <summary>
    /// Monotonic clock in milliseconds. A test seam: TTL expiry and cooldown are time-based, and a
    /// controllable clock is the difference between testing them and sleeping in tests.
    /// </summary>
    internal Func<long> Clock { get; set; } = static () => Environment.TickCount64;

    /// <summary>
    /// Connects to <paramref name="address"/> (e.g. <c>https://host:port</c>, or <c>http://…</c> for
    /// plaintext h2c in dev). The channel's TLS trust policy and keep-alive tuning come from
    /// <paramref name="options"/>. Addresses named in <c>options.Routing.NodeAddresses</c> join the
    /// endpoint pool alongside this bootstrap address.
    /// </summary>
    public static CamusConnection Connect(string address, CamusGrpcOptions? options = null)
        => Connect([address], options);

    /// <summary>
    /// Connects over a set of endpoint addresses. All addresses get the same TLS, credential and
    /// timeout policy from <paramref name="options"/>; unpinned work rotates over them, and with
    /// routing enabled learned advice biases the choice. The routing configuration is snapshotted
    /// here — mutating <c>options.Routing</c> afterwards does not affect this connection.
    /// </summary>
    public static CamusConnection Connect(IReadOnlyList<string> addresses, CamusGrpcOptions? options = null)
    {
        options ??= new CamusGrpcOptions();
        if (addresses.Count == 0)
            throw new ArgumentException("At least one endpoint address is required", nameof(addresses));

        // The pool is the ordered distinct union of the bootstrap addresses and the trust map's
        // addresses, so advice can only ever select an operator-configured destination.
        List<string> pool = new(addresses.Count);
        HashSet<string> seen = new(StringComparer.Ordinal);
        foreach (string address in addresses)
        {
            if (seen.Add(address))
                pool.Add(address);
        }
        foreach (string mapped in options.Routing.NodeAddresses.Values)
        {
            if (seen.Add(mapped))
                pool.Add(mapped);
        }

        RoutedEndpoint[] endpoints = new RoutedEndpoint[pool.Count];
        CamusSql.CamusSqlClient? firstClient = null;
        for (int i = 0; i < pool.Count; i++)
        {
            GrpcChannel channel = ChannelFactory.Create(pool[i], options);
            CamusSql.CamusSqlClient client = new(channel);
            firstClient ??= client;
            endpoints[i] = new RoutedEndpoint(
                pool[i],
                new GrpcBatcher(options, id => new GrpcBatchTransport(id, client)),
                channel);
        }

        return new CamusConnection(options, endpoints, BuildNodeMap(options, endpoints), firstClient);
    }

    /// <summary>
    /// Test seam: a single-endpoint connection over an injected batcher, routing off. This is the
    /// pre-routing constructor shape, kept so existing harnesses stay valid.
    /// </summary>
    internal CamusConnection(GrpcBatcher batcher, CamusSql.CamusSqlClient? unaryClient = null, GrpcChannel? ownedChannel = null)
        : this(
            new CamusGrpcOptions(),
            [new RoutedEndpoint("test://local", batcher, ownedChannel)],
            new Dictionary<string, RoutedEndpoint>(StringComparer.Ordinal),
            unaryClient)
    {
    }

    /// <summary>
    /// Test seam: a multi-endpoint connection over injected batchers, honoring
    /// <c>options.Routing</c> with the given per-endpoint node identities. A factory rather than a
    /// constructor because negotiation is derived from the finished node map, which must therefore
    /// exist before the real constructor runs.
    /// </summary>
    internal static CamusConnection CreateForTesting(
        CamusGrpcOptions options,
        IReadOnlyList<(string Address, string? NodeId, GrpcBatcher Batcher)> testEndpoints)
    {
        RoutedEndpoint[] built = BuildTestEndpoints(testEndpoints);
        Dictionary<string, RoutedEndpoint> map = new(StringComparer.Ordinal);
        for (int i = 0; i < testEndpoints.Count; i++)
        {
            if (testEndpoints[i].NodeId is string nodeId)
                map[nodeId] = built[i];
        }

        return new CamusConnection(options, built, map, unaryClient: null);
    }

    private CamusConnection(
        CamusGrpcOptions options,
        RoutedEndpoint[] endpoints,
        Dictionary<string, RoutedEndpoint> byNodeId,
        CamusSql.CamusSqlClient? unaryClient)
    {
        this.endpoints = endpoints;
        this.byNodeId = byNodeId;
        this.unaryClient = unaryClient;

        maxHintAgeMs = (long)options.Routing.MaxHintAge.TotalMilliseconds;
        cooldownMs = (long)options.Routing.EndpointCooldown.TotalMilliseconds;

        // Learned/Auto need at least one usable identity mapping to do anything; Auto additionally
        // demands two distinct reachable addresses — one destination is not a routing decision.
        int distinctMapped = byNodeId.Count == 0
            ? 0
            : new HashSet<string>(byNodeId.Values.Select(static e => e.Address), StringComparer.Ordinal).Count;
        negotiate = options.Routing.Mode switch
        {
            CamusRoutingMode.Learned => byNodeId.Count > 0,
            CamusRoutingMode.Auto => distinctMapped >= 2,
            _ => false,
        };

        routeCache = negotiate
            ? new StatementRouteCache(options.Routing.RouteCacheMaxEntries, options.Routing.RouteCacheMaxBytes)
            : null;
    }

    private static RoutedEndpoint[] BuildTestEndpoints(
        IReadOnlyList<(string Address, string? NodeId, GrpcBatcher Batcher)> testEndpoints)
    {
        RoutedEndpoint[] built = new RoutedEndpoint[testEndpoints.Count];
        for (int i = 0; i < testEndpoints.Count; i++)
            built[i] = new RoutedEndpoint(testEndpoints[i].Address, testEndpoints[i].Batcher, ownedChannel: null);
        return built;
    }

    private static Dictionary<string, RoutedEndpoint> BuildNodeMap(CamusGrpcOptions options, RoutedEndpoint[] endpoints)
    {
        Dictionary<string, RoutedEndpoint> map = new(options.Routing.NodeAddresses.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> pair in options.Routing.NodeAddresses)
        {
            foreach (RoutedEndpoint endpoint in endpoints)
            {
                if (string.Equals(endpoint.Address, pair.Value, StringComparison.Ordinal))
                {
                    map[pair.Key] = endpoint;
                    break;
                }
            }
        }
        return map;
    }

    // ─── Routing internals (shared with CamusPreparedStatement) ───────────────

    internal IReadOnlyList<RoutedEndpoint> Endpoints => endpoints;

    internal bool RoutingNegotiated => negotiate;

    /// <summary>Entry count of the learned route cache; 0 with routing off. For tests/diagnostics.</summary>
    internal int LearnedRouteCount => routeCache?.Count ?? 0;

    /// <summary>
    /// Picks the endpoint for one unpinned operation: a fresh learned route that maps to a
    /// configured, non-suppressed endpoint wins; anything else falls back to rotation. Also
    /// captures the route entry's revision so the reply's advice can be applied conditionally —
    /// a late reply must never overwrite a route a faster reply already refreshed.
    /// </summary>
    internal RoutedEndpoint SelectEndpoint(in StatementRouteKey key, out long observedRevision)
    {
        observedRevision = 0;
        long now = Clock();

        if (routeCache is not null)
        {
            string? nodeId = routeCache.TryGet(key, now, out observedRevision);
            if (nodeId is not null
                && byNodeId.TryGetValue(nodeId, out RoutedEndpoint? learned)
                && !learned.IsSuppressed(now))
                return learned;
        }

        return NextRotation(now);
    }

    internal RoutedEndpoint NextRotation(long now)
    {
        int count = endpoints.Length;
        int start = (int)((uint)Interlocked.Increment(ref endpointRotation) % (uint)count);
        for (int i = 0; i < count; i++)
        {
            RoutedEndpoint candidate = endpoints[(start + i) % count];
            if (!candidate.IsSuppressed(now))
                return candidate;
        }

        // Every endpoint is cooling: proceed with the rotation choice rather than fail — cooldown
        // is a bias, and refusing to send would turn a hint mechanism into an availability hazard.
        return endpoints[start];
    }

    /// <summary>
    /// Applies a successful reply's advice to the route cache. Ignored unless it is version 1,
    /// well-formed, and — for a prefer — parameter-independent in scope, positive in TTL, and
    /// naming a configured identity. Learning failures must never fail the operation, so this
    /// swallows nothing because it throws nothing: every rejection is a plain early return.
    /// </summary>
    internal void LearnFromResult(in StatementRouteKey key, CamusRoutingAdvice? advice, long observedRevision)
    {
        if (routeCache is null || advice is null || advice.Version != RoutingWire.AcceptVersion)
            return;

        if (advice.Disposition == CamusRoutingDisposition.Clear)
        {
            routeCache.Clear(key, observedRevision);
            return;
        }

        if (advice.Disposition != CamusRoutingDisposition.Prefer
            || !advice.ParametersIndependentScope
            || advice.PreferredNodeId is null
            || advice.MaxAgeMs <= 0
            || !byNodeId.ContainsKey(advice.PreferredNodeId))
            return;

        long now = Clock();
        long ttl = Math.Min(advice.MaxAgeMs, maxHintAgeMs);
        if (ttl <= 0)
            return;

        routeCache.Learn(key, advice.PreferredNodeId, advice.DependencyToken, now + ttl, observedRevision, now);
    }

    /// <summary>
    /// Puts an endpoint on cooldown after a transport failure so future unpinned work skips it
    /// briefly. Domain SQL errors never come through here — the server answered, so the endpoint
    /// is healthy.
    /// </summary>
    internal void NoteTransportFailure(RoutedEndpoint endpoint)
    {
        if (cooldownMs > 0)
            endpoint.SuppressUntil(Clock() + cooldownMs);
    }

    /// <summary>
    /// True for failures of the transport itself. Deliberately narrow: a
    /// <see cref="CamusGrpcException"/> is a server answer, an <see cref="OperationCanceledException"/>
    /// is the caller's own token, and neither says anything about endpoint health.
    /// </summary>
    internal static bool IsTransportFailure(Exception ex)
        => ex is IOException or global::Grpc.Core.RpcException;

    // ─── Autocommit ───────────────────────────────────────────────────────────

    public async Task<QueryResult> ExecuteQueryAsync(string database, string sql, CancellationToken cancellationToken = default)
    {
        StatementRouteKey key = new(database, sql, RouteOpKind.Query);
        RoutedEndpoint endpoint = SelectEndpoint(key, out long observed);

        SqlRequest request = new() { Database = database, Sql = sql };
        if (negotiate)
            request.RoutingAcceptVersion = RoutingWire.AcceptVersion;

        try
        {
            QueryResult result = await endpoint.Batcher
                .EnqueueQueryAsync(request, slotIndex: null, cancellationToken).ConfigureAwait(false);
            LearnFromResult(key, result.Routing, observed);
            return result;
        }
        catch (Exception ex) when (IsTransportFailure(ex))
        {
            NoteTransportFailure(endpoint);
            throw;
        }
    }

    public async Task<NonQueryResult> ExecuteNonQueryAsync(string database, string sql, CancellationToken cancellationToken = default)
    {
        StatementRouteKey key = new(database, sql, RouteOpKind.NonQuery);
        RoutedEndpoint endpoint = SelectEndpoint(key, out long observed);

        SqlRequest request = new() { Database = database, Sql = sql };
        if (negotiate)
            request.RoutingAcceptVersion = RoutingWire.AcceptVersion;

        try
        {
            NonQueryResult result = await endpoint.Batcher
                .EnqueueNonQueryAsync(request, slotIndex: null, cancellationToken).ConfigureAwait(false);
            LearnFromResult(key, result.Routing, observed);
            return result;
        }
        catch (Exception ex) when (IsTransportFailure(ex))
        {
            NoteTransportFailure(endpoint);
            throw;
        }
    }

    // ─── Prepared statements ──────────────────────────────────────────────────

    /// <summary>
    /// Registers a statement so later executions send only its values, not the SQL and not the
    /// parameter names.
    ///
    /// <para>The registration is performed eagerly on one stream so the caller learns the parameter
    /// order immediately and a malformed statement fails here rather than at some later execution.
    /// Other streams — and other endpoints — register lazily, on first use — see
    /// <see cref="CamusPreparedStatement"/> for why a statement cannot simply hold one handle.
    /// A destination already learned for this SQL is used for the eager registration, so a warmed
    /// statement's first execution does not pay a registration round trip on a second node.</para>
    /// </summary>
    public async Task<CamusPreparedStatement> PrepareAsync(
        string database, string sql, CancellationToken cancellationToken = default)
    {
        StatementRouteKey routeKey = new(database, sql, RouteOpKind.Query);
        RoutedEndpoint endpoint = SelectEndpoint(routeKey, out _);
        int slot = endpoint.Batcher.ReserveSlot();
        PreparedStatementKey key = new(database, sql);
        PreparedSlotEntry entry = await endpoint.Batcher
            .EnsurePreparedAsync(slot, key, cancellationToken).ConfigureAwait(false);

        return new CamusPreparedStatement(this, key, entry.ParameterNames);
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
    ///
    /// <para><paramref name="affinity"/> optionally selects the endpoint from a warmed prepared
    /// statement's learned route <b>before</b> START. It is read once, here: after the transaction
    /// begins its endpoint is immutable — advice can inform the next transaction, never relocate
    /// this one. Cold advice (or a statement from another connection) simply uses rotation, adding
    /// no discovery round trip.</para>
    /// </summary>
    public async Task<CamusTransactionSession> BeginTransactionAsync(
        string database,
        IsolationLevel isolation = IsolationLevel.Unspecified,
        TransactionMode mode = TransactionMode.Unspecified,
        LockingMode locking = LockingMode.Unspecified,
        CancellationToken cancellationToken = default,
        CamusPreparedStatement? affinity = null)
    {
        RoutedEndpoint endpoint = affinity is not null && ReferenceEquals(affinity.Owner, this)
            ? SelectEndpoint(affinity.QueryRouteKey, out _)
            : NextRotation(Clock());

        int slot = endpoint.Batcher.ReserveSlot();
        SqlRequest request = new()
        {
            Database         = database,
            IsolationLevel   = isolation,
            TransactionMode  = mode,
            Locking          = locking,
        };
        TxnHandle handle = await endpoint.Batcher.EnqueueStartAsync(request, slot, cancellationToken).ConfigureAwait(false);
        return new CamusTransactionSession(endpoint.Batcher, database, handle, slot);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (RoutedEndpoint endpoint in endpoints)
            await endpoint.DisposeAsync().ConfigureAwait(false);
    }
}
