
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
/// A statement registered once and executed many times with different values, so neither the SQL nor
/// the parameter names travel again per execution.
///
/// <para><b>One statement, many streams — and many endpoints.</b> A server-side handle belongs to
/// the stream that minted it, but the client multiplexes autocommit work across per-endpoint pools
/// of streams and rebuilds a stream after a fault. This object therefore stands for the logical
/// <em>statement</em>, not for a single handle: an autocommit execution first selects an endpoint
/// (learned advice or rotation, via the owning connection), then registers itself lazily on
/// whichever stream it lands on there; a rebuilt stream simply gets a fresh registration on next
/// use. Registrations are scoped per endpoint per stream incarnation and never travel between
/// them. Callers never see handles, streams, or endpoints.</para>
///
/// <para><b>Values are positional.</b> <see cref="ParameterNames"/> gives the binding order the
/// server published (names keep their leading <c>@</c>), so a caller preferring to bind by name maps
/// its own arguments onto ordinals here, in the client. Names deliberately do not travel on the
/// wire — removing them is a large part of why prepared statements are cheaper.</para>
/// </summary>
public sealed class CamusPreparedStatement : IAsyncDisposable
{
    private readonly CamusConnection owner;

    /// <summary>
    /// The cache key, built once here rather than per execution. Every execution looks the statement
    /// up on its slot, so composing a key from the SQL each time would copy the whole statement text
    /// on the hot path — the exact allocation prepared statements exist to remove.
    /// </summary>
    private readonly PreparedStatementKey key;

    /// <summary>Route-cache identities, precomputed for the same hot-path reason as <see cref="key"/>.</summary>
    private readonly StatementRouteKey queryRouteKey;

    private readonly StatementRouteKey nonQueryRouteKey;

    /// <summary>
    /// Lifecycle state, read and written with <see cref="Interlocked"/> because disposal races
    /// execution: a plain field would let an execution observe "live", then have disposal complete
    /// underneath it and re-register a handle nobody will ever close.
    /// </summary>
    private int state = StateLive;

    private const int StateLive = 0;
    private const int StateDisposing = 1;

    internal CamusPreparedStatement(
        CamusConnection owner, PreparedStatementKey key, IReadOnlyList<string> parameterNames)
    {
        this.owner = owner;
        this.key = key;
        queryRouteKey = new StatementRouteKey(key.Database, key.Sql, RouteOpKind.Query);
        nonQueryRouteKey = new StatementRouteKey(key.Database, key.Sql, RouteOpKind.NonQuery);
        ParameterNames = parameterNames;
        NameOrdinals = BuildNameOrdinals(parameterNames);
    }

    /// <summary>The connection this statement belongs to; transaction-start affinity checks it.</summary>
    internal CamusConnection Owner => owner;

    /// <summary>The statement's query-side route identity, used for transaction-start affinity.</summary>
    internal StatementRouteKey QueryRouteKey => queryRouteKey;

    /// <summary>The statement's mutation-side route identity, used when it starts a deferred transaction.</summary>
    internal StatementRouteKey NonQueryRouteKey => nonQueryRouteKey;

    /// <summary>The parameter names in binding order, verbatim including the leading <c>@</c>.</summary>
    public IReadOnlyList<string> ParameterNames { get; }

    /// <summary>
    /// Name → ordinal, for the by-name binding overloads. Built once at construction: the mapping is
    /// fixed for the statement's lifetime, and rebuilding it per execution would reintroduce
    /// per-call allocation on the hot path.
    ///
    /// <para>Both the published <c>@name</c> form and the bare <c>name</c> form are accepted, because
    /// a caller writing an anonymous object cannot put an <c>@</c> in a C# property name.</para>
    /// </summary>
    private IReadOnlyDictionary<string, int> NameOrdinals { get; }

    /// <summary>The SQL this statement stands for.</summary>
    public string Sql => key.Sql;

    private static IReadOnlyDictionary<string, int> BuildNameOrdinals(IReadOnlyList<string> names)
    {
        Dictionary<string, int> ordinals = new(names.Count * 2, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < names.Count; i++)
        {
            string name = names[i];
            ordinals[name] = i;
            if (name.Length > 1 && name[0] == '@')
                ordinals[name[1..]] = i;
        }
        return ordinals;
    }

    // ─── Autocommit execution ─────────────────────────────────────────────────

    /// <summary>
    /// Executes the statement as an autocommit query, binding <paramref name="values"/> by ordinal.
    /// The endpoint is selected first (learned route or rotation), then the statement is registered
    /// there on demand, then the execution is sent — so a learned destination pays its registration
    /// round trip once and every later execution rides the warmed handle.
    /// </summary>
    public async Task<QueryResult> ExecuteQueryAsync(
        IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        RoutedEndpoint endpoint = owner.SelectEndpoint(queryRouteKey, out long observed);
        try
        {
            QueryResult result = await ExecuteAsync(
                endpoint.Batcher, endpoint.Batcher.ReserveSlot(), values, txn: null, negotiate: owner.RoutingNegotiated,
                static (b, request, slot, transportId, ct) => b.EnqueueQueryAsync(request, slot, ct, transportId),
                cancellationToken).ConfigureAwait(false);
            owner.LearnFromResult(queryRouteKey, result.Routing, observed);
            return result;
        }
        catch (Exception ex) when (CamusConnection.IsTransportFailure(ex))
        {
            owner.NoteTransportFailure(endpoint);
            throw;
        }
    }

    /// <inheritdoc cref="ExecuteQueryAsync(IReadOnlyList{object?}, CancellationToken)"/>
    public async Task<NonQueryResult> ExecuteNonQueryAsync(
        IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        RoutedEndpoint endpoint = owner.SelectEndpoint(nonQueryRouteKey, out long observed);
        try
        {
            NonQueryResult result = await ExecuteAsync(
                endpoint.Batcher, endpoint.Batcher.ReserveSlot(), values, txn: null, negotiate: owner.RoutingNegotiated,
                static (b, request, slot, transportId, ct) => b.EnqueueNonQueryAsync(request, slot, ct, transportId),
                cancellationToken).ConfigureAwait(false);
            owner.LearnFromResult(nonQueryRouteKey, result.Routing, observed);
            return result;
        }
        catch (Exception ex) when (CamusConnection.IsTransportFailure(ex))
        {
            owner.NoteTransportFailure(endpoint);
            throw;
        }
    }

    // ─── Binding by name ──────────────────────────────────────────────────────

    /// <summary>
    /// Executes the statement binding an object's properties to parameters <b>by name</b>:
    /// <c>ExecuteQueryAsync(new { id, name, year })</c>. Matching is case-insensitive and accepts a
    /// property either with or without the leading <c>@</c>.
    ///
    /// <para>The mapping to ordinals happens here, in the client. Names never reach the wire — sending
    /// them per execution is the cost prepared statements exist to remove — so this is ergonomics over
    /// the same positional call, not a second protocol.</para>
    /// </summary>
    /// <exception cref="ArgumentException">
    /// A declared parameter has no matching property, or the object carries a property that matches no
    /// parameter. Both are refused rather than defaulted: silently binding NULL for a misspelled
    /// property would turn a typo into a wrong answer.
    /// </exception>
    public Task<QueryResult> ExecuteQueryAsync(object parameters, CancellationToken cancellationToken = default)
        => ExecuteQueryAsync(BindByName(parameters), cancellationToken);

    /// <inheritdoc cref="ExecuteQueryAsync(object, CancellationToken)"/>
    public Task<NonQueryResult> ExecuteNonQueryAsync(object parameters, CancellationToken cancellationToken = default)
        => ExecuteNonQueryAsync(BindByName(parameters), cancellationToken);

    /// <summary>
    /// Projects <paramref name="parameters"/>' public properties onto this statement's declared
    /// ordinal positions.
    /// </summary>
    internal object?[] BindByName(object parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        object?[] values = new object?[ParameterNames.Count];
        bool[] bound = new bool[ParameterNames.Count];

        foreach (System.Reflection.PropertyInfo property in parameters.GetType().GetProperties())
        {
            if (!property.CanRead)
                continue;

            if (!NameOrdinals.TryGetValue(property.Name, out int ordinal))
                throw new ArgumentException(
                    $"'{property.Name}' does not match any parameter of this statement " +
                    $"({string.Join(", ", ParameterNames)})",
                    nameof(parameters));

            values[ordinal] = property.GetValue(parameters);
            bound[ordinal] = true;
        }

        for (int i = 0; i < bound.Length; i++)
        {
            if (!bound[i])
                throw new ArgumentException(
                    $"No value supplied for parameter '{ParameterNames[i]}'", nameof(parameters));
        }

        return values;
    }

    // ─── Execution inside a transaction ───────────────────────────────────────

    /// <summary>
    /// Transactional executions take the session's own batcher and slot: the transaction is pinned
    /// to the endpoint and stream it started on, so the statement must register and execute there —
    /// never on a learned route. Routing metadata is still negotiated when the session asks for it:
    /// the advice names the statement's table leader and the session learns it for the next
    /// transaction's start, without moving this one.
    /// </summary>
    internal Task<QueryResult> ExecuteQueryAsync(
        GrpcBatcher batcher, int slot, TxnHandle txn, IReadOnlyList<object?> values, bool negotiate, CancellationToken ct)
        => ExecuteAsync(
            batcher, slot, values, txn, negotiate,
            static (b, request, s, transportId, c) => b.EnqueueQueryAsync(request, s, c, transportId),
            ct);

    /// <inheritdoc cref="ExecuteQueryAsync(GrpcBatcher, int, TxnHandle, IReadOnlyList{object?}, bool, CancellationToken)"/>
    internal Task<NonQueryResult> ExecuteNonQueryAsync(
        GrpcBatcher batcher, int slot, TxnHandle txn, IReadOnlyList<object?> values, bool negotiate, CancellationToken ct)
        => ExecuteAsync(
            batcher, slot, values, txn, negotiate,
            static (b, request, s, transportId, c) => b.EnqueueNonQueryAsync(request, s, c, transportId),
            ct);

    /// <summary>
    /// Registers the statement on <paramref name="slot"/> if needed, then executes it there.
    ///
    /// <para>Retries exactly once, and only for the two ways a registration can go stale underneath a
    /// correct caller: the stream was rebuilt between the check and the write, or the server does not
    /// know the handle (a rebuild the client had not noticed yet). Both mean "prepare again and
    /// resend", and both are invisible to the caller. Every other failure — including a transport
    /// fault on the execution itself — propagates unchanged, because those are the caller's to handle
    /// under the normal retry taxonomy, and because retrying a mutation that may have been applied is
    /// not this layer's decision to make. The single attempt also stops a flapping stream from
    /// spinning here.</para>
    ///
    /// <para>Disposal is re-checked before <em>every</em> registration, not only on entry. The retry
    /// path registers the statement again, so a single check at the top would let an execution that
    /// began before disposal leave a fresh handle behind after <c>DisposeAsync</c> had returned —
    /// invisible to the client and alive until the stream ends.</para>
    /// </summary>
    private async Task<TResult> ExecuteAsync<TResult>(
        GrpcBatcher batcher,
        int slot,
        IReadOnlyList<object?> values,
        TxnHandle? txn,
        bool negotiate,
        Func<GrpcBatcher, SqlRequest, int, long, CancellationToken, Task<TResult>> send,
        CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref state) != StateLive, this);

            PreparedSlotEntry entry = await batcher
                .EnsurePreparedAsync(slot, key, cancellationToken).ConfigureAwait(false);

            // Disposal may have run while the registration was in flight. It removes registrations
            // from the batcher, so this one is now unreferenced; close it here rather than let it
            // outlive the statement that owns it.
            if (Volatile.Read(ref state) != StateLive)
            {
                await batcher.ClosePreparedAsync(slot, entry, CancellationToken.None).ConfigureAwait(false);
                throw new ObjectDisposedException(nameof(CamusPreparedStatement));
            }

            if (values.Count != entry.ParameterNames.Length)
                throw new ArgumentException(
                    $"Statement declares {entry.ParameterNames.Length} parameter(s) " +
                    $"({string.Join(", ", entry.ParameterNames)}) but {values.Count} value(s) were supplied",
                    nameof(values));

            SqlRequest request = new() { StatementId = entry.StatementId };
            foreach (object? value in values)
                request.PositionalParameters.Add(CamusValue.From(value));
            if (txn is not null)
                request.TxnHandle = txn;
            if (negotiate)
                request.RoutingAcceptVersion = RoutingWire.AcceptVersion;

            try
            {
                return await send(batcher, request, slot, entry.TransportId, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt == 0 && IsStaleRegistration(ex))
            {
                batcher.InvalidatePrepared(slot, key, entry);
            }
        }
    }

    /// <summary>
    /// True for the failures that mean "this registration is gone", as opposed to a real error: the
    /// pre-write transport check, and the server's own unknown-statement code, which is the backstop
    /// for anything the check misses.
    /// </summary>
    private static bool IsStaleRegistration(Exception ex) =>
        ex is PreparedStatementStaleException ||
        (ex is CamusGrpcException grpc && grpc.Code == "CADB0520");

    /// <summary>
    /// Releases the statement on every stream — of every endpoint — it was registered on.
    ///
    /// <para>Disposal marks the statement first, so no execution can start a new registration, and
    /// then <b>awaits</b> the registrations it took — including any still in flight. Closing only the
    /// finished ones would leave a registration that completed a moment later holding a handle nobody
    /// references, alive until the stream ends. When this returns, every id this statement ever
    /// minted has been closed or belongs to a stream that is already gone. Every endpoint is swept
    /// because routing may have registered this statement on any of them.</para>
    ///
    /// <para>Each close is best-effort: a stream that has ended already freed its handles, so a
    /// failure here means the work was done for us. Skipping disposal entirely is safe for the same
    /// reason, but a long-lived connection preparing many distinct statements should dispose them to
    /// stay under the server's per-stream cap.</para>
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref state, StateDisposing) != StateLive)
            return;

        foreach (RoutedEndpoint endpoint in owner.Endpoints)
        {
            foreach ((int slot, Task<PreparedSlotEntry> registration) in endpoint.Batcher.TakePrepared(key))
            {
                PreparedSlotEntry entry;
                try
                {
                    entry = await registration.ConfigureAwait(false);
                }
                catch
                {
                    continue;   // that registration never produced a handle; nothing to release.
                }

                await endpoint.Batcher.ClosePreparedAsync(slot, entry, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
