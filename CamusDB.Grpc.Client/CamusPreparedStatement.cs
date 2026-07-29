
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client;

/// <summary>
/// A statement registered once and executed many times with different values, so neither the SQL nor
/// the parameter names travel again per execution.
///
/// <para><b>One statement, many streams.</b> A server-side handle belongs to the stream that minted
/// it, but the client multiplexes autocommit work across a pool of streams and rebuilds one after a
/// fault. This object therefore stands for the <em>statement</em>, not for a single handle: it
/// registers itself lazily on whichever stream an execution lands on, and a stream that was rebuilt
/// simply gets a fresh registration on next use. Callers never see handles or streams.</para>
///
/// <para><b>Values are positional.</b> <see cref="ParameterNames"/> gives the binding order the
/// server published (names keep their leading <c>@</c>), so a caller preferring to bind by name maps
/// its own arguments onto ordinals here, in the client. Names deliberately do not travel on the
/// wire — removing them is a large part of why prepared statements are cheaper.</para>
/// </summary>
public sealed class CamusPreparedStatement : IAsyncDisposable
{
    private readonly GrpcBatcher batcher;

    /// <summary>
    /// The cache key, built once here rather than per execution. Every execution looks the statement
    /// up on its slot, so composing a key from the SQL each time would copy the whole statement text
    /// on the hot path — the exact allocation prepared statements exist to remove.
    /// </summary>
    private readonly PreparedStatementKey key;

    /// <summary>
    /// Lifecycle state, read and written with <see cref="Interlocked"/> because disposal races
    /// execution: a plain field would let an execution observe "live", then have disposal complete
    /// underneath it and re-register a handle nobody will ever close.
    /// </summary>
    private int state = StateLive;

    private const int StateLive = 0;
    private const int StateDisposing = 1;

    internal CamusPreparedStatement(
        GrpcBatcher batcher, PreparedStatementKey key, IReadOnlyList<string> parameterNames)
    {
        this.batcher = batcher;
        this.key = key;
        ParameterNames = parameterNames;
        NameOrdinals = BuildNameOrdinals(parameterNames);
    }

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

    /// <summary>Executes the statement as an autocommit query, binding <paramref name="values"/> by ordinal.</summary>
    public Task<QueryResult> ExecuteQueryAsync(
        IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
        => ExecuteAsync(
            batcher.ReserveSlot(), values, txn: null,
            static (b, request, slot, transportId, ct) => b.EnqueueQueryAsync(request, slot, ct, transportId),
            cancellationToken);

    /// <summary>Executes the statement as an autocommit non-query, binding <paramref name="values"/> by ordinal.</summary>
    public Task<NonQueryResult> ExecuteNonQueryAsync(
        IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
        => ExecuteAsync(
            batcher.ReserveSlot(), values, txn: null,
            static (b, request, slot, transportId, ct) => b.EnqueueNonQueryAsync(request, slot, ct, transportId),
            cancellationToken);

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

    internal Task<QueryResult> ExecuteQueryAsync(
        int slot, TxnHandle txn, IReadOnlyList<object?> values, CancellationToken ct)
        => ExecuteAsync(
            slot, values, txn,
            static (b, request, s, transportId, c) => b.EnqueueQueryAsync(request, s, c, transportId),
            ct);

    internal Task<NonQueryResult> ExecuteNonQueryAsync(
        int slot, TxnHandle txn, IReadOnlyList<object?> values, CancellationToken ct)
        => ExecuteAsync(
            slot, values, txn,
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
        int slot,
        IReadOnlyList<object?> values,
        TxnHandle? txn,
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
    /// Releases the statement on every stream it was registered on.
    ///
    /// <para>Disposal marks the statement first, so no execution can start a new registration, and
    /// then <b>awaits</b> the registrations it took — including any still in flight. Closing only the
    /// finished ones would leave a registration that completed a moment later holding a handle nobody
    /// references, alive until the stream ends. When this returns, every id this statement ever
    /// minted has been closed or belongs to a stream that is already gone.</para>
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

        foreach ((int slot, Task<PreparedSlotEntry> registration) in batcher.TakePrepared(key))
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

            await batcher.ClosePreparedAsync(slot, entry, CancellationToken.None).ConfigureAwait(false);
        }
    }
}
