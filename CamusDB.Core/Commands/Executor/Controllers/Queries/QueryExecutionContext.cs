
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The ambient facts a query operator needs that are not the rows themselves: which engine's
/// configuration applies, when to stop, and where spill files belong.
///
/// <para>The query pipeline is deliberately built from small operators that hand streams to one
/// another — a sort feeds a distinct, which feeds a projection — and most of them are pure functions
/// of their input. Configuration is the exception: an operator has to know whether spilling is enabled
/// and at what threshold before it can decide how to process a stream at all. Passing each of those
/// values down every call chain would thread half a dozen parameters through operators that otherwise
/// only pass data, so they travel together here instead.</para>
///
/// <para>It carries no per-operator state and is safe to share across the operators of one query. It
/// is not a general-purpose bag: values belong here only when the whole pipeline needs them. Anything
/// one operator needs stays that operator's parameter.</para>
/// </summary>
public sealed class QueryExecutionContext
{
    /// <summary>Configuration of the engine running this query.</summary>
    public CamusDBOptions Options { get; }

    /// <summary>
    /// Cancellation for the whole query; operators propagate it into their own awaits.
    ///
    /// <para>An operator can be reached two ways, so it can see a token twice. Executed as part of
    /// a plan it gets this one, taken from the query ticket, and the scan below it obeys the same
    /// token. Driven directly — by a test, or by a caller that composes operators by hand — it gets
    /// one through its <c>[EnumeratorCancellation]</c> parameter instead, and this one is
    /// <see cref="CancellationToken.None"/>. <see cref="Effective"/> settles which applies.</para>
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    /// Directory that backs any spill file this query creates. Defaults to the engine's data
    /// directory, and is separate from <see cref="CamusDBOptions.DataDirectory"/> so a caller can
    /// place spill files elsewhere without pretending the whole engine moved.
    /// </summary>
    public string SpillDirectory { get; }

    /// <summary>
    /// Per-statement diagnostic accumulator for the slow query log, or null when the log is off.
    ///
    /// <para>It belongs here by the same test as the rest of this type: the whole pipeline needs it,
    /// because any blocking operator may be the one that spills. An operator reports the spill
    /// through this reference instead of returning the fact upwards through stages that would
    /// otherwise have no reason to know about it.</para>
    /// </summary>
    public Diagnostics.StatementProbe? Probe { get; }

    public QueryExecutionContext(
        CamusDBOptions options,
        CancellationToken cancellationToken = default,
        string? spillDirectory = null,
        Diagnostics.StatementProbe? probe = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        Options = options;
        CancellationToken = cancellationToken;
        SpillDirectory = spillDirectory ?? options.DataDirectory;
        Probe = probe;
    }

    /// <summary>
    /// Context for a query against <paramref name="database"/>, taking the configuration of the engine
    /// that database belongs to. This is the usual way to build one: any code holding a descriptor is
    /// already holding the right configuration.
    /// </summary>
    public static QueryExecutionContext For(DatabaseDescriptor database, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        return new QueryExecutionContext(database.Options, cancellationToken);
    }

    /// <summary>
    /// Context for a query against <paramref name="database"/> executing <paramref name="ticket"/>,
    /// taking both the request's cancellation token and its diagnostic probe from the ticket.
    ///
    /// <para>Prefer this over the token-only overload anywhere a ticket is in hand. The two values
    /// are per statement and always travel together, and passing only the token is how an operator
    /// silently stops reporting the spill it just performed.</para>
    /// </summary>
    public static QueryExecutionContext For(DatabaseDescriptor database, Models.Tickets.QueryTicket ticket)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(ticket);

        return new QueryExecutionContext(database.Options, ticket.CancellationToken, spillDirectory: null, ticket.Probe);
    }

    /// <summary>Same context, re-scoped to a different cancellation token.</summary>
    public QueryExecutionContext WithCancellation(CancellationToken cancellationToken)
        => new(Options, cancellationToken, SpillDirectory, Probe);

    /// <summary>
    /// The token an operator should obey, given the one its enumerator was started with.
    ///
    /// <para>The enumerator token wins whenever it can actually be cancelled, so a caller that
    /// drives one operator keeps full control of it. Otherwise this context's token applies, which
    /// is the plan case: the intermediate stages between an operator and the transport do not all
    /// forward an enumerator token, so the context is the only path the request's token has.</para>
    ///
    /// <para>Both being cancellable at once does not happen on any current path, so no linked
    /// source is built — a link would allocate on every operator of every query to cover a case
    /// that cannot arise. If one ever does, this is the single place to change.</para>
    /// </summary>
    public CancellationToken Effective(CancellationToken enumeratorToken)
        => enumeratorToken.CanBeCanceled ? enumeratorToken : CancellationToken;
}
