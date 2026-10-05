
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct ExecuteSQLTicket
{
    public KvTransaction TxnState { get; }

    public string DatabaseName { get; }

    public string Sql { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    /// <summary>
    /// The authenticated caller, resolved by the transport from a bearer token and used by the
    /// privilege gate. Null when authentication is disabled (the default) — the gate then does nothing.
    /// A null principal with authentication <b>enabled</b> is rejected as unauthenticated.
    /// </summary>
    public Principal? Principal { get; }

    /// <summary>
    /// The transport's request token, so a client that disconnects stops the read instead of
    /// leaving it to run to completion against a caller that is already gone.
    ///
    /// <para><b>This token bounds reads only.</b> It reaches the scan and the query operators, and
    /// it must never be handed to a commit, a rollback, or a lock release: a cancelled rollback
    /// abandons the transaction's locks until their lease expires, which is a worse outcome than
    /// the wasted read it was meant to prevent. Write phases ignore it for the same reason — once
    /// the first mutation lands, the statement runs to its commit or its rollback.</para>
    ///
    /// <para>Defaults to <see cref="CancellationToken.None"/>, so an internal caller that has no
    /// request behind it (DML, DDL, a background job) keeps the previous uncancellable behavior.</para>
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    /// Per-statement diagnostic accumulator for the slow query log, or null when the log is off.
    ///
    /// <para>This is where a probe enters the engine. Every <see cref="QueryTicket"/> a statement
    /// builds — for its own scan, for a subquery, for a derived table, for the locate scan of an
    /// UPDATE — is built from this ticket through <c>QueryTicketAdapter</c>, so attaching the probe
    /// once here is what makes one statement report one set of counters.</para>
    ///
    /// <para>An internal caller that has no statement behind it leaves it null, and every write site
    /// is a null-conditional call.</para>
    /// </summary>
    public Diagnostics.StatementProbe? Probe { get; }

    /// <summary>
    /// Per-statement collector for advisory routing metadata, or null when the client did not
    /// negotiate it. The transport creates it, the engine's record sites classify the statement
    /// onto it with null-conditional calls, and the transport reads it back only after the
    /// statement succeeded — a failed statement's collector is never read. Follows the same
    /// carry-forward rule as <see cref="Probe"/>: every rebuild of this ticket must preserve it.
    /// </summary>
    public Routing.StatementRoutingCollector? Routing { get; }

    /// <summary>
    /// Where the statement records a retryable Kahuna abort instead of throwing it, or null to throw.
    /// Created by a transport that reports the abort itself: after the statement returns it tests
    /// <see cref="RetryableAbortSink.HasAbort"/> before it looks at the result, and it never commits
    /// a transaction whose statement recorded one. Carried forward like <see cref="Probe"/> by every
    /// rebuild of this ticket for the same statement, and never into a ticket for a subquery, whose
    /// consumer would not know to test it. See <see cref="RetryableAbortSink"/>.
    /// </summary>
    public RetryableAbortSink? RetryableAborts { get; }

    /// <summary>
    /// True when the caller wants only the row count of an <c>INSERT … RETURNING</c>, not its rows.
    /// The statement still validates its RETURNING list and still demands the SELECT privilege the
    /// list needs, so the same statement fails the same way with and without the flag; only the
    /// buffer, the projection and the serialization of the rows are skipped.
    ///
    /// <para>Only the no-rows entry point honors it. The row-returning entry point refuses a ticket
    /// that sets it, because a query that asks for no rows is a client error. It has no effect on a
    /// statement without a RETURNING list. Carried forward like <see cref="Probe"/> by every rebuild
    /// of this ticket.</para>
    /// </summary>
    public bool DiscardReturningRows { get; }

    public ExecuteSQLTicket(
        KvTransaction txnState,
        string database,
        string sql,
        Dictionary<string, ColumnValue>? parameters,
        Principal? principal = null,
        CancellationToken cancellationToken = default,
        Diagnostics.StatementProbe? probe = null,
        Routing.StatementRoutingCollector? routing = null,
        RetryableAbortSink? retryableAborts = null,
        bool discardReturningRows = false)
    {
        TxnState = txnState;
        DatabaseName = database;
        Sql = sql;
        Parameters = parameters;
        Principal = principal;
        CancellationToken = cancellationToken;
        Probe = probe;
        Routing = routing;
        RetryableAborts = retryableAborts;
        DiscardReturningRows = discardReturningRows;
    }

    /// <summary>
    /// The same ticket carrying <paramref name="probe"/>. Used at the engine boundary, where the
    /// slow query log creates the probe after the ticket has already been built by the transport.
    /// </summary>
    public ExecuteSQLTicket WithProbe(Diagnostics.StatementProbe? probe)
        => new(TxnState, DatabaseName, Sql, Parameters, Principal, CancellationToken, probe, Routing, RetryableAborts, DiscardReturningRows);
}
