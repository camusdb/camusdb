
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

    public ExecuteSQLTicket(
        KvTransaction txnState,
        string database,
        string sql,
        Dictionary<string, ColumnValue>? parameters,
        Principal? principal = null,
        CancellationToken cancellationToken = default,
        Diagnostics.StatementProbe? probe = null)
    {
        TxnState = txnState;
        DatabaseName = database;
        Sql = sql;
        Parameters = parameters;
        Principal = principal;
        CancellationToken = cancellationToken;
        Probe = probe;
    }

    /// <summary>
    /// The same ticket carrying <paramref name="probe"/>. Used at the engine boundary, where the
    /// slow query log creates the probe after the ticket has already been built by the transport.
    /// </summary>
    public ExecuteSQLTicket WithProbe(Diagnostics.StatementProbe? probe)
        => new(TxnState, DatabaseName, Sql, Parameters, Principal, CancellationToken, probe);
}
