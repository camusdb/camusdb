
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

    public ExecuteSQLTicket(
        KvTransaction txnState,
        string database,
        string sql,
        Dictionary<string, ColumnValue>? parameters,
        Principal? principal = null)
    {
        TxnState = txnState;
        DatabaseName = database;
        Sql = sql;
        Parameters = parameters;
        Principal = principal;
    }
}
