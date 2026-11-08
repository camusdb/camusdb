
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DeleteTicket
{
    public KvTransaction TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public NodeAst? Where { get; }

    public List<QueryFilter>? Filters { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public NodeAst? Limit { get; }

    /// <summary>
    /// Per-statement diagnostic accumulator for the slow query log, or null when the log is off.
    /// Taken from the SQL ticket this one was created from, and handed to the locate scan's
    /// <see cref="QueryTicket"/> so a mutation reports the same scan facts a SELECT does.
    /// </summary>
    public Diagnostics.StatementProbe? Probe { get; }

    /// <summary>
    /// The correlated subqueries of <see cref="Where"/>, prepared against the target table, or null when it
    /// has none. The locate scan carries it on its query ticket, so the filter can evaluate each one for
    /// each row of the table.
    /// </summary>
    public ExistsSubqueryRegistry? ExistsSubqueries { get; }

    public DeleteTicket(
        KvTransaction txnState,
        string databaseName,
        string tableName,
        NodeAst? where,
        List<QueryFilter>? filters,
        Dictionary<string, ColumnValue>? parameters = null,
        NodeAst? limit = null,
        Diagnostics.StatementProbe? probe = null,
        ExistsSubqueryRegistry? existsSubqueries = null
    )
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Where = where;
        Filters = filters;
        Parameters = parameters;
        Limit = limit;
        Probe = probe;
        ExistsSubqueries = existsSubqueries;
    }
}
