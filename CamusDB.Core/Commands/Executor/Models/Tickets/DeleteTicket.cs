
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

    public DeleteTicket(
        KvTransaction txnState,
        string databaseName,
        string tableName,
        NodeAst? where,
        List<QueryFilter>? filters,
        Dictionary<string, ColumnValue>? parameters = null
    )
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Where = where;
        Filters = filters;
        Parameters = parameters;
    }
}
