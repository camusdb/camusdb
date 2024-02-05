
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class QueryTicket
{
    public TransactionState TxnState { get; }

    public TransactionType TxnType { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string? IndexName { get; }

    public List<NodeAst>? Projection { get; }

    public List<QueryFilter>? Filters { get; }

    public NodeAst? Where { get; }

    public List<QueryOrderBy>? OrderBy { get; }

    public NodeAst? Limit { get; }

    public NodeAst? Offset { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public QueryTicket(
        TransactionState txnState,
        TransactionType txnType,
        string databaseName,
        string tableName,
        string? index,
        List<NodeAst>? projection,
        List<QueryFilter>? filters,
        NodeAst? where,
        List<QueryOrderBy>? orderBy,
        NodeAst? limit,
        NodeAst? offset,
        Dictionary<string, ColumnValue>? parameters)
    {
        TxnState = txnState;
        TxnType = txnType;
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = index;
        Projection = projection;
        Filters = filters;
        Where = where;
        OrderBy = orderBy;
        Limit = limit;
        Offset = offset;
        Parameters = parameters;
    }
}
