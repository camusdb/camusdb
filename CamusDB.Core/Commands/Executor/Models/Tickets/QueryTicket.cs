
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct QueryTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string? IndexName { get; }

    public List<QueryFilter>? Filters { get; }

    public NodeAst? Where { get; }

    public List<QueryOrderBy>? OrderBy { get; }

    public QueryTicket(
        HLCTimestamp txnId,
        string databaseName,
        string tableName,
        string? index,
        List<QueryFilter>? filters,
        NodeAst? where,
        List<QueryOrderBy>? orderBy)
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = index;
        Filters = filters;
        Where = where;
        OrderBy = orderBy;
    }
}
