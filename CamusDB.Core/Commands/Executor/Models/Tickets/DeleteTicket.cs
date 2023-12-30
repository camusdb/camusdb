
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DeleteTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public NodeAst? Where { get; }

    public List<QueryFilter>? Filters { get; }

    public DeleteTicket(
        HLCTimestamp txnId,
        string databaseName,
        string tableName,
        NodeAst? where,
        List<QueryFilter>? filters
    )
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        Where = where;
        Filters = filters;
    }
}
