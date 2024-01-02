
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct UpdateTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public NodeAst? Where { get; }

    public List<QueryFilter>? Filters { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public UpdateTicket(
        HLCTimestamp txnId,
        string databaseName,
        string tableName,
        Dictionary<string, ColumnValue> values,
        NodeAst? where,
        List<QueryFilter>? filters,
        Dictionary<string, ColumnValue>? parameters)
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        Values = values;
        Where = where;
        Filters = filters;
        Parameters = parameters;
    }
}
