
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct UpdateByIdTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string Id { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public UpdateByIdTicket(HLCTimestamp txnId, string databaseName, string tableName, string id, Dictionary<string, ColumnValue> values)
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        Id = id;
        Values = values;
    }
}