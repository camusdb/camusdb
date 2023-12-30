
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DeleteByIdTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string Id { get; }

    public DeleteByIdTicket(HLCTimestamp txnId, string databaseName, string tableName, string id)
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        Id = id;
    }
}
