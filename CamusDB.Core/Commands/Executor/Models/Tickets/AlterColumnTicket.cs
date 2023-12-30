
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterColumnTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string ColumnName { get; }

    public AlterColumnTicket(HLCTimestamp txnId, string databaseName, string tableName, string columnName)
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        ColumnName = columnName;
    }
}
