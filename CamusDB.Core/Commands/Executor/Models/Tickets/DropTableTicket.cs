
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DropTableTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public bool IfExists { get; }

    public DropTableTicket(TransactionState txnState, string databaseName, string tableName, bool ifExists)
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        IfExists = ifExists;
    }
}
