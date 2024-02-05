
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct QueryByIdTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string Id { get; }

    public QueryByIdTicket(TransactionState txnState, string databaseName, string tableName, string id)
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Id = id;
    }
}
