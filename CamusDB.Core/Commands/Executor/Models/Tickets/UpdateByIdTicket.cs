
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct UpdateByIdTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string Id { get; }

    public Dictionary<string, ColumnValue> Values { get; }

    public UpdateByIdTicket(TransactionState txnState, string databaseName, string tableName, string id, Dictionary<string, ColumnValue> values)
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Id = id;
        Values = values;
    }
}