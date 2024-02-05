
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct InsertTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }
    
    public string TableName { get; }

    public List<Dictionary<string, ColumnValue>> Values { get; }    

    public InsertTicket(
        TransactionState txnState,
        string databaseName,
        string tableName,
        List<Dictionary<string, ColumnValue>> values
    )
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Values = values;
    }
}

