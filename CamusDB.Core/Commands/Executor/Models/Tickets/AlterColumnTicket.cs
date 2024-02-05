
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterColumnTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo Column { get; }

    public AlterTableOperation Operation { get; }

    public AlterColumnTicket(TransactionState txnState, string databaseName, string tableName, ColumnInfo column, AlterTableOperation operation)
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Column = column;
        Operation = operation;
    }
}
