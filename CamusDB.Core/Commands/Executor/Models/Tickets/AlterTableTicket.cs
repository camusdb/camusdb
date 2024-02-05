
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterTableTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public AlterTableOperation Operation { get; }

    public ColumnInfo Column { get; }

    public AlterTableTicket(
        TransactionState txnState,
        string databaseName,
        string tableName,
        AlterTableOperation operation,
        ColumnInfo column
    )
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        Operation = operation;
        Column = column;
    }
}

