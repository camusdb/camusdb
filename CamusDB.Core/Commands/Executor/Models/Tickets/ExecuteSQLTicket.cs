
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct ExecuteSQLTicket
{
    public TransactionState TxnState { get; }

    public string DatabaseName { get; }

    public string Sql { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public ExecuteSQLTicket(TransactionState txnState, string database, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        TxnState = txnState;
        DatabaseName = database;
        Sql = sql;
        Parameters = parameters;
    }
}
