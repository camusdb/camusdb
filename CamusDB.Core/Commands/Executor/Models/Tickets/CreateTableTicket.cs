
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct CreateTableTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo[] Columns { get; }

    public ConstraintInfo[] Constraints { get; }

    public bool IfNotExists { get; }

    public CreateTableTicket(
        HLCTimestamp txnId,
        string databaseName,
        string tableName,
        ColumnInfo[] columns,
        ConstraintInfo[] constraints,
        bool ifNotExists
    )
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        Columns = columns;
        Constraints = constraints;
        IfNotExists = ifNotExists;
    }
}

