
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterIndexTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string IndexName { get; }

    public ColumnIndexInfo[] Columns { get; }

    public AlterIndexOperation Operation { get; }    

    public AlterIndexTicket(
        HLCTimestamp txnId,
        string databaseName,
        string tableName,
        string indexName,
        ColumnIndexInfo[] columns,
        AlterIndexOperation operation
    )
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = indexName;
        Columns = columns;
        Operation = operation;        
    }
}

