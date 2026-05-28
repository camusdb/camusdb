
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterIndexTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string IndexName { get; }

    public ColumnIndexInfo[] Columns { get; }

    public AlterIndexOperation Operation { get; }    

    public AlterIndexTicket(
        string databaseName,
        string tableName,
        string indexName,
        ColumnIndexInfo[] columns,
        AlterIndexOperation operation
    )
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = indexName;
        Columns = columns;
        Operation = operation;        
    }
}

