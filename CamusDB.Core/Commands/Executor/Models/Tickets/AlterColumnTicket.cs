
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterColumnTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo Column { get; }

    public AlterTableOperation Operation { get; }

    public AlterColumnTicket(string databaseName, string tableName, ColumnInfo column, AlterTableOperation operation)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Column = column;
        Operation = operation;
    }
}
