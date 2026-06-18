
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public AlterTableOperation Operation { get; }

    public ColumnInfo Column { get; }

    public string? NewName { get; }

    public AlterTableTicket(
        string databaseName,
        string tableName,
        AlterTableOperation operation,
        ColumnInfo column,
        string? newName = null
    )
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Operation = operation;
        Column = column;
        NewName = newName;
    }
}

