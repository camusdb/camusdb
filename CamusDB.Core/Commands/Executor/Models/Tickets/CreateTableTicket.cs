
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct CreateTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo[] Columns { get; }

    public bool IfNotExists { get; }

    public CreateTableTicket(string databaseName, string tableName, ColumnInfo[] columns, bool ifNotExists)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Columns = columns;
        IfNotExists = ifNotExists;
    }
}

