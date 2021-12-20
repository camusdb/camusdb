
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class CreateTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo[] Columns { get; }

    public CreateTableTicket(string database, string name, ColumnInfo[] columns)
    {
        DatabaseName = database;
        TableName = name;
        Columns = columns;
    }
}

