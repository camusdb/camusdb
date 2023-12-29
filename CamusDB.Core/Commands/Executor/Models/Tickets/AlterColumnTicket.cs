
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

    public string ColumnName { get; }

    public AlterColumnTicket(string database, string name, string column)
    {
        DatabaseName = database;
        TableName = name;
        ColumnName = column;
    }
}
