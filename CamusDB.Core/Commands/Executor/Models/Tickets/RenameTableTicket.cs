
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct RenameTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string NewName { get; }

    public RenameTableTicket(string databaseName, string tableName, string newName)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        NewName = newName;
    }
}
