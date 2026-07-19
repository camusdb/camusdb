
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DropTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public bool IfExists { get; }

    /// <summary>
    /// When <c>true</c> (SQL <c>DROP TABLE ... FORCE</c>), the table's rows and index entries are
    /// physically deleted immediately and no orphan record is written — the pre-deferred-drop behavior.
    /// When <c>false</c> (default), a table in a root database is retained as a recoverable orphan;
    /// tables in branch databases are always dropped immediately regardless of this flag.
    /// </summary>
    public bool Force { get; }

    public DropTableTicket(string databaseName, string tableName, bool ifExists, bool force = false)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IfExists = ifExists;
        Force = force;
    }
}
