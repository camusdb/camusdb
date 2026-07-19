
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct DropDatabaseTicket
{
    public string DatabaseName { get; }

    public bool IfExists { get; }

    /// <summary>
    /// When <c>true</c> (SQL <c>DROP DATABASE ... FORCE</c>), the database's keyspace is physically
    /// purged immediately and no orphan record is written — the pre-deferred-drop behavior, and the
    /// only way to reclaim without waiting for the retention window. When <c>false</c> (default), a
    /// root database is retained as a recoverable orphan; branch databases are always purged
    /// immediately regardless of this flag.
    /// </summary>
    public bool Force { get; }

    public DropDatabaseTicket(string name, bool ifExists = false, bool force = false)
    {
        DatabaseName = name;
        IfExists = ifExists;
        Force = force;
    }
}
