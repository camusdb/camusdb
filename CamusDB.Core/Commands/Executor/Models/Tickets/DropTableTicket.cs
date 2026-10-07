
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

    /// <summary>
    /// When <c>false</c> (default), a materialized view is refused with "use DROP MATERIALIZED VIEW":
    /// a materialized view is stored as a relation, so without this the statement that drops tables
    /// would happily remove one. <c>DROP MATERIALIZED VIEW</c> and the engine's own removal of a
    /// staging relation set it to <c>true</c>. It rides the forwarded request, so a follower's
    /// statement meets the same rule on the schema leader.
    /// </summary>
    public bool AllowMaterializedView { get; }

    public DropTableTicket(string databaseName, string tableName, bool ifExists, bool force = false, bool allowMaterializedView = false)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IfExists = ifExists;
        Force = force;
        AllowMaterializedView = allowMaterializedView;
    }
}
