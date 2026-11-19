
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

    /// <summary>
    /// When <c>false</c> (default), a materialized view is refused with "use ALTER MATERIALIZED VIEW":
    /// a materialized view is stored as a relation, so without this the statement that renames tables
    /// would happily rename one — the same asymmetry <see cref="DropTableTicket.AllowMaterializedView"/>
    /// closes for <c>DROP TABLE</c>. <c>ALTER MATERIALIZED VIEW … RENAME TO</c> sets it. It rides the
    /// forwarded request, so a follower's statement meets the same rule on the schema leader.
    /// </summary>
    public bool AllowMaterializedView { get; }

    public RenameTableTicket(string databaseName, string tableName, string newName, bool allowMaterializedView = false)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        NewName = newName;
        AllowMaterializedView = allowMaterializedView;
    }
}
