
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

    /// <summary>
    /// The id the caller resolved <see cref="TableName"/> to, or null to drop whatever relation has the
    /// name. Set by an engine path that resolved the name earlier and then awaited, such as
    /// <c>DROP MATERIALIZED VIEW</c>: a rename in that wait can give the name to a different relation,
    /// and dropping by name alone would remove it. The drop is refused with
    /// <c>ConcurrentSchemaChange</c> when the name resolves to another id. It rides the forwarded
    /// request, so the schema leader checks it too.
    /// </summary>
    public string? ExpectedTableId { get; }

    public DropTableTicket(
        string databaseName,
        string tableName,
        bool ifExists,
        bool force = false,
        bool allowMaterializedView = false,
        string? expectedTableId = null)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IfExists = ifExists;
        Force = force;
        AllowMaterializedView = allowMaterializedView;
        ExpectedTableId = expectedTableId;
    }
}
