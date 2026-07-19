
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Recovers an orphaned (deferred-dropped) table in <see cref="DatabaseName"/> by re-attaching a new
/// name to its preserved id and retained row/index data — the executor form of
/// <c>CREATE TABLE {NewTableName} RELINK TO '{OrphanTableId}'</c>. The table definition is reconstructed
/// from the orphan record; the id is reused so the reattached table reads its old rows. Valid only while
/// the table's orphan record still exists.
/// </summary>
public readonly struct RelinkTableTicket
{
    /// <summary>Database that owns the orphan and will hold the recovered table.</summary>
    public string DatabaseName { get; }

    /// <summary>New name to give the recovered table. Must be currently free in the database.</summary>
    public string NewTableName { get; }

    /// <summary>Orphan table id to recover (as shown by <c>SHOW ORPHAN TABLES</c>).</summary>
    public string OrphanTableId { get; }

    public RelinkTableTicket(string databaseName, string newTableName, string orphanTableId)
    {
        DatabaseName = databaseName;
        NewTableName = newTableName;
        OrphanTableId = orphanTableId;
    }
}
