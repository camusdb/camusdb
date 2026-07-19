
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Recovers an orphaned (deferred-dropped) root database by re-attaching a new name to its preserved
/// id and retained keyspace — the executor form of <c>CREATE DATABASE {NewName} RELINK TO '{OrphanId}'</c>.
/// The id is reused as-is; all row/index/meta keys are already on disk, so the database opens fully
/// populated. Valid only while the orphan record still exists (the garbage collector has not reclaimed it).
/// </summary>
public readonly struct RelinkDatabaseTicket
{
    /// <summary>New name to give the recovered database. Must be currently free.</summary>
    public string NewName { get; }

    /// <summary>Orphan database id to recover (as shown by <c>SHOW ORPHAN DATABASES</c>).</summary>
    public string OrphanId { get; }

    public RelinkDatabaseTicket(string newName, string orphanId)
    {
        NewName = newName;
        OrphanId = orphanId;
    }
}
