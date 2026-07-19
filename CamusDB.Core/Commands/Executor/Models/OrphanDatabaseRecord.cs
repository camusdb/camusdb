
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Persistent record marking a dropped root database as an <em>orphan</em>: its keyspace and all
/// meta keys are still on disk and referenced by no live name, awaiting either recovery
/// (<c>CREATE DATABASE ... RELINK TO &lt;id&gt;</c>) or physical reclamation by the garbage collector
/// once the retention window elapses.
///
/// <para>Stored under <c>_system/dbregistry/orphan:{Id}</c> in the shared node, alongside the
/// registry name entries and drop-lifecycle markers. Written by <c>DROP DATABASE</c> (non-<c>FORCE</c>,
/// root only) <em>before</em> the name is unregistered, so a crash between the two leaves the database
/// still live rather than data stranded with no recovery path.</para>
///
/// <para><b>Retained-data invariant:</b> while this record exists, every <c>{Id}:...</c> row/index key
/// and every <c>{Id}/meta/...</c> key of the dropped database remains physically present and unchanged.
/// Deleting this record and purging that keyspace must happen together (record deleted last) so
/// reclamation is idempotent from the id alone. Only root databases are orphaned; branch databases
/// keep the immediate-purge drop path because their recovery would require holding the parent's
/// snapshot floor for the whole retention window.</para>
/// </summary>
public sealed class OrphanDatabaseRecord
{
    /// <summary>Preserved database id (short base-62). Never reused; a relink re-attaches a new name to it.</summary>
    public string Id { get; set; } = "";

    /// <summary>Name the database had at drop time, surfaced by <c>SHOW ORPHAN DATABASES</c> for identification.</summary>
    public string FormerName { get; set; } = "";

    /// <summary>
    /// HLC timestamp of the drop. The garbage collector may physically reclaim the orphan once
    /// <c>now - DroppedAt</c> exceeds the configured retention window. HLC (not wall clock) so the
    /// eligibility decision is consistent across cluster nodes.
    /// </summary>
    public HLCTimestamp DroppedAt { get; set; }
}
