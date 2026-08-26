
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Persistent record marking a dropped table as an <em>orphan</em>: its rows, index entries, and
/// schema-history keys are still on disk but the table is referenced by no live schema, awaiting
/// either recovery (<c>CREATE TABLE ... RELINK TO &lt;id&gt;</c>) or physical reclamation once the
/// retention window elapses.
///
/// <para>Stored under <c>{dbId}/meta/orphan:{TableId}</c>, written inside the same DDL transaction that
/// detaches the table from the live schema, so the detach and the orphan record commit atomically.</para>
///
/// <para><b>Self-contained on purpose:</b> the record carries the full <see cref="Schema"/> (columns,
/// indexes, check constraints, version) because the normal drop path deletes the per-table meta key
/// (<c>{dbId}/meta/table:{TableId}</c>). Relink reconstructs the table entirely from this record — it
/// does not depend on the deleted meta key — while the row/index KV data and the append-only
/// schema-history keys (which the drop leaves untouched) are reattached under the preserved
/// <see cref="TableId"/>.</para>
///
/// <para><b>Retained-data invariant:</b> while this record exists, every <c>{dbId}:{TableId}:r/...</c>
/// row key and <c>{dbId}:{TableId}:i:{indexId}/...</c> index key remains physically present. Deleting
/// this record and purging that data must happen together (record deleted last) so reclamation is
/// idempotent. Only tables in root databases are orphaned; tables in branch databases keep the
/// immediate drop path.</para>
/// </summary>
public sealed class OrphanTableRecord
{
    /// <summary>
    /// What this record stands for. Absent (the legacy shape, and the default) means
    /// <see cref="OrphanKind.DroppedTable"/>.
    ///
    /// <para>The distinction matters because reclamation asks a different question of each kind. A
    /// dropped table is gone from the live schema, so a live meta key at its id proves a relink
    /// finished and the record is stale. Retired contents belong to a relation that is still live at
    /// that very id on its first truncate, so the same test would discard the record and leak the
    /// data it protects.</para>
    /// </summary>
    public OrphanKind Kind { get; set; }

    /// <summary>
    /// For <see cref="OrphanKind.RetiredContents"/>, the still-live relation whose contents these
    /// were. Empty for a dropped table.
    /// </summary>
    public string SourceTableId { get; set; } = "";

    /// <summary>
    /// For <see cref="OrphanKind.RetiredContents"/>, the physical key-space that holds the retained
    /// rows and index entries. Empty for a dropped table, whose data lives under
    /// <see cref="TableId"/> (or the storage id carried by <see cref="Schema"/>).
    /// </summary>
    public string RetiredStorageId { get; set; } = "";

    /// <summary>
    /// The logical id of the relation a recovery published for this record, written <b>before</b> that
    /// relation is published so a crash mid-recovery is resumable. Null until a recovery starts.
    ///
    /// <para>This is the only proof that retired contents were relinked. A live relation at
    /// <see cref="SourceTableId"/> proves nothing — for a truncate that relation never stopped being
    /// live — so reclamation must look here, and must additionally confirm the named relation's
    /// effective storage really is <see cref="RetiredStorageId"/>.</para>
    /// </summary>
    public string? RelinkTargetId { get; set; }

    /// <summary>Preserved table id (short base-62 or legacy 24-hex). Never reused; relink re-attaches a new name to it.</summary>
    public string TableId { get; set; } = "";

    /// <summary>Name the table had at drop time, surfaced by <c>SHOW ORPHAN TABLES</c> for identification.</summary>
    public string FormerName { get; set; } = "";

    /// <summary>
    /// HLC timestamp of the drop. The garbage collector may physically reclaim the orphan once
    /// <c>now - DroppedAt</c> exceeds the configured retention window. HLC (not wall clock) so the
    /// eligibility decision is consistent across cluster nodes.
    /// </summary>
    public HLCTimestamp DroppedAt { get; set; }

    /// <summary>
    /// Full table definition captured at drop time (without lazily-loaded schema history), sufficient to
    /// reconstruct the table on relink. Its column layout, index ids, and check constraints must match
    /// the retained row/index data so the reattached table reads its old rows correctly.
    /// </summary>
    public TableSchema Schema { get; set; } = new();
}
