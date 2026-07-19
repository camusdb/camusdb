
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
