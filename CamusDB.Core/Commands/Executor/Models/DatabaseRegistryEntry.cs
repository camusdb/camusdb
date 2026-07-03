
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Persistent record in the <see cref="DatabaseRegistry"/> that maps a human-readable
/// database name to its stable opaque id.  The id never changes; only the name can.
///
/// <see cref="Ancestors"/> records the full branch ancestry chain, nearest parent first.
/// It is empty for root databases and immutable after creation — only <see cref="Name"/>
/// and <see cref="CreatedAt"/> are ever updated (by RENAME DATABASE).
/// </summary>
public sealed class DatabaseRegistryEntry
{
    /// <summary>Short base-62 opaque id allocated from a per-store monotonic counter at database creation. Never reused.</summary>
    public string Id { get; set; } = "";

    /// <summary>User-visible database name. Mutable (rename changes this; id stays).</summary>
    public string Name { get; set; } = "";

    /// <summary>UTC timestamp when the database was registered.</summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// Immutable branch ancestry chain, nearest parent first.  Empty for root databases.
    /// <c>Ancestors[0]</c> is the immediate parent with the fork timestamp; subsequent
    /// entries are grandparent and deeper.  Older registry entries without this field
    /// deserialize with an empty list (backwards-compatible default).
    /// </summary>
    public List<DatabaseBranchAncestor> Ancestors { get; set; } = [];

    /// <summary>
    /// Id of the Kahuna snapshot-floor hold this branch owns on its <em>immediate</em> parent
    /// (<see cref="Ancestors"/><c>[0]</c>), acquired at fork time to pin the parent's MVCC history
    /// at <c>forkT</c>. Empty for root databases and for branches created before snapshot-floor
    /// durability landed. This branch is responsible for renewing the hold while it lives and
    /// releasing it on leaf drop; deeper ancestor levels are protected by their own branches' holds
    /// (a database with live descendants cannot be dropped, so those holds stay live). It names the
    /// hold, not the protected timestamp — the timestamp is <c>Ancestors[0].ForkTimestamp</c>.
    /// </summary>
    public string ImmediateParentHoldId { get; set; } = "";
}
