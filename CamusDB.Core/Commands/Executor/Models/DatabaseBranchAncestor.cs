
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One entry in the immutable branch ancestry chain stored on a
/// <see cref="DatabaseRegistryEntry"/>.  Records that the owning database was forked
/// from <see cref="DatabaseId"/> at the HLC instant <see cref="ForkTimestamp"/>.
///
/// Ancestry is ordered nearest-parent-first: <c>Ancestors[0]</c> is the immediate
/// parent, <c>Ancestors[1]</c> is the grandparent, and so on.
///
/// <see cref="ForkTimestamp"/> is the HLC timestamp minted after the source schema
/// was confirmed stable.  Reads against that ancestor level always use this exact
/// timestamp as the read-snapshot floor, never wall-clock time.
/// </summary>
public sealed class DatabaseBranchAncestor
{
    /// <summary>Stable opaque id of the ancestor database.</summary>
    public string DatabaseId { get; set; } = "";

    /// <summary>
    /// HLC timestamp at which the owning database was forked from this ancestor.
    /// Reads against this level use this timestamp as their snapshot boundary.
    /// </summary>
    public HLCTimestamp ForkTimestamp { get; set; }
}
