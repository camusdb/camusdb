
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
/// </summary>
public sealed class DatabaseRegistryEntry
{
    /// <summary>Short base-62 opaque id allocated from a per-store monotonic counter at database creation. Never reused.</summary>
    public string Id { get; set; } = "";

    /// <summary>User-visible database name. Mutable (rename changes this; id stays).</summary>
    public string Name { get; set; } = "";

    /// <summary>UTC timestamp when the database was registered.</summary>
    public DateTime CreatedAt { get; set; }
}
