/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// How a composite foreign key treats a child row whose referencing columns are partly NULL
/// (<c>MATCH SIMPLE | FULL | PARTIAL</c>).
///
/// <para>Only <see cref="Simple"/> is enforced today. The other members are persisted so that a later
/// release needs no schema migration, and the DDL path refuses them until then. Persisted as an
/// integer: append members only and never renumber.</para>
/// </summary>
public enum ForeignKeyMatch
{
    /// <summary>
    /// A NULL in any referencing column satisfies the constraint; only a row whose referencing columns
    /// are all non-NULL must match a parent. The SQL default.
    /// </summary>
    Simple = 0,

    /// <summary>The referencing columns must be all NULL or all non-NULL and matching. Not enforced yet.</summary>
    Full = 1,

    /// <summary>The non-NULL referencing columns must match some parent. Not enforced yet.</summary>
    Partial = 2
}
