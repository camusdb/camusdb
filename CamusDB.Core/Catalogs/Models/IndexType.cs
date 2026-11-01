
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The kind of an index. The integer values are persisted in schema JSON and the schema log: never
/// renumber them, and give a new member a new value.
/// </summary>
public enum IndexType
{
    None = 0,
    Unique = 1,
    Multi = 2,

    /// <summary>
    /// A GIN (generalized inverted) index over one jsonb column: one entry per path and leaf value
    /// of each document, not one ordered key per row (<c>JsonbGinEntries</c>). It answers
    /// <c>@&gt;</c>, <c>?</c>, <c>?|</c>, <c>?&amp;</c> and field equality through candidate rows that
    /// the full predicate then checks. It is never an ordered index: <c>TableDescriptor.Indexes</c>
    /// leaves it out, so no ordered access path, join or foreign key can pick it; the write paths
    /// and the DDL reach it through <c>TableDescriptor.AllIndexes</c>.
    /// </summary>
    Inverted = 3,
}
