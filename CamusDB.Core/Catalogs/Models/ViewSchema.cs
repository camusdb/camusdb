
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// A non-materialized view: a named, stored query expanded at every reference.
///
/// <para>Unlike a <see cref="TableSchema"/> this owns no rows and no keyspace — the only durable
/// state is the definition and the column list, both inside <see cref="Definition"/>. There is
/// deliberately no schema-version counter here: a view has no stored bytes to decode, so nothing
/// needs versioning for MVCC. What does need to be watched is the database-level
/// <c>Schema.SchemaVersion</c>, which every view DDL bumps and which the expansion cache keys on so
/// a <c>CREATE OR REPLACE</c> on any node invalidates every node's cached body.</para>
/// </summary>
public sealed class ViewSchema
{
    /// <summary>
    /// Immutable identifier, allocated from the same per-store sequence table ids come from. Sharing
    /// the sequence is what guarantees a view and a table can never collide in the meta keyspace,
    /// and keeping it immutable is what lets a rename be metadata-only and lets dependents refer to
    /// this view by something a rename cannot invalidate.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>The view's name. Changed by <c>ALTER VIEW … RENAME TO</c>; the <see cref="Id"/> is
    /// not.</summary>
    public string? Name { get; set; }

    /// <summary>The stored body, column list, dependencies, check option and owner.</summary>
    public ViewDefinition? Definition { get; set; }

    /// <summary>Free-text description attached via <c>COMMENT ON VIEW</c>. Null means no comment; an
    /// empty string is a present-but-empty comment, and the two stay distinguishable so
    /// <c>SHOW CREATE VIEW</c> can omit the clause entirely for null — the same rule
    /// <c>TableSchema.Comment</c> follows.</summary>
    public string? Comment { get; set; }
}
