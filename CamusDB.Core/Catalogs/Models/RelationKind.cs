
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// What a <see cref="TableSchema"/> actually is. The numeric values are persisted, so they must
/// remain stable.
///
/// <para>A materialized view is deliberately modelled as a <see cref="TableSchema"/> rather than as
/// a separate kind of object: it has columns, rows, indexes, statistics and a primary key, and every
/// one of those already works on <c>TableSchema</c>/<c>TableDescriptor</c>/<c>KvTableStore</c>. That
/// choice is what gives materialized views backup/PITR, branch copy-on-write, orphan reclaim,
/// <c>ANALYZE</c>, covering indexes and the query-result cache with no new integration surface — and
/// it is what makes the atomic refresh swap in <c>MaterializedViewRefresher</c> a metadata-only
/// rename rather than a bulk data move.</para>
///
/// <para>The kinds are separated only where the difference is observable: DML is refused on a
/// materialized view, <c>REFRESH</c> is permitted only on one, reads of an unpopulated one fail, and
/// introspection reports each under its own statement.</para>
/// </summary>
public enum RelationKind
{
    /// <summary>An ordinary table whose rows come from user DML. The default, and the value every
    /// table created before this field existed decodes to.</summary>
    Table = 0,

    /// <summary>A materialized view: a real relation whose rows come from re-running
    /// <see cref="TableSchema.ViewDefinition"/> at <c>REFRESH</c> time.</summary>
    MaterializedView = 1
}
