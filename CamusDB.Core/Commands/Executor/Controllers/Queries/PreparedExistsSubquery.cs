
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A correlated EXISTS subquery, bound once at prepare time and re-evaluated per outer row.
///
/// <see cref="SeekPlan"/> is the chosen inner access path; null means no index qualified and the
/// executor scans the inner table. It is carried here rather than recomputed per row because it
/// depends only on the inner predicate's shape and the inner table's indexes, neither of which
/// varies across outer rows.
/// </summary>
internal sealed record PreparedExistsSubquery(
    NodeAst? InnerWhere,
    BoundSelectQuery InnerBound,
    IReadOnlyList<BoundTableSource> OuterSources,
    IReadOnlyList<BoundDerivedTableSource> OuterDerivedSources,
    CorrelatedExistsSeekPlan? SeekPlan = null);
