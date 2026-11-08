
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
/// A correlated EXISTS subquery, bound once at prepare time and re-evaluated per outer row. Only its
/// FROM and WHERE run. <see cref="ExistsSubqueryPreparer"/> refuses a body that needs more, and
/// <see cref="InnerBound"/> holds the body without its output, see <see cref="ExistsBody"/>.
///
/// <see cref="SeekPlan"/> is the chosen inner access path; null means no index qualified and the
/// executor scans the inner table. It is carried here rather than recomputed per row because it
/// depends only on the inner predicate's shape and the inner table's indexes, neither of which
/// varies across outer rows.
///
/// <para><see cref="RowNames"/> resolves the names of <see cref="InnerWhere"/>: a name resolves to the
/// inner query first and then to the outer query, so a column that both have is the inner column. It is
/// built and frozen once here, not for each outer row. <see cref="OuterReferences"/> holds the names of
/// the body that resolve to the outer query, bare names included, so the outer scan decodes those
/// columns.</para>
/// </summary>
internal sealed record PreparedExistsSubquery(
    NodeAst? InnerWhere,
    BoundSelectQuery InnerBound,
    IReadOnlyList<BoundTableSource> OuterSources,
    IReadOnlyList<BoundDerivedTableSource> OuterDerivedSources,
    QueryRowNameResolver RowNames,
    IReadOnlyList<string> OuterReferences,
    CorrelatedExistsSeekPlan? SeekPlan = null);
