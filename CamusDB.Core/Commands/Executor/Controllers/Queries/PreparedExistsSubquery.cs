
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
/// A correlated EXISTS subquery, prepared once when the statement binds and evaluated for each outer row.
/// Exactly one of <see cref="Scan"/> and <see cref="Body"/> is set.
///
/// <para><see cref="Scan"/> is the fast form: a body that reads one table and needs only its FROM and
/// WHERE. The executor reads the inner rows itself and tests the WHERE, with an index seek when one fits.
/// <see cref="Body"/> is every other body: it runs through the whole pipeline for each outer row.</para>
///
/// <para><see cref="OuterReferences"/> holds the names of the body that resolve to the outer query, bare
/// names included, so the outer scan decodes those columns.</para>
/// </summary>
internal sealed record PreparedExistsSubquery(
    IReadOnlyList<string> OuterReferences,
    CorrelatedExistsScan? Scan,
    CorrelatedSubqueryBody? Body);

/// <summary>
/// The fast form of a correlated EXISTS: one table, and only its FROM and WHERE run.
/// <see cref="ExistsSubqueryPreparer"/> uses it only for such a body, and <see cref="InnerBound"/> holds the
/// body without its output, see <see cref="ExistsBody"/>.
///
/// <para><see cref="SeekPlan"/> is the chosen inner access path; null means no index qualified and the
/// executor scans the inner table. It is carried here rather than recomputed per row because it depends
/// only on the inner predicate's shape and the inner table's indexes, neither of which varies across outer
/// rows.</para>
///
/// <para><see cref="RowNames"/> resolves the names of <see cref="InnerWhere"/>: a name resolves to the
/// inner query first and then to the outer query, so a column that both have is the inner column. It is
/// built and frozen once here, not for each outer row.</para>
/// </summary>
internal sealed record CorrelatedExistsScan(
    NodeAst? InnerWhere,
    BoundSelectQuery InnerBound,
    IReadOnlyList<BoundTableSource> OuterSources,
    IReadOnlyList<BoundDerivedTableSource> OuterDerivedSources,
    QueryRowNameResolver RowNames,
    CorrelatedExistsSeekPlan? SeekPlan = null);
