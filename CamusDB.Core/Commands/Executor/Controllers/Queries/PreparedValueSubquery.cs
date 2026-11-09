/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A correlated scalar, <c>IN</c>, <c>NOT IN</c> or quantified subquery, prepared once when the statement
/// binds and run for each outer row. The node that the registry maps to it says which form it is: an
/// <c>ExprScalarSubquery</c>, an <c>ExprInSubquery</c> or <c>ExprNotInSubquery</c>, or an
/// <c>ExprQuantifiedComparison</c> whose right operand is the subquery. See
/// <see cref="CorrelatedSubqueryValues"/> for how each form gives its value.
///
/// <para><see cref="OuterReferences"/> holds the names of the body that resolve to the outer query, bare
/// names included, so the outer scan decodes those columns.</para>
///
/// <para><see cref="Results"/> keeps the result for each distinct set of outer values, for this execution,
/// or is null when a kept result could be wrong; see <see cref="CorrelatedResultCache{T}"/>.</para>
/// </summary>
internal sealed record PreparedValueSubquery(
    IReadOnlyList<string> OuterReferences,
    CorrelatedSubqueryBody Body,
    CorrelatedResultCache<KeptSubqueryResult>? Results = null);

/// <summary>
/// The kept result of one run of a correlated value subquery. A scalar subquery keeps its <see cref="Value"/>.
/// An <c>IN</c>, <c>NOT IN</c> or quantified subquery keeps its <see cref="Test"/>: the membership or value-set
/// node built from the rows of the body, which holds its own copy of the values and which each outer row
/// evaluates with its own tested value.
/// </summary>
internal readonly record struct KeptSubqueryResult(ColumnValue? Value, NodeAst? Test);
