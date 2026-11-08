/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// </summary>
internal sealed record PreparedValueSubquery(
    IReadOnlyList<string> OuterReferences,
    CorrelatedSubqueryBody Body);
