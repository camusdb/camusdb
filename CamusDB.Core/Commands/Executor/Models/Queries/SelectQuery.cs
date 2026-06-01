
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Logical select query produced after parsing and, later, binding (QP1.2–QP1.3).
/// Replaces the single-table <c>QueryTicket</c> shape for multi-source SQL.
/// </summary>
/// <param name="Source"><c>FROM</c> clause tree.</param>
/// <param name="Projections">Selected expressions or <c>*</c>.</param>
/// <param name="Where">Optional filter predicate.</param>
/// <param name="GroupBy">Optional grouping expressions (QP3).</param>
/// <param name="Having">Optional post-aggregate filter predicate (QP3.5).</param>
/// <param name="OrderBy">Optional sort keys.</param>
/// <param name="Limit">Optional limit expression AST.</param>
/// <param name="Offset">Optional offset expression AST.</param>
/// <param name="IsDistinct">When true, duplicate output rows are eliminated after projection (QP3.6).</param>
public sealed record SelectQuery(
    QuerySource Source,
    IReadOnlyList<ProjectionItem> Projections,
    BoundPredicate? Where = null,
    IReadOnlyList<NodeAst>? GroupBy = null,
    BoundPredicate? Having = null,
    IReadOnlyList<OrderByItem>? OrderBy = null,
    NodeAst? Limit = null,
    NodeAst? Offset = null,
    bool IsDistinct = false);
