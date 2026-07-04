
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Cache;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Logical select query produced after parsing and, later, binding.
/// Replaces the single-table <c>QueryTicket</c> shape for multi-source SQL.
/// </summary>
/// <param name="Source"><c>FROM</c> clause tree.</param>
/// <param name="Projections">Selected expressions or <c>*</c>.</param>
/// <param name="Where">Optional filter predicate.</param>
/// <param name="GroupBy">Optional grouping expressions.</param>
/// <param name="Having">Optional post-aggregate filter predicate.</param>
/// <param name="OrderBy">Optional sort keys.</param>
/// <param name="Limit">Optional limit expression AST.</param>
/// <param name="Offset">Optional offset expression AST.</param>
/// <param name="IsDistinct">When true, duplicate output rows are eliminated after projection.</param>
/// <param name="CacheHint">
/// Query-level cache hint extracted from the first <c>{cache=name}</c> table-reference hint
/// found in the <c>FROM</c> clause. Null when the query carries no cache hint and must
/// follow the uncached execution path.
/// </param>
public sealed record SelectQuery(
    QuerySource Source,
    IReadOnlyList<ProjectionItem> Projections,
    BoundPredicate? Where = null,
    IReadOnlyList<NodeAst>? GroupBy = null,
    BoundPredicate? Having = null,
    IReadOnlyList<OrderByItem>? OrderBy = null,
    NodeAst? Limit = null,
    NodeAst? Offset = null,
    bool IsDistinct = false,
    CacheHintOptions? CacheHint = null);
