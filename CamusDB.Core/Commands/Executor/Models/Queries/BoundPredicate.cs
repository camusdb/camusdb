
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A predicate attached to a logical query after parsing and, later, binding (QP1.3).
/// </summary>
/// <param name="Expression">
/// Parsed predicate tree (<c>WHERE</c>, join <c>ON</c>, or <c>HAVING</c> when supported).
/// Predicate trees are analyzed by <see cref="Controllers.Queries.PredicateAnalyzer"/> at ticket creation.
/// </param>
public sealed record BoundPredicate(NodeAst Expression);
