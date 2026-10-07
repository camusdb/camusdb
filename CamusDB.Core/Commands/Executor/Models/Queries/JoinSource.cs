
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A binary join between two <see cref="QuerySource"/> nodes.
/// </summary>
/// <param name="Left">
/// Left input source. For <see cref="JoinKind.LeftOuter"/> it is the preserved side: every one of
/// its rows reaches the output.
/// </param>
/// <param name="Right">
/// Right input source. The planner requires it to be one table or one derived table, never a
/// join subtree, so a <c>RIGHT JOIN</c> can only be rewritten when its left operand is a leaf.
/// </param>
/// <param name="Kind">Join kind: <see cref="JoinKind.Inner"/> or <see cref="JoinKind.LeftOuter"/>.</param>
/// <param name="OnPredicate">
/// Parsed <c>ON</c> predicate, evaluated after row merge and before any outer-join padding. A
/// cross join carries the literal true here.
/// </param>
public sealed record JoinSource(
    QuerySource Left,
    QuerySource Right,
    JoinKind Kind,
    NodeAst OnPredicate) : QuerySource;
