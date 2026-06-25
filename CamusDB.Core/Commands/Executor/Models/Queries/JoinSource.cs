
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
/// <param name="Left">Left input source (preserved for deterministic nested-loop order).</param>
/// <param name="Right">Right input source.</param>
/// <param name="Kind">Join kind; only <see cref="JoinKind.Inner"/> is supported initially.</param>
/// <param name="OnPredicate">Parsed <c>ON</c> predicate. Evaluated after row merge.</param>
public sealed record JoinSource(
    QuerySource Left,
    QuerySource Right,
    JoinKind Kind,
    NodeAst OnPredicate) : QuerySource;
