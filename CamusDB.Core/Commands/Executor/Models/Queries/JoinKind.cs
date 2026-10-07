
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// The join kinds the planner and the executor know. There is no right-outer and no cross member:
/// a <c>RIGHT JOIN</c> is rewritten into a left outer join with its operands swapped, and a
/// <c>CROSS JOIN</c> into a comma join or an inner join on the literal true, when the logical query
/// is created (see <c>SelectQueryCreator</c>). Every later stage therefore handles two kinds only.
/// </summary>
public enum JoinKind
{
    /// <summary>A row is emitted only when a right row satisfies the <c>ON</c> predicate.</summary>
    Inner,

    /// <summary>
    /// Every left row is emitted at least once. A left row with no right row that satisfies
    /// <c>ON</c> is emitted once with every right output column set to NULL (a padded row). The
    /// left input is always the preserved side, so an operator never needs a matched flag on
    /// the right.
    /// </summary>
    LeftOuter,
}
