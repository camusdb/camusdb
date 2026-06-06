
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>Discriminates the three semi/anti-join flavours produced by the R11 rewrite.</summary>
public enum SemiJoinMode
{
    /// <summary>Emit the outer row when the inner probe finds at least one match (IN rewrite).</summary>
    Semi,

    /// <summary>Emit the outer row when the inner probe finds zero matches; inner column is NOT NULL
    /// so SQL three-valued-logic does not apply (NOT IN over a NOT NULL column).</summary>
    Anti,

    /// <summary>Emit the outer row only when the inner probe finds zero matches AND the inner column
    /// contains no NULL values; otherwise returns UNKNOWN (→ false, row excluded).</summary>
    NullAwareAnti,
}
