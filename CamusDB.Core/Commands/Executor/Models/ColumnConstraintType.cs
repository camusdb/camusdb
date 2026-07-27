
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public enum ColumnConstraintType
{
    PrimaryKey,
    Null,
    NotNull,
    Unique,
    Default,
    DefaultFunction,
    Check,
    ForeignKey,

    /// <summary>
    /// Inline <c>COMMENT '&lt;text&gt;'</c> on a column definition. The decoded text is carried as a
    /// String <c>ColumnValue</c>, so a present-but-empty comment stays distinguishable from an absent
    /// one (which produces no entry at all).
    /// </summary>
    Comment
}
