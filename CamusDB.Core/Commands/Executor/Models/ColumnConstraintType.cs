
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
    Comment,

    /// <summary>
    /// Inline <c>STORAGE &lt;strategy&gt;</c> on a column definition. The strategy name is carried as a
    /// String <c>ColumnValue</c> and parsed by the creator, which also rejects it on a column type that
    /// has no variable-length value.
    /// </summary>
    Storage
}
