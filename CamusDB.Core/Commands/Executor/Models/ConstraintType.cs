
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public enum ConstraintType
{
    PrimaryKey,
    IndexMulti,
    IndexUnique,

    /// <summary>A GIN index over one jsonb column (the inline <c>INVERTED INDEX name (col)</c>).</summary>
    IndexInverted,
}
