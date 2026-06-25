
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A single equi-key pair extracted from a join ON predicate for use by hash join and merge join.
/// Unlike <see cref="JoinEquiJoinIndexMatch"/>, no index is required.
/// </summary>
internal sealed class JoinEquiKeyPair
{
    /// <summary>Qualified left-side (probe) key in the merged row, e.g. <c>o.id</c>.</summary>
    public string LeftLookupColumn { get; }

    /// <summary>Unqualified column name on the right (build) side, e.g. <c>order_id</c>.</summary>
    public string RightColumnName { get; }

    public JoinEquiKeyPair(string leftLookupColumn, string rightColumnName)
    {
        LeftLookupColumn = leftLookupColumn;
        RightColumnName = rightColumnName;
    }
}
