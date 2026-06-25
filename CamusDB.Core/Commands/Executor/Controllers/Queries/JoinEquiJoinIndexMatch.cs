
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Describes an indexed equi-join probe on the right join input.
/// </summary>
internal sealed class JoinEquiJoinIndexMatch
{
    public TableIndexSchema Index { get; }

    /// <summary>Qualified left-side lookup key in merged rows, e.g. <c>u.id</c>.</summary>
    public string LeftLookupColumn { get; }

    /// <summary>Unqualified indexed column on the right table, e.g. <c>user_id</c>.</summary>
    public string RightIndexColumn { get; }

    public JoinEquiJoinIndexMatch(TableIndexSchema index, string leftLookupColumn, string rightIndexColumn)
    {
        Index = index;
        LeftLookupColumn = leftLookupColumn;
        RightIndexColumn = rightIndexColumn;
    }

    public bool UseUniqueLookup => Index.Type == IndexType.Unique;
}
