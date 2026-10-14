/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Produces the joined row of each candidate pair for one join execution, as a pair-backed
/// <see cref="QueryRow"/> (<see cref="QueryRow.FromPair"/>) rather than an eagerly merged one.
///
/// <para>Every join operator builds the joined row first and evaluates its <c>ON</c> on it. The
/// eager merge allocated an array as wide as the join and read every cell of both inputs for every
/// candidate, so a pair the <c>ON</c> rejected paid the whole cost of an output row. A pair-backed
/// row reads only the cells the predicate reads; a rejected pair costs one small object, and an
/// accepted one flows on unchanged, so a later projection reads only the cells it needs.</para>
///
/// <para>The <see cref="JoinPairMap"/> is built from the first pair and kept while the join
/// layout is the same object; a left outer join can rebind its layout, and the map follows.
/// One instance belongs to one execution of one operator and is used from one thread.</para>
/// </summary>
internal sealed class JoinRowPairer
{
    private JoinPairMap? map;

    /// <summary>The joined row of <paramref name="leftQualified"/> and <paramref name="rightRow"/>.</summary>
    public QueryRow Pair(
        IReadOnlyDictionary<string, ColumnValue> leftQualified,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        RowLayout joinLayout,
        Dictionary<string, int> rightKeyOrdinalMap)
    {
        if (map is null || !ReferenceEquals(map.Layout, joinLayout))
            map = JoinPairMap.Build(joinLayout, leftQualified, rightRow, rightKeyOrdinalMap);

        return QueryRow.FromPair(joinLayout, map, leftQualified, rightRow);
    }
}
