
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Index nested-loop join: probe the right table via a secondary index per left row. For
/// <see cref="JoinKind.LeftOuter"/> a left row whose probe returns no accepted row, or whose lookup
/// value is NULL (never probed), is emitted once, padded with NULL right columns.
/// </summary>
public sealed class IndexNestedLoopJoinNode : PhysicalPlanNode
{
    /// <summary>Inner or left outer; the left input is the preserved side of an outer join.</summary>
    public JoinKind Kind { get; init; } = JoinKind.Inner;

    public BoundTableSource RightSource { get; }

    public NodeAst OnPredicate { get; }

    public NodeAst? RightExecutionFilter { get; init; }

    public TableIndexSchema Index { get; }

    public string LeftLookupColumn { get; }

    public string RightIndexColumn { get; }

    public IndexNestedLoopJoinNode(
        PhysicalPlanNode left,
        BoundTableSource rightSource,
        NodeAst onPredicate,
        TableIndexSchema index,
        string leftLookupColumn,
        string rightIndexColumn)
    {
        Input = left;
        RightSource = rightSource;
        OnPredicate = onPredicate;
        Index = index;
        LeftLookupColumn = leftLookupColumn;
        RightIndexColumn = rightIndexColumn;
    }

    public bool UseUniqueLookup => Index.Type == IndexType.Unique;
}
