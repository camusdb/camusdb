
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
/// Index nested-loop inner join: probe the right table via a secondary index per left row (QP4.5).
/// </summary>
public sealed class IndexNestedLoopJoinNode : PhysicalPlanNode
{
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
