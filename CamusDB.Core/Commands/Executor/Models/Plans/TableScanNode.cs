
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

/// <summary>Full scan over table rows or a forced index bucket.</summary>
public sealed class TableScanNode : PhysicalPlanNode
{
    public TableScanSource Source { get; }

    /// <summary>Required when <see cref="Source"/> is <see cref="TableScanSource.ForcedIndex"/>.</summary>
    public TableIndexSchema? Index { get; }

    /// <summary>Bound table source for join-plan table scans (QP4.3).</summary>
    public BoundTableSource? BoundSource { get; init; }

    /// <summary>Single-table predicate pushed into this scan (QP4.4).</summary>
    public NodeAst? ExecutionFilter { get; init; }

    public override bool CanDecomposeToLocalPlusMerge => true;

    public TableScanNode(TableScanSource source, TableIndexSchema? index = null)
    {
        Source = source;
        Index = index;
    }
}

public enum TableScanSource
{
    PrimaryRows,
    ForcedIndex
}
