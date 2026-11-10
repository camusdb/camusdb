
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
/// Drives N index seeks for <c>column IN (v1, v2, …)</c> and unions the results.
/// For a unique index of one column each value becomes one <c>LookupUnique</c> point lookup
/// (<see cref="IsPointLookup"/>). For every other index each value becomes one equality range
/// scan over the entries that share that leading value: a non-unique index, and a unique index of
/// several columns whose leading column is the list column, such as a composite primary key.
/// Duplicate row IDs (from a repeated value or overlapping non-unique entries)
/// are suppressed so every matched row is emitted at most once.
/// </summary>
public sealed class IndexInListScanNode : PhysicalPlanNode
{
    public TableIndexSchema Index { get; }

    /// <summary>The indexed column being tested against the value list.</summary>
    public string ColumnName { get; }

    /// <summary>Non-null, non-duplicate constant values to seek (NULLs already stripped).</summary>
    public IReadOnlyList<ColumnValue> Values { get; }

    /// <summary>Bound table source when used as a join leaf; null for single-table plans.</summary>
    public BoundTableSource? BoundSource { get; init; }

    /// <summary>Residual filter applied to fetched rows after the IN-list scan; null means no additional filter.</summary>
    public NodeAst? ExecutionFilter { get; init; }

    /// <summary>
    /// True when each value is one point lookup, false when each value is a range seek. The index
    /// type alone does not decide this: a unique index of several columns holds many entries for
    /// one leading value, and a point lookup of that partial key finds nothing.
    /// </summary>
    public bool IsPointLookup => Index.Type == IndexType.Unique && Index.Columns.Length == 1;

    public override bool CanDecomposeToLocalPlusMerge => true;

    public IndexInListScanNode(TableIndexSchema index, string columnName, IReadOnlyList<ColumnValue> values)
    {
        Index = index;
        ColumnName = columnName;
        Values = values;
    }
}
