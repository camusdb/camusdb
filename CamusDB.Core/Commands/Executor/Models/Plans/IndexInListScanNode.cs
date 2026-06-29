
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
/// For a unique index each value becomes one <c>LookupUnique</c> point lookup;
/// for a non-unique index each value becomes one equality range scan.
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

    public override bool CanDecomposeToLocalPlusMerge => true;

    public IndexInListScanNode(TableIndexSchema index, string columnName, IReadOnlyList<ColumnValue> values)
    {
        Index = index;
        ColumnName = columnName;
        Values = values;
    }
}
