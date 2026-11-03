/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Answers the per-alias questions a join leaf asks before it reads a row: which columns this
/// alias must decode, which columns a row fetched through an index carries, which schema version
/// its rows were written under, what the key columns of an index decode to, and which alias a
/// left-side row belongs to.
///
/// <para>The plan carries per-alias answers only for the aliases the binder resolved; each
/// lookup falls back to the plan-wide value, so a leaf never has to know whether its alias was
/// listed.</para>
/// </summary>
internal static class JoinAliasMetadata
{
    internal static IReadOnlySet<string>? GetRequiredColumnsForAlias(QueryPlan plan, string alias)
    {
        if (plan.RequiredColumnsByAlias?.TryGetValue(alias, out IReadOnlySet<string>? required) == true)
            return required;

        return plan.ScanRequiredColumns;
    }

    /// <summary>
    /// The columns a row of <paramref name="alias"/> carries once a leaf decoded it: the alias's
    /// required set, plus the key columns of <paramref name="index"/> when the row was fetched through
    /// an index entry, because the entry check (<see cref="IndexEntryCheck"/>) compares those columns
    /// with the row. This is the one rule for a right row's key set. The page that decodes a right row
    /// (<see cref="JoinLeafRowPage"/>) and the padding of a left outer join, which builds the right half
    /// of its layout from metadata before any right row exists (<see cref="OuterJoinPadding"/>), must
    /// agree key for key; a key the padding did not list made the merge of a real right row throw
    /// "shape diverged" whenever the index had a key column the query did not read.
    /// </summary>
    internal static IReadOnlySet<string>? GetDecodedColumnsForAlias(QueryPlan plan, string alias, TableIndexSchema? index)
    {
        IReadOnlySet<string>? required = GetRequiredColumnsForAlias(plan, alias);
        return index is null ? required : IndexEntryCheck.RequiredColumns(required, index);
    }

    /// <summary>
    /// The index a right-side leaf fetches its rows through, or null for a primary-row scan, a derived
    /// table or no access path of its own: an index-ordered scan (<see cref="TableScanSource.ForcedIndex"/>),
    /// an index range leaf or an IN-list leaf, each possibly under the sort a merge join adds. A row
    /// read through an index decodes the index's key columns as well (<see cref="GetDecodedColumnsForAlias"/>).
    /// </summary>
    internal static TableIndexSchema? IndexOfRightLeaf(PhysicalPlanNode? node)
    {
        while (node is not null)
        {
            switch (node)
            {
                case TableScanNode { Source: TableScanSource.ForcedIndex, Index: { } forced }:
                    return forced;

                case IndexRangeScanNode range:
                    return range.Index;

                case IndexInListScanNode inList:
                    return inList.Index;

                case SortNode sort:
                    node = sort.Input;
                    continue;

                default:
                    return null;
            }
        }

        return null;
    }

    internal static int GetTableSchemaVersionForAlias(QueryPlan plan, string alias)
    {
        return plan.TableSchemaVersionByAlias.TryGetValue(alias, out int version)
            ? version
            : plan.TableSchemaVersion;
    }

    internal static ColumnType[] GetIndexColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
        {
            string colName = index.Columns[i];
            TableColumnSchema? col = table.Schema.Columns?.Find(c => string.Equals(c.Name, colName, StringComparison.OrdinalIgnoreCase));
            types[i] = col?.Type ?? ColumnType.String;
        }

        return types;
    }

    internal static string ResolveLeftAlias(PhysicalPlanNode leftNode, QueryResultRow leftRow)
    {
        switch (leftNode)
        {
            case TableScanNode { BoundSource: not null } scanNode:
                return scanNode.BoundSource.Alias;

            case IndexRangeScanNode { BoundSource: not null } rangeScanNode:
                return rangeScanNode.BoundSource.Alias;

            case IndexInListScanNode { BoundSource: not null } inListScanNode:
                return inListScanNode.BoundSource.Alias;

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
                return derivedScanNode.BoundSource.Alias;

            // Left side may be wrapped in a SortNode; delegate to its inner scan.
            case SortNode { Input: not null } sortNode:
                return ResolveLeftAlias(sortNode.Input!, leftRow);

            default:
                break;
        }

        foreach (KeyValuePair<string, ColumnValue> entry in leftRow.Row)
        {
            if (!QueryRowMerger.IsQualifiedKey(entry.Key))
                continue;

            int dotIndex = entry.Key.IndexOf('.');
            return entry.Key[..dotIndex];
        }

        // A nested join whose columns the query never reads yields rows with no cells: projection
        // pushdown keeps none of them. Such a row has no key to qualify, so any alias is correct. Without
        // this case `a CROSS JOIN b CROSS JOIN c` that reads only c's columns failed here.
        if (leftRow.Row.Count == 0)
            return string.Empty;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Could not resolve alias for nested join left row");
    }
}
