
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds the row that the predicate of a correlated EXISTS reads: the inner row, under its bare and its
/// qualified keys, and the outer columns. An inner key wins over an outer key with the same text, because
/// a name resolves to the inner query first.
/// </summary>
internal static class CorrelatedRowMerger
{
    public static Dictionary<string, ColumnValue> MergeForEvaluation(
        Dictionary<string, ColumnValue> innerRow,
        BoundTableSource innerSource,
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        Dictionary<string, ColumnValue> merged = new(StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, ColumnValue> entry in innerRow)
        {
            merged[entry.Key] = entry.Value;
            merged[QueryRowNameResolver.FormatQualifiedKey(innerSource.Alias, entry.Key)] = entry.Value;
        }

        MergeOuterRow(merged, outerRow, outerTableSources, outerDerivedSources);
        return merged;
    }

    /// <summary>
    /// The outer half of <see cref="MergeForEvaluation"/>: each outer column under its bare and its
    /// qualified key. A correlated subquery that runs through the whole pipeline reads its outer values
    /// from this row, see <see cref="CorrelatedSubqueryBody"/>.
    /// </summary>
    public static Dictionary<string, ColumnValue> MergeOuterForEvaluation(
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        Dictionary<string, ColumnValue> merged = new(StringComparer.OrdinalIgnoreCase);
        MergeOuterRow(merged, outerRow, outerTableSources, outerDerivedSources);
        return merged;
    }

    private static void MergeOuterRow(
        Dictionary<string, ColumnValue> merged,
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        bool useQualifiedOuterKeys = UsesQualifiedOuterKeys(outerTableSources, outerDerivedSources);

        foreach (BoundTableSource outerSource in outerTableSources)
            MergeOuterColumns(merged, outerRow, outerSource.Alias, GetTableColumnNames(outerSource), useQualifiedOuterKeys);

        foreach (BoundDerivedTableSource outerSource in outerDerivedSources)
        {
            List<string> columnNames = new(outerSource.Columns.Count);

            foreach (DerivedColumnSchema column in outerSource.Columns)
                columnNames.Add(column.Name);

            MergeOuterColumns(merged, outerRow, outerSource.Alias, columnNames, useQualifiedOuterKeys);
        }
    }

    private static IEnumerable<string> GetTableColumnNames(BoundTableSource outerSource)
    {
        foreach (TableColumnSchema column in outerSource.Table.Schema.Columns ?? [])
        {
            if (!SchemaElementStateRules.IsReadable(column))
                continue;

            yield return column.Name;
        }
    }

    private static void MergeOuterColumns(
        Dictionary<string, ColumnValue> merged,
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        string alias,
        IEnumerable<string> columnNames,
        bool useQualifiedOuterKeys)
    {
        foreach (string columnName in columnNames)
        {
            string lookupKey = useQualifiedOuterKeys
                ? QueryRowNameResolver.FormatQualifiedKey(alias, columnName)
                : columnName;

            if (!outerRow.TryGetValue(lookupKey, out ColumnValue? value)
                && !useQualifiedOuterKeys
                && outerRow.TryGetValue(columnName, out ColumnValue? bareValue))
            {
                value = bareValue;
            }

            if (value is null)
                continue;

            // A name resolves to the inner query first, so an outer column never replaces an inner
            // key: a bare inner column, or an inner source with the alias of an outer one.
            merged.TryAdd(columnName, value);
            merged.TryAdd(QueryRowNameResolver.FormatQualifiedKey(alias, columnName), value);
        }
    }

    private static bool UsesQualifiedOuterKeys(
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        if (outerTableSources.Count == 1 && outerDerivedSources.Count == 0)
            return false;

        if (outerTableSources.Count == 0 && outerDerivedSources.Count == 1)
            return false;

        return true;
    }
}
