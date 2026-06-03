
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

internal static class CorrelatedRowMerger
{
    public static Dictionary<string, ColumnValue> MergeForEvaluation(
        Dictionary<string, ColumnValue> innerRow,
        BoundTableSource innerSource,
        Dictionary<string, ColumnValue> outerRow,
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources)
    {
        Dictionary<string, ColumnValue> merged = new(StringComparer.Ordinal);

        foreach (KeyValuePair<string, ColumnValue> entry in innerRow)
        {
            merged[entry.Key] = entry.Value;
            merged[QueryRowNameResolver.FormatQualifiedKey(innerSource.Alias, entry.Key)] = entry.Value;
        }

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

        return merged;
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
        Dictionary<string, ColumnValue> outerRow,
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

            merged[columnName] = value;
            merged[QueryRowNameResolver.FormatQualifiedKey(alias, columnName)] = value;
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
