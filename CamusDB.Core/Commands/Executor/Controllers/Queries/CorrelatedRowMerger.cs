
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
    /// The outer half of <see cref="MergeForEvaluation"/>: each outer column under its bare and its qualified
    /// key. <see cref="OuterRowKeysByMergedKey"/> follows the same rules.
    /// </summary>
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

            // A derived row keeps each cell under its row key, which differs from the column name for a
            // * over a join; the resolver reads a derived column through that key too.
            foreach (DerivedColumnSchema column in outerSource.Columns)
                columnNames.Add(column.RowKey);

            MergeOuterColumns(merged, outerRow, outerSource.Alias, columnNames, useQualifiedOuterKeys);
        }
    }

    /// <summary>
    /// For each key of the outer half of a merged row (<see cref="MergeOuterRow"/>), the keys of the outer row
    /// that can fill it, in the order of the merge: the first of them that the outer row holds gives the value.
    /// It follows the merge rules exactly, so a reader of these keys (<see cref="CorrelatedOuterValues"/>) gets
    /// the values that the merged row holds, and builds no merged row.
    /// </summary>
    /// <param name="rowKeysAreQualified">True when the outer row keys each cell as <c>{alias}.{column}</c>
    /// whatever the number of sources, as a row of a join does. A row of a query over one source keys its cells
    /// by the bare column name.</param>
    internal static Dictionary<string, List<string>> OuterRowKeysByMergedKey(
        IReadOnlyList<BoundTableSource> outerTableSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        bool rowKeysAreQualified)
    {
        Dictionary<string, List<string>> keys = new(StringComparer.OrdinalIgnoreCase);
        bool useQualifiedOuterKeys = rowKeysAreQualified || UsesQualifiedOuterKeys(outerTableSources, outerDerivedSources);

        foreach (BoundTableSource outerSource in outerTableSources)
            AddOuterRowKeys(keys, outerSource.Alias, GetTableColumnNames(outerSource), useQualifiedOuterKeys);

        foreach (BoundDerivedTableSource outerSource in outerDerivedSources)
        {
            List<string> columnNames = new(outerSource.Columns.Count);

            foreach (DerivedColumnSchema column in outerSource.Columns)
                columnNames.Add(column.RowKey);

            AddOuterRowKeys(keys, outerSource.Alias, columnNames, useQualifiedOuterKeys);
        }

        return keys;
    }

    private static void AddOuterRowKeys(
        Dictionary<string, List<string>> keys,
        string alias,
        IEnumerable<string> columnNames,
        bool useQualifiedOuterKeys)
    {
        foreach (string columnName in columnNames)
        {
            // MergeOuterColumns also tries the bare name when the lookup key is not qualified, but then the
            // lookup key is the bare name, so one key covers both reads.
            string lookupKey = useQualifiedOuterKeys
                ? QueryRowNameResolver.FormatQualifiedKey(alias, columnName)
                : columnName;

            AddOuterRowKey(keys, columnName, lookupKey);
            AddOuterRowKey(keys, QueryRowNameResolver.FormatQualifiedKey(alias, columnName), lookupKey);
        }
    }

    private static void AddOuterRowKey(Dictionary<string, List<string>> keys, string mergedKey, string lookupKey)
    {
        if (!keys.TryGetValue(mergedKey, out List<string>? lookupKeys))
            keys[mergedKey] = lookupKeys = new(1);

        lookupKeys.Add(lookupKey);
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
