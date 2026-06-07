
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Chooses an index scan from analyzed column comparisons and optional ORDER BY (QP2.3).
/// </summary>
internal static class IndexScanSelector
{
    private const int UniqueLookupScore = 10000;
    private const int RangeScanScore = 5000;
    private const int OrderByScanScore = 1000;

    public static QueryPlanStep? TrySelectScan(
        TableDescriptor table,
        PredicateAnalysis analysis,
        IReadOnlyList<QueryOrderBy>? orderBy = null)
    {
        Dictionary<string, List<AnalyzedComparison>> byColumn = BuildColumnMap(analysis);

        QueryPlanStep? bestStep = null;
        int bestScore = -1;
        int bestIndexColumnCount = int.MaxValue;

        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                continue;

            if (!TryMatchPredicateIndex(table, index, byColumn, out QueryPlanStep step, out int score))
                continue;

            if (score > bestScore
                || (score == bestScore && index.Columns.Length < bestIndexColumnCount))
            {
                bestStep = step;
                bestScore = score;
                bestIndexColumnCount = index.Columns.Length;
            }
        }

        if (bestStep is not null)
            return bestStep;

        return TrySelectOrderByScan(table, orderBy);
    }

    private static Dictionary<string, List<AnalyzedComparison>> BuildColumnMap(PredicateAnalysis analysis)
    {
        Dictionary<string, List<AnalyzedComparison>> byColumn = new(StringComparer.Ordinal);

        foreach (AnalyzedComparison comparison in analysis.IndexableComparisons)
        {
            if (!byColumn.TryGetValue(comparison.ColumnName, out List<AnalyzedComparison>? list))
                byColumn[comparison.ColumnName] = list = new();

            list.Add(comparison);
        }

        return byColumn;
    }

    private static bool TryMatchPredicateIndex(
        TableDescriptor table,
        TableIndexSchema index,
        Dictionary<string, List<AnalyzedComparison>> byColumn,
        out QueryPlanStep step,
        out int score)
    {
        step = default;
        score = -1;

        if (byColumn.Count == 0)
            return false;

        string[] columns = index.Columns;
        ColumnValue[] equalityValues = new ColumnValue[columns.Length];
        int equalityPrefixLength = 0;

        for (int i = 0; i < columns.Length; i++)
        {
            if (!TryGetEquality(byColumn, columns[i], out ColumnValue? equalityValue))
                break;

            equalityValues[i] = equalityValue!;
            equalityPrefixLength++;
        }

        if (equalityPrefixLength == columns.Length)
        {
            CompositeColumnValue lookupKey = new(equalityValues);

            if (index.Type == IndexType.Unique)
            {
                step = new QueryPlanStep(QueryPlanStepType.QueryFromIndex, index, lookupKey);
                score = UniqueLookupScore + columns.Length;
                return true;
            }

            if (!SupportsExactEqualityPrefixUpperBound(table, columns, equalityPrefixLength))
                return false;

            CompositeColumnValue? upperBound = BuildPrefixScanUpperBound(table, columns, equalityValues, equalityPrefixLength);

            if (upperBound is null)
                return false;

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                lookupKey,
                fromInclusive: true,
                upperBound,
                toInclusive: false);
            score = RangeScanScore + columns.Length * 10;
            return true;
        }

        if (equalityPrefixLength < columns.Length
            && byColumn.TryGetValue(columns[equalityPrefixLength], out List<AnalyzedComparison>? rangeComparisons)
            && TryBuildRangeBounds(
                equalityValues,
                equalityPrefixLength,
                rangeComparisons,
                out CompositeColumnValue? fromBound,
                out bool fromInclusive,
                out CompositeColumnValue? toBound,
                out bool toInclusive))
        {
            if (equalityPrefixLength > 0)
            {
                if (!SupportsExactEqualityPrefixUpperBound(table, columns, equalityPrefixLength))
                    return false;

                CompositeColumnValue? prefixUpperBound = BuildPrefixScanUpperBound(
                    table,
                    columns,
                    equalityValues,
                    equalityPrefixLength);

                if (prefixUpperBound is null)
                    return false;

                (toBound, toInclusive) = TightenUpperBound(toBound, toInclusive, prefixUpperBound);
            }

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                fromBound,
                fromInclusive,
                toBound,
                toInclusive);
            score = RangeScanScore + equalityPrefixLength * 10 + 1;
            return true;
        }

        if (equalityPrefixLength > 0)
        {
            ColumnValue[] prefixValues = new ColumnValue[equalityPrefixLength];
            Array.Copy(equalityValues, prefixValues, equalityPrefixLength);
            CompositeColumnValue prefixBound = new(prefixValues);
            if (!SupportsExactEqualityPrefixUpperBound(table, columns, equalityPrefixLength))
                return false;

            CompositeColumnValue? upperBound = BuildPrefixScanUpperBound(table, columns, equalityValues, equalityPrefixLength);

            if (upperBound is null)
                return false;

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                prefixBound,
                fromInclusive: true,
                upperBound,
                toInclusive: false);
            score = RangeScanScore + equalityPrefixLength;
            return true;
        }

        return false;
    }

    private static QueryPlanStep? TrySelectOrderByScan(
        TableDescriptor table,
        IReadOnlyList<QueryOrderBy>? orderBy)
    {
        if (orderBy is null || orderBy.Count == 0)
            return null;

        if (!IsAscendingOrderBy(orderBy))
            return null;

        TableIndexSchema? bestIndex = null;
        int bestMatchLength = 0;
        int bestColumnCount = int.MaxValue;

        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                continue;

            int matchLength = MatchOrderByPrefixLength(index, orderBy);
            if (matchLength == 0)
                continue;

            if (matchLength < bestMatchLength)
                continue;

            if (matchLength == bestMatchLength && index.Columns.Length >= bestColumnCount)
                continue;

            bestIndex = index;
            bestMatchLength = matchLength;
            bestColumnCount = index.Columns.Length;
        }

        if (bestIndex is null)
            return null;

        return new QueryPlanStep(
            QueryPlanStepType.RangeScanFromIndex,
            bestIndex,
            fromBound: null,
            fromInclusive: true,
            toBound: null,
            toInclusive: true);
    }

    /// <summary>
    /// Finds a readable index whose first <c>distinctColumns.Count</c> columns are exactly the set of
    /// DISTINCT key columns. Such an index guarantees that equal rows are adjacent in the scan,
    /// enabling streaming deduplication without a hash set (R12).
    /// Returns the first matching index, or null if none qualifies.
    /// </summary>
    internal static TableIndexSchema? TryFindStreamingDistinctIndex(
        TableDescriptor table,
        IReadOnlyList<string> distinctColumns)
    {
        if (distinctColumns.Count == 0)
            return null;

        HashSet<string> distinctSet = new(distinctColumns, StringComparer.Ordinal);

        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                continue;

            if (index.Columns.Length < distinctColumns.Count)
                continue;

            // The first distinctColumns.Count index columns must form exactly the distinct key set.
            HashSet<string> prefix = new(StringComparer.Ordinal);
            for (int i = 0; i < distinctColumns.Count; i++)
                prefix.Add(index.Columns[i]);

            if (prefix.SetEquals(distinctSet))
                return index;
        }

        return null;
    }

    /// <summary>
    /// Returns true when the scan step uses an index whose first N columns cover exactly the
    /// DISTINCT key set, guaranteeing that equal rows are adjacent in the scan (R12).
    /// </summary>
    internal static bool ScanStepCoversDistinctColumns(
        QueryPlanStep scanStep,
        IReadOnlyList<string> distinctColumns)
    {
        if (scanStep.Index is null || distinctColumns.Count == 0)
            return false;

        if (scanStep.Index.Columns.Length < distinctColumns.Count)
            return false;

        HashSet<string> distinctSet = new(distinctColumns, StringComparer.Ordinal);
        HashSet<string> prefix = new(StringComparer.Ordinal);
        for (int i = 0; i < distinctColumns.Count; i++)
            prefix.Add(scanStep.Index.Columns[i]);

        return prefix.SetEquals(distinctSet);
    }

    internal static bool ScanSatisfiesOrderBy(
        TableDescriptor table,
        QueryPlanStep scanStep,
        IReadOnlyList<QueryOrderBy>? orderBy)
    {
        if (orderBy is null || orderBy.Count == 0)
            return true;

        if (!IsAscendingOrderBy(orderBy))
            return false;

        return scanStep.Type switch
        {
            QueryPlanStepType.QueryFromIndex => true,
            QueryPlanStepType.RangeScanFromIndex => scanStep.Index is not null
                && MatchOrderByPrefixLength(scanStep.Index, orderBy) >= orderBy.Count,
            QueryPlanStepType.FullScanFromIndex => scanStep.Index is not null
                && MatchOrderByPrefixLength(scanStep.Index, orderBy) >= orderBy.Count,
            _ => false
        };
    }

    private static bool IsAscendingOrderBy(IReadOnlyList<QueryOrderBy> orderBy)
    {
        for (int i = 0; i < orderBy.Count; i++)
        {
            if (orderBy[i].Type != OrderType.Ascending)
                return false;
        }

        return true;
    }

    private static int MatchOrderByPrefixLength(TableIndexSchema index, IReadOnlyList<QueryOrderBy> orderBy)
    {
        int max = Math.Min(index.Columns.Length, orderBy.Count);
        int matched = 0;

        for (int i = 0; i < max; i++)
        {
            if (!string.Equals(index.Columns[i], orderBy[i].ColumnName, StringComparison.Ordinal))
                break;

            matched++;
        }

        return matched;
    }

    private static bool TryGetEquality(
        Dictionary<string, List<AnalyzedComparison>> byColumn,
        string columnName,
        out ColumnValue? value)
    {
        value = null;

        if (!byColumn.TryGetValue(columnName, out List<AnalyzedComparison>? comparisons))
            return false;

        foreach (AnalyzedComparison comparison in comparisons)
        {
            if (comparison.Operator == "=")
            {
                value = comparison.Constant;
                return true;
            }
        }

        return false;
    }

    private static bool TryBuildRangeBounds(
        ColumnValue[] equalityPrefixValues,
        int rangeColumnIndex,
        List<AnalyzedComparison> comparisons,
        out CompositeColumnValue? fromBound,
        out bool fromInclusive,
        out CompositeColumnValue? toBound,
        out bool toInclusive)
    {
        fromBound = null;
        toBound = null;
        fromInclusive = true;
        toInclusive = true;
        bool hasRange = false;

        foreach (AnalyzedComparison comparison in comparisons)
        {
            if (comparison.Operator == "=")
                continue;

            switch (comparison.Operator)
            {
                case ">":
                    fromBound = BoundWithPrefix(equalityPrefixValues, rangeColumnIndex, comparison.Constant);
                    fromInclusive = false;
                    hasRange = true;
                    break;
                case ">=":
                    fromBound = BoundWithPrefix(equalityPrefixValues, rangeColumnIndex, comparison.Constant);
                    fromInclusive = true;
                    hasRange = true;
                    break;
                case "<":
                    toBound = BoundWithPrefix(equalityPrefixValues, rangeColumnIndex, comparison.Constant);
                    toInclusive = false;
                    hasRange = true;
                    break;
                case "<=":
                    toBound = BoundWithPrefix(equalityPrefixValues, rangeColumnIndex, comparison.Constant);
                    toInclusive = true;
                    hasRange = true;
                    break;
            }
        }

        return hasRange;
    }

    private static CompositeColumnValue BoundWithPrefix(
        ColumnValue[] equalityPrefixValues,
        int rangeColumnIndex,
        ColumnValue boundValue)
    {
        ColumnValue[] values = new ColumnValue[rangeColumnIndex + 1];

        for (int i = 0; i < rangeColumnIndex; i++)
            values[i] = equalityPrefixValues[i];

        values[rangeColumnIndex] = boundValue;
        return new CompositeColumnValue(values);
    }

    /// <summary>
    /// When a trailing range follows an equality prefix, cap the scan so later prefix values
    /// (e.g. year = 2001 after year = 2000) cannot leak through.
    /// </summary>
    private static (CompositeColumnValue? toBound, bool toInclusive) TightenUpperBound(
        CompositeColumnValue? rangeToBound,
        bool rangeToInclusive,
        CompositeColumnValue? prefixUpperBound)
    {
        const bool prefixUpperInclusive = false;

        if (prefixUpperBound is null)
            return (rangeToBound, rangeToInclusive);

        if (rangeToBound is null)
            return (prefixUpperBound, prefixUpperInclusive);

        string prefixEncoded = KeyEncoder.Encode(prefixUpperBound);
        string rangeEncoded = KeyEncoder.Encode(rangeToBound);
        int cmp = string.CompareOrdinal(rangeEncoded, prefixEncoded);

        if (cmp < 0)
            return (rangeToBound, rangeToInclusive);

        if (cmp > 0)
            return (prefixUpperBound, prefixUpperInclusive);

        return (prefixUpperBound, rangeToInclusive && prefixUpperInclusive);
    }

    private static CompositeColumnValue? BuildPrefixScanUpperBound(
        TableDescriptor table,
        string[] indexColumns,
        ColumnValue[] equalityValues,
        int prefixLength)
    {
        if (prefixLength <= 0)
            return null;

        ColumnValue[] upperValues = new ColumnValue[prefixLength];
        Array.Copy(equalityValues, upperValues, prefixLength - 1);

        string lastColumn = indexColumns[prefixLength - 1];
        ColumnType columnType = GetColumnType(table, lastColumn);
        ColumnValue? nextValue = NextSortValue(columnType, equalityValues[prefixLength - 1]);

        if (nextValue is null)
            return null;

        upperValues[prefixLength - 1] = nextValue;
        return new CompositeColumnValue(upperValues);
    }

    private static ColumnType GetColumnType(TableDescriptor table, string columnName)
    {
        TableColumnSchema? column = table.Schema.Columns?.Find(c => c.Name == columnName);
        return column?.Type ?? ColumnType.String;
    }

    internal static bool SupportsExactEqualityPrefixUpperBound(
        TableDescriptor table,
        string[] indexColumns,
        int prefixLength)
    {
        if (prefixLength <= 0)
            return false;

        ColumnType columnType = GetColumnType(table, indexColumns[prefixLength - 1]);
        return columnType is not (ColumnType.String or ColumnType.Id);
    }

    internal static ColumnValue? NextSortValue(ColumnType columnType, ColumnValue value)
    {
        return columnType switch
        {
            ColumnType.Integer64 when value.LongValue < long.MaxValue
                => new ColumnValue(ColumnType.Integer64, value.LongValue + 1),
            ColumnType.Float64 when TryNextFloat64(value.FloatValue, out ColumnValue? nextFloat)
                => nextFloat,
            ColumnType.Bool when !value.BoolValue
                => new ColumnValue(ColumnType.Bool, true),
            _ => null,
        };
    }

    private static bool TryNextFloat64(double value, out ColumnValue? next)
    {
        next = null;

        if (double.IsNaN(value) || double.IsPositiveInfinity(value))
            return false;

        double candidate = Math.BitIncrement(value);

        if (double.IsNaN(candidate) || candidate <= value)
            return false;

        next = new ColumnValue(ColumnType.Float64, candidate);
        return true;
    }
}
