
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
/// Chooses an index scan from analyzed column comparisons and optional ORDER BY.
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

    /// <summary>
    /// Returns one viable step per matching index — the best usage of each individual index
    /// given the predicate. Unlike <see cref="TrySelectScan"/>, which returns only the single
    /// highest-scored step across all indexes, this returns all candidates so the caller can
    /// cost them independently and pick the cheapest rather than the highest-scored.
    ///
    /// Does not include the full-table-scan baseline (always the caller's implicit option) or
    /// ORDER BY-only scans (those remain rule-based in the planner).
    /// </summary>
    internal static IReadOnlyList<QueryPlanStep> EnumerateViableSteps(
        TableDescriptor table,
        PredicateAnalysis analysis)
    {
        Dictionary<string, List<AnalyzedComparison>> byColumn = BuildColumnMap(analysis);
        if (byColumn.Count == 0)
            return [];

        var steps = new List<QueryPlanStep>();
        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                continue;

            if (TryMatchPredicateIndex(table, index, byColumn, out QueryPlanStep step, out _))
                steps.Add(step);
        }
        return steps;
    }

    /// <summary>
    /// Re-binds the current query's predicate literals into a step for a specific pre-chosen
    /// index (identified by name), without re-running the index-scoring or cost-enumeration loop.
    /// Used on a plan-cache hit.
    /// Returns null when the index cannot be found or its predicates are no longer compatible.
    /// </summary>
    internal static QueryPlanStep? TrySelectScanForForcedIndex(
        TableDescriptor table,
        PredicateAnalysis analysis,
        string indexName)
    {
        if (!table.Indexes.TryGetValue(indexName, out TableIndexSchema? index))
            return null;

        if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
            return null;

        Dictionary<string, List<AnalyzedComparison>> byColumn = BuildColumnMap(analysis);
        if (byColumn.Count == 0)
            return null;

        return TryMatchPredicateIndex(table, index, byColumn, out QueryPlanStep step, out _) ? step : null;
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

            CompositeColumnValue? upperBound;
            bool equalityToInclusive;
            if (SupportsExactEqualityPrefixUpperBound(table, columns, equalityPrefixLength))
            {
                upperBound = BuildPrefixScanUpperBound(table, columns, equalityValues, equalityPrefixLength);
                if (upperBound is null)
                    return false;
                equalityToInclusive = false;
            }
            else if (IsStringOrIdType(table, columns[equalityPrefixLength - 1]))
            {
                // String/Id has no computable successor value. Use an inclusive [v, v] equality
                // range instead — the same approach the IN-list path uses. ScanIndex appends a
                // high sentinel for non-unique indexes so all {encode(v)+rowIdHex} entries are
                // captured by the raw scan; the decoded-key bounds filter trims to key == v.
                upperBound = lookupKey;
                equalityToInclusive = true;
            }
            else
                return false;

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                lookupKey,
                fromInclusive: true,
                upperBound,
                equalityToInclusive);
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
                if (SupportsExactEqualityPrefixUpperBound(table, columns, equalityPrefixLength))
                {
                    CompositeColumnValue? prefixUpperBound = BuildPrefixScanUpperBound(
                        table,
                        columns,
                        equalityValues,
                        equalityPrefixLength);

                    if (prefixUpperBound is null)
                        return false;

                    (toBound, toInclusive) = TightenUpperBound(toBound, toInclusive, prefixUpperBound);
                }
                else if (IsStringOrIdType(table, columns[equalityPrefixLength - 1]))
                {
                    // String/Id equality prefix: no computable successor, so cap any open side
                    // with an inclusive prefix sentinel. ScanIndex appends U+FFFF for non-unique
                    // indexes, so [prefix] inclusive bounds the scan to the prefix's rows exactly.
                    ColumnValue[] prefixVals = new ColumnValue[equalityPrefixLength];
                    Array.Copy(equalityValues, prefixVals, equalityPrefixLength);
                    CompositeColumnValue prefixSentinel = new(prefixVals);
                    if (toBound is null)
                        (toBound, toInclusive) = (prefixSentinel, true);
                    if (fromBound is null)
                        (fromBound, fromInclusive) = (prefixSentinel, true);
                    // Residual filter on the equality column guards against off-prefix rows.
                }
                else
                    return false;
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

            CompositeColumnValue? upperBound;
            bool prefixToInclusive;
            if (SupportsExactEqualityPrefixUpperBound(table, columns, equalityPrefixLength))
            {
                upperBound = BuildPrefixScanUpperBound(table, columns, equalityValues, equalityPrefixLength);
                if (upperBound is null)
                    return false;
                prefixToInclusive = false;
            }
            else if (IsStringOrIdType(table, columns[equalityPrefixLength - 1]))
            {
                // String/Id prefix: use inclusive [v, v] equality range.
                upperBound = prefixBound;
                prefixToInclusive = true;
            }
            else
                return false;

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                prefixBound,
                fromInclusive: true,
                upperBound,
                prefixToInclusive);
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

            // A unique index omits entries for rows with a NULL in any indexed column (NULLs are
            // distinct). An unbounded ordered scan over such an index would silently drop those rows,
            // so it cannot satisfy ORDER BY unless every indexed column is NOT NULL. The query falls
            // back to a full table scan plus an explicit sort. (Multi indexes always contain NULL rows.)
            if (index.Type == IndexType.Unique && !AllColumnsNotNull(table, index))
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
    /// Returns true when every column of the index is declared NOT NULL. A unique index whose columns
    /// are all NOT NULL can never omit a row, so it remains eligible for an unbounded ordered scan.
    /// </summary>
    private static bool AllColumnsNotNull(TableDescriptor table, TableIndexSchema index)
    {
        foreach (string columnName in index.Columns)
        {
            TableColumnSchema? schema = table.Schema.Columns!.Find(c => c.Name == columnName);
            if (schema is null || !schema.NotNull)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Finds a readable index whose first <c>distinctColumns.Count</c> columns are exactly the set of
    /// DISTINCT key columns. Such an index guarantees that equal rows are adjacent in the scan,
    /// enabling streaming deduplication without a hash set.
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
    /// DISTINCT key set, guaranteeing that equal rows are adjacent in the scan.
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
            // Index columns are bare; an ORDER BY column may be alias-qualified ("u.position").
            // Compare against the bare column name so aliased single-table ORDER BY can elide.
            if (!string.Equals(index.Columns[i], BareColumnName(orderBy[i].ColumnName), StringComparison.Ordinal))
                break;

            matched++;
        }

        return matched;
    }

    private static string BareColumnName(string columnName)
    {
        int dot = columnName.LastIndexOf('.');
        return dot >= 0 && dot < columnName.Length - 1 ? columnName[(dot + 1)..] : columnName;
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
                case ">=":
                {
                    // Keep the tightest (highest) lower bound. When values encode identically,
                    // exclusive (>) is tighter than inclusive (>=).
                    bool isInclusive = comparison.Operator == ">=";
                    CompositeColumnValue candidate = BoundWithPrefix(equalityPrefixValues, rangeColumnIndex, comparison.Constant);
                    if (fromBound is null)
                    {
                        fromBound = candidate;
                        fromInclusive = isInclusive;
                    }
                    else
                    {
                        int cmp = string.CompareOrdinal(KeyEncoder.Encode(candidate), KeyEncoder.Encode(fromBound));
                        if (cmp > 0 || (cmp == 0 && fromInclusive && !isInclusive))
                        {
                            fromBound = candidate;
                            fromInclusive = isInclusive;
                        }
                    }
                    hasRange = true;
                    break;
                }

                case "<":
                case "<=":
                {
                    // Keep the tightest (lowest) upper bound. When values encode identically,
                    // exclusive (<) is tighter than inclusive (<=).
                    bool isInclusive = comparison.Operator == "<=";
                    CompositeColumnValue candidate = BoundWithPrefix(equalityPrefixValues, rangeColumnIndex, comparison.Constant);
                    if (toBound is null)
                    {
                        toBound = candidate;
                        toInclusive = isInclusive;
                    }
                    else
                    {
                        int cmp = string.CompareOrdinal(KeyEncoder.Encode(candidate), KeyEncoder.Encode(toBound));
                        if (cmp < 0 || (cmp == 0 && toInclusive && !isInclusive))
                        {
                            toBound = candidate;
                            toInclusive = isInclusive;
                        }
                    }
                    hasRange = true;
                    break;
                }
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

    internal static bool IsStringOrIdType(TableDescriptor table, string columnName)
    {
        ColumnType columnType = GetColumnType(table, columnName);
        return columnType is ColumnType.String or ColumnType.Id;
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
                => ColumnValue.True,
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
