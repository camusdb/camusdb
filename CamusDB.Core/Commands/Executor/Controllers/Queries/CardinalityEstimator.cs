
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;


namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Per-predicate selectivity estimator using equi-depth histograms and NDV counts from
/// StatisticsManager. Replaces the fixed FilterSelectivity = 0.10 constant in CostEstimator
/// when stats are available.
///
/// Selectivity strategy:
///   1. Run <see cref="PredicateAnalyzer.Analyze"/> to get indexable comparisons and IN-list predicates.
///   2. For each comparison, estimate selectivity using NDV or histogram bucket data.
///   3. Multiply per-column selectivities (column independence assumption).
///   4. Fall back to <see cref="FallbackSelectivity"/> (0.10) when no stats are available.
///
/// NDV takes priority over histogram for equality predicates: NDV gives a tighter 1/NDV estimate
/// without needing a histogram bucket lookup. For range predicates the histogram is always used.
/// </summary>
internal static class CardinalityEstimator
{
    internal const double FallbackSelectivity = 0.10;

    /// <summary>
    /// Estimates the overall filter selectivity of <paramref name="predicate"/> using histogram
    /// and NDV statistics from <paramref name="stats"/>. Falls back to
    /// <see cref="FallbackSelectivity"/> when no stats are available for any column.
    /// </summary>
    public static double EstimateFilterSelectivity(
        NodeAst predicate,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
    {
        // parameters: null — parameterized predicates (WHERE year > @p) leave the constant
        // unresolved; TryGetBound returns null and range operators fall back to FallbackSelectivity.
        // Acceptable for cost estimation: over-estimated selectivity is conservative (safe).
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(predicate, parameters: null);

        if (analysis.IndexableComparisons.Count == 0 && analysis.InListComparisons.Count == 0)
            return FallbackSelectivity;

        return EstimatePredicateSelectivity(
            analysis.IndexableComparisons,
            analysis.InListComparisons,
            database, table, stats);
    }

    /// <summary>
    /// Estimates selectivity for a pre-analyzed set of comparisons and IN-list predicates.
    ///
    /// For equality comparisons whose columns form a prefix of a composite index, uses
    /// <c>KeyNdv</c> for that tuple instead of the column-independence product — corrects
    /// the common over-selectivity from correlated columns (e.g. city+zip, tenant+user).
    /// For unindexed correlated columns, column independence is kept (see <see cref="TryApplyCompositeKeyNdv"/>).
    /// </summary>
    public static double EstimatePredicateSelectivity(
        IReadOnlyList<AnalyzedComparison> comparisons,
        IReadOnlyList<AnalyzedInList> inLists,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
    {
        double sel = 1.0;

        // Try composite-key NDV for correlated equality columns before independence loop.
        HashSet<string>? compositeHandled = TryApplyCompositeKeyNdv(
            comparisons, database, table, stats, ref sel);

        // Group same-column range comparisons to avoid double-counting. A two-sided range
        // "x > lo AND x < hi" is two AnalyzedComparisons on the same column; multiplying
        // their individual selectivities treats them as independent events and under-estimates
        // the intersection. Call EstimateRangeFraction(lo, hi) once instead.
        //
        // Only pairs of a lower bound (>/>=) and an upper bound (</<=) are merged here;
        // single one-sided bounds, equalities, and ≠ comparisons remain in the independence loop.
        HashSet<string>? rangePairHandled = null;

        // Bucket comparisons by column → tightest lower bound and tightest upper bound.
        // "x > 5 AND x > 10" must keep > 10 (the larger lower bound); "x < 20 AND x <= 15"
        // must keep <= 15 (the smaller upper bound). On an equal value the exclusive form is
        // tighter than the inclusive one. Keeping merely the FIRST bound per side would
        // silently widen the range and overestimate cardinality.
        Dictionary<string, (ScalarBound? lo, bool loInc, ScalarBound? hi, bool hiInc)>? rangePairs = null;
        foreach (AnalyzedComparison c in comparisons)
        {
            bool isLo = c.Operator is ">" or ">=";
            bool isHi = c.Operator is "<" or "<=";
            if (!isLo && !isHi) continue;

            // Skip columns already handled by composite-KeyNdv (they are equalities, so
            // this branch is unreachable for them, but guard anyway for clarity).
            if (compositeHandled?.Contains(c.ColumnName) == true) continue;

            rangePairs ??= new(StringComparer.OrdinalIgnoreCase);
            if (!rangePairs.TryGetValue(c.ColumnName, out var pair))
                pair = (null, false, null, false);

            ScalarBound? bound = TryGetBound(c.Constant);
            if (bound is null)
            {
                rangePairs[c.ColumnName] = pair;
                continue;
            }

            bool inclusive = c.Operator is ">=" or "<=";

            if (isLo)
            {
                int cmp = pair.lo is null ? 1 : bound.CompareTo(pair.lo);
                if (cmp > 0 || (cmp == 0 && !inclusive))
                    pair = (bound, inclusive, pair.hi, pair.hiInc);
            }
            else
            {
                int cmp = pair.hi is null ? -1 : bound.CompareTo(pair.hi);
                if (cmp < 0 || (cmp == 0 && !inclusive))
                    pair = (pair.lo, pair.loInc, bound, inclusive);
            }

            rangePairs[c.ColumnName] = pair;
        }

        if (rangePairs is not null)
        {
            foreach (KeyValuePair<string, (ScalarBound? lo, bool loInc, ScalarBound? hi, bool hiInc)> kv in rangePairs)
            {
                // Only merge when BOTH bounds are present — that is the two-sided case.
                // A single one-sided bound is left for the independence loop below.
                if (kv.Value.lo is null || kv.Value.hi is null) continue;

                sel *= EstimateRangeFraction(
                    kv.Value.lo, kv.Value.loInc, kv.Value.hi, kv.Value.hiInc,
                    kv.Key, database, table, stats);
                rangePairHandled ??= new(StringComparer.OrdinalIgnoreCase);
                rangePairHandled.Add(kv.Key);
            }
        }

        foreach (AnalyzedComparison c in comparisons)
        {
            // Skip equality comparisons already priced by the composite KeyNdv path.
            if (c.Operator == "=" && compositeHandled?.Contains(c.ColumnName) == true)
                continue;
            // Skip range comparisons already merged into a two-sided range fraction above.
            if ((c.Operator is ">" or ">=" or "<" or "<=") && rangePairHandled?.Contains(c.ColumnName) == true)
                continue;
            sel *= SingleComparisonSelectivity(c.Operator, c.Constant, c.ColumnName, database, table, stats);
        }

        foreach (AnalyzedInList inList in inLists)
            sel *= InListSelectivity(inList, database, table, stats);

        return Clamp(sel);
    }

    /// <summary>
    /// Finds the longest composite-index prefix that is fully covered by equality comparisons in
    /// <paramref name="comparisons"/>, looks up <c>KeyNdv</c> for that prefix, and multiplies
    /// its selectivity (1 / <c>KeyNdv</c>) into <paramref name="sel"/>.
    ///
    /// Returns the set of column names already priced (so the caller's independence loop can skip
    /// them), or null when no qualifying composite key path was found.
    ///
    /// Known limitation: for correlated columns that are NOT covered by a composite index the
    /// independence assumption is kept, which may over-estimate selectivity.
    /// </summary>
    private static HashSet<string>? TryApplyCompositeKeyNdv(
        IReadOnlyList<AnalyzedComparison> comparisons,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats,
        ref double sel)
    {
        if (table.Indexes is null) return null;

        // Collect columns that appear as equality predicates — these are the candidates.
        var equalityCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (AnalyzedComparison c in comparisons)
            if (c.Operator == "=") equalityCols.Add(c.ColumnName);

        if (equalityCols.Count < 2) return null; // single-column conjuncts get no benefit

        // Find the composite index whose prefix is longest and fully covered by equality cols.
        int bestPrefixLen = 1; // must reach ≥ 2 to apply
        string[]? bestIndexCols = null;

        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema idx = kv.Value;
            if (!SchemaElementStateRules.IsReadable(idx)) continue;
            if (idx.Columns is not { Length: >= 2 }) continue; // skip single-column indexes

            int prefixLen = 0;
            foreach (string col in idx.Columns)
            {
                if (!equalityCols.Contains(col)) break;
                prefixLen++;
            }

            if (prefixLen >= 2 && prefixLen > bestPrefixLen)
            {
                bestPrefixLen = prefixLen;
                bestIndexCols = idx.Columns;
            }
        }

        if (bestIndexCols is null) return null;

        string[] prefixCols = bestIndexCols[..bestPrefixLen];
        long? keyNdv = stats.GetKeyNdv(database, table, prefixCols);
        if (keyNdv is not > 0) return null;

        sel *= Math.Min(1.0, 1.0 / keyNdv.Value);
        return new HashSet<string>(prefixCols, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Estimates the fraction of rows satisfying <c>loBound &lt; col &lt;= hiBound</c> using
    /// the equi-depth histogram for <paramref name="columnName"/>. Either bound may be null
    /// (open range). Returns <see cref="FallbackSelectivity"/> when no histogram is available.
    /// </summary>
    public static double EstimateRangeFraction(
        ScalarBound? loBound,
        ScalarBound? hiBound,
        string columnName,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
        => EstimateRangeFraction(loBound, loInclusive: false, hiBound, hiInclusive: true,
            columnName, database, table, stats);

    /// <summary>
    /// Inclusivity-aware variant: an inclusive lower bound keeps the equality mass at
    /// <paramref name="loBound"/> and an exclusive upper bound drops the mass at
    /// <paramref name="hiBound"/> (see <see cref="ColumnHistogram.RangeFraction(ScalarBound?, bool, ScalarBound?, bool)"/>).
    /// </summary>
    public static double EstimateRangeFraction(
        ScalarBound? loBound,
        bool loInclusive,
        ScalarBound? hiBound,
        bool hiInclusive,
        string columnName,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
    {
        columnName = StripAliasPrefix(columnName);
        ColumnHistogram? hist = stats.GetColumnHistogram(database, table, columnName);
        if (hist is null || hist.TotalRows == 0)
            return FallbackSelectivity;

        return Clamp(hist.RangeFraction(loBound, loInclusive, hiBound, hiInclusive));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Join cardinality estimation
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Estimates the output row count of an equi-join using NDV statistics.
    ///   |A ⋈ B| ≈ |A| × |B| / max(NDV_A.key, NDV_B.key)
    ///
    /// FK-aware: when the join key is unique on one side that side contributes ≤1 match per row
    /// on the other side → cardinality ≈ the other side's row count.  Uniqueness is detected
    /// from the table's index descriptors (<see cref="IndexType.Unique"/>; covers both PK and UNIQUE).
    /// Falls back to |A| × |B| × <see cref="FallbackSelectivity"/> when no stats are available.
    /// </summary>
    /// <param name="leftRows">Estimated row count of the left (probe) input.</param>
    /// <param name="rightRows">Estimated row count of the right (build/inner) input.</param>
    /// <param name="rightKeyColumns">Unqualified join-key column names on the right side.</param>
    /// <param name="rightTable">Right table descriptor (may be null for derived sources).</param>
    /// <param name="leftKeyColumns">Unqualified join-key column names on the left side.</param>
    /// <param name="leftTable">Left table descriptor (may be null for complex left subtrees).</param>
    /// <param name="database">Database descriptor used for stats cache key.</param>
    /// <param name="stats">Statistics manager; may be null when no stats are loaded.</param>
    public static long EstimateJoinCardinality(
        long leftRows,
        long rightRows,
        IReadOnlyList<string> rightKeyColumns,
        TableDescriptor? rightTable,
        IReadOnlyList<string> leftKeyColumns,
        TableDescriptor? leftTable,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        // FK-aware: unique key on the right → at most 1 right match per left row.
        if (rightTable is not null && rightKeyColumns.Count > 0
            && IsJoinKeyUnique(rightTable, rightKeyColumns))
            return Math.Max(1, leftRows);

        // FK-aware: unique key on the left → at most 1 left match per right row.
        if (leftTable is not null && leftKeyColumns.Count > 0
            && IsJoinKeyUnique(leftTable, leftKeyColumns))
            return Math.Max(1, rightRows);

        // NDV-based: |A ⋈ B| ≈ |A| × |B| / max(NDV_A, NDV_B).
        if (stats is not null)
        {
            long? ndvRight = GetJoinKeyNdv(database, rightTable, rightKeyColumns, stats);
            long? ndvLeft  = GetJoinKeyNdv(database, leftTable, leftKeyColumns, stats);
            long maxNdv = Math.Max(ndvLeft ?? 0L, ndvRight ?? 0L);
            if (maxNdv > 0)
                return Math.Max(1, (long)((double)leftRows * rightRows / maxNdv));
        }

        // Fallback: fixed selectivity.
        return Math.Max(1, (long)((double)leftRows * rightRows * FallbackSelectivity));
    }

    /// <summary>
    /// Returns true when <paramref name="keyColumns"/> constrain <em>every</em> column of some
    /// public unique index (or the PK, stored as <see cref="IndexType.Unique"/>) on
    /// <paramref name="table"/> — i.e. the index's columns are a subset of the join key.
    ///
    /// Uniqueness guarantees ≤1 match per row only when the join binds the whole unique key; a
    /// join on a proper <em>prefix</em> of a composite unique index (e.g. <c>(tenant_id)</c> of a
    /// <c>UNIQUE(tenant_id, email)</c>) is NOT unique and must fall through to the NDV estimate.
    /// Column order is irrelevant to uniqueness, so set-containment is the correct test.
    /// </summary>
    private static bool IsJoinKeyUnique(TableDescriptor table, IReadOnlyList<string> keyColumns)
    {
        if (table.Indexes is null || keyColumns.Count == 0) return false;

        var keySet = new HashSet<string>(keyColumns, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            TableIndexSchema idx = kv.Value;
            if (idx.Type != IndexType.Unique) continue;
            if (!SchemaElementStateRules.IsReadable(idx)) continue;
            if (idx.Columns is { Length: > 0 } && Array.TrueForAll(idx.Columns, keySet.Contains))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Looks up NDV for <paramref name="keyColumns"/> on <paramref name="table"/>.
    /// For multi-column keys: tries composite KeyNdv first, falls back to min of per-column NDVs.
    /// Returns null when stats are unavailable.
    /// </summary>
    private static long? GetJoinKeyNdv(
        DatabaseDescriptor database,
        TableDescriptor? table,
        IReadOnlyList<string> keyColumns,
        StatisticsManager stats)
    {
        if (table is null || keyColumns.Count == 0) return null;

        if (keyColumns.Count == 1)
            return stats.GetColumnNdv(database, table, keyColumns[0]);

        // Multi-column key: composite NDV preferred; fall back to min of individual NDVs.
        long? keyNdv = stats.GetKeyNdv(database, table, keyColumns);
        if (keyNdv is not null) return keyNdv;

        long? minNdv = null;
        foreach (string col in keyColumns)
        {
            long? ndv = stats.GetColumnNdv(database, table, col);
            if (ndv is not null)
                minNdv = minNdv is null ? ndv : Math.Min(minNdv.Value, ndv.Value);
        }
        return minNdv;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Estimates output rows for an IN-list index scan.
    /// <list type="bullet">
    ///   <item>Unique index: at most one match per value → <c>min(n, trc)</c>.</item>
    ///   <item>Non-unique with stats: per-value equality selectivity summed across the list
    ///     (via histogram / NDV) × trc.</item>
    ///   <item>Non-unique without stats: conservative heuristic
    ///     (<see cref="FallbackSelectivity"/> per value × trc).</item>
    /// </list>
    /// Always returns at least 1 for a non-empty list.
    /// </summary>
    internal static long EstimateInListRows(
        string columnName,
        IReadOnlyList<ColumnValue> values,
        bool isUnique,
        long trc,
        DatabaseDescriptor database,
        TableDescriptor? table,
        StatisticsManager? stats)
    {
        int n = values.Count;
        if (n == 0) return 0L;

        if (isUnique)
            return Math.Min(n, trc);

        if (stats is not null && table is not null)
        {
            string colKey = StripAliasPrefix(columnName);
            ColumnHistogram? hist = stats.GetColumnHistogram(database, table, colKey);
            long? ndv = stats.GetColumnNdv(database, table, colKey);

            double total = 0.0;
            foreach (ColumnValue val in values)
                total += EqualitySelectivity(TryGetBound(val), hist, ndv);

            return Math.Max(1L, (long)(trc * Math.Min(1.0, total)));
        }

        // Conservative no-stats heuristic: FallbackSelectivity per value.
        double heuristicSel = Math.Min(1.0, n * FallbackSelectivity);
        return Math.Max(1L, (long)(trc * heuristicSel));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static double SingleComparisonSelectivity(
        string op,
        ColumnValue constant,
        string columnName,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
    {
        columnName = StripAliasPrefix(columnName);
        ColumnHistogram? hist = stats.GetColumnHistogram(database, table, columnName);
        long? ndv = stats.GetColumnNdv(database, table, columnName);
        ScalarBound? bound = TryGetBound(constant);

        switch (op)
        {
            case "=":
                return EqualitySelectivity(bound, hist, ndv);

            case "!=":
                return Clamp(1.0 - EqualitySelectivity(bound, hist, ndv));

            case ">":
            case ">=":
                // ">=" carries the equality mass at the bound; on a skewed low-NDV column
                // that mass can be most of the table, so dropping it flips scan decisions.
                if (bound is not null && hist is not null && hist.TotalRows > 0)
                    return Clamp(hist.RangeFraction(bound, loInclusive: op == ">=", null, hiInclusive: true));
                return FallbackSelectivity;

            case "<":
            case "<=":
                // "<" must exclude the equality mass at the bound (cumulative is ≤-based).
                if (bound is not null && hist is not null && hist.TotalRows > 0)
                    return Clamp(hist.RangeFraction(null, loInclusive: false, bound, hiInclusive: op == "<="));
                return FallbackSelectivity;

            default:
                return FallbackSelectivity;
        }
    }

    /// <summary>
    /// Estimates selectivity for <c>col = constant</c>.
    /// NDV takes priority; falls back to bucket-level estimation; ultimate fallback is
    /// <see cref="FallbackSelectivity"/>.
    /// </summary>
    private static double EqualitySelectivity(ScalarBound? bound, ColumnHistogram? hist, long? ndv)
    {
        // NULL bound means the constant is SQL NULL.  col = NULL is UNKNOWN in 3VL → 0 rows.
        // Guard this before the NDV branch so we don't return 1/NDV for a NULL constant.
        // (PredicateAnalyzer currently emits IS NULL rather than = NULL, so this is latent.)
        if (bound is null)
            return 0.0;

        // 1. NDV: 1/NDV is the most direct equality estimate when available.
        if (ndv is > 0)
            return Math.Min(1.0, 1.0 / ndv.Value);

        // 2. Histogram bucket: rows-in-bucket / totalRows / distinct-in-bucket.
        if (bound is not null && hist is not null && hist.TotalRows > 0)
        {
            // Out-of-domain: equality on a value above every bucket (or below the observed
            // minimum) matches ~nothing — estimate one row rather than last-bucket density.
            if (IsOutsideHistogramDomain(hist, bound))
                return Math.Min(1.0, 1.0 / hist.TotalRows);

            int bi = FindBucketIndex(hist, bound);
            if (bi >= 0)
            {
                long prevCum = bi > 0 ? hist.Buckets[bi - 1].CumulativeRows : 0;
                long bucketRows = hist.Buckets[bi].CumulativeRows - prevCum;
                long distinct = hist.Buckets[bi].DistinctInBucket;
                if (distinct > 0 && bucketRows > 0)
                    return Math.Min(1.0, (double)bucketRows / hist.TotalRows / distinct);
            }
        }

        return FallbackSelectivity;
    }

    private static double InListSelectivity(
        AnalyzedInList inList,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
    {
        if (inList.Values.Count == 0)
            return 0.0;

        ColumnHistogram? hist = stats.GetColumnHistogram(database, table, inList.ColumnName);
        long? ndv = stats.GetColumnNdv(database, table, inList.ColumnName);

        // Sum individual equality selectivities — each value contributes independently.
        double total = 0.0;
        foreach (ColumnValue val in inList.Values)
            total += EqualitySelectivity(TryGetBound(val), hist, ndv);

        return Math.Min(1.0, total);
    }

    /// <summary>
    /// Returns the index of the first bucket whose upper bound is ≥ <paramref name="bound"/>.
    /// When <paramref name="bound"/> exceeds every bucket's upper bound, returns the last bucket index.
    /// Returns -1 on an empty histogram.
    /// </summary>
    private static int FindBucketIndex(ColumnHistogram hist, ScalarBound bound)
    {
        for (int i = 0; i < hist.Buckets.Count; i++)
        {
            if (hist.Buckets[i].UpperBound is { } ub && bound.CompareTo(ub) <= 0)
                return i;
        }
        return hist.Buckets.Count > 0 ? hist.Buckets.Count - 1 : -1;
    }

    // True when the value lies outside the histogram's observed [min, max] domain: above the
    // last bucket's upper bound, or (when the histogram records MinValue) below the minimum.
    private static bool IsOutsideHistogramDomain(ColumnHistogram hist, ScalarBound bound)
    {
        if (hist.Buckets.Count == 0)
            return false;

        if (hist.MinValue is not null && bound.CompareTo(hist.MinValue) < 0)
            return true;

        ColumnHistogramBucket last = hist.Buckets[^1];
        return last.UpperBound is not null && bound.CompareTo(last.UpperBound) > 0;
    }

    private static ScalarBound? TryGetBound(ColumnValue val) =>
        val.Type is ColumnType.Null ? null : ScalarBound.FromColumnValue(val);

    private static double Clamp(double v) => Math.Max(0.0, Math.Min(1.0, v));

    // Strips a table-alias prefix ("alias.column" → "column") so that stats lookups work
    // regardless of whether a column name was extracted from a qualified (join-context)
    // or unqualified (single-table) identifier.
    private static string StripAliasPrefix(string columnName)
    {
        int dot = columnName.IndexOf('.');
        return dot > 0 ? columnName[(dot + 1)..] : columnName;
    }
}
