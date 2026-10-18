
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
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
    /// and NDV statistics from <paramref name="stats"/>: the comparisons and <c>IN</c> lists
    /// (<see cref="EstimatePredicateSelectivity"/>) times each pattern predicate among the residual
    /// conjuncts (<see cref="PatternSelectivity"/>). Any other residual conjunct is not priced.
    /// Falls back to <see cref="FallbackSelectivity"/> when nothing in the predicate can be priced.
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
        // Estimate over constants in the column's own type, so a Float64 literal on an Integer64
        // column is compared against the histogram in the histogram's domain.
        analysis = PredicateAnalyzer.CoerceConstantsForColumns(analysis, table);

        double sel = 1.0;
        bool priced = false;

        if (analysis.IndexableComparisons.Count > 0 || analysis.InListComparisons.Count > 0)
        {
            sel = EstimatePredicateSelectivity(
                analysis.IndexableComparisons,
                analysis.InListComparisons,
                database, table, stats);
            priced = true;
        }

        sel *= PatternConjunctsSelectivity(analysis.ResidualConjuncts, database, table, stats, out int unrecognized);
        if (unrecognized < analysis.ResidualConjuncts.Count)
            priced = true;

        return priced ? Clamp(sel) : FallbackSelectivity;
    }

    /// <summary>
    /// The selectivity of the pattern predicates among <paramref name="residual"/>
    /// (<see cref="PredicateAnalyzer.TryAnalyzePattern"/>), as one product. The patterns on one
    /// column are priced together over the histogram's bucket bounds, a bound counting when it
    /// satisfies every one of them: two patterns on one column are seldom independent
    /// (<c>note LIKE '%(voice)%' AND note LIKE '%(uncredited)%'</c> both hold on the value
    /// <c>(voice) (uncredited)</c>), and the product of their shares would price the pair at a
    /// tenth of its rows. A column with one pattern predicate takes
    /// <see cref="PatternSelectivity"/>, which reads a prefix as a range.
    /// <paramref name="unrecognized"/> counts the residual conjuncts that are not patterns.
    /// </summary>
    internal static double PatternConjunctsSelectivity(
        IReadOnlyList<NodeAst> residual,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats,
        out int unrecognized)
    {
        unrecognized = 0;
        Dictionary<string, List<AnalyzedPattern>>? byColumn = null;

        foreach (NodeAst conjunct in residual)
        {
            if (!PredicateAnalyzer.TryAnalyzePattern(conjunct, parameters: null, out AnalyzedPattern? pattern))
            {
                unrecognized++;
                continue;
            }

            byColumn ??= new(StringComparer.OrdinalIgnoreCase);
            string column = StripAliasPrefix(pattern!.ColumnName);
            if (!byColumn.TryGetValue(column, out List<AnalyzedPattern>? group))
                byColumn[column] = group = [];

            group.Add(pattern);
        }

        if (byColumn is null)
            return 1.0;

        double sel = 1.0;
        foreach ((string column, List<AnalyzedPattern> group) in byColumn)
        {
            if (group.Count == 1)
            {
                sel *= PatternSelectivity(group[0], database, table, stats);
                continue;
            }

            ColumnHistogram? hist = stats.GetColumnHistogram(database, table, column);
            if (hist is null || hist.TotalRows <= 0 || hist.Buckets.Count == 0)
            {
                foreach (AnalyzedPattern pattern in group)
                    sel *= pattern.Negated ? 1.0 - FallbackSelectivity : FallbackSelectivity;
                continue;
            }

            sel *= JointPatternFraction(hist, group);
        }

        return Clamp(sel);
    }

    /// <summary>
    /// The share of the rows whose bucket bound satisfies every pattern predicate of
    /// <paramref name="group"/> (a bound satisfies a predicate when one of its patterns matches,
    /// or none does for a negated one); half a bucket when no bound does.
    /// </summary>
    private static double JointPatternFraction(ColumnHistogram hist, List<AnalyzedPattern> group)
    {
        double matched = 0;
        long previous = 0;

        foreach (ColumnHistogramBucket bucket in hist.Buckets)
        {
            long rows = bucket.CumulativeRows - previous;
            previous = bucket.CumulativeRows;

            if (rows <= 0 || bucket.UpperBound is not { Type: ColumnType.String, StrValue: { } text })
                continue;

            bool all = true;
            foreach (AnalyzedPattern predicate in group)
            {
                bool any = false;
                foreach (string pattern in predicate.Patterns)
                {
                    if (predicate.IgnoreCase ? SQLExecutorBaseCreator.ILike(text, pattern) : SQLExecutorBaseCreator.Like(text, pattern))
                    {
                        any = true;
                        break;
                    }
                }

                if (any == predicate.Negated)
                {
                    all = false;
                    break;
                }
            }

            if (all)
                matched += rows;
        }

        return Math.Max(matched, hist.TotalRows / (2.0 * hist.Buckets.Count)) / hist.TotalRows;
    }

    /// <summary>
    /// Selectivity of a pattern predicate (<see cref="AnalyzedPattern"/>) from the column's
    /// histogram, where the flat fallback priced every pattern at a tenth of the rows and the join
    /// search then could not tell <c>name LIKE '%An%'</c> from <c>title LIKE 'Kung Fu Panda%'</c>.
    /// A single pattern with a fixed prefix and nothing but a trailing wildcard after it is the
    /// range of the prefix, <c>[prefix, next(prefix))</c>, read like any range; with more literal
    /// text after the prefix (<c>'USA:% 200%'</c>) the range is scaled by the share of the bucket
    /// bounds inside it that match the whole pattern. Any other pattern, and an <c>OR</c> chain of
    /// patterns, is the share of the histogram's bucket bounds that match, each bound weighted by
    /// its bucket's rows: the bounds are the values at every hundredth rank of the sorted column,
    /// a sample that a pattern unrelated to the sort order hits in proportion, and a heavy value
    /// that fills whole buckets weighs in with all of them. No matching bound estimates half a
    /// bucket. A pattern without a wildcard is an equality. <c>ILIKE</c> uses the sample only,
    /// because the histogram's order is case-sensitive. Without a histogram,
    /// <see cref="FallbackSelectivity"/>. <c>NOT</c> takes the complement.
    /// </summary>
    internal static double PatternSelectivity(
        AnalyzedPattern pattern,
        DatabaseDescriptor database,
        TableDescriptor table,
        StatisticsManager stats)
    {
        string columnName = StripAliasPrefix(pattern.ColumnName);
        ColumnHistogram? hist = stats.GetColumnHistogram(database, table, columnName);

        double sel;
        if (hist is null || hist.TotalRows <= 0 || hist.Buckets.Count == 0)
            sel = FallbackSelectivity;
        else if (pattern.Patterns.Count == 1)
            sel = SinglePatternFraction(hist, stats.GetColumnNdv(database, table, columnName), pattern.Patterns[0], pattern.IgnoreCase);
        else
            sel = MatchingBoundsFraction(hist, pattern.Patterns, pattern.IgnoreCase);

        sel = Clamp(sel);
        return pattern.Negated ? 1.0 - sel : sel;
    }

    private static double SinglePatternFraction(ColumnHistogram hist, long? ndv, string pattern, bool ignoreCase)
    {
        // Only '%' is a wildcard; '_' is literal (the matcher's semantics).
        int wildcard = pattern.IndexOf('%');

        if (wildcard < 0 && !ignoreCase)
        {
            ScalarBound value = new() { Type = ColumnType.String, StrValue = pattern };
            return EqualitySelectivity(value, hist, ndv, EqualityDistribution(hist, ndv));
        }

        if (wildcard <= 0 || ignoreCase)
            return MatchingBoundsFraction(hist, [pattern], ignoreCase);

        string prefix = pattern[..wildcard];
        ScalarBound lo = new() { Type = ColumnType.String, StrValue = prefix };
        ScalarBound? hi = PrefixUpperBound(prefix);
        double range = hist.RangeFraction(lo, loInclusive: true, hi, hiInclusive: false);

        if (wildcard == pattern.Length - 1)
            return range;

        return range * MatchingBoundsShare(hist, pattern, lo, hi);
    }

    /// <summary>
    /// The share of the rows whose bucket bound matches one of <paramref name="patterns"/>; half a
    /// bucket when no bound matches.
    /// </summary>
    private static double MatchingBoundsFraction(ColumnHistogram hist, IReadOnlyList<string> patterns, bool ignoreCase)
    {
        double matched = 0;
        long previous = 0;

        foreach (ColumnHistogramBucket bucket in hist.Buckets)
        {
            long rows = bucket.CumulativeRows - previous;
            previous = bucket.CumulativeRows;

            if (rows <= 0 || bucket.UpperBound is not { Type: ColumnType.String, StrValue: { } text })
                continue;

            foreach (string pattern in patterns)
            {
                if (ignoreCase ? SQLExecutorBaseCreator.ILike(text, pattern) : SQLExecutorBaseCreator.Like(text, pattern))
                {
                    matched += rows;
                    break;
                }
            }
        }

        return Math.Max(matched, hist.TotalRows / (2.0 * hist.Buckets.Count)) / hist.TotalRows;
    }

    /// <summary>
    /// Among the bucket bounds inside <c>[lo, hi)</c>, the share of their rows whose bound matches
    /// <paramref name="pattern"/>, at least half a bucket; 1 when no bound lies in the range (the
    /// range is narrower than a bucket, and the range fraction stands alone).
    /// </summary>
    private static double MatchingBoundsShare(ColumnHistogram hist, string pattern, ScalarBound lo, ScalarBound? hi)
    {
        double matched = 0;
        double candidates = 0;
        long previous = 0;

        foreach (ColumnHistogramBucket bucket in hist.Buckets)
        {
            long rows = bucket.CumulativeRows - previous;
            previous = bucket.CumulativeRows;

            if (rows <= 0 || bucket.UpperBound is not { Type: ColumnType.String, StrValue: { } text } bound)
                continue;

            if (bound.CompareTo(lo) < 0 || (hi is not null && bound.CompareTo(hi) >= 0))
                continue;

            candidates += rows;
            if (SQLExecutorBaseCreator.Like(text, pattern))
                matched += rows;
        }

        if (candidates <= 0)
            return 1.0;

        return Math.Min(1.0, Math.Max(matched, hist.TotalRows / (2.0 * hist.Buckets.Count)) / candidates);
    }

    /// <summary>
    /// The smallest string above every string that starts with <paramref name="prefix"/>, in the
    /// ordinal order of the histogram: the prefix with its last character raised by one. Null when
    /// that character cannot be raised, which leaves the range open above.
    /// </summary>
    private static ScalarBound? PrefixUpperBound(string prefix)
    {
        char last = prefix[^1];
        if (last == char.MaxValue)
            return null;

        return new ScalarBound { Type = ColumnType.String, StrValue = string.Concat(prefix.AsSpan(0, prefix.Length - 1), new string((char)(last + 1), 1)) };
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
    /// A single-column key with a histogram on both sides, unique on neither, uses the value
    /// distributions (<see cref="JoinKeyDistribution"/>), so two columns skewed on the same values
    /// multiply as they do in the data. Falls back to |A| × |B| × <see cref="FallbackSelectivity"/>
    /// when no stats are available.
    /// </summary>
    /// <param name="leftRows">Estimated row count of the left (probe) input.</param>
    /// <param name="rightRows">Estimated row count of the right (build/inner) input.</param>
    /// <param name="rightKeyColumns">Unqualified join-key column names on the right side.</param>
    /// <param name="rightTable">Right table descriptor (may be null for derived sources).</param>
    /// <param name="leftKeyColumns">Unqualified join-key column names on the left side.</param>
    /// <param name="leftTable">Left table descriptor (may be null for complex left subtrees).</param>
    /// <param name="database">Database descriptor used for stats cache key.</param>
    /// <param name="stats">Statistics manager; may be null when no stats are loaded.</param>
    /// <param name="leftIsJoinResult">
    /// True when the left input is a join and <paramref name="leftTable"/> is the base table that
    /// the left key comes from. A key that is unique in that table is not unique in the join: an
    /// earlier join can repeat each of its rows. The unique-left shortcut then does not apply, and
    /// the key's distinct count (the table row count, for a unique key) enters the NDV formula.
    /// </param>
    public static long EstimateJoinCardinality(
        long leftRows,
        long rightRows,
        IReadOnlyList<string> rightKeyColumns,
        TableDescriptor? rightTable,
        IReadOnlyList<string> leftKeyColumns,
        TableDescriptor? leftTable,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        bool leftIsJoinResult = false)
    {
        // FK-aware: unique key on the right → at most 1 right match per left row.
        if (rightTable is not null && rightKeyColumns.Count > 0
            && IsJoinKeyUnique(rightTable, rightKeyColumns))
            return Math.Max(1, leftRows);

        // FK-aware: unique key on the left → at most 1 left match per right row.
        if (!leftIsJoinResult && leftTable is not null && leftKeyColumns.Count > 0
            && IsJoinKeyUnique(leftTable, leftKeyColumns))
            return Math.Max(1, rightRows);

        // NDV-based: |A ⋈ B| ≈ |A| × |B| / max(NDV_A, NDV_B).
        if (stats is not null)
        {
            long? ndvRight = GetJoinKeyNdv(database, rightTable, rightKeyColumns, stats);
            long? ndvLeft  = leftIsJoinResult
                ? KeyDistinctCount(leftTable, leftKeyColumns, TableRowCount(leftTable, database, stats), database, stats)
                : GetJoinKeyNdv(database, leftTable, leftKeyColumns, stats);

            // Neither key is unique (both shortcuts above failed), so a single-column key on each
            // side with a histogram gets the skew-aware estimate, the same one the join-order
            // search uses, so that EXPLAIN and the search agree on a join of two skewed columns.
            if (ndvLeft is { } left && ndvRight is { } right
                && leftKeyColumns.Count == 1 && rightKeyColumns.Count == 1
                && leftTable is not null && rightTable is not null
                && SkewedDistribution(leftTable, leftKeyColumns[0], left, database, stats) is { } leftDistribution
                && SkewedDistribution(rightTable, rightKeyColumns[0], right, database, stats) is { } rightDistribution
                && JoinKeyDistribution.JoinSelectivity(leftDistribution, rightDistribution) is { } selectivity)
            {
                return Math.Max(1, (long)((double)leftRows * rightRows * selectivity));
            }

            long maxNdv = Math.Max(ndvLeft ?? 0L, ndvRight ?? 0L);
            if (maxNdv > 0)
                return Math.Max(1, (long)((double)leftRows * rightRows / maxNdv));
        }

        // Fallback: fixed selectivity.
        return Math.Max(1, (long)((double)leftRows * rightRows * FallbackSelectivity));
    }

    /// <summary>
    /// Selectivity of an equi-join between two tables on the key columns of each side:
    /// <c>1 / max(ndvLeft, ndvRight)</c>, or <see cref="FallbackSelectivity"/> when neither side
    /// has a count. The distinct-value count of a side is its table row count when its key
    /// columns cover a unique index, else its column or composite-key NDV.
    ///
    /// <para>A single-column key with a histogram on both sides, unique on neither, is estimated
    /// from the two value distributions instead (<see cref="JoinKeyDistribution.JoinSelectivity"/>):
    /// the values common on both sides multiply, which the distinct counts alone cannot show.
    /// A unique side needs no histogram, because each of its values matches once whatever the
    /// skew of the other side; the distinct-count formula is then exact.</para>
    ///
    /// <para>The inputs are the tables and columns that the conjuncts name, never the shape of a
    /// join tree, so every plan for one set of tables gets the same estimate. The join-order
    /// enumerator needs that: it compares plans for a set and reuses the row estimate of the set.
    /// A unique side gives the standard estimate <c>leftRows × filteredRightRows / rightTableRows</c>;
    /// a scan filter on the unique side reduces it.</para>
    /// </summary>
    /// <param name="leftTable">Left table; null for a derived source, whose side then has no count.</param>
    /// <param name="leftKeyColumns">Bare key columns of the left side, in conjunct order.</param>
    /// <param name="leftTableRows">Unfiltered row count of the left table.</param>
    /// <param name="rightTable">Right table; null for a derived source.</param>
    /// <param name="rightKeyColumns">Bare key columns of the right side, paired with the left ones.</param>
    /// <param name="rightTableRows">Unfiltered row count of the right table.</param>
    /// <param name="database">Database descriptor used for the stats cache key.</param>
    /// <param name="stats">Statistics manager; may be null when no stats are loaded.</param>
    /// <param name="leftKeys">The known values of the left key column, read at plan time (<see cref="PlanTimeLeafLookup"/>), or null.</param>
    /// <param name="rightKeys">The known values of the right key column, or null.</param>
    internal static double EstimateEquiJoinSelectivity(
        TableDescriptor? leftTable,
        IReadOnlyList<string> leftKeyColumns,
        long leftTableRows,
        TableDescriptor? rightTable,
        IReadOnlyList<string> rightKeyColumns,
        long rightTableRows,
        DatabaseDescriptor database,
        StatisticsManager? stats,
        IReadOnlyList<ColumnValue>? leftKeys = null,
        IReadOnlyList<ColumnValue>? rightKeys = null)
    {
        // Known keys on one side: the join keeps, for each key, the other column's share of that
        // key, and the mean of those shares is the selectivity (the row count of the known side is
        // the number of keys, so the product gives the sum of the shares).
        if (leftKeyColumns.Count == 1 && rightKeyColumns.Count == 1 && stats is not null)
        {
            if (leftKeys is { Count: > 0 } && rightTable is not null
                && stats.GetColumnHistogram(database, rightTable, rightKeyColumns[0]) is { } rightHistogram
                && KeyShareSelectivity(rightHistogram, leftKeys) is { } leftShare)
                return leftShare;

            if (rightKeys is { Count: > 0 } && leftTable is not null
                && stats.GetColumnHistogram(database, leftTable, leftKeyColumns[0]) is { } leftHistogram
                && KeyShareSelectivity(leftHistogram, rightKeys) is { } rightShare)
                return rightShare;
        }

        long? ndvLeft = KeyDistinctCount(leftTable, leftKeyColumns, leftTableRows, database, stats);
        long? ndvRight = KeyDistinctCount(rightTable, rightKeyColumns, rightTableRows, database, stats);

        if (ndvLeft is { } left && ndvRight is { } right
            && leftKeyColumns.Count == 1 && rightKeyColumns.Count == 1
            && leftTable is not null && rightTable is not null
            && !IsJoinKeyUnique(leftTable, leftKeyColumns) && !IsJoinKeyUnique(rightTable, rightKeyColumns)
            && SkewedDistribution(leftTable, leftKeyColumns[0], left, database, stats) is { } leftDistribution
            && SkewedDistribution(rightTable, rightKeyColumns[0], right, database, stats) is { } rightDistribution
            && JoinKeyDistribution.JoinSelectivity(leftDistribution, rightDistribution) is { } selectivity)
        {
            return selectivity;
        }

        long maxNdv = Math.Max(ndvLeft ?? 0L, ndvRight ?? 0L);

        return maxNdv > 0 ? 1.0 / maxNdv : FallbackSelectivity;
    }

    /// <summary>
    /// The mean share of the rows of <paramref name="histogram"/>'s column behind the values
    /// <paramref name="keys"/>, each read with <see cref="ColumnHistogram.EqualityFraction"/> (a
    /// heavy value by its whole buckets, a rare one by its bucket's density) and floored at one row.
    /// This is the selectivity of a join through keys read at plan time
    /// (<see cref="PlanTimeLeafLookup"/>): <c>keys.Count × rightRows × share</c> is the sum of the
    /// rows behind the keys. Null when the histogram is empty or no key has a value.
    /// </summary>
    internal static double? KeyShareSelectivity(ColumnHistogram histogram, IReadOnlyList<ColumnValue> keys)
    {
        if (histogram.TotalRows <= 0 || histogram.Buckets.Count == 0)
            return null;

        double floor = 1.0 / histogram.TotalRows;
        double sum = 0.0;
        int counted = 0;

        foreach (ColumnValue key in keys)
        {
            if (TryGetBound(key) is not { } bound)
                continue;

            sum += Math.Max(floor, histogram.EqualityFraction(bound));
            counted++;
        }

        return counted == 0 ? null : Math.Min(1.0, sum / counted);
    }

    /// <summary>
    /// The value distribution of one join-key column, for an estimate that multiplies the
    /// distributions of several equal columns (<see cref="JoinKeyDistribution"/>): uniform over
    /// the table rows for a unique key, the histogram's heavy values plus a uniform residual for
    /// a column with a histogram and a distinct count, uniform over the distinct count for a
    /// column with a count and no histogram, and null when the column has no count at all.
    /// </summary>
    internal static JoinKeyDistribution? EstimateJoinKeyDistribution(
        TableDescriptor? table,
        string keyColumn,
        long tableRows,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        if (table is null)
            return null;

        if (IsJoinKeyUnique(table, [keyColumn]))
            return JoinKeyDistribution.Uniform(Math.Max(1L, tableRows));

        long? distinct = stats is null ? null : GetJoinKeyNdv(database, table, [keyColumn], stats);
        if (distinct is not { } d || d <= 0)
            return null;

        return SkewedDistribution(table, keyColumn, d, database, stats) ?? JoinKeyDistribution.Uniform(d);
    }

    /// <summary>
    /// The histogram-based distribution of a non-unique column, or null when the column has no
    /// histogram. A column whose histogram shows no heavy value gets a distribution too: its
    /// join with a skewed column still differs from the textbook estimate, because the heavy
    /// values of the other side meet its residual at an average frequency.
    /// </summary>
    private static JoinKeyDistribution? SkewedDistribution(
        TableDescriptor table,
        string keyColumn,
        long distinct,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        ColumnHistogram? histogram = stats?.GetColumnHistogram(database, table, keyColumn);
        return histogram is null ? null : JoinKeyDistribution.FromHistogram(histogram, distinct);
    }

    /// <summary>
    /// The distinct count of <paramref name="keyColumns"/> in <paramref name="table"/>: the table row
    /// count when the columns cover a unique index, else the column or composite-key NDV, else null.
    /// </summary>
    internal static long? EstimateKeyDistinctCount(
        TableDescriptor? table,
        IReadOnlyList<string> keyColumns,
        long tableRows,
        DatabaseDescriptor database,
        StatisticsManager? stats) =>
        KeyDistinctCount(table, keyColumns, tableRows, database, stats);

    private static long TableRowCount(TableDescriptor? table, DatabaseDescriptor database, StatisticsManager stats) =>
        (table is not null ? stats.GetRowCountEstimate(database, table) : null) ?? CostEstimator.DefaultTableRowCount;

    private static long? KeyDistinctCount(
        TableDescriptor? table,
        IReadOnlyList<string> keyColumns,
        long tableRows,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        if (table is null || keyColumns.Count == 0)
            return null;

        if (IsJoinKeyUnique(table, keyColumns))
            return Math.Max(1L, tableRows);

        return stats is null ? null : GetJoinKeyNdv(database, table, keyColumns, stats);
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
            JoinKeyDistribution? distribution = EqualityDistribution(hist, ndv);

            double total = 0.0;
            foreach (ColumnValue val in values)
                total += EqualitySelectivity(TryGetBound(val), hist, ndv, distribution);

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
                return EqualitySelectivity(bound, hist, ndv, EqualityDistribution(hist, ndv));

            case "!=":
                return Clamp(1.0 - EqualitySelectivity(bound, hist, ndv, EqualityDistribution(hist, ndv)));

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
    /// The value distribution that prices an equality on a column: its heavy values and the
    /// residual (<see cref="JoinKeyDistribution.FromHistogram"/>), when the column has both a
    /// histogram and a distinct count. Built once per column, so an <c>IN</c> list prices every
    /// value against one distribution.
    /// </summary>
    private static JoinKeyDistribution? EqualityDistribution(ColumnHistogram? hist, long? ndv) =>
        hist is not null && ndv is > 0 ? JoinKeyDistribution.FromHistogram(hist, ndv.Value) : null;

    /// <summary>
    /// Estimates selectivity for <c>col = constant</c>. A value that fills whole buckets of the
    /// column's histogram (<see cref="ColumnHistogram.IsHeavyValue"/>) takes the share those
    /// buckets hold: the distinct count alone prices the popular values of a skewed column like
    /// its rare ones, a cast note that a tenth of the rows carry at one row in ten thousand, and
    /// a join search then starts from that filter. Any other value takes the residual of the
    /// distribution: the rows that no heavy value holds, spread over the remaining distinct
    /// values, the model the join estimate uses (<see cref="JoinKeyDistribution"/>); with no
    /// heavy value that is <c>1 / NDV</c>. Without a distinct count the containing bucket's
    /// density stands in; without a histogram or a count, <see cref="FallbackSelectivity"/>.
    /// </summary>
    private static double EqualitySelectivity(ScalarBound? bound, ColumnHistogram? hist, long? ndv, JoinKeyDistribution? distribution)
    {
        // NULL bound means the constant is SQL NULL.  col = NULL is UNKNOWN in 3VL → 0 rows.
        // Guard this before the NDV branch so we don't return 1/NDV for a NULL constant.
        // (PredicateAnalyzer currently emits IS NULL rather than = NULL, so this is latent.)
        if (bound is null)
            return 0.0;

        // 1. A heavy value: its own share of the rows, from the buckets it fills.
        if (hist is not null && hist.TotalRows > 0 && hist.IsHeavyValue(bound))
            return Math.Min(1.0, hist.EqualityFraction(bound));

        // 2. NDV: the residual share over the residual distinct values when the column has heavy
        //    values, else 1/NDV.
        if (ndv is > 0)
        {
            if (distribution is { HeavyValues.Count: > 0 })
                return Math.Min(1.0, distribution.ResidualFraction / distribution.ResidualDistinct);

            return Math.Min(1.0, 1.0 / ndv.Value);
        }

        // 3. Histogram bucket, no distinct count: rows-in-bucket / totalRows / distinct-in-bucket.
        if (hist is not null && hist.TotalRows > 0)
        {
            // Out-of-domain: equality on a value above every bucket (or below the observed
            // minimum) matches ~nothing — estimate one row rather than last-bucket density.
            if (IsOutsideHistogramDomain(hist, bound))
                return Math.Min(1.0, 1.0 / hist.TotalRows);

            return Math.Min(1.0, Math.Max(hist.EqualityFraction(bound), 1.0 / hist.TotalRows));
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

        // A list on a joined table carries its alias ("k.keyword"); the statistics are keyed by
        // the bare column name, as for a single comparison.
        string columnName = StripAliasPrefix(inList.ColumnName);
        ColumnHistogram? hist = stats.GetColumnHistogram(database, table, columnName);
        long? ndv = stats.GetColumnNdv(database, table, columnName);
        JoinKeyDistribution? distribution = EqualityDistribution(hist, ndv);

        // Sum individual equality selectivities — each value contributes independently.
        double total = 0.0;
        foreach (ColumnValue val in inList.Values)
            total += EqualitySelectivity(TryGetBound(val), hist, ndv, distribution);

        return Math.Min(1.0, total);
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
