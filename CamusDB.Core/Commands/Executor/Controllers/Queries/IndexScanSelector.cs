
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
        Dictionary<string, List<AnalyzedComparison>> byColumn = BuildColumnMap(table, analysis);

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
        Dictionary<string, List<AnalyzedComparison>> byColumn = BuildColumnMap(table, analysis);
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

        Dictionary<string, List<AnalyzedComparison>> byColumn = BuildColumnMap(table, analysis);
        if (byColumn.Count == 0)
            return null;

        return TryMatchPredicateIndex(table, index, byColumn, out QueryPlanStep step, out _) ? step : null;
    }

    /// <summary>
    /// Groups the indexable comparisons by column, keeping only those whose constant already has
    /// the column's type. <see cref="PredicateAnalyzer.CoerceConstantsForColumns"/> is expected to
    /// have rewritten every constant before this point; a comparison that still mismatches cannot
    /// address an index key (the key encoding and the comparer are per-type), so it is left out here
    /// and stays with the residual filter, where the evaluator decides. This is the last line of
    /// defense: a caller that forgets the coercion loses the index shortcut, never the right rows.
    /// </summary>
    private static Dictionary<string, List<AnalyzedComparison>> BuildColumnMap(TableDescriptor table, PredicateAnalysis analysis)
    {
        Dictionary<string, List<AnalyzedComparison>> byColumn = new(StringComparer.OrdinalIgnoreCase);

        foreach (AnalyzedComparison comparison in analysis.IndexableComparisons)
        {
            if (!ConstantMatchesColumnType(table, comparison))
                continue;

            if (!byColumn.TryGetValue(comparison.ColumnName, out List<AnalyzedComparison>? list))
                byColumn[comparison.ColumnName] = list = new();

            list.Add(comparison);
        }

        return byColumn;
    }

    private static bool ConstantMatchesColumnType(TableDescriptor table, AnalyzedComparison comparison)
    {
        TableColumnSchema? column = table.Schema.Columns?.Find(
            c => string.Equals(c.Name, comparison.ColumnName, StringComparison.OrdinalIgnoreCase));

        // An unknown column cannot match any index column below, so there is nothing to reject.
        return column is null || column.Type == comparison.Constant.Type;
    }

    /// <summary>
    /// Matches one index against the query's column comparisons and builds the best step that
    /// index can serve: a full-key unique lookup, an equality prefix plus a range on the next
    /// column, or an equality prefix alone. This is the single matcher behind heuristic selection
    /// (<see cref="TrySelectScan"/>), cost enumeration (<see cref="EnumerateViableSteps"/>),
    /// plan-cache replay (<see cref="TrySelectScanForForcedIndex"/>) and join leaves, so a rule
    /// placed here holds everywhere.
    /// <para>One such rule is completeness for unique indexes: a unique index has no entry for a
    /// row with NULL in any key column, so a scan over a prefix of it returns only the rows whose
    /// unconstrained columns are non-NULL. <see cref="UniqueIndexHoldsEveryQualifyingRow"/> rejects
    /// the index for that query unless the schema proves those columns NOT NULL. A residual filter
    /// cannot repair this — the missing row never enters the pipeline — so the index is not a
    /// candidate at all, and the planner falls through to another index or the table scan.</para>
    /// </summary>
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

            // [v, next(v)) when the last column is ascending and its type has a successor, otherwise
            // the inclusive [v, v]. A type with no successor (String, Id, Date, DateTime, Float32,
            // Bytes) or a value at its type's maximum still seeks the index.
            (CompositeColumnValue upperBound, bool equalityToInclusive) = EqualitySeekUpperBound(table, index, lookupKey);

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                lookupKey,
                fromInclusive: true,
                upperBound,
                equalityToInclusive)
            {
                EqualityPrefixLength = columns.Length,
            };
            score = RangeScanScore + columns.Length * 10;
            return true;
        }

        // A trailing range bound is only absorbed when its column is ascending: the tightest-bound
        // selection and the [v, next) prefix caps below assume ascending encoded order. A descending
        // range column is left to the executor's residual filter (the index may still be chosen for
        // its equality prefix or to satisfy ORDER BY).
        if (equalityPrefixLength < columns.Length
            && IsAscending(index, equalityPrefixLength)
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
            // The equality prefix and the range column are constrained; every column after them
            // must be provably non-NULL for a unique index to hold every qualifying row.
            if (!UniqueIndexHoldsEveryQualifyingRow(table, index, byColumn, equalityPrefixLength + 1))
                return false;

            if (equalityPrefixLength > 0)
            {
                ColumnValue[] prefixVals = new ColumnValue[equalityPrefixLength];
                Array.Copy(equalityValues, prefixVals, equalityPrefixLength);
                CompositeColumnValue prefixBound = new(prefixVals);

                (CompositeColumnValue prefixUpperBound, bool prefixUpperInclusive) = EqualitySeekUpperBound(table, index, prefixBound);

                if (!prefixUpperInclusive)
                    (toBound, toInclusive) = TightenUpperBound(toBound, toInclusive, prefixUpperBound);
                else if (toBound is null)
                {
                    // No usable ascending successor for the prefix column (a type with none, a value
                    // at its type's maximum, or a descending column whose successor moves the wrong
                    // way), so an open upper side is capped with the inclusive prefix. ScanIndex
                    // appends the index sentinel to a prefix bound, so [prefix] inclusive covers
                    // every row of the prefix; the residual filter guards against off-prefix rows.
                    (toBound, toInclusive) = (prefixBound, true);
                }

                // When the range column contributed only an upper bound (e.g. a = 5 AND b < 10), the
                // scan still needs a LOWER bound at the equality prefix: without it the scan starts at
                // the beginning of the index and reads (and discards via the residual filter) every
                // row below the prefix — and under Serializable the range lock covers that whole low
                // keyspace.
                if (fromBound is null)
                    (fromBound, fromInclusive) = (prefixBound, true);
            }

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                fromBound,
                fromInclusive,
                toBound,
                toInclusive)
            {
                EqualityPrefixLength = equalityPrefixLength,
            };
            score = RangeScanScore + equalityPrefixLength * 10 + 1;
            return true;
        }

        if (equalityPrefixLength > 0)
        {
            if (!UniqueIndexHoldsEveryQualifyingRow(table, index, byColumn, equalityPrefixLength))
                return false;

            ColumnValue[] prefixValues = new ColumnValue[equalityPrefixLength];
            Array.Copy(equalityValues, prefixValues, equalityPrefixLength);
            CompositeColumnValue prefixBound = new(prefixValues);

            (CompositeColumnValue upperBound, bool prefixToInclusive) = EqualitySeekUpperBound(table, index, prefixBound);

            step = new QueryPlanStep(
                QueryPlanStepType.RangeScanFromIndex,
                index,
                prefixBound,
                fromInclusive: true,
                upperBound,
                prefixToInclusive)
            {
                EqualityPrefixLength = equalityPrefixLength,
            };
            score = RangeScanScore + equalityPrefixLength;
            return true;
        }

        return false;
    }

    /// <summary>
    /// How many leading key columns the step's bounds address: the lookup key of a unique lookup,
    /// the longer bound of a range scan. The cost-based choice breaks a cost tie with it, because a
    /// step that binds one more key column reads a subset of what the shorter step reads. Two steps
    /// tie in cost when the statistics cannot tell them apart (a prefix column with no distinct count,
    /// a range column with no histogram), and the tie must not go to the index whose name sorts first.
    /// </summary>
    internal static int BoundColumnCount(in QueryPlanStep step)
    {
        return step.Type switch
        {
            QueryPlanStepType.QueryFromIndex => step.LookupKey?.Values.Length ?? (step.ColumnValue is null ? 0 : 1),
            QueryPlanStepType.RangeScanFromIndex => Math.Max(step.FromBound?.Values.Length ?? 0, step.ToBound?.Values.Length ?? 0),
            _ => 0,
        };
    }

    internal static QueryPlanStep? TrySelectOrderByScan(
        TableDescriptor table,
        IReadOnlyList<QueryOrderBy>? orderBy)
    {
        if (orderBy is null || orderBy.Count == 0)
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
    /// are all NOT NULL can never omit a row, so it remains eligible for a scan that reads it as the
    /// complete row set: the unbounded ORDER BY scan, the user-forced full-index scan in
    /// <see cref="QueryPlanner"/>, and the predicate-free scans that streaming DISTINCT/GROUP BY,
    /// the correlated EXISTS seek and the merge join build. This is the "no constrained columns"
    /// case of <see cref="UniqueIndexHoldsEveryQualifyingRow"/>.
    /// </summary>
    internal static bool AllColumnsNotNull(TableDescriptor table, TableIndexSchema index)
        => UniqueIndexHoldsEveryQualifyingRow(table, index, byColumn: null, constrainedPrefixLength: 0, forceUniqueRule: true);

    /// <summary>
    /// The completeness rule for unique indexes. A unique index carries no entry for a row with a
    /// NULL in <b>any</b> key column, so a scan over its first <paramref name="constrainedPrefixLength"/>
    /// columns silently omits every row whose later key columns hold NULL — and such a row can
    /// still satisfy the query (<c>(a=1, b=NULL)</c> satisfies <c>a = 1</c>). The index may serve
    /// the query only if each key column beyond the constrained prefix is either constrained by
    /// an equality or range comparison in <paramref name="byColumn"/> (a NULL there fails the
    /// predicate, so the omitted row was never a qualifying row) or declared NOT NULL in the
    /// schema. A non-unique index stores every row and always passes. A <c>!=</c> comparison
    /// does not count as a constraint: under three-valued logic a NULL fails it, but the index
    /// never absorbs it, so the schema must carry the proof.
    /// </summary>
    internal static bool UniqueIndexHoldsEveryQualifyingRow(
        TableDescriptor table,
        TableIndexSchema index,
        Dictionary<string, List<AnalyzedComparison>>? byColumn,
        int constrainedPrefixLength,
        bool forceUniqueRule = false)
    {
        if (index.Type != IndexType.Unique && !forceUniqueRule)
            return true;

        string[] columns = index.Columns;
        for (int i = constrainedPrefixLength; i < columns.Length; i++)
        {
            if (byColumn is not null && HasAbsorbableComparison(byColumn, columns[i]))
                continue;

            TableColumnSchema? schema = table.Schema.Columns!.Find(c => string.Equals(c.Name, columns[i], StringComparison.OrdinalIgnoreCase));
            if (schema is null || !schema.NotNull)
                return false;
        }

        return true;
    }

    private static bool HasAbsorbableComparison(Dictionary<string, List<AnalyzedComparison>> byColumn, string column)
    {
        if (!byColumn.TryGetValue(column, out List<AnalyzedComparison>? comparisons))
            return false;

        foreach (AnalyzedComparison comparison in comparisons)
        {
            if (comparison.Operator is "=" or "<" or "<=" or ">" or ">=")
                return true;
        }

        return false;
    }

    /// <summary>
    /// Finds a readable index whose first <c>distinctColumns.Count</c> columns are exactly the set of
    /// DISTINCT (or GROUP BY) key columns. Such an index guarantees that equal rows are adjacent in
    /// the scan, enabling streaming deduplication or aggregation without a hash table.
    /// Returns the first matching index, or null if none qualifies.
    /// <para>The caller replaces the primary-row scan with an end-to-end scan of this index, so the
    /// index must hold <b>every</b> row. A non-unique index does (NULL keys are stored with the row
    /// id suffix). A unique index omits any row with a NULL in any of its key columns — including
    /// columns after the DISTINCT prefix — so it qualifies only when
    /// <see cref="AllColumnsNotNull"/> holds. The caller's own NOT NULL check on the DISTINCT
    /// columns is not enough: <c>SELECT DISTINCT a</c> over <c>UNIQUE (a, b)</c> with a nullable
    /// <c>b</c> would lose every <c>a</c> whose only rows carry a NULL <c>b</c>.</para>
    /// </summary>
    internal static TableIndexSchema? TryFindStreamingDistinctIndex(
        TableDescriptor table,
        IReadOnlyList<string> distinctColumns)
    {
        if (distinctColumns.Count == 0)
            return null;

        HashSet<string> distinctSet = new(distinctColumns, StringComparer.OrdinalIgnoreCase);

        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                continue;

            if (index.Type == IndexType.Unique && !AllColumnsNotNull(table, index))
                continue;

            if (index.Columns.Length < distinctColumns.Count)
                continue;

            // The first distinctColumns.Count index columns must form exactly the distinct key set.
            HashSet<string> prefix = new(StringComparer.OrdinalIgnoreCase);
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

        HashSet<string> distinctSet = new(distinctColumns, StringComparer.OrdinalIgnoreCase);
        HashSet<string> prefix = new(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < distinctColumns.Count; i++)
            prefix.Add(scanStep.Index.Columns[i]);

        return prefix.SetEquals(distinctSet);
    }

    /// <summary>
    /// True when the rows <paramref name="scanStep"/> delivers are already in
    /// <paramref name="orderBy"/> order, so the sort can be dropped.
    ///
    /// <para><paramref name="constantColumns"/> are the columns the query's top-level <c>=</c>
    /// conjuncts fix to one value: every result row holds the same value there. Such a column orders
    /// nothing, so it is skipped on both sides — an index column it names is passed over, and an
    /// ORDER BY key on it is ignored. That is what lets <c>WHERE category = ? ORDER BY created</c> use
    /// an index on <c>(category, created)</c> with no sort. A scan delivers its index order whether
    /// or not the equality is applied in the scan bounds or in the residual filter, so the rule holds
    /// either way. Null or empty means no column is known to be constant.</para>
    /// </summary>
    internal static bool ScanSatisfiesOrderBy(
        TableDescriptor table,
        QueryPlanStep scanStep,
        IReadOnlyList<QueryOrderBy>? orderBy,
        IReadOnlySet<string>? constantColumns = null)
    {
        if (orderBy is null || orderBy.Count == 0)
            return true;

        return scanStep.Type switch
        {
            QueryPlanStepType.QueryFromIndex => true,
            QueryPlanStepType.RangeScanFromIndex => scanStep.Index is not null
                && IndexOrderSatisfies(scanStep.Index, orderBy, constantColumns),
            QueryPlanStepType.FullScanFromIndex => scanStep.Index is not null
                && IndexOrderSatisfies(scanStep.Index, orderBy, constantColumns),
            _ => false
        };
    }

    /// <summary>
    /// Walks the ORDER BY keys against the index columns, skipping the columns in
    /// <paramref name="constantColumns"/> on both sides (see <see cref="ScanSatisfiesOrderBy"/>).
    /// Every other key must name the next index column, in that column's direction: there is no
    /// backward scan to flip a direction.
    /// </summary>
    private static bool IndexOrderSatisfies(
        TableIndexSchema index,
        IReadOnlyList<QueryOrderBy> orderBy,
        IReadOnlySet<string>? constantColumns)
    {
        if (constantColumns is null || constantColumns.Count == 0)
            return MatchOrderByPrefixLength(index, orderBy) >= orderBy.Count;

        int position = 0;

        foreach (QueryOrderBy key in orderBy)
        {
            // A computed key is never provided by an index; its label is a diagnostic, not a column.
            if (key.IsExpression)
                return false;

            string column = BareColumnName(key.ColumnName);

            if (constantColumns.Contains(column))
                continue;

            while (position < index.Columns.Length && constantColumns.Contains(index.Columns[position]))
                position++;

            if (position >= index.Columns.Length)
                return false;

            if (!string.Equals(index.Columns[position], column, StringComparison.Ordinal)
                || index.DirectionAt(position) != key.Type)
                return false;

            position++;
        }

        return true;
    }

    /// <summary>
    /// The bare names of the columns that a top-level <c>=</c> conjunct with a constant fixes to
    /// one value, or null when there is none. Feeds <see cref="ScanSatisfiesOrderBy"/>.
    /// </summary>
    internal static IReadOnlySet<string>? ConstantColumns(PredicateAnalysis analysis)
    {
        HashSet<string>? columns = null;

        foreach (AnalyzedComparison comparison in analysis.IndexableComparisons)
        {
            if (comparison.Operator != "=")
                continue;

            columns ??= new HashSet<string>(StringComparer.Ordinal);
            columns.Add(BareColumnName(comparison.ColumnName));
        }

        return columns;
    }

    private static int MatchOrderByPrefixLength(TableIndexSchema index, IReadOnlyList<QueryOrderBy> orderBy)
    {
        int max = Math.Min(index.Columns.Length, orderBy.Count);
        int matched = 0;

        for (int i = 0; i < max; i++)
        {
            // Index columns are bare; an ORDER BY column may be alias-qualified ("u.position").
            // Compare against the bare column name so aliased single-table ORDER BY can elide.
            // A computed key is never provided by an index; its label is a diagnostic, not a column.
            // Stop here rather than comparing it, so the prefix length reflects only real columns.
            if (orderBy[i].IsExpression)
                break;

            if (!string.Equals(index.Columns[i], BareColumnName(orderBy[i].ColumnName), StringComparison.Ordinal))
                break;

            // Direction must match exactly. A forward-only scan yields the index's own per-column
            // order, so an ascending index column satisfies ORDER BY ... ASC and a descending one
            // satisfies ... DESC — never the reverse (there is no backward scan to flip it).
            if (index.DirectionAt(i) != orderBy[i].Type)
                break;

            matched++;
        }

        return matched;
    }

    /// <summary>Direction of the index's column at <paramref name="position"/> (Ascending when unset).</summary>
    private static bool IsAscending(TableIndexSchema index, int position)
        => index.DirectionAt(position) == OrderType.Ascending;

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

    private static ColumnType GetColumnType(TableDescriptor table, string columnName)
    {
        TableColumnSchema? column = table.Schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        return column?.Type ?? ColumnType.String;
    }

    /// <summary>
    /// Returns the end bound of an executor seek for one key value: <paramref name="lookupKey"/> holds
    /// the leading key columns of <paramref name="index"/>, and the scan starts inclusive at that key.
    /// Every path that seeks a single value through <see cref="KvIndexAccessor.ScanIndex"/> must take
    /// its range from here: the planner's equality and equality-prefix steps in
    /// <see cref="TryMatchPredicateIndex"/>, IN-list seeks, join-leaf IN lists and semi-join probes.
    /// The planner must never refuse an index because a value has no successor, since the inclusive
    /// form below is always complete. The
    /// non-unique probe of an index nested-loop join uses <see cref="KvIndexAccessor.ScanIndexEquality"/>
    /// instead, whose window is <c>[v, v + sentinel]</c> in the index's own direction.
    /// <para>The exclusive successor bound <c>[v, next(v))</c> is used only when the last bound column
    /// is ascending and its type has a successor. <see cref="KvIndexAccessor.ScanIndex"/> encodes the
    /// bounds in the index's own direction, so on a descending column <c>next(v)</c> encodes
    /// <em>before</em> <c>v</c> and that range is empty: the seek silently returns no rows. Every
    /// other case (a descending column, a type with no successor such as String or Id, or a value at
    /// its type's maximum) gets the inclusive <c>[v, v]</c>. That range is complete: ScanIndex appends
    /// the index sentinel to a non-unique or prefix end bound, which covers every row-id suffix and
    /// every trailing key column, and its decoded-key filter trims the result to the key in either
    /// direction.</para>
    /// </summary>
    internal static (CompositeColumnValue ToBound, bool ToInclusive) EqualitySeekUpperBound(
        TableDescriptor table,
        TableIndexSchema index,
        CompositeColumnValue lookupKey)
    {
        int prefixLength = lookupKey.Values.Length;
        if (prefixLength == 0 || !IsAscending(index, prefixLength - 1))
            return (lookupKey, true);

        ColumnType columnType = GetColumnType(table, index.Columns[prefixLength - 1]);
        ColumnValue? nextValue = NextSortValue(columnType, lookupKey.Values[prefixLength - 1]);
        if (nextValue is null)
            return (lookupKey, true);

        ColumnValue[] upperValues = new ColumnValue[prefixLength];
        Array.Copy(lookupKey.Values, upperValues, prefixLength - 1);
        upperValues[prefixLength - 1] = nextValue;
        return (new CompositeColumnValue(upperValues), false);
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
            ColumnType.Uuid when TryNextUuid(value, out ColumnValue? nextUuid)
                => nextUuid,
            // The NUMERIC successor is one unit of the last fractional digit (10⁻⁹) higher.
            ColumnType.Numeric when value.NumericUnscaled < NumericMath.MaxUnscaled
                => ColumnValue.FromNumeric(value.NumericUnscaled + 1),
            _ => null,
        };
    }

    /// <summary>
    /// Computes the next-larger Uuid (unsigned 128-bit +1) so an exact-equality scan on a non-unique
    /// Uuid index can use the fixed-width upper bound <c>[v, v+1)</c>. Returns false at the maximum
    /// value (no successor), which makes the caller use the inclusive <c>[v, v]</c> range.
    /// </summary>
    private static bool TryNextUuid(ColumnValue value, out ColumnValue? next)
    {
        next = null;

        UInt128 v = ((UInt128)(ulong)value.UuidHigh << 64) | (ulong)value.LongValue;
        if (v == UInt128.MaxValue)
            return false;

        v += 1;
        next = new ColumnValue(ColumnType.Uuid, (long)(ulong)(v >> 64), (long)(ulong)v);
        return true;
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
