
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Removes duplicate output tuples for <c>SELECT DISTINCT</c>.
/// Comparison uses SQL DISTINCT null semantics: two NULL values are equal.
///
/// <b>Flag-off (default):</b> deduplication via a <see cref="HashSet{T}"/> of
/// <see cref="DistinctRowKey"/> — O(distinct-count) memory.
///
/// <b>Flag-on (<see cref="CamusDBConfig.SpillEnabled"/> = <c>true</c>):</b> rows are sorted
/// by all projected columns using the external merge sort (<see cref="QuerySorter.SortAsync"/>),
/// which spills sorted runs to disk when the input exceeds
/// <see cref="CamusDBConfig.SpillEffectiveThreshold"/>. After sorting, equal rows are adjacent
/// and are removed by the O(1)-memory <see cref="StreamingDistinctRows"/> streaming dedup.
/// </summary>
internal sealed class QueryDistincter
{
    internal IAsyncEnumerable<QueryResultRow> DistinctResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (!CamusDBConfig.SpillEnabled)
            return DistinctRows(dataCursor);

        return DistinctWithSpill(dataCursor);
    }

    /// <summary>
    /// Streaming deduplication: compares each row to the previously emitted row.
    /// Requires the input to arrive in an order that groups equal rows adjacently (i.e. an
    /// index scan whose prefix covers all projected DISTINCT columns).
    /// Uses O(1) memory (one key) instead of the O(distinct-count) hash set.
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> StreamingDistinctRows(
        IAsyncEnumerable<QueryResultRow> dataCursor) =>
        StreamingRows(dataCursor);

    private static async IAsyncEnumerable<QueryResultRow> StreamingRows(
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        IReadOnlyDictionary<string, ColumnValue>? lastRow = null;
        string[]? sortedNames = null;
        // Ordinal-parallel array resolved from the first row's RowLayout. The fast path is
        // guarded on ReferenceEquals(row layout, resolvedLayout) so that rows carrying a
        // different layout instance (different schema version, join node, future DDL reorder)
        // degrade safely to the dictionary path rather than reading the wrong column ordinals.
        int[]? sortedOrdinals = null;
        RowLayout? resolvedLayout = null;
        QueryRow? lastQr = null;

        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
        {
            if (lastRow is null)
            {
                // Hoist sorted column names on first row — schema is fixed across the cursor.
                sortedNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                lastRow = row.Row;
                if (row.Row is QueryRow qrFirst)
                {
                    lastQr = qrFirst;
                    resolvedLayout = qrFirst.Layout;
                    sortedOrdinals = BuildSortedOrdinals(sortedNames, resolvedLayout);
                }
                yield return row;
                continue;
            }

            bool equal = true;

            if (sortedOrdinals is not null
                && row.Row  is QueryRow qrCurr && ReferenceEquals(qrCurr.Layout, resolvedLayout)
                && lastQr is not null          && ReferenceEquals(lastQr.Layout,  resolvedLayout))
            {
                for (int i = 0; i < sortedOrdinals.Length; i++)
                {
                    int ord = sortedOrdinals[i];
                    ColumnValue cv = ord >= 0 ? qrCurr.Values[ord] : ((IReadOnlyDictionary<string, ColumnValue>)qrCurr)[sortedNames![i]];
                    ColumnValue lv = ord >= 0 ? lastQr.Values[ord]  : ((IReadOnlyDictionary<string, ColumnValue>)lastQr)[sortedNames![i]];
                    if (!DistinctValuesEqual(cv, lv)) { equal = false; break; }
                }
            }
            else
            {
                for (int i = 0; i < sortedNames!.Length; i++)
                {
                    string name = sortedNames[i];
                    if (!DistinctValuesEqual(row.Row[name], lastRow[name]))
                    {
                        equal = false;
                        break;
                    }
                }
            }

            if (!equal)
            {
                lastRow = row.Row;
                lastQr = row.Row as QueryRow;
                yield return row;
            }
        }
    }

    /// <summary>
    /// Sorts <paramref name="dataCursor"/> by all projected columns using the external merge sort,
    /// then streams the sorted sequence through <see cref="StreamingRows"/> which removes
    /// adjacent equal rows in O(1) memory. The sort order places NULL before any non-null value
    /// so that two NULLs are adjacent — consistent with the SQL DISTINCT equality rule
    /// (<c>NULL = NULL</c> for dedup purposes) enforced by <see cref="DistinctValuesEqual"/>.
    /// </summary>
    private static IAsyncEnumerable<QueryResultRow> DistinctWithSpill(
        IAsyncEnumerable<QueryResultRow> dataCursor,
        CancellationToken ct = default)
    {
        IComparer<QueryResultRow> comparer = new DistinctRowComparer();
        return StreamingRows(QuerySorter.SortAsync(dataCursor, comparer, ct));
    }

    /// <summary>
    /// Compares two <see cref="QueryResultRow"/> values by all their columns in alphabetical
    /// column-name order. NULL sorts before any non-null value; non-null values compare via
    /// <see cref="ColumnValue.CompareTo"/>. This ordering guarantees that two rows are adjacent
    /// after sorting if and only if they would be considered equal by
    /// <see cref="DistinctValuesEqual"/>, so streaming dedup correctly deduplicates them.
    ///
    /// <para>Column names are hoisted on the first call and reused for subsequent comparisons.
    /// When the first row is a <see cref="QueryRow"/>, ordinals into
    /// <see cref="QueryRow.Values"/> are resolved once and used for all subsequent comparisons,
    /// replacing per-row dictionary lookups with direct array access.</para>
    /// </summary>
    private sealed class DistinctRowComparer : IComparer<QueryResultRow>
    {
        private string[]? _sortedNames;
        private int[]? _sortedOrdinals;

        // The RowLayout instance the ordinals were resolved from. The fast path is only taken
        // when both rows carry this exact instance — different layouts silently degrade to the
        // dictionary path, which is always correct via the IReadOnlyDictionary adapter.
        private RowLayout? _resolvedLayout;

        public int Compare(QueryResultRow x, QueryResultRow y)
        {
            if (_sortedNames is null)
            {
                _sortedNames = x.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                if (x.Row is QueryRow qrInit)
                {
                    _resolvedLayout = qrInit.Layout;
                    _sortedOrdinals = BuildSortedOrdinals(_sortedNames, _resolvedLayout);
                }
            }

            if (_sortedOrdinals is not null
                && x.Row is QueryRow qrX && ReferenceEquals(qrX.Layout, _resolvedLayout)
                && y.Row is QueryRow qrY && ReferenceEquals(qrY.Layout, _resolvedLayout))
            {
                for (int i = 0; i < _sortedOrdinals.Length; i++)
                {
                    int ord = _sortedOrdinals[i];
                    ColumnValue xv = ord >= 0 ? qrX.Values[ord] : (((IReadOnlyDictionary<string, ColumnValue>)qrX).TryGetValue(_sortedNames[i], out ColumnValue? xf) ? xf : ColumnValue.Null);
                    ColumnValue yv = ord >= 0 ? qrY.Values[ord] : (((IReadOnlyDictionary<string, ColumnValue>)qrY).TryGetValue(_sortedNames[i], out ColumnValue? yf) ? yf : ColumnValue.Null);

                    if (xv.Type == ColumnType.Null && yv.Type == ColumnType.Null) continue;
                    if (xv.Type == ColumnType.Null) return -1;
                    if (yv.Type == ColumnType.Null) return 1;
                    int cmp = xv.CompareTo(yv);
                    if (cmp != 0) return cmp;
                }
                return 0;
            }

            foreach (string name in _sortedNames!)
            {
                ColumnValue xv = x.Row.TryGetValue(name, out ColumnValue? xval) ? xval : ColumnValue.Null;
                ColumnValue yv = y.Row.TryGetValue(name, out ColumnValue? yval) ? yval : ColumnValue.Null;

                if (xv.Type == ColumnType.Null && yv.Type == ColumnType.Null) continue;
                if (xv.Type == ColumnType.Null) return -1;
                if (yv.Type == ColumnType.Null) return 1;

                int cmp = xv.CompareTo(yv);
                if (cmp != 0) return cmp;
            }

            return 0;
        }
    }

    private static int[] BuildSortedOrdinals(string[] sortedNames, RowLayout layout)
    {
        int[] ordinals = new int[sortedNames.Length];
        for (int i = 0; i < sortedNames.Length; i++)
            ordinals[i] = layout.IndexOf(sortedNames[i]);
        return ordinals;
    }

    private static bool DistinctValuesEqual(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null && right.Type == ColumnType.Null)
            return true;

        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return false;

        return left.CompareTo(right) == 0;
    }

    private static int DistinctValueHash(ColumnValue value)
    {
        if (value.Type == ColumnType.Null)
            return 0;

        return value.Type switch
        {
            ColumnType.Integer64 => HashCode.Combine(value.Type, value.LongValue),
            ColumnType.Float64 => HashCode.Combine(value.Type, value.FloatValue),
            ColumnType.Bool => HashCode.Combine(value.Type, value.BoolValue),
            ColumnType.String or ColumnType.Id => HashCode.Combine(value.Type, value.StrValue, StringComparer.Ordinal),
            _ => value.Type.GetHashCode(),
        };
    }

    private static async IAsyncEnumerable<QueryResultRow> DistinctRows(
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        HashSet<QueryResultRow> seen = new(new DistinctRowEqualityComparer());

        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
        {
            if (seen.Add(row))
                yield return row;
        }
    }

    /// <summary>
    /// Deduplicates output rows for <c>SELECT DISTINCT</c> without building a per-row key object.
    /// The canonical sorted-name→ordinal mapping is resolved <b>once</b> from the first
    /// <see cref="QueryRow"/>'s fixed <see cref="RowLayout"/>; thereafter same-layout rows hash and
    /// compare directly against <see cref="QueryRow.Values"/> by ordinal — no per-row name sort,
    /// tuple array, or key allocation.
    /// <para>
    /// Rows whose layout differs from the resolved one (a dictionary-backed row, or a different
    /// schema version) fall back to a per-row canonical computation that compares column <b>names</b>
    /// as well as values — preserving the rule that DISTINCT equality includes names when layouts
    /// differ. Hash inputs are identical across both paths (sorted-name order; each column contributes
    /// its name plus <see cref="DistinctValueHash"/>), so two equal rows hash equally regardless of
    /// which path computed the hash. NULL semantics (<c>NULL = NULL</c>) come from
    /// <see cref="DistinctValuesEqual"/>, matching the streaming and sort-based dedup paths.
    /// </para>
    /// </summary>
    private sealed class DistinctRowEqualityComparer : IEqualityComparer<QueryResultRow>
    {
        private string[]? _sortedNames;
        private int[]? _sortedOrdinals;
        private RowLayout? _resolvedLayout;

        public int GetHashCode(QueryResultRow row)
        {
            EnsureResolved(row);

            if (_sortedOrdinals is not null
                && row.Row is QueryRow qr && ReferenceEquals(qr.Layout, _resolvedLayout))
            {
                HashCode hash = new();
                for (int i = 0; i < _sortedOrdinals.Length; i++)
                {
                    int ord = _sortedOrdinals[i];
                    ColumnValue v = ord >= 0 ? qr.Values[ord] : ColumnValue.Null;
                    hash.Add(_sortedNames![i], StringComparer.Ordinal);
                    hash.Add(DistinctValueHash(v));
                }
                return hash.ToHashCode();
            }

            (string[] names, ColumnValue[] values) = Canonical(row);
            HashCode fallback = new();
            for (int i = 0; i < names.Length; i++)
            {
                fallback.Add(names[i], StringComparer.Ordinal);
                fallback.Add(DistinctValueHash(values[i]));
            }
            return fallback.ToHashCode();
        }

        public bool Equals(QueryResultRow x, QueryResultRow y)
        {
            // Fast path: both rows carry the resolved layout, so the sorted column names are identical
            // by construction — compare values by ordinal only.
            if (_sortedOrdinals is not null
                && x.Row is QueryRow qx && ReferenceEquals(qx.Layout, _resolvedLayout)
                && y.Row is QueryRow qy && ReferenceEquals(qy.Layout, _resolvedLayout))
            {
                for (int i = 0; i < _sortedOrdinals.Length; i++)
                {
                    int ord = _sortedOrdinals[i];
                    ColumnValue xv = ord >= 0 ? qx.Values[ord] : ColumnValue.Null;
                    ColumnValue yv = ord >= 0 ? qy.Values[ord] : ColumnValue.Null;
                    if (!DistinctValuesEqual(xv, yv))
                        return false;
                }
                return true;
            }

            // Fallback: a differing layout means the names may differ too, so compare names and values.
            (string[] xn, ColumnValue[] xvv) = Canonical(x);
            (string[] yn, ColumnValue[] yvv) = Canonical(y);
            if (xn.Length != yn.Length)
                return false;

            for (int i = 0; i < xn.Length; i++)
            {
                if (!string.Equals(xn[i], yn[i], StringComparison.Ordinal))
                    return false;
                if (!DistinctValuesEqual(xvv[i], yvv[i]))
                    return false;
            }
            return true;
        }

        private void EnsureResolved(QueryResultRow row)
        {
            if (_sortedNames is not null)
                return;

            _sortedNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
            if (row.Row is QueryRow qr)
            {
                _resolvedLayout = qr.Layout;
                _sortedOrdinals = BuildSortedOrdinals(_sortedNames, _resolvedLayout);
            }
        }

        /// <summary>
        /// Materializes a row's columns in canonical sorted-name order. Used only off the fixed-layout
        /// fast path (dictionary rows or a layout that does not match the resolved one), so its
        /// allocation is not on the steady-state DISTINCT path. Names follow the row's own key set, so
        /// two rows with different column names are correctly unequal.
        /// </summary>
        private static (string[] Names, ColumnValue[] Values) Canonical(QueryResultRow row)
        {
            if (row.Row is QueryRow qr)
            {
                string[] names = qr.Layout.OutputNames.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                ColumnValue[] values = new ColumnValue[names.Length];
                for (int i = 0; i < names.Length; i++)
                {
                    int ord = qr.Layout.IndexOf(names[i]);
                    values[i] = ord >= 0 ? qr.Values[ord] : ColumnValue.Null;
                }
                return (names, values);
            }

            string[] dictNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
            ColumnValue[] dictValues = new ColumnValue[dictNames.Length];
            for (int i = 0; i < dictNames.Length; i++)
                dictValues[i] = row.Row[dictNames[i]];
            return (dictNames, dictValues);
        }
    }
}
