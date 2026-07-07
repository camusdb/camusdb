
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
        HashSet<DistinctRowKey> seen = new();

        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
        {
            DistinctRowKey key = DistinctRowKey.FromRow(row);

            if (seen.Add(key))
                yield return row;
        }
    }

    private sealed class DistinctRowKey : IEquatable<DistinctRowKey>
    {
        private readonly (string Name, ColumnValue Value)[] _values;

        private DistinctRowKey((string Name, ColumnValue Value)[] values)
        {
            _values = values;
        }

        public static DistinctRowKey FromRow(QueryResultRow row)
        {
            if (row.Row is QueryRow qr)
            {
                // Use layout ordinal order for names so the key is consistent with the
                // ordinal-based comparers in DistinctRowComparer and StreamingRows.
                string[] names = qr.Layout.OutputNames.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                (string Name, ColumnValue Value)[] values = new (string Name, ColumnValue Value)[names.Length];
                for (int i = 0; i < names.Length; i++)
                {
                    int ord = qr.Layout.IndexOf(names[i]);
                    values[i] = (names[i], ord >= 0 ? qr.Values[ord] : ColumnValue.Null);
                }
                return new DistinctRowKey(values);
            }

            string[] dictNames = row.Row.Keys.OrderBy(static name => name, StringComparer.Ordinal).ToArray();
            (string Name, ColumnValue Value)[] dictValues = new (string Name, ColumnValue Value)[dictNames.Length];
            for (int i = 0; i < dictNames.Length; i++)
                dictValues[i] = (dictNames[i], row.Row[dictNames[i]]);

            return new DistinctRowKey(dictValues);
        }

        public bool Equals(DistinctRowKey? other)
        {
            if (other is null || _values.Length != other._values.Length)
                return false;

            for (int i = 0; i < _values.Length; i++)
            {
                if (!string.Equals(_values[i].Name, other._values[i].Name, StringComparison.Ordinal))
                    return false;

                if (!QueryDistincter.DistinctValuesEqual(_values[i].Value, other._values[i].Value))
                    return false;
            }

            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as DistinctRowKey);

        public override int GetHashCode()
        {
            HashCode hash = new();

            foreach ((string name, ColumnValue value) in _values)
            {
                hash.Add(name, StringComparer.Ordinal);
                hash.Add(QueryDistincter.DistinctValueHash(value));
            }

            return hash.ToHashCode();
        }
    }
}
