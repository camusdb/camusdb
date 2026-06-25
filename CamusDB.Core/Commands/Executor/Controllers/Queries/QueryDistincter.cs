
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Removes duplicate output tuples for <c>SELECT DISTINCT</c>.
/// Comparison uses SQL DISTINCT null semantics: two NULL values are equal.
/// </summary>
internal sealed class QueryDistincter
{
    internal IAsyncEnumerable<QueryResultRow> DistinctResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor) =>
        DistinctRows(dataCursor);

    /// <summary>
    /// Streaming deduplication for R12: compares each row to the previously emitted row.
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
        Dictionary<string, ColumnValue>? lastRow = null;
        string[]? sortedNames = null;

        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
        {
            if (lastRow is null)
            {
                // Hoist sorted column names on first row — schema is fixed across the cursor.
                sortedNames = row.Row.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
                lastRow = row.Row;
                yield return row;
                continue;
            }

            bool equal = true;
            for (int i = 0; i < sortedNames!.Length; i++)
            {
                string name = sortedNames[i];
                if (!DistinctValuesEqual(row.Row[name], lastRow[name]))
                {
                    equal = false;
                    break;
                }
            }

            if (!equal)
            {
                lastRow = row.Row;
                yield return row;
            }
        }
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
            string[] names = row.Row.Keys.OrderBy(static name => name, StringComparer.Ordinal).ToArray();
            (string Name, ColumnValue Value)[] values = new (string Name, ColumnValue Value)[names.Length];

            for (int i = 0; i < names.Length; i++)
                values[i] = (names[i], row.Row[names[i]]);

            return new DistinctRowKey(values);
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
