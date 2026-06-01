
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
/// Removes duplicate output tuples for <c>SELECT DISTINCT</c> (QP3.6).
/// Comparison uses SQL DISTINCT null semantics: two NULL values are equal.
/// </summary>
internal sealed class QueryDistincter
{
    internal IAsyncEnumerable<QueryResultRow> DistinctResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor) =>
        DistinctRows(dataCursor);

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

                if (!DistinctValuesEqual(_values[i].Value, other._values[i].Value))
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
                hash.Add(DistinctValueHash(value));
            }

            return hash.ToHashCode();
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
    }
}
