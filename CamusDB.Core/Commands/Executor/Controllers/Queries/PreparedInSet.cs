
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Pre-materialized IN-list values for a single <c>column IN (v1, v2, …)</c> predicate.
/// Built once at ticket-creation time from the values already extracted by
/// <see cref="PredicateAnalyzer"/>. Avoids per-row AST re-evaluation and, for large lists,
/// replaces O(n) linear scan with an O(1) hash lookup.
///
/// Threshold: lists of 8 or fewer values use a linear scan over a small array (avoids
/// hash overhead for the common tiny-IN case). Larger lists use a <see cref="HashSet{T}"/>
/// with <see cref="SqlColumnValueComparer"/> (consistent with <see cref="ColumnValue.CompareTo"/>).
///
/// NULL semantics: NULL is never a member of any IN list (SQL three-valued logic).
/// NULL values in the source list are dropped when the set is built; a NULL lhs returns false.
///
/// Cross-type semantics: <c>x IN (a, b, c)</c> is defined as <c>x = a OR x = b OR x = c</c>, so each
/// element uses the equality <c>=</c> uses (<see cref="MixedNumericComparison.EqualsForMembership"/>):
/// a mixed numeric pair widens to double, so <c>1 IN (1.0)</c> is true, and any other cross-type
/// element (e.g. <c>5 IN (1, 'foo')</c>) is a non-match rather than a hard error. This matches the
/// AST reference path (<see cref="SubqueryValueListAst.ContainsValue"/>), so prepared and reference
/// paths stay identical — and it matches the index IN-list seek, which rewrites list items into the
/// column's type. IN lists are not type-checked at bind time, so mixed-type lists are reachable and
/// must not throw.
/// </summary>
public sealed class PreparedInSet
{
    private const int HashThreshold = 8;

    private readonly ColumnValue[] _values;
    private readonly HashSet<ColumnValue>? _set;

    public PreparedInSet(IReadOnlyList<ColumnValue> values)
    {
        // PredicateAnalyzer already drops NULLs, but filter defensively in case the
        // caller supplies raw AST-extracted values that still contain NULLs.
        int nonNullCount = 0;
        for (int i = 0; i < values.Count; i++)
            if (values[i].Type != ColumnType.Null) nonNullCount++;

        _values = new ColumnValue[nonNullCount];
        int idx = 0;
        for (int i = 0; i < values.Count; i++)
        {
            if (values[i].Type != ColumnType.Null)
                _values[idx++] = values[i];
        }

        if (_values.Length > HashThreshold)
            _set = new HashSet<ColumnValue>(_values, MembershipComparer.Instance);
    }

    /// <summary>
    /// Hash comparer with the membership equality above. Numeric values hash by their widened
    /// double regardless of type, so an Integer64 probe lands in the bucket of an equal Float64
    /// member; every other type hashes as <see cref="SqlColumnValueComparer"/> does.
    /// </summary>
    private sealed class MembershipComparer : IEqualityComparer<ColumnValue>
    {
        public static readonly MembershipComparer Instance = new();

        public bool Equals(ColumnValue? x, ColumnValue? y)
        {
            if (x is null || y is null)
                return x is null && y is null;

            return MixedNumericComparison.EqualsForMembership(x, y);
        }

        public int GetHashCode(ColumnValue obj) =>
            MixedNumericComparison.IsNumeric(obj.Type)
                ? MixedNumericComparison.ToDouble(obj).GetHashCode()
                : SqlColumnValueComparer.Instance.GetHashCode(obj);
    }

    /// <summary>
    /// Returns true if <paramref name="lhs"/> is a member of the prepared list.
    /// A NULL <paramref name="lhs"/> always returns false (SQL three-valued logic).
    /// </summary>
    public bool Contains(ColumnValue lhs)
    {
        if (lhs.Type == ColumnType.Null)
            return false;

        if (_set is not null)
            return _set.Contains(lhs);

        foreach (ColumnValue v in _values)
        {
            if (MixedNumericComparison.EqualsForMembership(lhs, v))
                return true;
        }
        return false;
    }
}
