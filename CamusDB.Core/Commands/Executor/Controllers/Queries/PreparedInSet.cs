
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
/// hash overhead for the common tiny-IN case). Larger lists use hash sets, one for each comparison
/// domain; see "Numeric domains" below.
///
/// NULL semantics: NULL is never a member of any IN list. NULL values in the source list are
/// dropped when the set is built, and <see cref="Contains"/> returns false for a NULL lhs. The SQL
/// value of the predicate is three-valued, so <see cref="Evaluate"/> also needs to know whether a
/// NULL item was dropped, here or earlier by <see cref="PredicateAnalyzer"/>: with no match, that
/// NULL item makes the result UNKNOWN instead of FALSE.
///
/// Cross-type semantics: <c>x IN (a, b, c)</c> is defined as <c>x = a OR x = b OR x = c</c>, so each
/// element uses the equality <c>=</c> uses (<see cref="MixedNumericComparison.EqualsForMembership"/>):
/// a mixed numeric pair widens to double, so <c>1 IN (1.0)</c> is true; a String and a Uuid or Id
/// are equal when the string parses to that value (<see cref="StringOperandCoercion"/>); and any
/// other cross-type element (e.g. <c>5 IN (1, 'foo')</c>) is a non-match rather than a hard error.
/// This matches the AST reference path (<see cref="SubqueryValueListAst.EvaluateMembership"/>), so
/// prepared and reference paths stay identical — and it matches the index IN-list seek, which
/// rewrites list items into the column's type. IN lists are not type-checked at bind time, so
/// mixed-type lists are reachable and must not throw.
///
/// <para>The String-to-Uuid/Id rule is the trap. The set is built before the planner knows the
/// column's type, so a uuid column filtered by string parameters gives a set of strings and a probe
/// of Uuid values. The index seek converts its items and finds the rows; a table scan that compared
/// the probe with the raw strings would find none. The planner prefers the table scan once the list
/// names about half the table, so the query would silently return no rows past that point. The hash
/// set therefore uses an equality that never crosses String and Uuid/Id (the two hash differently),
/// and <see cref="Contains"/> converts before it looks up: the string items into the probe's type
/// for a Uuid/Id probe, or a String probe into the type of any Uuid/Id items.</para>
///
/// <para><b>Numeric domains.</b> Mixed numeric equality is not transitive, so it cannot key one hash
/// set. INT64 and NUMERIC compare exactly, but a FLOAT64 compares with either as a double:
/// <c>NUMERIC '9007199254740992'</c> and <c>NUMERIC '9007199254740993'</c> are unequal, yet both equal
/// <c>9007199254740992.0</c>. One set built with that equality drops the float item as a duplicate of
/// the first NUMERIC item, and the second NUMERIC value then misses the float match, so adding
/// unrelated items to a list changes its result. Each domain is transitive on its own, so the items go
/// into one set per domain, and no item is ever dropped through the mixed rule:</para>
/// <list type="bullet">
/// <item>INT64 and NUMERIC items: their exact NUMERIC unscaled values, hashed at full width. A set of
///   closely spaced large NUMERIC values does not share one bucket, as it does when it is hashed
///   through a double.</item>
/// <item>FLOAT64 and FLOAT32 items: their widened doubles (<see cref="MixedNumericComparison.ToDouble"/>).
///   <see cref="double.Equals(double)"/> agrees with <see cref="ColumnValue.CompareTo"/> for the
///   float types: NaN equals NaN, and 0 equals -0.</item>
/// <item>Every other item: <see cref="MixedNumericComparison.EqualsWithoutStringCoercion"/>, which is
///   same-type <see cref="ColumnValue.CompareTo"/> for them.</item>
/// </list>
/// <para>An INT64 or NUMERIC probe looks in its exact set, then looks for its double in the float set.
/// A float probe looks for its double in the float set, then in the doubles of the exact items. That
/// is the mixed rule for each pair of domains, so the result is the OR over every item, as on the
/// reference path.</para>
///
/// <para>Thread safety: a set is shared by every worker of a parallel scan. The converted item sets
/// are built on first use and published once; a race builds the same set twice and keeps one.</para>
/// </summary>
public sealed class PreparedInSet
{
    private const int HashThreshold = 8;

    private readonly ColumnValue[] _values;
    private readonly bool _containsNull;

    // Built only for a list longer than HashThreshold; null otherwise. See "Numeric domains".
    private readonly HashSet<ColumnValue>? _set;        // items that are not numeric
    private readonly HashSet<Int128>? _exactItems;      // INT64 and NUMERIC items, NUMERIC unscaled
    private readonly HashSet<double>? _floatItems;      // FLOAT64 and FLOAT32 items, widened
    private HashSet<double>? _exactItemsAsDouble;       // the exact items widened, built on the first float probe

    // Item types that take part in the String-to-Uuid/Id rule; see the class summary.
    private readonly bool _hasStringItems;
    private readonly bool _hasUuidItems;
    private readonly bool _hasIdItems;

    // The string items converted into Uuid or Id, built on the first probe of that type.
    private HashSet<ColumnValue>? _stringItemsAsUuid;
    private HashSet<ColumnValue>? _stringItemsAsId;

    /// <param name="values">The list items. NULL items are allowed and are dropped.</param>
    /// <param name="containsNull">True when the caller already dropped a NULL item from
    /// <paramref name="values"/>. A NULL item still present in <paramref name="values"/> sets it
    /// too.</param>
    public PreparedInSet(IReadOnlyList<ColumnValue> values, bool containsNull = false)
    {
        // PredicateAnalyzer already drops NULLs, but filter defensively in case the
        // caller supplies raw AST-extracted values that still contain NULLs.
        int nonNullCount = 0;
        for (int i = 0; i < values.Count; i++)
            if (values[i].Type != ColumnType.Null) nonNullCount++;

        _containsNull = containsNull || nonNullCount < values.Count;
        _values = new ColumnValue[nonNullCount];
        int idx = 0;
        for (int i = 0; i < values.Count; i++)
        {
            if (values[i].Type != ColumnType.Null)
                _values[idx++] = values[i];
        }

        foreach (ColumnValue value in _values)
        {
            switch (value.Type)
            {
                case ColumnType.String: _hasStringItems = true; break;
                case ColumnType.Uuid: _hasUuidItems = true; break;
                case ColumnType.Id: _hasIdItems = true; break;
            }
        }

        if (_values.Length > HashThreshold)
        {
            _set = new HashSet<ColumnValue>(MembershipComparer.Instance);
            _exactItems = [];
            _floatItems = [];

            foreach (ColumnValue value in _values)
            {
                switch (value.Type)
                {
                    case ColumnType.Integer64:
                    case ColumnType.Numeric:
                        _exactItems.Add(MixedNumericComparison.ToNumericUnscaled(value));
                        break;

                    case ColumnType.Float64:
                    case ColumnType.Float32:
                        _floatItems.Add(MixedNumericComparison.ToDouble(value));
                        break;

                    default:
                        _set.Add(value);
                        break;
                }
            }
        }
    }

    /// <summary>
    /// Hash comparer for the items that are not numeric: the membership equality above, minus the
    /// String-to-Uuid/Id step (see <see cref="MixedNumericComparison.EqualsWithoutStringCoercion"/>),
    /// which for these types is same-type <see cref="ColumnValue.CompareTo"/>. Numeric items never go
    /// in a set with this comparer; see "Numeric domains" in the class summary.
    /// </summary>
    private sealed class MembershipComparer : IEqualityComparer<ColumnValue>
    {
        public static readonly MembershipComparer Instance = new();

        public bool Equals(ColumnValue? x, ColumnValue? y)
        {
            if (x is null || y is null)
                return x is null && y is null;

            return MixedNumericComparison.EqualsWithoutStringCoercion(x, y);
        }

        public int GetHashCode(ColumnValue obj) => SqlColumnValueComparer.Instance.GetHashCode(obj);
    }

    /// <summary>
    /// Returns the SQL value of <c>lhs IN (list)</c>: TRUE, FALSE, or a NULL value for UNKNOWN. It
    /// must agree with <see cref="SubqueryValueListAst.EvaluateMembership"/>: a NULL
    /// <paramref name="lhs"/> is UNKNOWN, a match is TRUE, and no match is UNKNOWN when the list held
    /// a NULL item and FALSE when it did not.
    ///
    /// <para>The source list is never empty. A set is built only for a membership node that has a
    /// list (a literal list, or a subquery that returned a non-NULL value), so the "empty list is
    /// FALSE even for NULL" rule never applies. The set itself can hold no values, when
    /// <see cref="PredicateAnalyzer"/> drops every item that the column type cannot equal
    /// (<c>int_col IN (1.5)</c>); a NULL lhs is still UNKNOWN then, as on the reference path.</para>
    /// </summary>
    public ColumnValue Evaluate(ColumnValue lhs)
    {
        if (lhs.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (Contains(lhs))
            return ColumnValue.True;

        return _containsNull ? ColumnValue.Null : ColumnValue.False;
    }

    /// <summary>
    /// Returns true if <paramref name="lhs"/> is a member of the prepared list.
    /// A NULL <paramref name="lhs"/> always returns false (SQL three-valued logic).
    /// </summary>
    public bool Contains(ColumnValue lhs)
    {
        if (lhs.Type == ColumnType.Null)
            return false;

        if (_set is null)
        {
            foreach (ColumnValue v in _values)
            {
                if (MixedNumericComparison.EqualsForMembership(lhs, v))
                    return true;
            }
            return false;
        }

        switch (lhs.Type)
        {
            case ColumnType.Integer64:
            case ColumnType.Numeric:
                return _exactItems!.Contains(MixedNumericComparison.ToNumericUnscaled(lhs))
                    || _floatItems!.Contains(MixedNumericComparison.ToDouble(lhs));

            case ColumnType.Float64:
            case ColumnType.Float32:
            {
                double widened = MixedNumericComparison.ToDouble(lhs);
                return _floatItems!.Contains(widened)
                    || (_exactItems!.Count > 0 && (_exactItemsAsDouble ?? BuildExactItemsAsDouble()).Contains(widened));
            }
        }

        if (_set.Contains(lhs))
            return true;

        switch (lhs.Type)
        {
            case ColumnType.Uuid when _hasStringItems:
                return (_stringItemsAsUuid ?? BuildStringItemsAs(ColumnType.Uuid, ref _stringItemsAsUuid)).Contains(lhs);

            case ColumnType.Id when _hasStringItems:
                return (_stringItemsAsId ?? BuildStringItemsAs(ColumnType.Id, ref _stringItemsAsId)).Contains(lhs);

            case ColumnType.String:
                return (_hasUuidItems && ContainsCoerced(lhs, ColumnType.Uuid))
                    || (_hasIdItems && ContainsCoerced(lhs, ColumnType.Id));

            default:
                return false;
        }
    }

    private bool ContainsCoerced(ColumnValue lhs, ColumnType target) =>
        StringOperandCoercion.TryCoerce(lhs, target, out ColumnValue coerced) && _set!.Contains(coerced);

    /// <summary>
    /// Builds the doubles of the exact items, for a float probe, and publishes them once. Several
    /// exact items can widen to one double; the set keeps one, which is correct here, because a float
    /// probe equals every exact item with that double.
    /// </summary>
    private HashSet<double> BuildExactItemsAsDouble()
    {
        HashSet<double> widened = new(_exactItems!.Count);

        foreach (ColumnValue value in _values)
        {
            if (value.Type is ColumnType.Integer64 or ColumnType.Numeric)
                widened.Add(MixedNumericComparison.ToDouble(value));
        }

        return Interlocked.CompareExchange(ref _exactItemsAsDouble, widened, null) ?? widened;
    }

    /// <summary>
    /// Builds the set of string items that parse as <paramref name="target"/>, converted into it, and
    /// publishes it in <paramref name="field"/>. A string that does not parse can equal no value of
    /// that type and is left out.
    /// </summary>
    private HashSet<ColumnValue> BuildStringItemsAs(ColumnType target, ref HashSet<ColumnValue>? field)
    {
        HashSet<ColumnValue> converted = new(MembershipComparer.Instance);

        foreach (ColumnValue value in _values)
        {
            if (StringOperandCoercion.TryCoerce(value, target, out ColumnValue coerced))
                converted.Add(coerced);
        }

        return Interlocked.CompareExchange(ref field, converted, null) ?? converted;
    }
}
